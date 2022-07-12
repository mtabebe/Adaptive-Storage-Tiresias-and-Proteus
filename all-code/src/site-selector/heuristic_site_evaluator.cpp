#include "heuristic_site_evaluator.h"
#include "multi_query_optimization.h"
#include "tracking_summary.h"
#include "transaction_preparer.h"

#include <cfloat>
#include <climits>
#include <glog/logging.h>

#include "../common/perf_tracking.h"
#include "../data-site/db/mvcc_chain.h"
#include "../data-site/db/partition.h"

double get_average_load_per_site(
    const std::vector<site_load_information> &site_loads ) {
    double average_load_per_site = 0;
    for( const auto &site_load : site_loads ) {
        average_load_per_site = average_load_per_site + site_load.cpu_load_;
        DVLOG( 40 ) << "Site CPU Load:" << site_load.cpu_load_;
    }

    average_load_per_site = average_load_per_site / site_loads.size();
    DVLOG( 40 ) << "Average CPU Load:" << average_load_per_site
                << ", num sites:" << site_loads.size();
    return average_load_per_site;
}

heuristic_site_evaluator::heuristic_site_evaluator(
    const heuristic_site_evaluator_configs &       configs,
    std::shared_ptr<cost_modeller2>                cost_model2,
    std::shared_ptr<partition_data_location_table> data_loc_tab,
    std::shared_ptr<sites_partition_version_information>
                                                    site_partition_version_info,
    std::shared_ptr<query_arrival_predictor>        query_predictor,
    std::shared_ptr<stat_tracked_enumerator_holder> enumerator_holder,
    std::shared_ptr<partition_tier_tracking>        tier_tracking )
    : configs_( configs ),
      cost_model2_( cost_model2 ),
      data_loc_tab_( data_loc_tab ),
      query_stats_( data_loc_tab->get_stats() ),
      storage_stats_( data_loc_tab->get_storage_stats() ),
      site_partition_version_info_( site_partition_version_info ),
      query_predictor_( query_predictor ),
      enumerator_holder_( enumerator_holder ),
      tier_tracking_( tier_tracking ),
      samplers_( NULL ),
      force_reservoirs_(),
      rand_dist_( nullptr ),
      min_part_sizes_() {

    DVLOG( k_heuristic_evaluator_log_level )
        << "Heuristic site evaluator, configs:" << configs_;

    force_reservoirs_.reserve( configs_.num_clients_ );
    for( uint32_t i = 0; i < configs_.num_clients_; i++ ) {
        force_reservoirs_.push_back( new adaptive_reservoir(
            configs_.force_change_reservoir_size_,
            configs_.force_change_reservoir_initial_multiplier_,
            configs_.force_change_reservoir_weight_increment_,
            configs_.force_change_reservoir_decay_rate_ ) );
    }
    build_min_partition_sizes( data_loc_tab->get_num_tables() );
}

heuristic_site_evaluator::~heuristic_site_evaluator() {
    for( uint32_t i = 0; i < configs_.num_clients_; i++ ) {
        delete force_reservoirs_.at( i );
        force_reservoirs_.at( i ) = nullptr;
    }
    force_reservoirs_.clear();
}

heuristic_site_evaluator_configs heuristic_site_evaluator::get_configs() const {
    return configs_;
}

void heuristic_site_evaluator::decay() {
    for( uint32_t i = 0; i < configs_.num_clients_; i++ ) {
        force_reservoirs_.at( i )->decay();
    }
    if( samplers_ ) {
        for( uint32_t i = 0; i < samplers_->size(); i++ ) {
            samplers_->at( i )->decay();
        }
    }
}

int heuristic_site_evaluator::get_no_change_destination(
    const std::vector<int> &no_change_destinations,
    const std::vector<std::shared_ptr<partition_payload>> &write_partitions,
    const std::vector<std::shared_ptr<partition_payload>> &read_partitions,
    const partition_column_identifier_set &                write_partitions_set,
    const partition_column_identifier_set &                read_partitions_set,
    ::snapshot_vector &                                    svv ) {
    start_timer( SS_GENERATE_NO_CHANGE_DESTINATION_TIMER_ID );

    std::vector<int> eligible_sites;

    if( configs_.enable_plan_reuse_ ) {
        auto plan_reuse_sites = generate_plan_reuse_sites(
            write_partitions, read_partitions, write_partitions_set,
            read_partitions_set );
        if( !plan_reuse_sites.empty() ) {
            for( int site : no_change_destinations ) {
                if( plan_reuse_sites.count( site ) == 1 ) {
                    eligible_sites.emplace_back( site );
                }
            }
        }
    }
    if( eligible_sites.empty() ) {
        eligible_sites = no_change_destinations;
    }
    int pos = rand_dist_.get_uniform_int( 0, eligible_sites.size() );
    int ret = eligible_sites.at( pos );

    stop_timer( SS_GENERATE_NO_CHANGE_DESTINATION_TIMER_ID );

    return ret;
}

bool heuristic_site_evaluator::get_should_force_change(
    const ::clientid id, const std::vector<::cell_key_ranges> &ckr_write_set,
    const std::vector<::cell_key_ranges> &ckr_read_set ) {
    bool force = false;
    if( configs_.allow_force_site_selector_changes_ and
        !ckr_write_set.empty() ) {
        force = force_reservoirs_.at( id )->should_sample();
    }
    if( force ) {
        DVLOG( 7 ) << "Heuristic site evaluator is forcing physical changes "
                   << "for client:" << id;
    }
    return force;
}

std::shared_ptr<pre_transaction_plan>
    heuristic_site_evaluator::generate_multi_site_insert_only_plan(
        const ::clientid                          id,
        const std::vector<site_load_information> &site_load_infos,
        const std::unordered_map<storage_tier_type::type, std::vector<double>>
            &                                            storage_sizes,
        std::vector<std::shared_ptr<partition_payload>> &new_partitions,
        bool                                             is_fully_replicated ) {

    double ori_storage_imbalance =
        get_site_storage_imbalance_cost( storage_sizes );

    auto loc_storage_sizes = storage_sizes;

    auto plan = std::make_shared<pre_transaction_plan>(
        enumerator_holder_->construct_decision_holder_ptr() );

    cached_ss_stat_holder      cached_stats;
    query_arrival_cached_stats cached_query_arrival_stats;

    std::unordered_map<int, std::vector<std::shared_ptr<partition_payload>>>
        per_site_dests;
    for( auto part : new_partitions ) {
        std::vector<std::shared_ptr<partition_payload>> parts = {part};
        int site = generate_insert_only_partition_destination( parts );
        if( is_fully_replicated ) {
            site = K_DATA_AT_ALL_SITES;
        }
        auto &parts_per_site = per_site_dests[site];
        parts_per_site.push_back( part );
    }
    for( auto &entry : per_site_dests ) {
        int  site = entry.first;
        auto parts = entry.second;

        auto found_types = internal_get_new_partition_types(
            site, plan, site_load_infos.at( site ), parts, loc_storage_sizes,
            ori_storage_imbalance, cached_stats, cached_query_arrival_stats );
        auto action = create_add_partition_pre_action(
            site, site, parts, std::get<0>( found_types ),
            std::get<1>( found_types ),
            get_new_partition_update_destination_slot( site, parts ) );
        DVLOG( k_heuristic_evaluator_log_level )
            << "PLAN: Add partitions: (" << payloads_to_identifiers( parts )
            << "), master:" << site;

        plan->add_pre_action_work_item( action );
    }

    return plan;
}

bool heuristic_site_evaluator::should_add_multi_query_plan_to_partitions(
    const ::clientid id ) {
    bool ret = false;
    if( configs_.enable_plan_reuse_ and samplers_ ) {
        DCHECK_LT( id, samplers_->size() );
        std::unique_ptr<adaptive_reservoir_sampler> &sampler =
            samplers_->at( id );
        ret = sampler->should_sample();
    }
    return ret;
}

std::shared_ptr<pre_transaction_plan> heuristic_site_evaluator::generate_plan(
    const ::clientid id, const std::vector<bool> &site_locations,
    const std::vector<std::shared_ptr<partition_payload>> &new_partitions,
    const std::vector<std::shared_ptr<partition_payload>> &write_partitions,
    const std::vector<std::shared_ptr<partition_payload>> &read_partitions,
    const partition_column_identifier_set &                write_partitions_set,
    const partition_column_identifier_set &                read_partitions_set,
    partition_column_identifier_map_t<std::shared_ptr<
        partition_location_information>> &     partition_location_informations,
    const partition_column_identifier_to_ckrs &write_pids_to_ckrs,
    const partition_column_identifier_to_ckrs &read_pids_to_ckrs,
    const std::vector<site_load_information> & site_load_infos,
    const std::unordered_map<storage_tier_type::type, std::vector<double>>
        &                                    storage_sizes,
    std::shared_ptr<multi_query_plan_entry> &mq_plan_entry,
    const snapshot_vector &                  session ) {

    start_timer( SS_GENERATE_PLAN_TIMER_ID );

    DVLOG( 10 ) << "Generate plan:" << id
                << ", write_partitions_set:" << write_partitions_set
                << ", read_partitions_set:" << read_partitions_set;

    build_multi_query_plan_entry( mq_plan_entry, write_partitions_set,
                                  read_partitions_set );

    mq_plan_entry->set_plan_type( multi_query_plan_type::PLANNING );

    bool added_mq_plan = should_add_multi_query_plan_to_partitions( id );

    if( added_mq_plan ) {
        add_multi_query_plan_entry_to_partitions( mq_plan_entry,
                                                  new_partitions );
        add_multi_query_plan_entry_to_partitions( mq_plan_entry,
                                                  write_partitions );
        add_multi_query_plan_entry_to_partitions( mq_plan_entry,
                                                  read_partitions );
    }

    std::shared_ptr<pre_transaction_plan> best_plan = nullptr;
    double                                best_plan_score = -DBL_MAX;

    plan_for_site_args args = generate_plan_for_site_args(
        id, new_partitions, write_partitions, read_partitions,
        partition_location_informations, write_pids_to_ckrs, read_pids_to_ckrs,
        site_load_infos, storage_sizes, session );

    int start_site = 0;
    int end_site = configs_.num_sites_ - 1;

    if( configs_.ss_mastering_type_ ==
        ss_mastering_type::SINGLE_MASTER_MULTI_SLAVE ) {
        if( ( write_partitions.size() > 0 ) or ( new_partitions.size() > 0 ) ) {
            end_site = 0;
        } else if( end_site > 0 ) {
            start_site = 1;
        }
    }

    std::vector<int> eligible_sites;

    if( configs_.enable_plan_reuse_ ) {
        auto plan_reuse_sites = generate_plan_reuse_sites(
            write_partitions, read_partitions, write_partitions_set,
            read_partitions_set );
        if( !plan_reuse_sites.empty() ) {
            for( auto site : plan_reuse_sites ) {
                eligible_sites.emplace_back( site );
            }
        }
    }
    if( eligible_sites.empty() ) {
        for( int site = start_site; site <= end_site; site++ ) {
            eligible_sites.emplace_back( site );
        }
    }

    for( int site : eligible_sites ) {
        /*
          if( !site_locations.at( site ) ) {
              continue;
          }
          */
        auto site_plan = generate_plan_for_site( site, id, args );

        DVLOG( k_heuristic_evaluator_log_level )
            << "PLAN: Got plan for site:" << site
            << ", has cost:" << site_plan->cost_
            << ", best plan score:" << best_plan_score << ", client:" << id;

        if( site_plan->cost_ > best_plan_score ) {
            best_plan_score = site_plan->cost_;
            best_plan = site_plan;
        }
    }

    if( best_plan == nullptr ) {
        DLOG( FATAL ) << "Best plan is nullptr, site_locations="
                      << site_locations << ", cost_modeller:" << *cost_model2_;
    }

    mq_plan_entry->set_plan_destination( best_plan->destination_,
                                         multi_query_plan_type::PLANNING );

    best_plan->mq_plan_ = mq_plan_entry;

    DVLOG( k_heuristic_evaluator_log_level )
        << "PLAN: Best plan site:" << best_plan->destination_
        << ", best plan score:" << best_plan_score << ", client:" << id;

    stop_timer( SS_GENERATE_PLAN_TIMER_ID );

    DCHECK( best_plan );
    return best_plan;
}

std::unordered_set<int> heuristic_site_evaluator::generate_plan_reuse_sites(
    const std::vector<std::shared_ptr<partition_payload>> &write_partitions,
    const std::vector<std::shared_ptr<partition_payload>> &read_partitions,
    const partition_column_identifier_set &                write_partitions_set,
    const partition_column_identifier_set &read_partitions_set ) const {
    std::unordered_set<int> generated_sites;
    if( !configs_.enable_plan_reuse_ ) {
        return generated_sites;
    }
    transaction_query_entry query_entry = build_transaction_query_entry(
        write_partitions, read_partitions, write_partitions_set,
        read_partitions_set );

    std::vector<
        std::tuple<double, uint64_t, std::shared_ptr<multi_query_plan_entry>>>
        sorted_plans = query_entry.get_sorted_plans();

    for( const auto &entry : sorted_plans ) {
        if( std::get<0>( entry ) < configs_.plan_reuse_threshold_ ) {
            break;
        }
        auto plan_entry = std::get<2>( entry );
        auto dest_site = plan_entry->get_plan_destination();
        if( std::get<0>( dest_site ) ) {
            generated_sites.emplace( std::get<1>( dest_site ) );
        }
    }

    return generated_sites;
}

std::shared_ptr<pre_transaction_plan>
    heuristic_site_evaluator::generate_plan_for_site(
        int site, const ::clientid id, plan_for_site_args &args ) {
    return generate_plan_for_site(
        site, id, args.new_partitions_, args.write_partitions_,
        args.read_partitions_, args.partition_location_informations_,
        args.partitions_to_split_, args.partitions_to_merge_,
        args.avg_write_count_, args.write_pids_to_ckrs_,
        args.read_pids_to_ckrs_, args.other_write_locked_partitions_,
        args.original_write_partition_set_, args.original_read_partition_set_,
        args.site_load_infos_, args.average_load_per_site_,
        args.site_storage_sizes_, args.site_storage_sizes_cost_, args.session_,
        args.sampled_partition_accesses_index_by_write_partition_,
        args.sampled_partition_accesses_index_by_read_partition_,
        args.cached_stats_, args.cached_query_arrival_stats_ );
}

void heuristic_site_evaluator::build_sampled_transactions(
    ::clientid id,
    partition_column_identifier_map_t<
        std::vector<transaction_partition_accesses>> &write_sample_holder,
    partition_column_identifier_map_t<
        std::vector<transaction_partition_accesses>> &read_sample_holder ) {
    if( !samplers_ ) {
        return;
    }

    start_timer( SS_PLAN_BUILD_SAMPLED_TRANSACTIONS_TIMER_ID );

    // get a client id that is in the range of the thigns that are actually
    // sampled
    ::clientid mod_id =
        ( ( id / configs_.astream_mod_ ) * configs_.astream_mod_ ) %
        samplers_->size();
    ::clientid rand_id = ( rand_dist_.get_uniform_int(
                               0, samplers_->size() / configs_.astream_mod_ ) *
                           configs_.astream_mod_ ) %
                         samplers_->size();

    std::vector<tracking_summary> &sampled_txns =
        samplers_->at( mod_id )->get_reservoir_ref();

    for( uint32_t i = 0; i < configs_.num_samples_for_client_; i++ ) {
        add_to_sample_mapping( write_sample_holder, read_sample_holder,
                               sampled_txns.at( rand_dist_.get_uniform_int(
                                   0, sampled_txns.size() ) ) );
    }
    for( uint32_t i = 0; i < configs_.num_samples_from_other_clients_; i++ ) {
        std::vector<tracking_summary> &local_sampled_txns =
            samplers_->at( rand_id )->get_reservoir_ref();
        add_to_sample_mapping(
            write_sample_holder, read_sample_holder,
            local_sampled_txns.at(
                rand_dist_.get_uniform_int( 0, local_sampled_txns.size() ) ) );
    }

    stop_timer( SS_PLAN_BUILD_SAMPLED_TRANSACTIONS_TIMER_ID );
}

void heuristic_site_evaluator::add_to_sample_mapping(
    partition_column_identifier_map_t<
        std::vector<transaction_partition_accesses>> &write_sample_holder,
    partition_column_identifier_map_t<
        std::vector<transaction_partition_accesses>> &read_sample_holder,
    const tracking_summary &                          summary ) {
    if( summary.is_placeholder_sample() ) {
        return;
    }
    for( const auto &part_access :
         summary.txn_partition_accesses_.partition_accesses_ ) {
        if( part_access.is_write_ ) {
            write_sample_holder[part_access.partition_id_].emplace_back(
                summary.txn_partition_accesses_ );
        } else {
            read_sample_holder[part_access.partition_id_].emplace_back(
                summary.txn_partition_accesses_ );
        }
    }
}

plan_for_site_args heuristic_site_evaluator::generate_plan_for_site_args(
    const ::clientid                                       id,
    const std::vector<std::shared_ptr<partition_payload>> &new_partitions,
    const std::vector<std::shared_ptr<partition_payload>> &write_partitions,
    const std::vector<std::shared_ptr<partition_payload>> &read_partitions,
    partition_column_identifier_map_t<std::shared_ptr<
        partition_location_information>> &     partition_location_informations,
    const partition_column_identifier_to_ckrs &write_pids_to_ckrs,
    const partition_column_identifier_to_ckrs &read_pids_to_ckrs,
    const std::vector<site_load_information> & site_load_infos,
    const std::unordered_map<storage_tier_type::type, std::vector<double>>
        &                  storage_sizes,
    const snapshot_vector &session ) {

    start_timer( SS_GENERATE_PLAN_FOR_SITE_ARGS_TIMER_ID );

    plan_for_site_args args( new_partitions, write_partitions, read_partitions,
                             partition_location_informations,
                             write_pids_to_ckrs, read_pids_to_ckrs,
                             site_load_infos, storage_sizes, session );
    build_sampled_transactions(
        id, args.sampled_partition_accesses_index_by_write_partition_,
        args.sampled_partition_accesses_index_by_read_partition_ );

    args.average_load_per_site_ = get_average_load_per_site( site_load_infos );

    args.avg_write_count_ = data_loc_tab_->compute_average_write_accesses();
    args.std_dev_write_count_ =
        data_loc_tab_->compute_standard_deviation_write_accesses();

    args.site_storage_sizes_cost_ =
        get_site_storage_imbalance_cost( storage_sizes );

    for( auto &part : new_partitions ) {
        args.other_write_locked_partitions_[part->identifier_] = part;
    }
    for( auto &part : write_partitions ) {
        args.other_write_locked_partitions_[part->identifier_] = part;
        args.original_write_partition_set_.emplace( part->identifier_ );
    }
    for( auto &part : read_partitions ) {
        args.other_write_locked_partitions_[part->identifier_] = part;
        args.original_read_partition_set_.emplace( part->identifier_ );
    }

    if( configs_.ss_mastering_type_ == ss_mastering_type::ADAPT ) {
        get_partitions_to_adjust(
            write_partitions, args.avg_write_count_, args.std_dev_write_count_,
            args.partitions_to_split_, args.partitions_to_merge_,
            partition_location_informations,
            args.other_write_locked_partitions_,
            args.sampled_partition_accesses_index_by_write_partition_,
            args.sampled_partition_accesses_index_by_read_partition_,
            args.site_storage_sizes_, args.site_storage_sizes_cost_,
            args.cached_stats_, args.cached_query_arrival_stats_ );
    }

    stop_timer( SS_GENERATE_PLAN_FOR_SITE_ARGS_TIMER_ID );

    return args;
}

void heuristic_site_evaluator::get_partitions_to_adjust(
    const std::vector<std::shared_ptr<partition_payload>> &all_write_partitions,
    double avg_write_count, double std_dev_write_count,
    std::vector<shared_split_plan_state> &           split_partitions,
    std::vector<shared_left_right_merge_plan_state> &merge_partitions,
    partition_column_identifier_map_t<std::shared_ptr<
        partition_location_information>> &partition_location_informations,
    partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &other_write_locked_partitions,
    const partition_column_identifier_map_t<
        std::vector<transaction_partition_accesses>>
        &sampled_partition_accesses_index_by_write_partition,
    const partition_column_identifier_map_t<
        std::vector<transaction_partition_accesses>>
        &sampled_partition_accesses_index_by_read_partition,
    const std::unordered_map<storage_tier_type::type, std::vector<double>>
        &  storage_sizes,
    double ori_storage_imbalance, cached_ss_stat_holder &cached_stats,
    query_arrival_cached_stats &cached_query_arrival_stats ) {

    start_timer( SS_GET_PARTITIONS_TO_ADJUST_TIMER_ID );

    DVLOG( 20 ) << "get_partitions_to_adjust, avg:" << avg_write_count
                << ", std_dev:" << std_dev_write_count;

    if( avg_write_count == 0 ) {
        // there haven't been writes, so just leave these alone
        return;
    }
    auto loc_storage_sizes = storage_sizes;

    for( std::shared_ptr<partition_payload> partition : all_write_partitions ) {
        auto location_information = get_location_information_for_partition(
            partition, partition_location_informations );
        DCHECK( location_information );
        DCHECK_NE( k_unassigned_master,
                   location_information->master_location_ );

        DVLOG( 40 ) << "Considering whether we should adjust partition: "
                    << partition->identifier_;
        auto split_state_decision = should_split_partition(
            partition, location_information, avg_write_count,
            std_dev_write_count,
            sampled_partition_accesses_index_by_write_partition,
            sampled_partition_accesses_index_by_read_partition,
            loc_storage_sizes, ori_storage_imbalance, cached_stats,
            cached_query_arrival_stats );
        if( std::get<0>( split_state_decision ) ) {
            DVLOG( 40 ) << "Partition " << partition->identifier_
                        << " added to the list of partitions to split.";
            split_partitions.push_back( std::get<1>( split_state_decision ) );
        } else {
            auto merge_state_decision = should_merge_partition(
                partition, location_information, avg_write_count,
                std_dev_write_count, partition_location_informations,
                other_write_locked_partitions,
                sampled_partition_accesses_index_by_write_partition,
                sampled_partition_accesses_index_by_read_partition,
                loc_storage_sizes, ori_storage_imbalance, cached_stats,
                cached_query_arrival_stats );

            if( std::get<0>( merge_state_decision ) ) {
                DVLOG( 40 ) << "Partition " << partition->identifier_
                            << " added to the list of partitions to merge.";
                merge_partitions.push_back(
                    std::get<1>( merge_state_decision ) );
            }
        }
    }

    stop_timer( SS_GET_PARTITIONS_TO_ADJUST_TIMER_ID );
}

double heuristic_site_evaluator::get_split_min_partition_size(
    uint32_t table_id ) const {
    uint64_t default_part_size =
        data_loc_tab_->get_default_partition_size_for_table( table_id );
    double min_size = default_part_size * configs_.split_partition_size_ratio_;
    DVLOG( 40 ) << "Split min partition size: table_id:" << table_id
                << ", default part size:" << default_part_size
                << ", split partition size ratio:"
                << configs_.split_partition_size_ratio_
                << ", min size:" << min_size;
    return min_size;
}

void heuristic_site_evaluator::build_min_partition_sizes(
    uint32_t num_tables ) {
    for( uint32_t tid = 0; tid < num_tables; tid++ ) {
        min_part_sizes_.emplace_back( get_split_min_partition_size( tid ) );
    }
}

std::tuple<bool, shared_split_plan_state>
    heuristic_site_evaluator::should_split_partition(
        const std::shared_ptr<partition_payload> &      partition,
        std::shared_ptr<partition_location_information> location_information,
        double avg_write_count, double std_dev_write_count,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>>
            &sampled_partition_accesses_index_by_write_partition,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>>
            &sampled_partition_accesses_index_by_read_partition,
        std::unordered_map<storage_tier_type::type, std::vector<double>>
            &  storage_sizes,
        double ori_storage_imbalance, cached_ss_stat_holder &cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats ) {
    shared_split_plan_state split_res;
    if( ( partition->identifier_.partition_start ==
          partition->identifier_.partition_end ) and
        ( partition->identifier_.column_start ==
          partition->identifier_.column_end ) ) {
        // can't split a one key partition
        return std::make_tuple<>( false, split_res );
    }
    DVLOG( 40 ) << "Considering whether we should split: "
                << partition->identifier_
                << ", write_accesses=" << partition->write_accesses_
                << ", avg_write_count:" << avg_write_count << ", weighted="
                << configs_.split_partition_ratio_ * avg_write_count
                << ", std_dev_write_count:" << std_dev_write_count;
    bool res = false;

    split_res.partition_ = partition;
    split_res.location_information_ = location_information;

    if( (double) partition->write_accesses_ >=
        configs_.split_partition_ratio_ * avg_write_count ) {

        // consider it
        split_res.col_split_ = split_partition_vertically_if_beneficial(
            partition, location_information,
            sampled_partition_accesses_index_by_write_partition,
            sampled_partition_accesses_index_by_read_partition, avg_write_count,
            std_dev_write_count, storage_sizes, ori_storage_imbalance,
            cached_stats, cached_query_arrival_stats );
        split_res.row_split_ = split_partition_horizontally_if_beneficial(
            partition, location_information,
            sampled_partition_accesses_index_by_write_partition,
            sampled_partition_accesses_index_by_read_partition, avg_write_count,
            std_dev_write_count, storage_sizes, ori_storage_imbalance,
            cached_stats, cached_query_arrival_stats );
        res = ( split_res.col_split_.splittable_ ||
                split_res.row_split_.splittable_ );
    }

    return std::make_tuple<>( res, split_res );
}

std::tuple<bool, shared_left_right_merge_plan_state>
    heuristic_site_evaluator::should_merge_partition(
        const std::shared_ptr<partition_payload> &partition,
        const std::shared_ptr<partition_location_information>
            &  location_information,
        double avg_write_count, double std_dev_write_count,
        partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>> &partition_location_informations,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &other_write_locked_partitions,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &write_sample_holder,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &read_sample_holder,
        std::unordered_map<storage_tier_type::type, std::vector<double>>
            &  storage_sizes,
        double ori_storage_imbalance, cached_ss_stat_holder &cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats ) {
    DVLOG( 40 ) << "Considering whether we should merge: "
                << partition->identifier_
                << ", write_accesses=" << partition->write_accesses_
                << ", avg_write_count:" << avg_write_count << ", weighted="
                << configs_.merge_partition_ratio_ * avg_write_count;
    bool                               res = false;
    shared_left_right_merge_plan_state merge_res;

    if( within_merge_ratio( partition, avg_write_count ) ) {

        merge_res = merge_partition_if_beneficial(
            partition, location_information, avg_write_count,
            std_dev_write_count, partition_location_informations,
            other_write_locked_partitions, write_sample_holder,
            read_sample_holder, storage_sizes, ori_storage_imbalance,
            cached_stats, cached_query_arrival_stats );
        res = merge_res.merge_left_col_.mergeable_ ||
              merge_res.merge_right_col_.mergeable_ ||
              merge_res.merge_left_row_.mergeable_ ||
              merge_res.merge_right_row_.mergeable_;
    }

    return std::make_tuple<>( res, merge_res );
}

bool heuristic_site_evaluator::within_merge_ratio(
    std::shared_ptr<partition_payload> partition, double avg_write_count ) {
    return ( (double) partition->write_accesses_ <=
             configs_.merge_partition_ratio_ * avg_write_count );
}

// Generate a candidate plan to execute the transaction at site _site_.
std::shared_ptr<pre_transaction_plan>
    heuristic_site_evaluator::generate_plan_for_site(
        int site, const ::clientid id,
        const std::vector<std::shared_ptr<partition_payload>> &new_partitions,
        const std::vector<std::shared_ptr<partition_payload>> &write_partitions,
        const std::vector<std::shared_ptr<partition_payload>> &read_partitions,
        partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>> &partition_location_informations,
        const std::vector<shared_split_plan_state> &partitions_to_split,
        const std::vector<shared_left_right_merge_plan_state>
            &                                      partitions_to_merge,
        double                                     avg_write_count,
        const partition_column_identifier_to_ckrs &write_pids_to_ckrs,
        const partition_column_identifier_to_ckrs &read_pids_to_ckrs,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &other_write_locked_partitions,
        const partition_column_identifier_unordered_set
            &original_write_partition_set,
        const partition_column_identifier_unordered_set
            &                                     original_read_partition_set,
        const std::vector<site_load_information> &site_loads,
        double                                    average_load_per_site,
        const std::unordered_map<storage_tier_type::type, std::vector<double>>
            &  storage_sizes,
        double storage_size_cost, const snapshot_vector &session,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &write_sample_holder,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &read_sample_holder,
        cached_ss_stat_holder &                           cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats ) {
    start_timer( SS_GENERATE_PLAN_FOR_SITE_TIMER_ID );

    DVLOG( 10 ) << "Generating plan for site:" << site;

    DVLOG( 20 ) << " === Write partitions ===";
    for( auto payload : write_partitions ) {
        DVLOG( 20 ) << "    " << payload->identifier_;
    }
    DVLOG( 20 ) << " === Read partitions ===";
    for( auto payload : read_partitions ) {
        DVLOG( 20 ) << "    " << payload->identifier_;
    }

    auto plan = std::make_shared<pre_transaction_plan>(
        enumerator_holder_->construct_decision_holder_ptr() );
    plan->destination_ = site;

    std::unordered_map<storage_tier_type::type, std::vector<double>>
        loc_storage_sizes = storage_sizes;

    partition_column_identifier_unordered_set no_merge_partitions;
    for( const auto &part : new_partitions ) {
        no_merge_partitions.emplace( part->identifier_ );
    }

    partition_column_identifier_to_ckrs write_modifiable_pids_to_ckrs;
    partition_column_identifier_to_ckrs read_modifiable_pids_to_ckrs;
    partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        read_partitions_map;
    partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        write_partitions_map;
    for( const auto &part : write_partitions ) {
        write_partitions_map[part->identifier_] = part;
        write_modifiable_pids_to_ckrs[part->identifier_] =
            write_pids_to_ckrs.at( part->identifier_ );
    }
    for( const auto &part : read_partitions ) {
        read_partitions_map[part->identifier_] = part;
        read_modifiable_pids_to_ckrs[part->identifier_] =
            read_pids_to_ckrs.at( part->identifier_ );
    }

    partition_column_identifier_map_t<std::shared_ptr<pre_action>> pid_dep_map;
    std::vector<std::shared_ptr<partition_payload>> write_part_payloads;
    std::vector<std::shared_ptr<partition_payload>> read_part_payloads;

    partition_column_identifier_map_t<
        std::unordered_map<uint32_t, partition_type::type>>
        created_partition_types;

    partition_column_identifier_map_t<
        std::unordered_map<uint32_t, storage_tier_type::type>>
        created_storage_types;

    if( configs_.ss_mastering_type_ == ss_mastering_type::ADAPT ) {

        // Should we split/merge any of the partitions we are going to access?
        split_partitions_and_add_to_plan(
            site, partitions_to_split, write_partitions_map,
            read_partitions_map, plan, pid_dep_map,
            write_modifiable_pids_to_ckrs, read_modifiable_pids_to_ckrs,
            created_partition_types, created_storage_types, loc_storage_sizes,
            no_merge_partitions );

        DVLOG( 40 ) << "Generating plan for site:" << site << "Before Merge";

        DVLOG( 40 ) << " === Write partitions ===";
        for( auto entry : write_partitions_map ) {
            DVLOG( 40 ) << "    " << entry.first;
        }
        DVLOG( 40 ) << " === Read partitions ===";
        for( auto entry : read_partitions_map ) {
            DVLOG( 40 ) << "    " << entry.first;
        }

        merge_partitions_and_add_to_plan(
            site, partitions_to_merge, partition_location_informations,
            write_partitions_map, read_partitions_map, plan, pid_dep_map,
            created_partition_types, created_storage_types, loc_storage_sizes,
            read_modifiable_pids_to_ckrs, write_modifiable_pids_to_ckrs,
            no_merge_partitions, other_write_locked_partitions,
            avg_write_count );
    }

    DVLOG( 40 ) << "Generating plan for site:" << site
                << "Before add all replicas";

    DVLOG( 40 ) << " === Write partitions ===";
    for( auto entry : write_partitions_map ) {
        DVLOG( 40 ) << "    " << entry.first;
    }
    DVLOG( 40 ) << " === Read partitions ===";
    for( auto entry : read_partitions_map ) {
        DVLOG( 40 ) << "    " << entry.first;
    }

    // need to do this, for non ADAPT because this is how it gets added to
    // parts
    add_all_replicas(
        site, partition_location_informations, write_partitions_map,
        read_partitions_map, plan, write_part_payloads, read_part_payloads,
        pid_dep_map, other_write_locked_partitions, created_partition_types,
        created_storage_types, original_write_partition_set,
        original_read_partition_set, site_loads, loc_storage_sizes,
        write_sample_holder, read_sample_holder, cached_stats,
        cached_query_arrival_stats );

    if( ( configs_.ss_mastering_type_ == ss_mastering_type::ADAPT ) or
        ( configs_.ss_mastering_type_ ==
          ss_mastering_type::DYNAMIC_MASTERING ) ) {

        add_necessary_remastering(
            site, partition_location_informations, write_partitions_map, plan,
            pid_dep_map, created_partition_types, created_storage_types,
            other_write_locked_partitions, original_write_partition_set,
            original_read_partition_set, site_loads, session, cached_stats,
            cached_query_arrival_stats );
    }

    if( configs_.ss_mastering_type_ == ss_mastering_type::ADAPT ) {
        change_partition_types(
            site, partition_location_informations, write_partitions_map,
            read_partitions_map, plan, pid_dep_map,
            other_write_locked_partitions, created_partition_types,
            created_storage_types, original_write_partition_set,
            original_read_partition_set, site_loads, loc_storage_sizes,
            write_sample_holder, read_sample_holder, cached_stats,
            cached_query_arrival_stats );
    }

    if( configs_.ss_mastering_type_ == ss_mastering_type::ADAPT ) {
        // Consider only the source and destination sites as placement locations
        std::unordered_set<uint32_t> candidate_sites =
            get_involved_sites( write_partitions_map, read_partitions_map,
                                partition_location_informations, site );

        tack_on_beneficial_spin_ups(
            site, id, partition_location_informations, write_partitions_map,
            read_partitions_map, plan, pid_dep_map, created_partition_types,
            created_storage_types, other_write_locked_partitions,
            original_write_partition_set, original_read_partition_set,
            candidate_sites, site_loads, average_load_per_site,
            loc_storage_sizes, write_sample_holder, read_sample_holder,
            cached_stats, cached_query_arrival_stats );

        remove_un_needed_replicas(
            site, id, partition_location_informations, write_partitions_map,
            read_partitions_map, plan, pid_dep_map, created_partition_types,
            created_storage_types, other_write_locked_partitions,
            original_write_partition_set, original_read_partition_set,
            candidate_sites, site_loads, average_load_per_site,
            loc_storage_sizes, write_sample_holder, read_sample_holder,
            cached_stats, cached_query_arrival_stats );
    }

    plan->estimated_number_of_updates_to_wait_for_ =
        site_partition_version_info_
            ->estimate_number_of_updates_need_to_wait_for(
                read_part_payloads, partition_location_informations, site,
                session );
    if( read_part_payloads.empty() ) {
        plan->estimated_number_of_updates_to_wait_for_ =
            cost_model2_->get_default_write_num_updates_required_count();
    }

    double ori_cost = plan->cost_;

    double lb_cost =
        get_site_load_imbalance_cost( site, site_loads, average_load_per_site );

    double storage_cost = get_change_in_storage_imbalance_costs(
        get_site_storage_imbalance_cost( loc_storage_sizes /* after */ ),
        storage_size_cost /* before */ );

    // MTODO-HTAP estimate txn execution cost here

    plan->cost_ = ori_cost +
                  ( lb_cost * configs_.plan_site_load_balance_weight_ ) +
                  ( storage_cost * configs_.plan_site_storage_balance_weight_ );

    DVLOG( k_heuristic_evaluator_log_level )
        << "PLAN: Generated plan for site:" << site
        << ", has cost:" << plan->cost_ << " (original cost:" << ori_cost
        << ", load_balance_cost:" << lb_cost << "), client:" << id;

    stop_timer( SS_GENERATE_PLAN_FOR_SITE_TIMER_ID );

    return plan;
}

double heuristic_site_evaluator::get_site_load_imbalance_cost(
    uint32_t site, const std::vector<site_load_information> &site_loads,
    double average_load_per_site ) {
    double site_load = site_loads.at( site ).cpu_load_;
    double load_input = 0;
    if( average_load_per_site > 0 ) {
        load_input = ( average_load_per_site - site_load ) *
                     ( site_load / average_load_per_site );
    }
    double lb_cost = cost_model2_->predict_wait_for_service_time( load_input )
                         .get_prediction();
    double zero_lb_cost =
        cost_model2_->predict_wait_for_service_time( 0 ).get_prediction();

    double normalized_lb_cost = lb_cost - zero_lb_cost;

    DVLOG( k_heuristic_evaluator_log_level )
        << "PLAN: Load balance cost:" << normalized_lb_cost
        << " ( site_load:" << site_load
        << ", average_load_per_site:" << average_load_per_site
        << ", load_input:" << load_input << ", lb_cost:" << lb_cost
        << ", zero_lb_cost:" << zero_lb_cost << " ) at site:" << site;

    return normalized_lb_cost;
}

double heuristic_site_evaluator::get_site_storage_imbalance_cost(
    const std::unordered_map<storage_tier_type::type, std::vector<double>>
        &site_storages ) const {
    // if any site is greater than 1, give it a very bad score
    // otherwise compute reward balance
    std::unordered_map<storage_tier_type::type, double> sums;
    std::unordered_map<storage_tier_type::type, double> sum_greater_one;
    std::unordered_map<storage_tier_type::type, std::vector<double>> vals;

    for( const auto &entry : site_storages ) {
        const auto &tier = entry.first;
        sums[tier] = 0;
        vals[tier] = std::vector<double>( entry.second.size(), 0 );
        for( uint32_t pos = 0; pos < entry.second.size(); pos++ ) {
            double ratio = storage_stats_->compute_storage_ratio(
                entry.second.at( pos ),
                storage_stats_->get_storage_limit( pos, tier ) );
            vals.at( tier ).at( pos ) = ratio;
            sums[tier] += ratio;
            if( ratio >= configs_.storage_removal_threshold_ ) {
                sum_greater_one[tier] += ratio;
            }
        }
    }

    double overlimit_score = 0;

    for( const auto &entry : sum_greater_one ) {
        const auto &tier = entry.first;
        DCHECK_EQ( configs_.plan_site_storage_overlimit_weights_.count( tier ),
                   1 );
        double multiplier =
            configs_.plan_site_storage_overlimit_weights_.at( tier );
        overlimit_score += ( entry.second * multiplier );
    }

    double variance_score = 0;
    // compute variance
    for( const auto &tier : configs_.acceptable_storage_types_ ) {
        DCHECK_EQ( sums.count( tier ), 1 );
        DCHECK_EQ( vals.count( tier ), 1 );
        DCHECK_EQ( configs_.plan_site_storage_variance_weights_.count( tier ),
                   1 );

        const auto &ratios = vals.at( tier );
        double      cnt = ratios.size();
        double      sum_squared = 0;
        double      variance = 0;

        if( cnt > 1 ) {
            double avg = sums.at( tier ) / cnt;
            for( const auto &d : ratios ) {
                double computed = ( d - avg );
                sum_squared += ( computed * computed );
            }
            variance = sum_squared / ( cnt - 1 );
        }
        double multiplier =
            configs_.plan_site_storage_variance_weights_.at( tier );
        variance_score += ( variance * multiplier );
    }

    double score =
        ( overlimit_score * configs_.plan_site_storage_overlimit_weight_ ) +
        ( variance_score * configs_.plan_site_storage_variance_weight_ );

    DVLOG( k_heuristic_evaluator_log_level )
        << "PLAN: Storage balance cost:" << score
        << " ( storage_sizes:" << site_storages
        << ", variance_score:" << variance_score
        << ", overlimit_score:" << overlimit_score << " )";

    return score;
}

double heuristic_site_evaluator::get_change_in_storage_imbalance_cost(
    const std::unordered_map<storage_tier_type::type, std::vector<double>>
        &after,
    const std::unordered_map<storage_tier_type::type, std::vector<double>>
        &before ) const {
    // if any site is greater than 1, give it a very bad score
    // otherwise compute reward balance
    std::unordered_map<storage_tier_type::type, double> after_sums;
    std::unordered_map<storage_tier_type::type, double> before_sums;
    std::unordered_map<storage_tier_type::type, double> after_sum_greater_one;
    std::unordered_map<storage_tier_type::type, double> before_sum_greater_one;
    std::unordered_map<storage_tier_type::type, std::vector<double>>
        before_vals;
    std::unordered_map<storage_tier_type::type, std::vector<double>> after_vals;

    DCHECK_EQ( after.size(), before.size() );

    for( const auto &after_entry : after ) {
        const auto &tier = after_entry.first;
        after_sums[tier] = 0;
        after_vals[tier] = std::vector<double>( after_entry.second.size(), 0 );
        before_sums[tier] = 0;
        before_vals[tier] = std::vector<double>( after_entry.second.size(), 0 );

        DCHECK_EQ( 1, before.count( tier ) );
        const auto &before_entry = before.at( tier );

        DCHECK_EQ( before_entry.size(), after_entry.second.size() );

        for( uint32_t pos = 0; pos < before_entry.size(); pos++ ) {
            double limit = storage_stats_->get_storage_limit( pos, tier );
            double after_ratio = storage_stats_->compute_storage_ratio(
                after_entry.second.at( pos ), limit );
            double before_ratio = storage_stats_->compute_storage_ratio(
                before_entry.at( pos ), limit );

            after_vals.at( tier ).at( pos ) = after_ratio;
            after_sums[tier] += after_ratio;
            before_vals.at( tier ).at( pos ) = before_ratio;
            before_sums[tier] += before_ratio;

            if( after_ratio >= configs_.storage_removal_threshold_ ) {
                after_sum_greater_one[tier] += after_ratio;
            }
            if( before_ratio >= configs_.storage_removal_threshold_ ) {
                before_sum_greater_one[tier] += before_ratio;
            }
        }
    }

    double before_overlimit_score = 0;
    double after_overlimit_score = 0;

    for( const auto &entry : before_sum_greater_one ) {
        const auto &tier = entry.first;
        DCHECK_EQ( configs_.plan_site_storage_overlimit_weights_.count( tier ),
                   1 );
        double multiplier =
            configs_.plan_site_storage_overlimit_weights_.at( tier );
        before_overlimit_score += ( entry.second * multiplier );
    }
    for( const auto &entry : after_sum_greater_one ) {
        const auto &tier = entry.first;
        DCHECK_EQ( configs_.plan_site_storage_overlimit_weights_.count( tier ),
                   1 );
        double multiplier =
            configs_.plan_site_storage_overlimit_weights_.at( tier );
        after_overlimit_score += ( entry.second * multiplier );
    }

    double after_variance_score = 0;
    double before_variance_score = 0;
    // compute variance
    for( const auto &tier : configs_.acceptable_storage_types_ ) {
        DCHECK_EQ( before_sums.count( tier ), 1 );
        DCHECK_EQ( before_vals.count( tier ), 1 );
        DCHECK_EQ( after_sums.count( tier ), 1 );
        DCHECK_EQ( after_vals.count( tier ), 1 );

        DCHECK_EQ( configs_.plan_site_storage_variance_weights_.count( tier ),
                   1 );

        const auto &after_ratios = after_vals.at( tier );
        double      after_cnt = after_ratios.size();
        double      after_sum_squared = 0;
        double      after_variance = 0;

        const auto &before_ratios = before_vals.at( tier );
        double      before_cnt = before_ratios.size();
        double      before_sum_squared = 0;
        double      before_variance = 0;

        DCHECK_GT( before_cnt, 0 );
        DCHECK_GT( after_cnt, 0 );
        DCHECK_EQ( before_cnt, after_cnt );

        double before_avg = before_sums.at( tier ) / before_cnt;
        double after_avg = before_sums.at( tier ) / before_cnt;

        for( uint32_t pos = 0; pos < before_cnt; pos++ ) {
            double before_computed =
                ( before_ratios.at( ( pos ) ) - before_avg );
            double after_computed = ( after_ratios.at( ( pos ) ) - after_avg );
            before_sum_squared += ( before_computed * before_computed );
            after_sum_squared += ( after_computed * after_computed );
        }
        if( before_cnt > 1 ) {
            before_variance = before_sum_squared / ( before_cnt - 1 );
        }
        if( after_cnt > 1 ) {
            after_variance = after_sum_squared / ( after_cnt - 1 );
        }
        double multiplier =
            configs_.plan_site_storage_variance_weights_.at( tier );
        before_variance_score += ( before_variance * multiplier );
        after_variance_score += ( after_variance * multiplier );
    }

    double before_score =
        ( before_overlimit_score *
          configs_.plan_site_storage_overlimit_weight_ ) +
        ( before_variance_score * configs_.plan_site_storage_variance_weight_ );
    double after_score =
        ( after_overlimit_score *
          configs_.plan_site_storage_overlimit_weight_ ) +
        ( after_variance_score * configs_.plan_site_storage_variance_weight_ );

    double score =
        get_change_in_storage_imbalance_costs( after_score, before_score );

    DVLOG( k_heuristic_evaluator_log_level )
        << "PLAN: Storage balance cost:" << score
        << " ( after_storage_sizes:" << after
        << ", after_variance_score:" << after_variance_score
        << ", after_overlimit_score:" << after_overlimit_score
        << ", before_storage_sizes:" << before
        << ", before_variance_score:" << before_variance_score
        << ", before_overlimit_score:" << before_overlimit_score << " )";

    return score;
}

double heuristic_site_evaluator::get_change_in_storage_imbalance_costs(
    double after, double before ) const {
    return before - after;
}

void heuristic_site_evaluator::tack_on_beneficial_spin_ups(
    uint32_t site, const ::clientid id,
    partition_column_identifier_map_t<std::shared_ptr<
        partition_location_information>> &partition_location_informations,
    const partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &write_partitions_map,
    const partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &                                 read_partitions_map,
    std::shared_ptr<pre_transaction_plan> plan,
    partition_column_identifier_map_t<std::shared_ptr<pre_action>> &pid_dep_map,
    partition_column_identifier_map_t<std::unordered_map<
        uint32_t, partition_type::type>> &created_partition_types,
    partition_column_identifier_map_t<std::unordered_map<
        uint32_t, storage_tier_type::type>> &created_storage_types,
    partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &other_write_locked_partitions,
    const partition_column_identifier_unordered_set
        &original_write_partition_set,
    const partition_column_identifier_unordered_set
        &                                     original_read_partition_set,
    const std::unordered_set<uint32_t> &      candidate_sites,
    const std::vector<site_load_information> &site_loads,
    double                                    average_load_per_site,
    std::unordered_map<storage_tier_type::type, std::vector<double>>
        &storage_sizes,
    const partition_column_identifier_map_t<
        std::vector<transaction_partition_accesses>> &write_sample_holder,
    const partition_column_identifier_map_t<
        std::vector<transaction_partition_accesses>> &read_sample_holder,
    cached_ss_stat_holder &                           cached_stats,
    query_arrival_cached_stats &cached_query_arrival_stats ) {
    start_timer( SS_PLAN_TACK_ON_BENEFICIAL_SPIN_UPS_TIMER_ID );

    DVLOG( 20 ) << "Checking if the plan should spin up partitions.";

    DCHECK( samplers_ );
    std::vector<tracking_summary> &sampled_txns =
        samplers_->at( id )->get_reservoir_ref();

    std::unordered_map<uint32_t, partition_column_identifier_unordered_set>
        spinup_replica_sets;

    for( uint32_t sample_number = 0;
         sample_number < configs_.num_samples_for_client_ +
                             configs_.num_samples_from_other_clients_;
         sample_number++ ) {
        /* With replacement, because without replacement is more complex */
        int32_t rand_slot =
            rand_dist_.get_uniform_int( 0, sampled_txns.size() - 1 );
        tracking_summary &summary = sampled_txns.at( rand_slot );
        tack_on_beneficial_spin_ups_for_sample(
            summary, candidate_sites, read_partitions_map,
            other_write_locked_partitions, partition_location_informations,
            spinup_replica_sets, site_loads, average_load_per_site );
    }

    add_spinup_replicas_to_plan(
        plan, spinup_replica_sets, pid_dep_map, created_partition_types,
        created_storage_types, read_partitions_map,
        other_write_locked_partitions, partition_location_informations,
        original_write_partition_set, original_read_partition_set, site_loads,
        storage_sizes, write_sample_holder, read_sample_holder, cached_stats,
        cached_query_arrival_stats );

    DVLOG( 20 ) << "Done looking to spin up a replica!";
    stop_timer( SS_PLAN_TACK_ON_BENEFICIAL_SPIN_UPS_TIMER_ID );
}

void heuristic_site_evaluator::add_spinup_replicas_to_plan(
    std::shared_ptr<pre_transaction_plan> plan,
    std::unordered_map<uint32_t, partition_column_identifier_unordered_set>
        &spinup_replica_sets,
    partition_column_identifier_map_t<std::shared_ptr<pre_action>> &pid_dep_map,
    partition_column_identifier_map_t<std::unordered_map<
        uint32_t, partition_type::type>> &created_partition_types,
    partition_column_identifier_map_t<std::unordered_map<
        uint32_t, storage_tier_type::type>> &created_storage_types,
    const partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &read_partitions_map,
    partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &other_write_locked_partitions,
    partition_column_identifier_map_t<std::shared_ptr<
        partition_location_information>> &partition_location_informations,
    const partition_column_identifier_unordered_set
        &original_write_partition_set,
    const partition_column_identifier_unordered_set
        &                                     original_read_partition_set,
    const std::vector<site_load_information> &site_loads,
    std::unordered_map<storage_tier_type::type, std::vector<double>>
        &storage_sizes,
    const partition_column_identifier_map_t<
        std::vector<transaction_partition_accesses>> &write_sample_holder,
    const partition_column_identifier_map_t<
        std::vector<transaction_partition_accesses>> &read_sample_holder,
    cached_ss_stat_holder &                           cached_stats,
    query_arrival_cached_stats &cached_query_arrival_stats ) {

    start_timer( SS_ADD_SPINUP_REPLICAS_TO_PLAN_TIMER_ID );

    partition_column_identifier_map_t<std::shared_ptr<pre_action>> new_deps;

    for( auto entry : spinup_replica_sets ) {
        uint32_t site = entry.first;
        auto     pids = entry.second;

        // build up per site state
        std::unordered_map<uint32_t, std::vector<std::shared_ptr<pre_action>>>
            deps_per_site;
        std::unordered_map<uint32_t, std::vector<partition_column_identifier>>
            pids_per_site;
        std::unordered_map<uint32_t,
                           std::vector<std::shared_ptr<partition_payload>>>
            parts_per_site;
        std::unordered_map<
            uint32_t,
            std::vector<std::shared_ptr<partition_location_information>>>
            part_locations_per_site;

        std::unordered_map<uint32_t, std::vector<add_replica_stats>>
            add_stats_per_site;
        std::unordered_map<uint32_t, std::vector<partition_type::type>>
            add_types_per_site;
        std::unordered_map<uint32_t, std::vector<storage_tier_type::type>>
            add_storage_types_per_site;

        for( auto &pid : pids ) {
            auto part_search = read_partitions_map.find( pid );
            DCHECK( part_search != read_partitions_map.end() );
            auto part = part_search->second;
            auto location_information =
                get_location_information_for_partition_or_from_payload(
                    part, partition_location_informations,
                    partition_lock_mode::no_lock );
            if( !location_information ) {
                continue;
            }
            DCHECK( location_information );
            DCHECK_NE( location_information->master_location_,
                       k_unassigned_master );

            pids_per_site[location_information->master_location_].push_back(
                pid );
            parts_per_site[location_information->master_location_].push_back(
                part );
            part_locations_per_site[location_information->master_location_]
                .push_back( location_information );

            auto found_types = get_replica_type(
                part, location_information,
                location_information->master_location_, site, plan,
                other_write_locked_partitions, partition_location_informations,
                original_write_partition_set, original_read_partition_set,
                site_loads, storage_sizes, write_sample_holder,
                read_sample_holder, cached_stats, cached_query_arrival_stats );
            partition_type::type    dest_type = std::get<0>( found_types );
            storage_tier_type::type dest_storage_type =
                std::get<1>( found_types );
            created_partition_types[part->identifier_][site] = dest_type;
            created_storage_types[part->identifier_][site] = dest_storage_type;

            // update storage_sizes
            auto part_storage_size =
                storage_stats_->get_partition_size( part->identifier_ );
            storage_sizes.at( dest_storage_type ).at( dest_type ) +=
                part_storage_size;

            add_replica_stats add_replica(
                std::get<1>( location_information->get_partition_type(
                    location_information->master_location_ ) ),
                std::get<1>( location_information->get_storage_type(
                    location_information->master_location_ ) ),
                dest_type, dest_storage_type,
                cost_model2_->normalize_contention_by_time(
                    part->get_contention() ),
                query_stats_->get_and_cache_cell_widths( part->identifier_,
                                                         cached_stats ),
                part->get_num_rows() );

            add_stats_per_site[location_information->master_location_]
                .emplace_back( add_replica );
            add_types_per_site[location_information->master_location_]
                .emplace_back( dest_type );
            add_storage_types_per_site[location_information->master_location_]
                .emplace_back( dest_storage_type );

            auto dep_search = pid_dep_map.find( pid );
            if( dep_search != pid_dep_map.end() ) {
                deps_per_site[location_information->master_location_].push_back(
                    dep_search->second );
            }
        }

        for( auto &pids_per_site_entry : pids_per_site ) {
            uint32_t master_site = pids_per_site_entry.first;
            auto     pids_to_replicate = pids_per_site_entry.second;
            auto     parts_to_replicate = parts_per_site.at( master_site );
            auto     part_locations_to_replicate =
                part_locations_per_site.at( master_site );
            auto add_stats = add_stats_per_site.at( master_site );
            auto partition_types = add_types_per_site.at( master_site );
            auto storage_types = add_storage_types_per_site.at( master_site );

            std::vector<std::shared_ptr<pre_action>> deps;
            auto dep_search = deps_per_site.find( master_site );
            if( dep_search != deps_per_site.end() ) {
                deps = dep_search->second;
            }

            DVLOG( 20 ) << "Going to spin up a replica of " << pids_to_replicate
                        << " on site: " << site;

            double dest_load = site_loads.at( master_site ).cpu_load_;
            double source_load = site_loads.at( site ).cpu_load_;

            cost_model_prediction_holder model_holder =
                cost_model2_->predict_add_replica_execution_time(
                    source_load, dest_load, add_stats );

            double expected_benefit = get_expected_benefit_of_adding_replicas(
                parts_to_replicate, part_locations_to_replicate,
                partition_types, storage_types, master_site, site,
                other_write_locked_partitions, partition_location_informations,
                original_write_partition_set, original_read_partition_set,
                site_loads, write_sample_holder, read_sample_holder,
                cached_stats, cached_query_arrival_stats );

            double cost = ( configs_.upfront_cost_weight_ *
                            -model_holder.get_prediction() ) +
                          ( configs_.horizon_weight_ * expected_benefit );

            DVLOG( k_heuristic_evaluator_log_level )
                << "BACKGROUND PLAN: Add replica partitions: ("
                << pids_to_replicate << "), master:" << master_site
                << ", replica to add:" << site << ", overall cost:" << cost
                << " ( upfront cost:" << model_holder.get_prediction()
                << ", expected_benefit:" << expected_benefit;

            // No cost, the heuristic says it is good!
            auto add_plan = create_add_replica_partition_pre_action(
                site, master_site, pids_to_replicate, partition_types,
                storage_types, deps, cost, model_holder );

            plan->add_background_work_item( add_plan );

#if 0  // BACKGROUND
            for( auto &pid : pids_to_replicate ) {
                new_deps[pid] = add_plan;
            }
#endif
        }
    }
#if 0  // BACKGROUND

    // update deps
    for( auto &dep_entry : new_deps ) {
        pid_dep_map[dep_entry.first] = dep_entry.second;
    }
#endif
    stop_timer( SS_ADD_SPINUP_REPLICAS_TO_PLAN_TIMER_ID );
}

void heuristic_site_evaluator::tack_on_beneficial_spin_ups_for_sample(
    tracking_summary &                  summary,
    const std::unordered_set<uint32_t> &candidate_sites,
    const partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &read_partitions_map,
    partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &other_write_locked_partitions,
    partition_column_identifier_map_t<std::shared_ptr<
        partition_location_information>> &partition_location_informations,
    std::unordered_map<uint32_t, partition_column_identifier_unordered_set>
        &                                     spinup_replica_sets,
    const std::vector<site_load_information> &site_loads,
    double                                    average_load_per_site ) {

    if( summary.is_placeholder_sample() ) {
        DVLOG( 50 ) << "Grabbed placeholder sample, going again!";
        return;
    }

    start_timer( SS_TACK_ON_BENEFICIAL_SPIN_UPS_FOR_SAMPLE_TIMER_ID );

    DVLOG( 50 ) << "Got real sample!";
    transaction_partition_accesses &txn_partition_accesses =
        summary.txn_partition_accesses_;
    /* If any of these are writes, then we can't move the txn with a new
     * replica. */
    bool txn_is_write = false;
    partition_column_identifier_unordered_set
        sample_pids_that_intersect_with_read_set;

    uint32_t num_sites = get_num_sites();

    std::vector<uint32_t> all_parts_site_counts( num_sites, 0 );
    std::vector<uint32_t> filtered_parts_site_counts( num_sites, 0 );
    uint32_t              all_part_counts = 0;
    uint32_t              filtered_part_counts = 0;
    std::vector<partition_column_identifier_unordered_set>
        read_partitions_missing_at_site( num_sites );

    // for every partition in the sample, break out if the sample is not
    // read-only.
    // Additionally:
    //  1) track the set of samples that are part of the read-set
    //  (sample_pids_that_intersect_with_read_set).
    //  2) for every site a partition is located as a replica or master,
    //  increment a per site counter, both for all partitions
    //  (all_parts_site_counts) and for partitions that do not intersect with
    //  the read set ( filtered_parts_site_counts). We use this information to
    //  determine if the transaction is runnable without the read set.
    //  3) If a partition that is in the read set is not located at a site
    //  location_information this information (read_partitions_missing_at_site)
    //  so that we can
    //  use this information to determine what replicas we need to add
    for( partition_access &part_access :
         txn_partition_accesses.partition_accesses_ ) {
        if( part_access.is_write_ ) {
            // not read-only
            txn_is_write = true;
            break;
        }
        const auto &access_pid = part_access.partition_id_;

        auto read_found = read_partitions_map.find( access_pid );
        bool part_of_read_set = read_found != read_partitions_map.end();
        if( part_of_read_set ) {
            sample_pids_that_intersect_with_read_set.insert( access_pid );
        }
        build_runnable_locations_information(
            part_of_read_set, all_parts_site_counts, &all_part_counts,
            filtered_parts_site_counts, &filtered_part_counts,
            read_partitions_missing_at_site, access_pid,
            other_write_locked_partitions, partition_location_informations,
            num_sites );
    }
    // Can't do anything about this txn, it is either a write partition, or not
    // relevant to our current transaction
    if( txn_is_write or sample_pids_that_intersect_with_read_set.empty() ) {
        DVLOG( 40 ) << "Skipping sample, is write:" << txn_is_write
                    << ", intersect size:"
                    << sample_pids_that_intersect_with_read_set.size();
        stop_timer( SS_TACK_ON_BENEFICIAL_SPIN_UPS_FOR_SAMPLE_TIMER_ID );
        return;
    }

    // Okay, so now we know that this txn is relevant: it is both read-only
    // and contains read to the partitions
    DVLOG( 40 ) << "Considering sample";

    // Determine the sites that the transaction can run under two transactions:
    // 1) can currently run without any additional changes
    // 2) could run at if we added replicas that are part of our read set
    // a transaction can run at a site, if the number of partitions at the site
    // matches the number of partitions in the transaction (without the
    // intersecting partitions with the read set for 2)
    std::unordered_set<uint32_t> current_runnable_locations;
    std::unordered_set<uint32_t> runnable_locations_without_read_set;
    for( uint32_t site = 0; site < num_sites; site++ ) {
        if( all_parts_site_counts[site] == all_part_counts ) {
            // it is possible to run here, because every partition is
            // already present at the site
            current_runnable_locations.insert( site );
        }
        DVLOG( 40 ) << "filtered_parts_site_counts[" << site
                    << "] =" << filtered_parts_site_counts[site]
                    << ", filtered_part_counts=" << filtered_part_counts;
        if( filtered_parts_site_counts[site] == filtered_part_counts ) {
            // it is possible to run here, if we add the read partitions here
            runnable_locations_without_read_set.insert( site );
        }
    }
    DVLOG( 40 ) << "runnable_locations_without_read_set="
                << runnable_locations_without_read_set;
    DVLOG( 40 ) << "current_runnable_locations=" << current_runnable_locations;

    // We can already run this on any site, a new replica will not improve
    // things
    if( current_runnable_locations.size() == num_sites ) {
        DVLOG( 40 ) << "Current runnable locations, match num sites:"
                    << num_sites;
        stop_timer( SS_TACK_ON_BENEFICIAL_SPIN_UPS_FOR_SAMPLE_TIMER_ID );
        return;
    }

    // add replicas to already candidate sites
    for( uint32_t site_number : candidate_sites ) {
        // it is already runnable
        DVLOG( 40 ) << "Considering sample at site:" << site_number;
        if( current_runnable_locations.count( site_number ) == 1 ) {
            DVLOG( 40 ) << "Already runnable at site:" << site_number;
            continue;
        }
        if( runnable_locations_without_read_set.count( site_number ) == 0 ) {
            DVLOG( 40 ) << "Not runnable without read set at site:"
                        << site_number;
            continue;
        }
        double site_load = site_loads.at( site_number ).cpu_load_;
        DVLOG( 40 ) << "Consider adding replicas at site:" << site_number
                    << ", site load:" << site_load
                    << ", avg load:" << average_load_per_site;

        if( site_load < average_load_per_site ) {
            DVLOG( 40 ) << "Underloaded at site, adding spinup replica sets";

            // it is runnable if we added partitions from the read set
            auto missing_pids = read_partitions_missing_at_site[site_number];
            // there must be something missing otherwise we wouldn't be runnable
            DCHECK( !missing_pids.empty() );
            spinup_replica_sets[site_number].insert( missing_pids.begin(),
                                                     missing_pids.end() );
        }
    }
    stop_timer( SS_TACK_ON_BENEFICIAL_SPIN_UPS_FOR_SAMPLE_TIMER_ID );
}

void heuristic_site_evaluator::remove_un_needed_replicas(
    uint32_t site, const ::clientid id,
    partition_column_identifier_map_t<std::shared_ptr<
        partition_location_information>> &partition_location_informations,
    const partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &write_partitions_map,
    const partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &                                 read_partitions_map,
    std::shared_ptr<pre_transaction_plan> plan,
    partition_column_identifier_map_t<std::shared_ptr<pre_action>> &pid_dep_map,
    partition_column_identifier_map_t<std::unordered_map<
        uint32_t, partition_type::type>> &created_partition_types,
    partition_column_identifier_map_t<std::unordered_map<
        uint32_t, storage_tier_type::type>> &created_storage_types,
    partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &other_write_locked_partitions,
    const partition_column_identifier_unordered_set
        &original_write_partition_set,
    const partition_column_identifier_unordered_set
        &                                     original_read_partition_set,
    const std::unordered_set<uint32_t> &      candidate_sites,
    const std::vector<site_load_information> &site_loads,
    double                                    average_load_per_site,
    std::unordered_map<storage_tier_type::type, std::vector<double>>
        &storage_sizes,
    const partition_column_identifier_map_t<
        std::vector<transaction_partition_accesses>> &write_sample_holder,
    const partition_column_identifier_map_t<
        std::vector<transaction_partition_accesses>> &read_sample_holder,
    cached_ss_stat_holder &                           cached_stats,
    query_arrival_cached_stats &cached_query_arrival_stats ) {
    start_timer( SS_PLAN_REMOVE_UN_NEEDED_REPLICAS_TIMER_ID );

    DVLOG( 20 ) << "Checking if the plan should remove any partitions.";

    DCHECK( samplers_ );
    std::vector<tracking_summary> &sampled_txns =
        samplers_->at( id )->get_reservoir_ref();

    std::unordered_map<uint32_t, partition_column_identifier_unordered_set>
        remove_replica_sets;

    for( uint32_t sample_number = 0;
         sample_number < configs_.num_samples_for_client_ +
                             configs_.num_samples_from_other_clients_;
         sample_number++ ) {
        /* With replacement, because without replacement is more complex */
        int32_t rand_slot =
            rand_dist_.get_uniform_int( 0, sampled_txns.size() - 1 );
        tracking_summary &summary = sampled_txns.at( rand_slot );
        remove_un_needed_replicas_for_sample(
            summary, candidate_sites, read_partitions_map,
            other_write_locked_partitions, partition_location_informations,
            remove_replica_sets, site_loads, average_load_per_site,
            storage_sizes );
    };

    master_destination_partition_entries to_change_types;

    std::unordered_map<storage_tier_type::type, std::vector<double>>
        loc_storage_sizes = storage_sizes;

    for( uint32_t site : candidate_sites ) {
        for( const auto &tier : configs_.acceptable_storage_types_ ) {
            if( storage_stats_->compute_storage_ratio(
                    storage_sizes.at( tier ).at( site ),
                    storage_stats_->get_storage_limit( site, tier ) ) >=
                configs_.storage_removal_threshold_ ) {
                remove_replicas_or_change_types_for_capacity(
                    site, tier, remove_replica_sets, to_change_types,
                    partition_location_informations, write_partitions_map,
                    read_partitions_map, pid_dep_map,
                    other_write_locked_partitions, created_partition_types,
                    created_storage_types, original_write_partition_set,
                    original_read_partition_set, site_loads, loc_storage_sizes,
                    write_sample_holder, read_sample_holder, cached_stats,
                    cached_query_arrival_stats );
            }
        }
    }

    for( auto &change_type_entry : to_change_types ) {
        const auto &master_dest = change_type_entry.first;
        auto &      parts = change_type_entry.second;
        add_change_types_to_plan(
            parts.pids_, parts.payloads_, parts.location_infos_,
            parts.partition_types_, parts.storage_types_, plan,
            master_dest.destination_, pid_dep_map,
            other_write_locked_partitions, original_write_partition_set,
            original_read_partition_set, partition_location_informations,
            site_loads, write_sample_holder, read_sample_holder, cached_stats,
            cached_query_arrival_stats );
    }

    add_remove_replicas_to_plan(
        plan, remove_replica_sets, pid_dep_map, read_partitions_map,
        write_partitions_map, other_write_locked_partitions,
        partition_location_informations, original_write_partition_set,
        original_read_partition_set, site_loads, storage_sizes, cached_stats,
        cached_query_arrival_stats );

    DVLOG( 20 ) << "Done looking to remove a replica!";
    stop_timer( SS_PLAN_REMOVE_UN_NEEDED_REPLICAS_TIMER_ID );
}

void heuristic_site_evaluator::remove_replicas_or_change_types_for_capacity(
    uint32_t site, const storage_tier_type::type &tier,
    std::unordered_map<uint32_t, partition_column_identifier_unordered_set>
        &                                 remove_replica_sets,
    master_destination_partition_entries &to_change_types,
    partition_column_identifier_map_t<std::shared_ptr<
        partition_location_information>> &partition_location_informations,
    const partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &write_partitions_map,
    const partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &read_partitions_map,
    partition_column_identifier_map_t<std::shared_ptr<pre_action>> &pid_dep_map,
    partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &other_write_locked_partitions,
    partition_column_identifier_map_t<std::unordered_map<
        uint32_t, partition_type::type>> &created_partition_types,
    partition_column_identifier_map_t<std::unordered_map<
        uint32_t, storage_tier_type::type>> &created_storage_types,
    const partition_column_identifier_unordered_set
        &original_write_partition_set,
    const partition_column_identifier_unordered_set
        &                                     original_read_partition_set,
    const std::vector<site_load_information> &site_loads,
    std::unordered_map<storage_tier_type::type, std::vector<double>>
        &storage_sizes,
    const partition_column_identifier_map_t<
        std::vector<transaction_partition_accesses>> &write_sample_holder,
    const partition_column_identifier_map_t<
        std::vector<transaction_partition_accesses>> &read_sample_holder,
    cached_ss_stat_holder &                           cached_stats,
    query_arrival_cached_stats &cached_query_arrival_stats ) {

    std::vector<partition_column_identifier> pids_to_consider =
        tier_tracking_->get_removal_candidates_in_order(
            site, tier, configs_.num_remove_replicas_iteration_ );

    double ori_storage_imbalance =
        get_site_storage_imbalance_cost( storage_sizes );

    uint32_t max_num_iters = std::min( configs_.num_remove_replicas_iteration_,
                                       (uint32_t) pids_to_consider.size() );

    bool can_change_storage_tiers = configs_.allow_change_storage_types_ and
                                    ( tier == storage_tier_type::type::MEMORY );
    if( can_change_storage_tiers ) {
        bool is_disk_allowed = false;
        for( const auto &tier : configs_.acceptable_storage_types_ ) {
            if( tier == storage_tier_type::type::DISK ) {
                is_disk_allowed = true;
                break;
            }
        }
        can_change_storage_tiers = can_change_storage_tiers and is_disk_allowed;
    }

    partition_column_identifier_unordered_set seen_set;

    uint32_t num_iters = 0;
    while( ( num_iters < max_num_iters ) and
           ( storage_stats_->compute_storage_ratio(
                 storage_sizes.at( tier ).at( site ),
                 storage_stats_->get_storage_limit( site, tier ) ) >=
             configs_.storage_removal_threshold_ ) ) {
        const auto pid = pids_to_consider.at( num_iters );
        num_iters += 1;

        bool can_change_pid = ( pid_dep_map.count( pid ) == 0 ) and
                              ( write_partitions_map.count( pid ) == 0 ) and
                              ( read_partitions_map.count( pid ) == 0 ) and
                              ( seen_set.count( pid ) == 0 );

        if( !can_change_pid ) {
            continue;
        }

        seen_set.insert( pid );

        auto lock_entry =
            get_partition_from_data_location_table_or_already_locked(
                pid, other_write_locked_partitions,
                partition_lock_mode::try_lock );
        auto part = std::get<0>( lock_entry );
        if( !part ) {
            continue;
        }
        auto part_location_information =
            get_location_information_for_partition_or_from_payload(
                part, partition_location_informations,
                partition_lock_mode::no_lock );
        if( !part_location_information ) {
            unlock_if_new( lock_entry, other_write_locked_partitions,
                           partition_location_informations,
                           partition_lock_mode::try_lock );

            continue;
        }

        if( ( part_location_information->master_location_ != site ) and
            ( part_location_information->replica_locations_.count( site ) ==
              0 ) ) {
            continue;
        }

        auto part_type = std::get<1>(
            part_location_information->get_partition_type( site ) );
        auto storage_tier =
            std::get<1>( part_location_information->get_storage_type( site ) );

        if( storage_tier != tier ) {
            unlock_if_new( lock_entry, other_write_locked_partitions,
                           partition_location_informations,
                           partition_lock_mode::try_lock );
            continue;
        }

        double partition_size = storage_stats_->get_partition_size( pid );

        double change_tier_cost = -DBL_MAX;
        double remove_cost = -DBL_MAX;

        double contention = cost_model2_->normalize_contention_by_time(
            part->get_contention() );
        auto cell_widths =
            query_stats_->get_and_cache_cell_widths( pid, cached_stats );

        storage_tier_type::type try_storage_type =
            storage_tier_type::type::DISK;

        std::vector<std::shared_ptr<partition_payload>> parts_to_change = {
            part};
        std::vector<std::shared_ptr<partition_location_information>>
                                             parts_to_change_locations = {part_location_information};
        std::vector<partition_type::type>    old_types = {part_type};
        std::vector<storage_tier_type::type> old_storage_types = {tier};

        if( can_change_storage_tiers ) {
            // MTODO GENERALIZE
            DCHECK_NE( try_storage_type, tier );

            storage_sizes.at( tier ).at( site ) -= partition_size;
            storage_sizes.at( try_storage_type ).at( site ) += partition_size;

            change_types_stats change_stat( part_type, part_type, tier,
                                            try_storage_type, contention,
                                            cell_widths, part->get_num_rows() );

            std::vector<change_types_stats> changes = {change_stat};

            cost_model_prediction_holder prediction =
                cost_model2_->predict_changing_type_execution_time(
                    site_loads.at( site ).cpu_load_, changes );
            double upfront_cost = prediction.get_prediction();

            std::vector<storage_tier_type::type> storage_types_to_change = {
                try_storage_type};

            double expected_benefit = get_expected_benefit_of_changing_types(
                parts_to_change, parts_to_change_locations,
                old_types /*types_to_change*/, storage_types_to_change,
                old_types, old_storage_types, changes, site,
                other_write_locked_partitions, partition_location_informations,
                original_write_partition_set, original_read_partition_set,
                site_loads, write_sample_holder, read_sample_holder,
                cached_stats, cached_query_arrival_stats );

            double storage_benefit = get_change_in_storage_imbalance_costs(
                get_site_storage_imbalance_cost( storage_sizes ),
                ori_storage_imbalance );

            // undo the change
            storage_sizes.at( tier ).at( site ) += partition_size;
            storage_sizes.at( try_storage_type ).at( site ) -= partition_size;

            change_tier_cost =
                ( configs_.upfront_cost_weight_ * upfront_cost ) +
                ( configs_.horizon_weight_ * expected_benefit ) +
                ( configs_.horizon_storage_weight_ * storage_benefit );
        }
        if( site != part_location_information->master_location_ ) {
            cost_model_prediction_holder model_holder;
            double                       load = site_loads.at( site ).cpu_load_;
            remove_replica_stats replica( part_type, contention, cell_widths,
                                          part->get_num_rows() );
            std::vector<remove_replica_stats> replicas = {replica};
            cost_model2_->predict_remove_replica_execution_time( load,
                                                                 replicas );

            double expected_benefit = get_expected_benefit_of_removing_replicas(
                parts_to_change, parts_to_change_locations, old_types, site,
                other_write_locked_partitions, partition_location_informations,
                original_write_partition_set, original_read_partition_set,
                site_loads, cached_stats, cached_query_arrival_stats );

            storage_sizes.at( tier ).at( site ) -= partition_size;

            double expected_storage_benefit =
                get_change_in_storage_imbalance_costs(
                    get_site_storage_imbalance_cost( storage_sizes ),
                    ori_storage_imbalance );

            storage_sizes.at( tier ).at( site ) += partition_size;

            remove_cost =
                ( configs_.upfront_cost_weight_ *
                  -model_holder.get_prediction() ) +
                ( configs_.horizon_weight_ * expected_benefit ) +
                ( configs_.horizon_storage_weight_ * expected_storage_benefit );
        }

        if( ( change_tier_cost == -DBL_MAX ) and ( remove_cost == -DBL_MAX ) ) {
            // unlock the part and continue
            unlock_if_new( lock_entry, other_write_locked_partitions,
                           partition_location_informations,
                           partition_lock_mode::try_lock );
            continue;
        }

        if( change_tier_cost > remove_cost ) {
            storage_sizes.at( tier ).at( site ) -= partition_size;
            storage_sizes.at( try_storage_type ).at( site ) += partition_size;

            created_storage_types[pid][site] = try_storage_type;

            master_destination_dependency_version master_dest( site, site, 0 );
            add_to_partition_operation_tracking(
                to_change_types, master_dest, pid, part,
                part_location_information, part_type, try_storage_type );

        } else {
            storage_sizes.at( tier ).at( site ) -= partition_size;
            remove_replica_sets[site].insert( pid );
        }
    }
}

void heuristic_site_evaluator::add_remove_replicas_to_plan(
    std::shared_ptr<pre_transaction_plan> plan,
    std::unordered_map<uint32_t, partition_column_identifier_unordered_set>
        &remove_replica_sets,
    partition_column_identifier_map_t<std::shared_ptr<pre_action>> &pid_dep_map,
    const partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &read_partitions_map,
    const partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &write_partitions_map,
    partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &other_write_locked_partitions,
    partition_column_identifier_map_t<std::shared_ptr<
        partition_location_information>> &partition_location_informations,
    const partition_column_identifier_unordered_set
        &original_write_partition_set,
    const partition_column_identifier_unordered_set
        &                                     original_read_partition_set,
    const std::vector<site_load_information> &site_loads,
    std::unordered_map<storage_tier_type::type, std::vector<double>>
        &                       storage_sizes,
    cached_ss_stat_holder &     cached_stats,
    query_arrival_cached_stats &cached_query_arrival_stats ) {

    start_timer( SS_ADD_REMOVE_REPLICAS_TO_PLAN_TIMER_ID );

    partition_column_identifier_map_t<std::shared_ptr<pre_action>> new_deps;

    for( auto entry : remove_replica_sets ) {
        uint32_t site = entry.first;
        auto     pids = entry.second;

        // build up per site state
        std::vector<std::shared_ptr<pre_action>>        deps;
        std::vector<partition_column_identifier>        pids_to_remove;
        std::vector<std::shared_ptr<partition_payload>> parts_to_remove;
        std::vector<std::shared_ptr<partition_location_information>>
                                          part_locations_to_remove;
        std::vector<partition_type::type> types_to_remove;

        double ori_storage_score =
            get_site_storage_imbalance_cost( storage_sizes );

        for( auto &pid : pids ) {
            std::shared_ptr<partition_payload> part = nullptr;
            std::shared_ptr<partition_location_information>
                 location_information = nullptr;
            auto part_search = read_partitions_map.find( pid );
            auto write_part_search = write_partitions_map.find( pid );
            if( part_search != read_partitions_map.end() ) {
                part = part_search->second;
                if( part ) {
                    location_information =
                        get_location_information_for_partition_or_from_payload(
                            part, partition_location_informations,
                            partition_lock_mode::no_lock );
                }
            } else if( write_part_search != write_partitions_map.end() ) {
                part = write_part_search->second;
                if( part ) {
                    location_information =
                        get_location_information_for_partition_or_from_payload(
                            part, partition_location_informations,
                            partition_lock_mode::no_lock );
                }
            } else {
                auto part_search =
                    get_partition_from_data_location_table_or_already_locked(
                        pid, other_write_locked_partitions,
                        partition_lock_mode::try_lock );

                part = std::get<0>( part_search );
                if( part != nullptr ) {
                    location_information =
                        get_location_information_for_partition_or_from_payload(
                            part, partition_location_informations,
                            partition_lock_mode::no_lock );

                    if( !location_information ) {
                        unlock_if_new( part_search,
                                       other_write_locked_partitions,
                                       partition_location_informations,
                                       partition_lock_mode::try_lock );
                    }
                }
            }
            if( !part or !location_information ) {
                continue;
            }
            DCHECK( location_information );
            DCHECK_NE( location_information->master_location_,
                       k_unassigned_master );
            DCHECK_NE( location_information->master_location_, site );
            DCHECK_EQ( location_information->replica_locations_.count( site ),
                       1 );
            auto type_to_remove =
                std::get<1>( location_information->get_partition_type( site ) );
            auto storage_to_remove =
                std::get<1>( location_information->get_storage_type( site ) );

            double storage_size = storage_stats_->get_partition_size( pid );

            storage_sizes.at( storage_to_remove ).at( site ) -= storage_size;

            pids_to_remove.push_back( pid );
            parts_to_remove.push_back( part );
            part_locations_to_remove.push_back( location_information );
            types_to_remove.push_back( type_to_remove );

            auto dep_search = pid_dep_map.find( pid );
            if( dep_search != pid_dep_map.end() ) {
                deps.push_back( dep_search->second );
            }
        }

        DVLOG( 20 ) << "Going to remove a replica of " << pids_to_remove
                    << " on site: " << site;

        cost_model_prediction_holder model_holder;
#if 0  // BACKGROUND
            double load = site_loads.at( site ).cpu_load_;
            cost_model_->predict_add_replica_execution_time(
                cost_model_->normalize_contention_by_time(
                    contention_per_site[master_site] ),
                size_per_site[master_site], load );
#endif

        double expected_benefit = get_expected_benefit_of_removing_replicas(
            parts_to_remove, part_locations_to_remove, types_to_remove, site,
            other_write_locked_partitions, partition_location_informations,
            original_write_partition_set, original_read_partition_set,
            site_loads, cached_stats, cached_query_arrival_stats );

        double expected_storage_benefit = get_change_in_storage_imbalance_costs(
            get_site_storage_imbalance_cost( storage_sizes ),
            ori_storage_score );

        double cost =
            ( configs_.upfront_cost_weight_ * -model_holder.get_prediction() ) +
            ( configs_.horizon_weight_ * expected_benefit ) +
            ( configs_.horizon_storage_weight_ * expected_storage_benefit );

        DVLOG( k_heuristic_evaluator_log_level )
            << "BACKGROUND PLAN: Add remove partitions: (" << pids_to_remove
            << ", remove:" << site << ", overall cost:" << cost
            << " ( upfront cost:" << model_holder.get_prediction()
            << ", expected_benefit:" << expected_benefit << " )";

        // No cost, the heuristic says it is good!
        auto remove_action = create_remove_partition_action(
            site, pids_to_remove, deps, cost, model_holder );

        plan->add_background_work_item( remove_action );

#if 0  // BACKGROUND
            for( auto &pid : pids_to_remove ) {
                new_deps[pid] = add_plan;
            }
#endif
    }
#if 0  // BACKGROUND

    // update deps
    for( auto &dep_entry : new_deps ) {
        pid_dep_map[dep_entry.first] = dep_entry.second;
    }
#endif
    stop_timer( SS_ADD_REMOVE_REPLICAS_TO_PLAN_TIMER_ID );
}

void heuristic_site_evaluator::remove_un_needed_replicas_for_sample(
    tracking_summary &                  summary,
    const std::unordered_set<uint32_t> &candidate_sites,
    const partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &read_partitions_map,
    partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &other_write_locked_partitions,
    partition_column_identifier_map_t<std::shared_ptr<
        partition_location_information>> &partition_location_informations,
    std::unordered_map<uint32_t, partition_column_identifier_unordered_set>
        &                                     remove_replica_sets,
    const std::vector<site_load_information> &site_loads,
    double                                    average_load_per_site,
    std::unordered_map<storage_tier_type::type, std::vector<double>>
        &storage_sizes ) {

    if( summary.is_placeholder_sample() ) {
        DVLOG( 50 ) << "Grabbed placeholder sample, going again!";
        return;
    }

    start_timer( SS_REMOVE_UN_NEEDED_REPLICAS_FOR_SAMPLE_TIMER_ID );

    DVLOG( 50 ) << "Got real sample!";
    transaction_partition_accesses &txn_partition_accesses =
        summary.txn_partition_accesses_;
    /* If any of these are writes, then we can't move the txn with a new
     * replica. */
    bool txn_is_write = false;
    partition_column_identifier_unordered_set
        sample_pids_that_intersect_with_read_set;

    uint32_t num_sites = get_num_sites();

    std::vector<uint32_t> all_parts_site_counts( num_sites, 0 );
    std::vector<uint32_t> filtered_parts_site_counts( num_sites, 0 );
    uint32_t              all_part_counts = 0;
    uint32_t              filtered_part_counts = 0;
    std::vector<partition_column_identifier_unordered_set>
        read_partitions_at_site( num_sites );

    // for every partition in the sample, break out if the sample is not
    // read-only.
    // Additionally:
    //  1) track the set of samples that are part of the read-set
    //  (sample_pids_that_intersect_with_read_set).
    //  2) for every site a partition is located as a replica
    //  increment a per site counter, both for all partitions
    //  (all_parts_site_counts) and for partitions that do not intersect with
    //  the read set ( filtered_parts_site_counts). We use this information to
    //  determine if the transaction is runnable without the read set.
    //  3) If a partition that is in the read set is located at a site
    //  location_information this information (read_partitions_at_site) so that
    //  we can use this information to determine what replicas we need to add
    DVLOG( 40 ) << "Considering transaction partition accesses to remove";
    for( partition_access &part_access :
         txn_partition_accesses.partition_accesses_ ) {
        if( part_access.is_write_ ) {
            // not read-only
            txn_is_write = true;
            break;
        }
        const auto &access_pid = part_access.partition_id_;

        auto read_found = read_partitions_map.find( access_pid );
        bool part_of_read_set = read_found != read_partitions_map.end();
        if( part_of_read_set ) {
            sample_pids_that_intersect_with_read_set.insert( access_pid );
        }
        build_remove_replica_runnable_locations_information(
            part_of_read_set, all_parts_site_counts, &all_part_counts,
            filtered_parts_site_counts, &filtered_part_counts,
            read_partitions_at_site, access_pid, other_write_locked_partitions,
            partition_location_informations, num_sites );
    }
    // Can't do anything about this txn, it is either a write partition, or not
    // relevant to our current transaction
    if( txn_is_write or sample_pids_that_intersect_with_read_set.empty() ) {
        DVLOG( 40 ) << "Skipping sample, is write:" << txn_is_write
                    << ", intersect size:"
                    << sample_pids_that_intersect_with_read_set.size();
        stop_timer( SS_REMOVE_UN_NEEDED_REPLICAS_FOR_SAMPLE_TIMER_ID );
        return;
    }

    // Okay, so now we know that this txn is relevant: it is both read-only
    // and contains read to the partitions
    DVLOG( 40 ) << "Considering sample";

    // Determine:
    // 1) the sites that the status of a transaction running does not
    // change, if we removed a replica at that site
    // a transaction can run at a site, if the number of partitions at the site
    // matches the number of partitions in the transaction
    std::unordered_map<uint32_t, bool> current_runnable_locations;
    std::unordered_map<uint32_t, bool> runnable_locations_without_read_set;
    for( uint32_t site = 0; site < num_sites; site++ ) {
        current_runnable_locations[site] =
            ( all_parts_site_counts.at( site ) == all_part_counts );
        runnable_locations_without_read_set[site] =
            ( filtered_parts_site_counts.at( site ) == all_part_counts );
        DVLOG( 40 ) << "Site:" << site
                    << ", all part counts:" << all_part_counts
                    << ", current_runnable_locations:"
                    << current_runnable_locations[site]
                    << ", runnable_locations_without_read_set:"
                    << runnable_locations_without_read_set[site];
    }
    DVLOG( 40 ) << "runnable_locations_without_read_set="
                << runnable_locations_without_read_set;
    DVLOG( 40 ) << "current_runnable_locations=" << current_runnable_locations;

    // We can't run this on any site, a removed replica will not reduce
    // things

    // remove replicas to already candidate sites
    for( uint32_t site_number : candidate_sites ) {
        // if it is the same
        if( current_runnable_locations[site_number] !=
            runnable_locations_without_read_set[site_number] ) {
            // it is already runnable
            DVLOG( 40 ) << "Considering sample at site:" << site_number
                        << ", current runnable:"
                        << current_runnable_locations[site_number]
                        << ",  runnable without read set:"
                        << runnable_locations_without_read_set[site_number];
            continue;
        }
        double site_load = site_loads.at( site_number ).cpu_load_;

        DVLOG( 40 ) << "Consider removing replicas at site:" << site_number
                    << ", site load:" << site_load
                    << ", avg load:" << average_load_per_site;

        bool over_space_limits = false;
        for( const auto &pid : read_partitions_at_site.at( site_number ) ) {
            auto location_info = partition_location_informations.at( pid );
            auto found_storage_tier =
                std::get<1>( location_info->get_storage_type( site_number ) );

            if( storage_stats_->compute_storage_ratio(
                    storage_sizes.at( found_storage_tier ).at( site_number ),
                    storage_stats_->get_storage_limit( site_number,
                                                       found_storage_tier ) ) >=
                configs_.storage_removal_threshold_ ) {
                over_space_limits = true;
            }
            if( over_space_limits ) {
                break;
            }
        }

        if( ( site_load > average_load_per_site ) or ( over_space_limits ) ) {
            DVLOG( 40 ) << "Overload at site, adding remove replica sets";
            // it is runnable if we added partitions from the read set
            auto existing_pids = read_partitions_at_site.at( site_number );
            if( existing_pids.size() > 0 ) {
                DVLOG( 40 )
                    << "Adding remove replica sets, site:" << site_number
                    << ", pids:" << existing_pids;
                remove_replica_sets[site_number].insert( existing_pids.begin(),
                                                         existing_pids.end() );
            }
        }
    }
    stop_timer( SS_REMOVE_UN_NEEDED_REPLICAS_FOR_SAMPLE_TIMER_ID );
}

void heuristic_site_evaluator::
    build_remove_replica_runnable_locations_information(
        bool part_of_read_set, std::vector<uint32_t> &all_parts_site_counts,
        uint32_t *             all_part_counts,
        std::vector<uint32_t> &filtered_parts_site_counts,
        uint32_t *             filtered_part_counts,
        std::vector<partition_column_identifier_unordered_set>
            &                              read_partitions_at_site,
        const partition_column_identifier &access_pid,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &other_write_locked_partitions,
        partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>> &partition_location_informations,
        uint32_t                              num_sites ) {

    DVLOG( 40 ) << "Considering remove replica runnable locations info:"
                << access_pid << ", part of read set:" << part_of_read_set;

    const partition_lock_mode lock_mode = partition_lock_mode::no_lock;
    auto part_search = get_partition_from_data_location_table_or_already_locked(
        access_pid, other_write_locked_partitions, lock_mode );

    std::shared_ptr<partition_payload> access_payload =
        std::get<0>( part_search );
    if( access_payload == nullptr ) {
        // Couldn't get the partition
        return;
    }

    auto access_location_information =
        get_location_information_for_partition_or_from_payload(
            access_payload, partition_location_informations, lock_mode );
    if( access_location_information == nullptr ) {
        // we don't have it
        unlock_if_new( part_search, other_write_locked_partitions,
                       partition_location_informations, lock_mode );
        return;
    }

    if( access_location_information->master_location_ == k_unassigned_master ) {
        // we don't have it
        unlock_if_new( part_search, other_write_locked_partitions,
                       partition_location_informations, lock_mode );
        return;
    }

    DVLOG( 40 ) << "Remove replica runnable locations info location info:"
                << *access_location_information
                << ", part of read set:" << part_of_read_set;

    uint32_t master_site = access_location_information->master_location_;
    *all_part_counts = *all_part_counts + 1;
    all_parts_site_counts[master_site] = 1 + all_parts_site_counts[master_site];
    if( !part_of_read_set ) {
        *filtered_part_counts = *filtered_part_counts + 1;
        filtered_parts_site_counts[master_site] =
            1 + filtered_parts_site_counts[master_site];
        DVLOG( 40 ) << "filtered_part_counts += 1 = " << *filtered_part_counts;
        DVLOG( 40 ) << "filtered_parts_site_counts[" << master_site
                    << "] += 1 = " << filtered_parts_site_counts[master_site];
    }

    for( auto replica_entry :
         access_location_information->replica_locations_ ) {
        // MTODO-STRATEGIES incoporate site info in cost
        uint32_t site = replica_entry;
        all_parts_site_counts[site] = 1 + all_parts_site_counts[site];
        if( part_of_read_set ) {
            DVLOG( 40 ) << "Adding read partition at site:" << site
                        << ", pid:" << access_pid
                        << ", access_location_information:"
                        << *access_location_information;
            read_partitions_at_site.at( site ).insert( access_pid );
        } else {
            filtered_parts_site_counts[site] =
                1 + filtered_parts_site_counts[site];
            DVLOG( 40 ) << "filtered_parts_site_counts[" << site
                        << "] += 1 = " << filtered_parts_site_counts[site];
        }
    }
}

void heuristic_site_evaluator::build_runnable_locations_information(
    bool part_of_read_set, std::vector<uint32_t> &all_parts_site_counts,
    uint32_t *             all_part_counts,
    std::vector<uint32_t> &filtered_parts_site_counts,
    uint32_t *             filtered_part_counts,
    std::vector<partition_column_identifier_unordered_set>
        &                              read_partitions_missing_at_site,
    const partition_column_identifier &access_pid,
    partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &other_write_locked_partitions,
    partition_column_identifier_map_t<std::shared_ptr<
        partition_location_information>> &partition_location_informations,
    uint32_t                              num_sites ) {
    const partition_lock_mode lock_mode = partition_lock_mode::no_lock;
    auto part_search = get_partition_from_data_location_table_or_already_locked(
        access_pid, other_write_locked_partitions, lock_mode );

    std::shared_ptr<partition_payload> access_payload =
        std::get<0>( part_search );
    if( access_payload == nullptr ) {
        // Couldn't get the partition
        return;
    }

    auto access_location_information =
        get_location_information_for_partition_or_from_payload(
            access_payload, partition_location_informations, lock_mode );
    if( access_location_information == nullptr ) {
        // we don't have it
        unlock_if_new( part_search, other_write_locked_partitions,
                       partition_location_informations, lock_mode );
        return;
    }

    if( access_location_information->master_location_ == k_unassigned_master ) {
        // we don't have it
        unlock_if_new( part_search, other_write_locked_partitions,
                       partition_location_informations, lock_mode );
        return;
    }

    std::vector<bool> parts_at_sites( num_sites, false );

    uint32_t master_site = access_location_information->master_location_;
    *all_part_counts = *all_part_counts + 1;
    all_parts_site_counts[master_site] = 1 + all_parts_site_counts[master_site];
    if( part_of_read_set ) {
        parts_at_sites[master_site] = true;
    } else {
        *filtered_part_counts = *filtered_part_counts + 1;
        filtered_parts_site_counts[master_site] =
            1 + filtered_parts_site_counts[master_site];
        DVLOG( 40 ) << "filtered_part_counts += 1 = " << *filtered_part_counts;
        DVLOG( 40 ) << "filtered_parts_site_counts[" << master_site
                    << "] += 1 = " << filtered_parts_site_counts[master_site];
    }

    for( auto replica_entry :
         access_location_information->replica_locations_ ) {
        // MTODO-STRATEGIES (incoporate partition type)
        uint32_t site = replica_entry;

        all_parts_site_counts[site] = 1 + all_parts_site_counts[site];
        if( part_of_read_set ) {
            parts_at_sites[site] = true;
        } else {
            filtered_parts_site_counts[site] =
                1 + filtered_parts_site_counts[site];
            DVLOG( 40 ) << "filtered_parts_site_counts[" << site
                        << "] += 1 = " << filtered_parts_site_counts[site];
        }
    }
    if( part_of_read_set ) {
        for( uint32_t site = 0; site < num_sites; site++ ) {
            if( !parts_at_sites.at( site ) ) {
                read_partitions_missing_at_site.at( site ).insert( access_pid );
            }
        }
    }
}

std::unordered_set<uint32_t> heuristic_site_evaluator::get_involved_sites(
    const partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &write_partitions_map,
    const partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &read_partitions_map,
    const partition_column_identifier_map_t<std::shared_ptr<
        partition_location_information>> &partition_location_informations,
    uint32_t                              site ) {

    start_timer( SS_GET_INVOLVED_SITES_TIMER_ID );

    std::unordered_set<uint32_t> involved_sites;
    for( int32_t site_id = 0; site_id < configs_.num_sites_; site_id++ ) {
        involved_sites.insert( (uint32_t) site_id );
    }
#if 0  // HDB-BACKGROUND

    for( const auto &part_entry : write_partitions_map ) {
        auto  part = part_entry.second;
        auto  part_location_information = get_location_information_for_partition( part, partition_location_informations );
        DCHECK( part_location_information );
        DCHECK_NE( part_location_information->master_location_, k_unassigned_master );

        // If the write partition is not mastered at destination, then that site
        // is involved.  Destination must be involved anyways
        if( part_location_information->master_location_ != (uint32_t) site ) {
            involved_sites.insert( part_location_information->master_location_ );
        }
    }

    for( const auto &part_entry : read_partitions_map ) {
        auto  part = part_entry.second;
        auto  part_location_information = get_location_information_for_partition( part, partition_location_informations );
        DCHECK( part_location_information );
        DCHECK_NE( part_location_information->master_location_, k_unassigned_master );

        // If the read partition is not present at destination, then its master
        // site is involved for spin up.
        if( ( part_location_information->master_location_ != (uint32_t) site ) and
            ( part_location_information->replica_locations_.count( site ) == 0 ) ) {
            involved_sites.insert( part_location_information->master_location_ );
        }
    }

    involved_sites.insert( site );
#endif

    stop_timer( SS_GET_INVOLVED_SITES_TIMER_ID );

    return involved_sites;
}

void heuristic_site_evaluator::add_remastering_to_plan(
    const partition_column_identifier &             pid,
    std::shared_ptr<partition_payload>              payload,
    std::shared_ptr<partition_location_information> part_location_information,
    std::shared_ptr<pre_transaction_plan> plan, int destination,
    partition_column_identifier_map_t<std::shared_ptr<pre_action>> &pid_dep_map,
    partition_column_identifier_map_t<std::unordered_map<
        uint32_t, partition_type::type>> &created_partition_types,
    partition_column_identifier_map_t<std::unordered_map<
        uint32_t, storage_tier_type::type>> &created_storage_types,
    partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &other_write_locked_partitions,
    partition_column_identifier_map_t<std::shared_ptr<
        partition_location_information>> &partition_location_informations,
    const partition_column_identifier_unordered_set
        &original_write_partition_set,
    const partition_column_identifier_unordered_set
        &                                     original_read_partition_set,
    const std::vector<site_load_information> &site_loads,
    const snapshot_vector &session, cached_ss_stat_holder &cached_stats,
    query_arrival_cached_stats &cached_query_arrival_stats ) {
    DCHECK_NE( destination, part_location_information->master_location_ );

    int master = part_location_information->master_location_;

    std::vector<partition_column_identifier> pids_to_remaster;
    std::vector<std::shared_ptr<partition_location_information>>
                                                    parts_to_remaster_locations;
    std::vector<std::shared_ptr<partition_payload>> parts_to_remaster;

    pids_to_remaster.push_back( pid );
    parts_to_remaster.push_back( payload );
    parts_to_remaster_locations.push_back( part_location_information );

    add_remasters_to_plan(
        pids_to_remaster, parts_to_remaster, parts_to_remaster_locations, plan,
        master, destination, pid_dep_map, created_partition_types,
        created_storage_types, other_write_locked_partitions,
        original_write_partition_set, original_read_partition_set,
        partition_location_informations, site_loads, session, cached_stats,
        cached_query_arrival_stats );
}

void heuristic_site_evaluator::add_remasters_to_plan(
    const std::vector<partition_column_identifier> & pids_to_remaster,
    std::vector<std::shared_ptr<partition_payload>> &parts_to_remaster,
    std::vector<std::shared_ptr<partition_location_information>>
        &                                 parts_to_remaster_infos,
    std::shared_ptr<pre_transaction_plan> plan, int master, int destination,
    partition_column_identifier_map_t<std::shared_ptr<pre_action>> &pid_dep_map,
    partition_column_identifier_map_t<std::unordered_map<
        uint32_t, partition_type::type>> &created_partition_types,
    partition_column_identifier_map_t<std::unordered_map<
        uint32_t, storage_tier_type::type>> &created_storage_types,
    partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &other_write_locked_partitions,
    const partition_column_identifier_unordered_set
        &original_write_partition_set,
    const partition_column_identifier_unordered_set
        &original_read_partition_set,
    partition_column_identifier_map_t<std::shared_ptr<
        partition_location_information>> &    partition_location_informations,
    const std::vector<site_load_information> &site_loads,
    const snapshot_vector &session, cached_ss_stat_holder &cached_stats,
    query_arrival_cached_stats &cached_query_arrival_stats ) {

    DVLOG( 40 ) << "Need to remaster:" << pids_to_remaster
                << ", to site:" << destination;

    DCHECK_EQ( pids_to_remaster.size(), parts_to_remaster_infos.size() );
    DCHECK_EQ( parts_to_remaster.size(), pids_to_remaster.size() );

    std::vector<std::shared_ptr<pre_action>> deps;
    std::unordered_set<uint32_t>             add_deps;

    std::vector<remaster_stats> stats_for_remaster;

    std::vector<uint32_t> update_destination_slots;

    snapshot_vector remaster_session;

    for( uint32_t pos = 0; pos < pids_to_remaster.size(); pos++ ) {
        const auto &pid = pids_to_remaster.at( pos );
        auto        payload = parts_to_remaster.at( pos );
        auto part_location_information = parts_to_remaster_infos.at( pos );

        DCHECK_EQ( master, part_location_information->master_location_ );

        add_to_deps( pid, pid_dep_map, deps, add_deps );

        update_destination_slots.push_back(
            get_remaster_update_destination_slot( destination, payload,
                                                  part_location_information ) );

        uint64_t master_version =
            site_partition_version_info_->get_version_of_partition(
                master, payload->identifier_ );
        if( master_version == K_NOT_COMMITTED ) {
            master_version = 0;
        }

        // wait for the current master's version
        set_snapshot_version(
            remaster_session, payload->identifier_,
            std::max( master_version,
                      get_snapshot_version( session, payload->identifier_ ) ) );

        std::vector<std::shared_ptr<partition_payload>> loc_parts_to_remaster =
            {payload};
        std::vector<std::shared_ptr<partition_location_information>>
            loc_parts_to_remaster_infos = {part_location_information};

        double num_updates_need =
            site_partition_version_info_
                ->estimate_number_of_updates_need_to_wait_for(
                    loc_parts_to_remaster, loc_parts_to_remaster_infos,
                    remaster_session, destination );

        if( num_updates_need == 0 ) {
            num_updates_need =
                cost_model2_->get_default_remaster_num_updates_required_count();
        }

        partition_type::type replica_type = get_partition_type(
            payload->identifier_, destination, part_location_information,
            created_partition_types );

        remaster_stats remaster_stat(
            std::get<1>( part_location_information->get_partition_type(
                part_location_information->master_location_ ) ),
            replica_type, num_updates_need,
            cost_model2_->normalize_contention_by_time(
                payload->get_contention() ) );

        stats_for_remaster.emplace_back( remaster_stat );
    }

    double expected_benefit = get_expected_benefit_of_remastering(
        parts_to_remaster, parts_to_remaster_infos, master, destination,
        created_partition_types, created_storage_types,
        other_write_locked_partitions, partition_location_informations,
        original_write_partition_set, original_read_partition_set, site_loads,
        cached_stats, cached_query_arrival_stats );

    cost_model_prediction_holder model_holder =
        cost_model2_->predict_remaster_execution_time(
            site_loads.at( master ).cpu_load_,
            site_loads.at( destination ).cpu_load_, stats_for_remaster );

    double cost =
        ( configs_.upfront_cost_weight_ * -model_holder.get_prediction() ) +
        ( configs_.horizon_weight_ * expected_benefit );
    auto remaster_action = create_remaster_partition_pre_action(
        master, destination, pids_to_remaster, deps, cost, model_holder,
        update_destination_slots );

    DVLOG( k_heuristic_evaluator_log_level )
        << "PLAN: Remaster partitions: (" << pids_to_remaster
        << "), master:" << master << ", new master:" << destination
        << ", overall cost:" << cost
        << " ( upfront cost:" << model_holder.get_prediction()
        << ", expected_benefit:" << expected_benefit;

    plan->add_pre_action_work_item( remaster_action );

    DVLOG( 20 ) << "Adding:" << pids_to_remaster
                << ": dep remaster action:" << remaster_action->id_;

    for( auto pid : pids_to_remaster ) {
        pid_dep_map[pid] = remaster_action;
    }
}

uint32_t heuristic_site_evaluator::get_remaster_update_destination_slot(
    int destination, std::shared_ptr<partition_payload>,
    std::shared_ptr<partition_location_information>
        part_location_information ) {
    return part_location_information->update_destination_slot_;
}

future_change_state_type compute_wr_score(
    const std::shared_ptr<partition_location_information> &part,
    const std::shared_ptr<partition_location_information> &correlated_read_part,
    double score_adjustment, int destination, bool also_reading_corr_part ) {

    future_change_state_type change_type =
        future_change_state_type::BOTH_REQUIRE_CHANGE;

    (void) also_reading_corr_part;
    double score = 0;
    std::unordered_map<uint32_t, partition_type::type> corr_replica_locations =
        correlated_read_part->partition_types_;

    bool has_corr_replica_at_src =
        ( corr_replica_locations.find( part->master_location_ ) !=
          corr_replica_locations.end() );

    bool has_corr_replica_at_dst =
        ( corr_replica_locations.find( destination ) !=
          corr_replica_locations.end() );

    if( has_corr_replica_at_src and !has_corr_replica_at_dst ) {
        // We will break the correlation!
        score -= score_adjustment;
        change_type = future_change_state_type::NOW_REQUIRE_CHANGE;
    } else if( !has_corr_replica_at_src and has_corr_replica_at_dst ) {
        // We will repair the correlation!
        score += score_adjustment;
        change_type = future_change_state_type::PREVIOUSLY_REQUIRED_CHANGE;
    } else if( has_corr_replica_at_src and has_corr_replica_at_dst ) {
        change_type = future_change_state_type::NEITHER_REQUIRE_CHANGE;
    }
    // Either both broken or both fine, no score adjument...
    // return score;
    return change_type;
}

future_change_state_type compute_ww_score(
    const std::shared_ptr<partition_location_information> &part,
    const std::shared_ptr<partition_location_information>
        &  correlated_write_part,
    double score_adjustment, int destination, bool also_writing_corr_part ) {

    future_change_state_type change_type =
        future_change_state_type::BOTH_REQUIRE_CHANGE;

    double score = 0;
    // If the partition is already at the destination, then we improve
    // correlations. (because we aren't!)
    if( correlated_write_part->master_location_ == (uint32_t) destination ) {
        score += score_adjustment;
        // return score;
        change_type = future_change_state_type::PREVIOUSLY_REQUIRED_CHANGE;
    } else if( correlated_write_part->master_location_ ==
               (uint32_t) part->master_location_ ) {
        if( also_writing_corr_part ) {
            change_type = future_change_state_type::NEITHER_REQUIRE_CHANGE;
        } else {
            // breaking
            change_type = future_change_state_type::NOW_REQUIRE_CHANGE;
            score -= score_adjustment;
        }
    }
    return change_type;
#if 0

    // If the partition is not already at the destination:
    // - If we are remastering both, and they are on separate sites,
    // improved correlations.
    bool corr_part_is_at_src =
        ( correlated_write_part->master_location_ == part->master_location_ );
    if( also_writing_corr_part ) {
        if( !corr_part_is_at_src ) {
            score += score_adjustment;
        }
        return score;
        // If we are remastering both, and they are on the same
        // site, same correlations.
    }
    // - If we aren't remastering the correlated partition, and from the
    // above it is not at the dst
    //   site, then either correlations stay the same (both are on
    //   different sites now) or worse
    //   (both are on the same site now).
    if( corr_part_is_at_src ) {
        score -= score_adjustment;
    }
    return score;
#endif
}

void heuristic_site_evaluator::add_necessary_remastering(
    int site, partition_column_identifier_map_t<
                  std::shared_ptr<partition_location_information>>
                  &partition_location_informations,
    const partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &                                  write_partitions_map,
    std::shared_ptr<pre_transaction_plan> &plan,
    partition_column_identifier_map_t<std::shared_ptr<pre_action>> &pid_dep_map,
    partition_column_identifier_map_t<std::unordered_map<
        uint32_t, partition_type::type>> &created_partition_types,
    partition_column_identifier_map_t<std::unordered_map<
        uint32_t, storage_tier_type::type>> &created_storage_types,
    partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &other_write_locked_partitions,
    const partition_column_identifier_unordered_set
        &original_write_partition_set,
    const partition_column_identifier_unordered_set
        &                                     original_read_partition_set,
    const std::vector<site_load_information> &site_loads,
    const snapshot_vector &session, cached_ss_stat_holder &cached_stats,
    query_arrival_cached_stats &cached_query_arrival_stats ) {
    start_timer( SS_PLAN_ADD_NECESSARY_REMASTERING_TIMER_ID );

    master_destination_partition_entries to_add_remasters;

    partition_column_identifier_unordered_set already_seen;

    // For each partition we wish to read/write, if a replica is not present at
    // the site, we must spin up a partition there
    for( auto part_entry : write_partitions_map ) {
        const auto &pid = part_entry.first;
        auto &      part = part_entry.second;

        if( already_seen.count( pid ) == 1 ) {
            continue;
        }

        already_seen.emplace( pid );

        auto part_location_information = get_location_information_for_partition(
            part, partition_location_informations );
        DCHECK( part_location_information );
        DCHECK_NE( part_location_information->master_location_,
                   k_unassigned_master );

        if( part_location_information->master_location_ == (uint32_t) site ) {
            continue;
        }

        int dep_version = -1;

        auto pid_dep_search = pid_dep_map.find( pid );
        if( pid_dep_search != pid_dep_map.end() ) {
            dep_version = pid_dep_search->second->id_;
        }

        master_destination_dependency_version master_dest(
            part_location_information->master_location_, site, dep_version );
        add_to_partition_operation_tracking( to_add_remasters, master_dest, pid,
                                             part, part_location_information );
    }

    for( auto &add_remaster_entry : to_add_remasters ) {
        const auto &master_dest = add_remaster_entry.first;
        auto &      parts = add_remaster_entry.second;
        add_remasters_to_plan(
            parts.pids_, parts.payloads_, parts.location_infos_, plan,
            master_dest.master_, master_dest.destination_, pid_dep_map,
            created_partition_types, created_storage_types,
            other_write_locked_partitions, original_write_partition_set,
            original_read_partition_set, partition_location_informations,
            site_loads, session, cached_stats, cached_query_arrival_stats );
    }

    stop_timer( SS_PLAN_ADD_NECESSARY_REMASTERING_TIMER_ID );
}

void heuristic_site_evaluator::change_partition_types(
    int site, partition_column_identifier_map_t<
                  std::shared_ptr<partition_location_information>>
                  &partition_location_informations,
    const partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &write_partitions_map,
    const partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &                                  read_partitions_map,
    std::shared_ptr<pre_transaction_plan> &plan,
    partition_column_identifier_map_t<std::shared_ptr<pre_action>> &pid_dep_map,
    partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &other_write_locked_partitions,
    partition_column_identifier_map_t<std::unordered_map<
        uint32_t, partition_type::type>> &created_partition_types,
    partition_column_identifier_map_t<std::unordered_map<
        uint32_t, storage_tier_type::type>> &created_storage_types,
    const partition_column_identifier_unordered_set
        &original_write_partition_set,
    const partition_column_identifier_unordered_set
        &                                     original_read_partition_set,
    const std::vector<site_load_information> &site_loads,
    std::unordered_map<storage_tier_type::type, std::vector<double>>
        &storage_sizes,
    const partition_column_identifier_map_t<
        std::vector<transaction_partition_accesses>> &write_sample_holder,
    const partition_column_identifier_map_t<
        std::vector<transaction_partition_accesses>> &read_sample_holder,
    cached_ss_stat_holder &                           cached_stats,
    query_arrival_cached_stats &cached_query_arrival_stats ) {

    if( !configs_.allow_change_partition_types_ and
        !configs_.allow_change_storage_types_ ) {
        return;
    }

    master_destination_partition_entries      to_change_types;
    partition_column_identifier_unordered_set already_seen;

    change_partition_types_as_needed(
        site, partition_location_informations, write_partitions_map, plan,
        pid_dep_map, created_partition_types, created_storage_types,
        other_write_locked_partitions, to_change_types,
        original_write_partition_set, original_read_partition_set, site_loads,
        storage_sizes, write_sample_holder, read_sample_holder, already_seen,
        cached_stats, cached_query_arrival_stats, true /* is write */ );
    change_partition_types_as_needed(
        site, partition_location_informations, read_partitions_map, plan,
        pid_dep_map, created_partition_types, created_storage_types,
        other_write_locked_partitions, to_change_types,
        original_write_partition_set, original_read_partition_set, site_loads,
        storage_sizes, write_sample_holder, read_sample_holder, already_seen,
        cached_stats, cached_query_arrival_stats, false /* is read */ );

    for( auto &change_type_entry : to_change_types ) {
        const auto &master_dest = change_type_entry.first;
        auto &      parts = change_type_entry.second;
        add_change_types_to_plan(
            parts.pids_, parts.payloads_, parts.location_infos_,
            parts.partition_types_, parts.storage_types_, plan,
            master_dest.destination_, pid_dep_map,
            other_write_locked_partitions, original_write_partition_set,
            original_read_partition_set, partition_location_informations,
            site_loads, write_sample_holder, read_sample_holder, cached_stats,
            cached_query_arrival_stats );
    }
}

void heuristic_site_evaluator::add_change_type_to_plan(
    const partition_column_identifier &             pid,
    std::shared_ptr<partition_payload>              payload,
    std::shared_ptr<partition_location_information> part_location_information,
    const partition_type::type &p_type, const storage_tier_type::type &s_type,
    std::shared_ptr<pre_transaction_plan> plan, int destination,
    partition_column_identifier_map_t<std::shared_ptr<pre_action>> &pid_dep_map,
    partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &other_write_locked_partitions,
    const partition_column_identifier_unordered_set
        &original_write_partition_set,
    const partition_column_identifier_unordered_set
        &original_read_partition_set,
    partition_column_identifier_map_t<std::shared_ptr<
        partition_location_information>> &    partition_location_informations,
    const std::vector<site_load_information> &site_loads,
    const partition_column_identifier_map_t<
        std::vector<transaction_partition_accesses>> &write_sample_holder,
    const partition_column_identifier_map_t<
        std::vector<transaction_partition_accesses>> &read_sample_holder,
    cached_ss_stat_holder &                           cached_stats,
    query_arrival_cached_stats &cached_query_arrival_stats ) {

    std::vector<partition_column_identifier>        pids_to_change;
    std::vector<std::shared_ptr<partition_payload>> parts_to_change;
    std::vector<std::shared_ptr<partition_location_information>>
                                         parts_to_change_locations;
    std::vector<partition_type::type>    partition_types;
    std::vector<storage_tier_type::type> storage_types;

    pids_to_change.push_back( pid );
    parts_to_change.push_back( payload );
    parts_to_change_locations.push_back( part_location_information );
    partition_types.push_back( p_type );
    storage_types.push_back( s_type );

    DCHECK( ( 1 ==
              part_location_information->replica_locations_.count(
                  destination ) ) or
            ( (uint32_t) destination ==
              part_location_information->master_location_ ) );

    add_change_types_to_plan(
        pids_to_change, parts_to_change, parts_to_change_locations,
        partition_types, storage_types, plan, destination, pid_dep_map,
        other_write_locked_partitions, original_write_partition_set,
        original_read_partition_set, partition_location_informations,
        site_loads, write_sample_holder, read_sample_holder, cached_stats,
        cached_query_arrival_stats );
}

void heuristic_site_evaluator::add_change_types_to_plan(
    const std::vector<partition_column_identifier> & pids_to_change,
    std::vector<std::shared_ptr<partition_payload>> &parts_to_change,
    std::vector<std::shared_ptr<partition_location_information>>
        &                                       parts_to_change_infos,
    const std::vector<partition_type::type> &   types_to_change,
    const std::vector<storage_tier_type::type> &storage_to_change,
    std::shared_ptr<pre_transaction_plan> plan, int destination,
    partition_column_identifier_map_t<std::shared_ptr<pre_action>> &pid_dep_map,
    partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &other_write_locked_partitions,
    const partition_column_identifier_unordered_set
        &original_write_partition_set,
    const partition_column_identifier_unordered_set
        &original_read_partition_set,
    partition_column_identifier_map_t<std::shared_ptr<
        partition_location_information>> &    partition_location_informations,
    const std::vector<site_load_information> &site_loads,
    const partition_column_identifier_map_t<
        std::vector<transaction_partition_accesses>> &write_sample_holder,
    const partition_column_identifier_map_t<
        std::vector<transaction_partition_accesses>> &read_sample_holder,
    cached_ss_stat_holder &                           cached_stats,
    query_arrival_cached_stats &cached_query_arrival_stats ) {

    DVLOG( 40 ) << "Need to change partition types of:" << pids_to_change
                << ", at site:" << destination;

    DCHECK_EQ( pids_to_change.size(), parts_to_change_infos.size() );
    DCHECK_EQ( parts_to_change.size(), pids_to_change.size() );
    DCHECK_EQ( types_to_change.size(), pids_to_change.size() );
    DCHECK_EQ( storage_to_change.size(), pids_to_change.size() );

    std::vector<std::shared_ptr<pre_action>> deps;
    std::unordered_set<uint32_t>             add_deps;

    std::vector<partition_type::type>    old_types;
    std::vector<storage_tier_type::type> old_storages;

    std::vector<change_types_stats> change_stats;

    for( uint32_t pos = 0; pos < pids_to_change.size(); pos++ ) {
        const auto &pid = pids_to_change.at( pos );
        auto        payload = parts_to_change.at( pos );
        auto        part_location_information = parts_to_change_infos.at( pos );

        DCHECK( ( 1 ==
                  part_location_information->replica_locations_.count(
                      destination ) ) or
                ( (uint32_t) destination ==
                  part_location_information->master_location_ ) );

        partition_type::type    new_type = types_to_change.at( pos );
        storage_tier_type::type new_storage = storage_to_change.at( pos );
        auto                    found_old_type =
            part_location_information->get_partition_type( destination );
        DCHECK( std::get<0>( found_old_type ) );
        partition_type::type old_type = std::get<1>( found_old_type );

        auto found_old_storage =
            part_location_information->get_storage_type( destination );
        DCHECK( std::get<0>( found_old_storage ) );
        storage_tier_type::type old_storage = std::get<1>( found_old_storage );

        old_types.emplace_back( old_type );
        old_storages.emplace_back( old_storage );

        add_to_deps( pid, pid_dep_map, deps, add_deps );

        change_types_stats change_stat(
            old_type, new_type, old_storage, new_storage,
            cost_model2_->normalize_contention_by_time(
                payload->get_contention() ),
            query_stats_->get_and_cache_cell_widths( pid, cached_stats ),
            payload->get_num_rows() );

        change_stats.emplace_back( change_stat );
    }

    cost_model_prediction_holder model_holder =
        cost_model2_->predict_changing_type_execution_time(
            site_loads.at( destination ).cpu_load_, change_stats );

    double expected_benefit = get_expected_benefit_of_changing_types(
        parts_to_change, parts_to_change_infos, types_to_change,
        storage_to_change, old_types, old_storages, change_stats, destination,
        other_write_locked_partitions, partition_location_informations,
        original_write_partition_set, original_read_partition_set, site_loads,
        write_sample_holder, read_sample_holder, cached_stats,
        cached_query_arrival_stats );

    double cost = -model_holder.get_prediction() +
                  ( configs_.horizon_weight_ * expected_benefit );

    auto change_plan = create_change_partition_type_action(
        destination, pids_to_change, types_to_change, storage_to_change, cost,
        model_holder, deps );
    plan->add_pre_action_work_item( change_plan );

    DVLOG( 20 ) << "Change partition types " << pids_to_change
                << ": dep change action:" << change_plan->id_;

    DVLOG( k_heuristic_evaluator_log_level )
        << "PLAN: Change partition types: (" << pids_to_change
        << "), site:" << destination << ", overall cost:" << cost
        << " ( upfront cost:" << model_holder.get_prediction()
        << ", expected_benefit:" << expected_benefit << " )";

    for( auto pid : pids_to_change ) {
        pid_dep_map[pid] = change_plan;
    }
}

double heuristic_site_evaluator::get_expected_benefit_of_changing_types(
    std::vector<std::shared_ptr<partition_payload>> &parts_to_change,
    const std::vector<std::shared_ptr<partition_location_information>>
        &                                       parts_to_change_locations,
    const std::vector<partition_type::type> &   types_to_change,
    const std::vector<storage_tier_type::type> &storage_to_change,
    const std::vector<partition_type::type> &   old_types,
    const std::vector<storage_tier_type::type> &old_storage,
    const std::vector<change_types_stats> &change_stats, int destination,
    partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &other_write_locked_partitions,
    partition_column_identifier_map_t<std::shared_ptr<
        partition_location_information>> &partition_location_informations,
    const partition_column_identifier_unordered_set
        &original_write_partition_set,
    const partition_column_identifier_unordered_set
        &                                     original_read_partition_set,
    const std::vector<site_load_information> &site_loads,
    const partition_column_identifier_map_t<
        std::vector<transaction_partition_accesses>> &write_sample_holder,
    const partition_column_identifier_map_t<
        std::vector<transaction_partition_accesses>> &read_sample_holder,
    cached_ss_stat_holder &                           cached_stats,
    query_arrival_cached_stats &cached_query_arrival_stats ) {

    double default_benefit = 0;  // there is none

    DCHECK_EQ( parts_to_change.size(), parts_to_change_locations.size() );
    DCHECK_EQ( parts_to_change.size(), types_to_change.size() );
    DCHECK_EQ( parts_to_change.size(), old_types.size() );
    DCHECK_EQ( parts_to_change.size(), storage_to_change.size() );
    DCHECK_EQ( parts_to_change.size(), old_storage.size() );
    DCHECK_EQ( parts_to_change.size(), change_stats.size() );

    double sample_benefit = 0;
    for( uint32_t pos = 0; pos < parts_to_change.size(); pos++ ) {
        sample_benefit += get_sample_benefit_of_changing_types(
            parts_to_change.at( pos ), parts_to_change_locations.at( pos ),
            change_stats.at( pos ), destination, other_write_locked_partitions,
            partition_location_informations, original_write_partition_set,
            original_read_partition_set, site_loads, write_sample_holder,
            read_sample_holder, cached_stats, cached_query_arrival_stats );
    }

    double benefit =
        sample_benefit +
        ( default_benefit * configs_.default_horizon_benefit_weight_ );

    DVLOG( 40 ) << "Get expected benefit of adding replicas: default benefit:"
                << default_benefit << ", sample benefit:" << sample_benefit
                << ", benefit:" << benefit;

    return benefit;
}

double heuristic_site_evaluator::get_sample_benefit_of_changing_types(
    const std::shared_ptr<partition_payload> &part_to_change,
    const std::shared_ptr<partition_location_information>
        &                     part_to_change_location,
    const change_types_stats &change_stat, int destination,
    partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &other_write_locked_partitions,
    partition_column_identifier_map_t<std::shared_ptr<
        partition_location_information>> &partition_location_informations,
    const partition_column_identifier_unordered_set
        &original_write_partition_set,
    const partition_column_identifier_unordered_set
        &                                     original_read_partition_set,
    const std::vector<site_load_information> &site_loads,
    const partition_column_identifier_map_t<
        std::vector<transaction_partition_accesses>> &write_sample_holder,
    const partition_column_identifier_map_t<
        std::vector<transaction_partition_accesses>> &read_sample_holder,
    cached_ss_stat_holder &                           cached_stats,
    query_arrival_cached_stats &cached_query_arrival_stats ) {

    auto write_scores = get_sample_score_of_changing_types(
        part_to_change, change_stat, destination, site_loads,
        write_sample_holder, cached_stats, cached_query_arrival_stats );
    auto read_scores = get_sample_score_of_changing_types(
        part_to_change, change_stat, destination, site_loads,
        read_sample_holder, cached_stats, cached_query_arrival_stats );

    double prev_write_score = std::get<0>( write_scores );
    double prev_read_score = std::get<0>( read_scores );
    double new_write_score = std::get<1>( write_scores );
    double new_read_score = std::get<1>( read_scores );

    double prev_score = prev_write_score + prev_read_score;
    double new_score = new_write_score + new_read_score;

    // compute over each sample
    double score = prev_score - new_score;
    return score;
}
std::tuple<double, double>
    heuristic_site_evaluator::get_sample_score_of_changing_types(
        const std::shared_ptr<partition_payload> &part_to_change,
        const change_types_stats &change_stat, int destination,
        const std::vector<site_load_information> &site_loads,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &sample_holder,
        cached_ss_stat_holder &                           cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats ) {

    auto   pid = part_to_change->identifier_;
    double scan_selectivity =
        data_loc_tab_->get_stats()->get_and_cache_average_scan_selectivity(
            pid, cached_stats );

    double num_updates_needed = 0;

    double prev_score = 0;
    double new_score = 0;

    double site_load = 0;
    double site_load_before = site_load;
    double site_load_after = site_load;

    auto found = sample_holder.find( pid );
    if( found == sample_holder.end() ) {
        return std::make_tuple<>( prev_score, new_score );
    }
    for( const auto txn_acesses : found->second ) {
        std::vector<transaction_prediction_stats> txn_stats_before;
        std::vector<transaction_prediction_stats> txn_stats_after;

        double arrival_prob = 0;

        for( const auto &ckr : txn_acesses.write_cids_ ) {
            if( is_ckr_partially_within_pcid( ckr, pid ) ) {
                auto     shaped_ckr = shape_ckr_to_pcid( ckr, pid );
                uint32_t num_updates = get_number_of_cells( shaped_ckr );

                arrival_prob = std::max(
                    arrival_prob, get_upcoming_query_arrival_score(
                                      pid, true, cached_query_arrival_stats ) );

                transaction_prediction_stats before_stats(
                    change_stat.ori_type_, change_stat.ori_storage_type_,
                    change_stat.contention_, num_updates_needed,
                    change_stat.avg_cell_widths_, change_stat.num_entries_,
                    false, 0, false, 0, true, num_updates );
                transaction_prediction_stats after_stats(
                    change_stat.new_type_, change_stat.new_storage_type_,
                    change_stat.contention_, num_updates_needed,
                    change_stat.avg_cell_widths_, change_stat.num_entries_,
                    false, 0, false, 0, true, num_updates );

                txn_stats_before.emplace_back( before_stats );
                txn_stats_after.emplace_back( after_stats );
            }
        }
        for( const auto &ckr : txn_acesses.read_cids_ ) {
            if( is_ckr_partially_within_pcid( ckr, pid ) ) {
                auto     shaped_ckr = shape_ckr_to_pcid( ckr, pid );
                uint32_t num_reads = get_number_of_cells( shaped_ckr );

                arrival_prob = std::max(
                    arrival_prob, get_upcoming_query_arrival_score(
                                      pid, true, cached_query_arrival_stats ) );

                transaction_prediction_stats before_stats(
                    change_stat.ori_type_, change_stat.ori_storage_type_,
                    change_stat.contention_, num_updates_needed,
                    change_stat.avg_cell_widths_, change_stat.num_entries_,
                    true, scan_selectivity, true, num_reads, false, 0 );
                transaction_prediction_stats after_stats(
                    change_stat.new_type_, change_stat.new_storage_type_,
                    change_stat.contention_, num_updates_needed,
                    change_stat.avg_cell_widths_, change_stat.num_entries_,
                    true, scan_selectivity, true, num_reads, false, 0 );

                txn_stats_before.emplace_back( before_stats );
                txn_stats_after.emplace_back( after_stats );
            }
        }

        double prev = cost_model2_
                          ->predict_single_site_transaction_execution_time(
                              site_load_before, txn_stats_before )
                          .get_prediction() *
                      arrival_prob;
        double after = cost_model2_
                           ->predict_single_site_transaction_execution_time(
                               site_load_after, txn_stats_after )
                           .get_prediction() *
                       arrival_prob;

        prev_score = prev_score + prev;
        new_score = new_score + after;
    }

    return std::make_tuple<>( prev_score, new_score );
}

void heuristic_site_evaluator::change_partition_types_as_needed(
    int site, partition_column_identifier_map_t<
                  std::shared_ptr<partition_location_information>>
                  &partition_location_informations,
    const partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &                                  partitions_map,
    std::shared_ptr<pre_transaction_plan> &plan,
    partition_column_identifier_map_t<std::shared_ptr<pre_action>> &pid_dep_map,
    partition_column_identifier_map_t<std::unordered_map<
        uint32_t, partition_type::type>> &created_partition_types,
    partition_column_identifier_map_t<std::unordered_map<
        uint32_t, storage_tier_type::type>> &created_storage_types,
    partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &                                 other_write_locked_partitions,
    master_destination_partition_entries &to_add_replicas,
    const partition_column_identifier_unordered_set
        &original_write_partition_set,
    const partition_column_identifier_unordered_set
        &                                     original_read_partition_set,
    const std::vector<site_load_information> &site_loads,
    std::unordered_map<storage_tier_type::type, std::vector<double>>
        &storage_sizes,
    const partition_column_identifier_map_t<
        std::vector<transaction_partition_accesses>> &write_sample_holder,
    const partition_column_identifier_map_t<
        std::vector<transaction_partition_accesses>> &read_sample_holder,
    partition_column_identifier_unordered_set &       already_seen,
    cached_ss_stat_holder &                           cached_stats,
    query_arrival_cached_stats &cached_query_arrival_stats, bool is_write ) {

    start_timer( SS_PLAN_CHANGE_PARTITION_TYPES_TIMER_ID );

    // MTODO-STRATEGIES do we need loads?
    double num_updates_needed = 0;
    double selectivity = 0;
    double num_ops = 1;

    // For each partition we wish to read/write, if a replica is not present at
    // the site, we must spin up a partition there
    for( auto part_entry : partitions_map ) {
        const auto &pid = part_entry.first;
        auto &      part = part_entry.second;

        DVLOG( 40 ) << "Consider changing type for partition:" << pid;

        if( already_seen.count( pid ) == 1 ) {
            continue;
        }
        already_seen.emplace( pid );

        auto part_location_information = get_location_information_for_partition(
            part, partition_location_informations );
        DCHECK( part_location_information );
        uint32_t master = part_location_information->master_location_;
        DCHECK_NE( master, k_unassigned_master );

        DVLOG( 40 ) << "Part location info:" << *part_location_information;

        uint32_t destination = site;

        partition_type::type    cur_type = partition_type::type::ROW;
        storage_tier_type::type store_type = storage_tier_type::type::MEMORY;

        transaction_prediction_stats txn_stat(
            cur_type, store_type, cost_model2_->normalize_contention_by_time(
                                      part->get_contention() ),
            num_updates_needed, query_stats_->get_and_cache_cell_widths(
                                    part->identifier_, cached_stats ),
            part->get_num_rows(), false /*is scan */, selectivity, !is_write,
            num_ops, is_write, num_ops );

        if( ( destination != master ) and
            ( part_location_information->replica_locations_.count(
                  destination ) == 0 ) ) {

            cur_type = created_partition_types[pid][site];
            store_type = created_storage_types[pid][site];
            txn_stat.part_type_ = cur_type;
            txn_stat.storage_type_ = store_type;

            plan->txn_stats_.emplace_back( txn_stat );

            // we have added a replica of the correct type so nothing to do
            // here
            continue;
        }

        auto found_cur_type =
            part_location_information->get_partition_type( destination );
        DCHECK( std::get<0>( found_cur_type ) );
        cur_type = std::get<1>( found_cur_type );
        auto found_storage_type =
            part_location_information->get_storage_type( destination );
        DCHECK( std::get<0>( found_storage_type ) );
        store_type = std::get<1>( found_storage_type );

        auto new_types = get_change_type(
            part, part_location_information, cur_type, store_type, site, plan,
            other_write_locked_partitions, partition_location_informations,
            original_write_partition_set, original_read_partition_set,
            site_loads, storage_sizes, write_sample_holder, read_sample_holder,
            cached_stats, cached_query_arrival_stats, is_write );
        partition_type::type    new_type = std::get<0>( ( new_types ) );
        storage_tier_type::type new_storage_type = std::get<1>( ( new_types ) );

        txn_stat.part_type_ = new_type;
        txn_stat.storage_type_ = new_storage_type;
        plan->txn_stats_.emplace_back( txn_stat );

        if( ( new_type == cur_type ) and ( new_storage_type == store_type ) ) {
            continue;
        }

        created_partition_types[part->identifier_][destination] = new_type;
        created_storage_types[part->identifier_][destination] =
            new_storage_type;

        // update the storage sizes
        double part_storage_size =
            storage_stats_->get_partition_size( part->identifier_ );
        storage_sizes.at( new_storage_type ).at( destination ) +=
            part_storage_size;
        storage_sizes.at( store_type ).at( destination ) -= part_storage_size;

        auto pid_dep_search = pid_dep_map.find( pid );
        if( pid_dep_search != pid_dep_map.end() ) {

            // Need to create one
            add_change_type_to_plan(
                pid, part, part_location_information, new_type,
                new_storage_type, plan, site, pid_dep_map,
                other_write_locked_partitions, original_write_partition_set,
                original_read_partition_set, partition_location_informations,
                site_loads, write_sample_holder, read_sample_holder,
                cached_stats, cached_query_arrival_stats );
        } else {
            master_destination_dependency_version master_dest( site, site, 0 );
            add_to_partition_operation_tracking(
                to_add_replicas, master_dest, pid, part,
                part_location_information, new_type, new_storage_type );
        }
    }

    stop_timer( SS_PLAN_CHANGE_PARTITION_TYPES_TIMER_ID );
}

std::tuple<partition_type::type, storage_tier_type::type>
    heuristic_site_evaluator::get_change_type(
        const std::shared_ptr<partition_payload> &part,
        const std::shared_ptr<partition_location_information>
            &                          part_location_info,
        const partition_type::type &   cur_type,
        const storage_tier_type::type &storage_type, uint32_t site,
        std::shared_ptr<pre_transaction_plan> &plan,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &other_write_locked_partitions,
        partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>> &partition_location_informations,
        const partition_column_identifier_unordered_set
            &original_write_partition_set,
        const partition_column_identifier_unordered_set
            &                                     original_read_partition_set,
        const std::vector<site_load_information> &site_loads,
        std::unordered_map<storage_tier_type::type, std::vector<double>>
            &storage_sizes,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &write_sample_holder,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &read_sample_holder,
        cached_ss_stat_holder &                           cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats,
        bool                        is_write ) {
    partition_type::type    found_type = cur_type;
    storage_tier_type::type found_storage_type = storage_type;
    double                  max_cost = -DBL_MAX;
    auto                    pid = part->identifier_;

    std::vector<std::shared_ptr<partition_payload>> parts_to_change = {part};
    std::vector<std::shared_ptr<partition_location_information>>
                                         parts_to_change_locations = {part_location_info};
    std::vector<partition_type::type>    types_to_change = {found_type};
    std::vector<partition_type::type>    old_types = {cur_type};
    std::vector<storage_tier_type::type> storage_types_to_change = {
        found_storage_type};
    std::vector<storage_tier_type::type> old_storage_types = {storage_type};

    double contention =
        cost_model2_->normalize_contention_by_time( part->get_contention() );

    std::vector<change_types_stats> changes;
    changes.push_back( change_types_stats(
        cur_type, found_type, storage_type, found_storage_type, contention,
        query_stats_->get_and_cache_cell_widths( pid, cached_stats ),
        get_number_of_rows( pid ) ) );

    auto considered_types =
        enumerator_holder_->get_change_types( changes.at( 0 ) );

    double partition_size = storage_stats_->get_partition_size( pid );
    double ori_storage_cost = get_site_storage_imbalance_cost( storage_sizes );

    for( const auto &try_change_type : considered_types ) {
        const auto &try_type = std::get<0>( try_change_type );
        const auto &try_storage_type = std::get<1>( try_change_type );
        changes.at( 0 ).new_type_ = try_type;
        changes.at( 0 ).new_storage_type_ = try_storage_type;
        types_to_change.at( 0 ) = try_type;
        storage_types_to_change.at( 0 ) = try_storage_type;
        double upfront_cost = 0;
        double expected_benefit = 0;
        double storage_benefit = 0;
        if( ( try_type != cur_type ) or ( try_storage_type != storage_type ) ) {

            if( !configs_.allow_change_partition_types_ and
                ( try_type != cur_type ) ) {
                continue;
            }
            if( !configs_.allow_change_storage_types_ and
                ( try_storage_type != storage_type ) ) {
                continue;
            }

            if( try_storage_type != storage_type ) {
                storage_sizes.at( storage_type ).at( site ) -= partition_size;
                storage_sizes.at( try_storage_type ).at( site ) +=
                    partition_size;
            }

            cost_model_prediction_holder prediction =
                cost_model2_->predict_changing_type_execution_time(
                    site_loads.at( site ).cpu_load_, changes );
            upfront_cost = prediction.get_prediction();
            expected_benefit = get_expected_benefit_of_changing_types(
                parts_to_change, parts_to_change_locations, types_to_change,
                storage_types_to_change, old_types, old_storage_types, changes,
                site, other_write_locked_partitions,
                partition_location_informations, original_write_partition_set,
                original_read_partition_set, site_loads, write_sample_holder,
                read_sample_holder, cached_stats, cached_query_arrival_stats );

            storage_benefit = get_change_in_storage_imbalance_costs(
                get_site_storage_imbalance_cost( storage_sizes ),
                ori_storage_cost );

            // undo the change
            if( try_storage_type != storage_type ) {
                storage_sizes.at( storage_type ).at( site ) += partition_size;
                storage_sizes.at( try_storage_type ).at( site ) -=
                    partition_size;
            }
        }
        double cost = ( configs_.upfront_cost_weight_ * upfront_cost ) +
                      ( configs_.horizon_weight_ * expected_benefit ) +
                      ( configs_.horizon_storage_weight_ * storage_benefit );

        if( cost > max_cost ) {
            max_cost = cost;
            found_type = try_type;
            found_storage_type = try_storage_type;
        }
    }

    plan->stat_decision_holder_->add_change_types_decision(
        changes.at( 0 ), std::make_tuple<>( found_type, found_storage_type ) );

    return std::make_tuple<>( found_type, found_storage_type );
}

void heuristic_site_evaluator::add_all_replicas(
    int site, partition_column_identifier_map_t<
                  std::shared_ptr<partition_location_information>>
                  &partition_location_informations,
    const partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &write_partitions_map,
    const partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &                                            read_partitions_map,
    std::shared_ptr<pre_transaction_plan> &          plan,
    std::vector<std::shared_ptr<partition_payload>> &write_part_payloads,
    std::vector<std::shared_ptr<partition_payload>> &read_part_payloads,
    partition_column_identifier_map_t<std::shared_ptr<pre_action>> &pid_dep_map,
    partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &other_write_locked_partitions,
    partition_column_identifier_map_t<std::unordered_map<
        uint32_t, partition_type::type>> &created_partition_types,
    partition_column_identifier_map_t<std::unordered_map<
        uint32_t, storage_tier_type::type>> &created_storage_types,
    const partition_column_identifier_unordered_set
        &original_write_partition_set,
    const partition_column_identifier_unordered_set
        &                                     original_read_partition_set,
    const std::vector<site_load_information> &site_loads,
    std::unordered_map<storage_tier_type::type, std::vector<double>>
        &storage_sizes,
    const partition_column_identifier_map_t<
        std::vector<transaction_partition_accesses>> &write_sample_holder,
    const partition_column_identifier_map_t<
        std::vector<transaction_partition_accesses>> &read_sample_holder,
    cached_ss_stat_holder &                           cached_stats,
    query_arrival_cached_stats &cached_query_arrival_stats ) {

    master_destination_partition_entries      to_add_replicas;
    partition_column_identifier_unordered_set already_seen;

    DVLOG( 40 ) << "Add necessary Replicas site:" << site << ", WRITE!";

    add_necessary_replicas(
        site, partition_location_informations, write_partitions_map, plan,
        plan->write_pids_, write_part_payloads, pid_dep_map,
        created_partition_types, created_storage_types,
        other_write_locked_partitions, to_add_replicas,
        original_write_partition_set, original_read_partition_set, site_loads,
        storage_sizes, write_sample_holder, read_sample_holder, already_seen,
        cached_stats, cached_query_arrival_stats );

    DVLOG( 40 ) << "Add necessary Replicas site:" << site << ", READ!";

    add_necessary_replicas(
        site, partition_location_informations, read_partitions_map, plan,
        plan->read_pids_, read_part_payloads, pid_dep_map,
        created_partition_types, created_storage_types,
        other_write_locked_partitions, to_add_replicas,
        original_write_partition_set, original_read_partition_set, site_loads,
        storage_sizes, write_sample_holder, read_sample_holder, already_seen,
        cached_stats, cached_query_arrival_stats );

    DVLOG( 40 ) << "Add necessary Replicas site:" << site
                << ", write_partitions_set:" << plan->write_pids_
                << ", read_partitions_set:" << plan->read_pids_;

    for( auto &add_replica_entry : to_add_replicas ) {
        const auto &master_dest = add_replica_entry.first;
        auto &      parts = add_replica_entry.second;
        add_replicas_to_plan(
            parts.pids_, parts.payloads_, parts.location_infos_,
            parts.partition_types_, parts.storage_types_, plan,
            master_dest.master_, master_dest.destination_, pid_dep_map,
            other_write_locked_partitions, original_write_partition_set,
            original_read_partition_set, partition_location_informations,
            site_loads, write_sample_holder, read_sample_holder, cached_stats,
            cached_query_arrival_stats );
    }
}

void heuristic_site_evaluator::add_necessary_replicas(
    int site, partition_column_identifier_map_t<
                  std::shared_ptr<partition_location_information>>
                  &partition_location_informations,
    const partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &                                            partitions_map,
    std::shared_ptr<pre_transaction_plan> &          plan,
    std::vector<partition_column_identifier> &       plan_pids,
    std::vector<std::shared_ptr<partition_payload>> &part_list,
    partition_column_identifier_map_t<std::shared_ptr<pre_action>> &pid_dep_map,
    partition_column_identifier_map_t<std::unordered_map<
        uint32_t, partition_type::type>> &created_partition_types,
    partition_column_identifier_map_t<std::unordered_map<
        uint32_t, storage_tier_type::type>> &created_storage_types,
    partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &                                 other_write_locked_partitions,
    master_destination_partition_entries &to_add_replicas,
    const partition_column_identifier_unordered_set
        &original_write_partition_set,
    const partition_column_identifier_unordered_set
        &                                     original_read_partition_set,
    const std::vector<site_load_information> &site_loads,
    std::unordered_map<storage_tier_type::type, std::vector<double>>
        &storage_sizes,

    const partition_column_identifier_map_t<
        std::vector<transaction_partition_accesses>> &write_sample_holder,
    const partition_column_identifier_map_t<
        std::vector<transaction_partition_accesses>> &read_sample_holder,
    partition_column_identifier_unordered_set &       already_seen,
    cached_ss_stat_holder &                           cached_stats,
    query_arrival_cached_stats &cached_query_arrival_stats ) {
    start_timer( SS_PLAN_ADD_NECESSARY_REPLICAS_TIMER_ID );

    // For each partition we wish to read/write, if a replica is not present at
    // the site, we must spin up a partition there
    for( auto part_entry : partitions_map ) {

        const auto &pid = part_entry.first;
        auto &      part = part_entry.second;

        DVLOG( 40 ) << "Add neccessary replicas:" << pid;

        DVLOG( 40 ) << "Consider adding necessary replicas for partition:"
                    << pid;

        plan_pids.push_back( pid );
        part_list.push_back( part );

        if( already_seen.count( pid ) == 1 ) {
            continue;
        }
        already_seen.emplace( pid );

        auto part_location_information = get_location_information_for_partition(
            part, partition_location_informations );
        DCHECK( part_location_information );
        uint32_t master = part_location_information->master_location_;
        DCHECK_NE( master, k_unassigned_master );

        DVLOG( 40 ) << "Part location info:" << *part_location_information;

        if( ( master == (uint32_t) site ) or
            ( part_location_information->replica_locations_.count( site ) ==
              1 ) ) {
            continue;
        }
        auto found_types = get_replica_type(
            part, part_location_information, master, site, plan,
            other_write_locked_partitions, partition_location_informations,
            original_write_partition_set, original_read_partition_set,
            site_loads, storage_sizes, write_sample_holder, read_sample_holder,
            cached_stats, cached_query_arrival_stats );
        partition_type::type    p_type = std::get<0>( found_types );
        storage_tier_type::type s_type = std::get<1>( found_types );
        created_partition_types[part->identifier_][site] = p_type;
        created_storage_types[part->identifier_][site] = s_type;

        // update storage_sizes
        auto part_storage_size =
            storage_stats_->get_partition_size( part->identifier_ );
        storage_sizes.at( s_type ).at( site ) += part_storage_size;

        auto pid_dep_search = pid_dep_map.find( pid );
        if( pid_dep_search != pid_dep_map.end() ) {

            // Need to create one
            add_replica_to_plan(
                pid, part, part_location_information, p_type, s_type, plan,
                site, pid_dep_map, other_write_locked_partitions,
                original_write_partition_set, original_read_partition_set,
                partition_location_informations, site_loads,
                write_sample_holder, read_sample_holder, cached_stats,
                cached_query_arrival_stats );
        } else {
            master_destination_dependency_version master_dest( master, site,
                                                               0 );
            add_to_partition_operation_tracking(
                to_add_replicas, master_dest, pid, part,
                part_location_information, p_type, s_type );
        }
    }

    stop_timer( SS_PLAN_ADD_NECESSARY_REPLICAS_TIMER_ID );
}

std::tuple<partition_type::type, storage_tier_type::type>
    heuristic_site_evaluator::get_replica_type(
        const std::shared_ptr<partition_payload> &part,
        const std::shared_ptr<partition_location_information>
            &    part_location_info,
        uint32_t master, uint32_t site,
        std::shared_ptr<pre_transaction_plan> plan,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &other_write_locked_partitions,
        partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>> &partition_location_informations,
        const partition_column_identifier_unordered_set
            &original_write_partition_set,
        const partition_column_identifier_unordered_set
            &                                     original_read_partition_set,
        const std::vector<site_load_information> &site_loads,
        std::unordered_map<storage_tier_type::type, std::vector<double>>
            &storage_sizes,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &write_sample_holder,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &read_sample_holder,
        cached_ss_stat_holder &                           cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats ) {
    partition_type::type found_part_type =
        configs_.acceptable_partition_types_.at( 0 );
    storage_tier_type::type found_storage_type =
        configs_.acceptable_storage_types_.at( 0 );

    double max_cost = -DBL_MAX;

    double partition_size =
        storage_stats_->get_partition_size( part->identifier_ );
    double ori_storage_cost = get_site_storage_imbalance_cost( storage_sizes );

    add_replica_stats add_stat(
        std::get<1>( part_location_info->get_partition_type(
            part_location_info->master_location_ ) ),
        std::get<1>( part_location_info->get_storage_type(
            part_location_info->master_location_ ) ),
        found_part_type, found_storage_type,
        cost_model2_->normalize_contention_by_time( part->get_contention() ),
        query_stats_->get_and_cache_cell_widths( part->identifier_,
                                                 cached_stats ),
        part->get_num_rows() );

    std::vector<std::shared_ptr<partition_payload>> parts_to_replicate = {part};
    std::vector<std::shared_ptr<partition_location_information>>
                                         part_locations_to_replicate = {part_location_info};
    std::vector<partition_type::type>    types_to_replicate = {found_part_type};
    std::vector<storage_tier_type::type> storage_types_to_replicate = {
        found_storage_type};

    double dest_load = site_loads.at( master ).cpu_load_;
    double source_load = site_loads.at( site ).cpu_load_;

    auto considered_types =
        enumerator_holder_->get_add_replica_types( add_stat );

    for( const auto &cons_type : considered_types ) {
        const auto replica_type = std::get<0>( cons_type );
        const auto storage_type = std::get<1>( cons_type );

        add_stat.dest_type_ = replica_type;
        add_stat.dest_storage_type_ = storage_type;

        types_to_replicate.at( 0 ) = replica_type;
        storage_types_to_replicate.at( 0 ) = storage_type;

        std::vector<add_replica_stats> add_stats = {add_stat};

        cost_model_prediction_holder model_holder =
            cost_model2_->predict_add_replica_execution_time(
                source_load, dest_load, add_stats );

        // temporarily do
        storage_sizes.at( storage_type ).at( site ) += partition_size;

        double expected_benefit = get_expected_benefit_of_adding_replicas(
            parts_to_replicate, part_locations_to_replicate, types_to_replicate,
            storage_types_to_replicate, master, site,
            other_write_locked_partitions, partition_location_informations,
            original_write_partition_set, original_read_partition_set,
            site_loads, write_sample_holder, read_sample_holder, cached_stats,
            cached_query_arrival_stats );
        double storage_stats = get_change_in_storage_imbalance_costs(
            get_site_storage_imbalance_cost( storage_sizes ),
            ori_storage_cost );

        // undo
        storage_sizes.at( storage_type ).at( site ) -= partition_size;

        double cost =
            ( configs_.upfront_cost_weight_ * -model_holder.get_prediction() ) +
            ( configs_.horizon_weight_ * expected_benefit ) +
            ( configs_.horizon_storage_weight_ * storage_stats );

        if( cost > max_cost ) {
            max_cost = cost;
            found_part_type = replica_type;
            found_storage_type = storage_type;
        }
    }
    plan->stat_decision_holder_->add_replica_types_decision(
        add_stat, std::make_tuple<>( found_part_type, found_storage_type ) );

    return std::make_tuple<>( found_part_type, found_storage_type );
}

double heuristic_site_evaluator::get_default_benefit_of_changing_load(
    const std::vector<site_load_information> &site_loads,
    const std::vector<site_load_information> &new_site_loads ) {

    double prev_txn_time = 0;
    double new_txn_time = 0;

    for( uint32_t site_id = 0; site_id < site_loads.size(); site_id++ ) {
        double new_site_cpu_load = cost_model2_->predict_site_load(
            cost_model2_->get_site_operation_counts(
                new_site_loads.at( site_id ) ) );

        DVLOG( 40 ) << "Get default benefit of changing load site:" << site_id
                    << ", current site load:" << site_loads.at( site_id )
                    << ", new site load:" << new_site_loads.at( site_id )
                    << ", new site CPU:" << new_site_cpu_load;

        double prev = cost_model2_
                          ->predict_wait_for_service_time(
                              site_loads.at( site_id ).cpu_load_ )
                          .get_prediction();
        site_load_information new_sl = new_site_loads.at( site_id );
        new_sl.cpu_load_ = new_site_cpu_load;
        double new_time =
            cost_model2_->predict_wait_for_service_time( new_sl.cpu_load_ )
                .get_prediction();

        DVLOG( 40 ) << "Get default benefit of changing load site:" << site_id
                    << ", prev:" << prev << ", new:" << new_time;

        prev_txn_time += prev;
        new_txn_time += new_time;
    }
    double benefit = ( prev_txn_time - new_txn_time );

    DVLOG( 40 ) << "Get default benefit of changing load prev:" << prev_txn_time
                << ", new:" << new_txn_time << ", benefit:" << benefit;

    return benefit;
}

std::vector<site_load_information>
    heuristic_site_evaluator::get_new_site_load_for_adding_replicas(
        const std::vector<std::shared_ptr<partition_payload>>
            &parts_to_replicate,
        const std::vector<std::shared_ptr<partition_location_information>>
            &parts_to_replicate_locations,
        int master, int destination,
        const std::vector<site_load_information> &site_loads ) {
    std::vector<site_load_information> new_loads = site_loads;

    DCHECK_EQ( parts_to_replicate_locations.size(), parts_to_replicate.size() );

    for( uint32_t i = 0; i < parts_to_replicate.size(); i++ ) {
        const auto &part = parts_to_replicate.at( i );
        const auto &location_information = parts_to_replicate_locations.at( i );

        double read_load =
            cost_model2_->normalize_contention_by_time( part->get_read_load() );
        double update_load = cost_model2_->normalize_contention_by_time(
            part->get_contention() );

        // assume spread across replicas and master
        double old_read_load_per_site =
            read_load / ( 1 + location_information->replica_locations_.size() );
        double new_read_load_per_site =
            read_load / ( 2 + location_information->replica_locations_.size() );

        new_loads.at( destination ).read_count_ += new_read_load_per_site;
        new_loads.at( destination ).update_count_ += update_load;

        double read_load_change =
            new_read_load_per_site - old_read_load_per_site;

        for( const auto &entry : location_information->replica_locations_ ) {
            uint32_t site_id = entry;

            new_loads.at( site_id ).read_count_ += read_load_change;
        }
        new_loads.at( location_information->master_location_ ).read_count_ +=
            read_load_change;
    }
    return new_loads;
}

std::vector<site_load_information>
    heuristic_site_evaluator::get_new_site_load_for_removing_replicas(
        const std::vector<std::shared_ptr<partition_payload>> &parts_to_remove,
        const std::vector<std::shared_ptr<partition_location_information>>
            &                                     parts_to_remove_locations,
        int                                       destination,
        const std::vector<site_load_information> &site_loads ) {
    std::vector<site_load_information> new_loads = site_loads;

    DCHECK_EQ( parts_to_remove_locations.size(), parts_to_remove.size() );

    for( uint32_t i = 0; i < parts_to_remove.size(); i++ ) {
        const auto &part = parts_to_remove.at( i );
        const auto &location_information = parts_to_remove_locations.at( i );

        double read_load =
            cost_model2_->normalize_contention_by_time( part->get_read_load() );
        double update_load = cost_model2_->normalize_contention_by_time(
            part->get_contention() );

        // assume spread across replicas and master
        double old_read_load_per_site =
            read_load / ( 1 + location_information->replica_locations_.size() );
        DCHECK_GT( location_information->replica_locations_.size(), 0 );
        double new_read_load_per_site =
            read_load / ( location_information->replica_locations_
                              .size() /* remove replica but add master */ );

        new_loads.at( destination ).read_count_ =
            new_loads.at( destination ).read_count_ - new_read_load_per_site;
        new_loads.at( destination ).update_count_ =
            new_loads.at( destination ).update_count_ - update_load;

        double read_load_change =
            new_read_load_per_site - old_read_load_per_site;

        // MTODO-STRATEGIES consider type in change in load?
        for( auto entry : location_information->replica_locations_ ) {
            uint32_t site_id = entry;
            if( site_id != (uint32_t) destination ) {
                new_loads.at( site_id ).read_count_ += read_load_change;
            }
        }
        new_loads.at( location_information->master_location_ ).read_count_ +=
            read_load_change;
    }
    return new_loads;
}

std::vector<site_load_information>
    heuristic_site_evaluator::get_new_site_load_for_remastering(
        const std::vector<std::shared_ptr<partition_payload>>
            &parts_to_remaster,
        int master, int destination,
        const std::vector<site_load_information> &site_loads ) {
    std::vector<site_load_information> new_loads = site_loads;

    for( const auto &part : parts_to_remaster ) {
        double update_load = cost_model2_->normalize_contention_by_time(
            part->get_contention() );

        // assume spread across replicas and master
        new_loads.at( destination ).write_count_ += update_load;
        new_loads.at( destination ).update_count_ -= update_load;

        new_loads.at( master ).write_count_ -= update_load;
        new_loads.at( master ).update_count_ += update_load;
    }
    for( uint32_t site_id = 0; site_id < new_loads.size(); site_id++ ) {
        new_loads.at( site_id ).cpu_load_ = cost_model2_->predict_site_load(
            cost_model2_->get_site_operation_counts(
                new_loads.at( site_id ) ) );
    }
    return new_loads;
}

double heuristic_site_evaluator::get_sample_benefit_of_remastering(
    std::vector<std::shared_ptr<partition_payload>> &parts_to_remaster,
    const std::vector<std::shared_ptr<partition_location_information>>
        &parts_to_remaster_location,
    int master, int destination,
    const partition_column_identifier_map_t<std::unordered_map<
        uint32_t, partition_type::type>> &created_partition_types,
    partition_column_identifier_map_t<std::unordered_map<
        uint32_t, storage_tier_type::type>> &created_storage_types,
    partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &other_write_locked_partitions,
    partition_column_identifier_map_t<std::shared_ptr<
        partition_location_information>> &partition_location_informations,
    const partition_column_identifier_unordered_set
        &original_write_partition_set,
    const partition_column_identifier_unordered_set
        &                                     original_read_partition_set,
    const std::vector<site_load_information> &site_loads,
    const std::vector<site_load_information> &new_site_loads,
    cached_ss_stat_holder &                   cached_stats,
    query_arrival_cached_stats &              cached_query_arrival_stats ) {

    double benefit = 0;
    DCHECK_EQ( parts_to_remaster.size(), parts_to_remaster_location.size() );

    for( uint32_t i = 0; i < parts_to_remaster.size(); i++ ) {
        auto &      part = parts_to_remaster.at( i );
        const auto &part_loc_info = parts_to_remaster_location.at( i );

        double part_contention = cost_model2_->normalize_contention_by_time(
            part->get_contention() );

        double part_benefit = compute_remastering_op_score(
            part->identifier_, part, part_loc_info, part_contention, master,
            destination, created_partition_types, created_storage_types,
            other_write_locked_partitions, partition_location_informations,
            original_write_partition_set, original_read_partition_set,
            site_loads, new_site_loads, cached_stats,
            cached_query_arrival_stats );

        benefit += part_benefit;
    }

    return benefit;
}

double heuristic_site_evaluator::get_sample_benefit_of_adding_replica(
    std::vector<std::shared_ptr<partition_payload>> &parts_to_replicate,
    const std::vector<std::shared_ptr<partition_location_information>>
        &                                       parts_to_replicate_location,
    const std::vector<partition_type::type> &   types_to_replicate,
    const std::vector<storage_tier_type::type> &storage_types_to_replicate,
    int master, int destination,
    partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &other_write_locked_partitions,
    partition_column_identifier_map_t<std::shared_ptr<
        partition_location_information>> &partition_location_informations,
    const partition_column_identifier_unordered_set
        &original_write_partition_set,
    const partition_column_identifier_unordered_set
        &                                     original_read_partition_set,
    const std::vector<site_load_information> &site_loads,
    const std::vector<site_load_information> &new_site_loads,
    cached_ss_stat_holder &                   cached_stats,
    query_arrival_cached_stats &              cached_query_arrival_stats ) {

    double benefit = 0;
    DCHECK_EQ( parts_to_replicate.size(), parts_to_replicate_location.size() );
    DCHECK_EQ( parts_to_replicate.size(), types_to_replicate.size() );
    DCHECK_EQ( parts_to_replicate.size(), storage_types_to_replicate.size() );

    for( uint32_t i = 0; i < parts_to_replicate.size(); i++ ) {
        auto &      part = parts_to_replicate.at( i );
        const auto &part_loc_info = parts_to_replicate_location.at( i );

        double part_contention = cost_model2_->normalize_contention_by_time(
            part->get_contention() );

        double part_benefit = compute_add_replica_op_score(
            part->identifier_, part, part_loc_info, part_contention,
            part_loc_info->master_location_, destination,
            types_to_replicate.at( i ), storage_types_to_replicate.at( i ),
            other_write_locked_partitions, partition_location_informations,
            original_write_partition_set, original_read_partition_set,
            site_loads, new_site_loads, cached_stats,
            cached_query_arrival_stats );

        benefit += part_benefit;
    }

    return benefit;
}

double heuristic_site_evaluator::get_sample_benefit_of_removing_replica(
    std::vector<std::shared_ptr<partition_payload>> &parts_to_remove,
    const std::vector<std::shared_ptr<partition_location_information>>
        &                                    parts_to_remove_location,
    const std::vector<partition_type::type> &types_to_remove, int destination,
    partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &other_write_locked_partitions,
    partition_column_identifier_map_t<std::shared_ptr<
        partition_location_information>> &partition_location_informations,
    const partition_column_identifier_unordered_set
        &original_write_partition_set,
    const partition_column_identifier_unordered_set
        &                                     original_read_partition_set,
    const std::vector<site_load_information> &site_loads,
    const std::vector<site_load_information> &new_site_loads,
    cached_ss_stat_holder &                   cached_stats,
    query_arrival_cached_stats &              cached_query_arrival_stats ) {

    double benefit = 0;
    DCHECK_EQ( parts_to_remove.size(), parts_to_remove_location.size() );
    DCHECK_EQ( parts_to_remove.size(), types_to_remove.size() );

    for( uint32_t i = 0; i < parts_to_remove.size(); i++ ) {
        auto &      part = parts_to_remove.at( i );
        const auto &part_loc_info = parts_to_remove_location.at( i );

        double part_contention = cost_model2_->normalize_contention_by_time(
            part->get_contention() );

        double part_benefit = compute_remove_replica_op_score(
            part->identifier_, part, part_loc_info, part_contention,
            part_loc_info->master_location_, destination,
            types_to_remove.at( i ), other_write_locked_partitions,
            partition_location_informations, original_write_partition_set,
            original_read_partition_set, site_loads, new_site_loads,
            cached_stats, cached_query_arrival_stats );

        benefit += part_benefit;
    }

    return benefit;
}

double get_change_in_remastering_cost(
    const std::shared_ptr<cost_modeller2> &               cost_model,
    const std::shared_ptr<partition_data_location_table> &data_loc_tab,
    const partition_column_identifier &                   pid,
    const future_change_state_type &future_change, double part_contention,
    double total_contention, int master, int destination,
    const partition_type::type &              source_type,
    const partition_type::type &              dest_type,
    const storage_tier_type::type &           source_storage_type,
    const storage_tier_type::type &           dest_storage_type,
    const std::vector<site_load_information> &site_loads,
    const std::vector<site_load_information> &new_site_loads,
    cached_ss_stat_holder &                   cached_stats,
    query_arrival_cached_stats &              cached_query_arrival_stats ) {
    double site_load_before = site_loads.at( master ).cpu_load_;
    double site_load_after = site_loads.at( destination ).cpu_load_;

    DVLOG( 40 ) << "Change in remastering cost:"
                << future_change_state_type_to_string( future_change )
                << ", part_contention:" << part_contention
                << ", total_contention:" << total_contention
                << ", source_type:" << source_type
                << ", dest_type:" << dest_type
                << ", source_storage_type:" << source_storage_type
                << ", dest_storage_type:" << dest_storage_type
                << ", site_load_before:" << site_load_before
                << ", site_load_after:" << site_load_after;

    std::vector<double> cell_widths =
        data_loc_tab->get_stats()->get_and_cache_cell_widths( pid,
                                                              cached_stats );
    uint32_t num_entries = get_number_of_rows( pid );
    double   scan_selectivity =
        data_loc_tab->get_stats()->get_and_cache_average_scan_selectivity(
            pid, cached_stats );
    uint32_t num_reads =
        data_loc_tab->get_stats()->get_and_cache_average_num_reads(
            pid, cached_stats );
    uint32_t num_updates =
        data_loc_tab->get_stats()->get_and_cache_average_num_updates(
            pid, cached_stats );
    uint32_t num_updates_needed = 0;

    std::vector<transaction_prediction_stats> txn_stats;
    transaction_prediction_stats              after_txn_stat(
        dest_type, dest_storage_type, total_contention, num_updates_needed,
        cell_widths, num_entries, true, scan_selectivity, true, num_reads, true,
        num_updates );
    transaction_prediction_stats before_txn_stat(
        source_type, source_storage_type, total_contention, num_updates_needed,
        cell_widths, num_entries, true, scan_selectivity, true, num_reads, true,
        num_updates );

    txn_stats.emplace_back( after_txn_stat );

    double txn_after = cost_model
                           ->predict_single_site_transaction_execution_time(
                               site_load_after, txn_stats )
                           .get_prediction();
    txn_stats.at( 0 ) = before_txn_stat;
    double txn_before = cost_model
                            ->predict_single_site_transaction_execution_time(
                                site_load_before, txn_stats )
                            .get_prediction();

    remaster_stats rem_before( source_type, dest_type, 0 /* num_updates*/,
                               part_contention );
    remaster_stats rem_after( dest_type, source_type, 0 /* num_updates*/,
                              part_contention );

    std::vector<remaster_stats> remaster_stat = {rem_before};

    double remaster_before =
        cost_model
            ->predict_remaster_execution_time( site_load_before,
                                               site_load_after, remaster_stat )
            .get_prediction();
    remaster_stat.at( 0 ) = rem_after;
    double remaster_after =
        cost_model
            ->predict_remaster_execution_time( site_load_after,
                                               site_load_before, remaster_stat )
            .get_prediction();
    double change = 0;

    switch( future_change ) {
        case NEITHER_REQUIRE_CHANGE:
            change = txn_before - txn_after;
            break;
        case PREVIOUSLY_REQUIRED_CHANGE:
            change = ( remaster_before + txn_before ) - txn_after;
            break;
        case NOW_REQUIRE_CHANGE:
            change = txn_before - ( remaster_after + txn_after );
            break;
        case BOTH_REQUIRE_CHANGE:
            change = ( remaster_before + txn_before ) -
                     ( remaster_after + txn_after );
            break;
        default:
            change = 0;
    }
    DVLOG( 40 ) << "Change in remastering cost:"
                << future_change_state_type_to_string( future_change )
                << ", txn_after:" << txn_after << ", txn_before:" << txn_before
                << ", remaster_before:" << remaster_before
                << ", remaster_after:" << remaster_after
                << ", change:" << change;

    return change;
}

double get_change_in_wr_remastering_cost(
    const std::shared_ptr<cost_modeller2> &                cost_model,
    const std::shared_ptr<partition_data_location_table> & data_loc_tab,
    const partition_column_identifier &                    pid,
    const std::shared_ptr<partition_location_information> &part,
    const std::shared_ptr<partition_location_information>
        &  correlated_write_part,
    double score_adjustment, int master, int destination,
    const partition_type::type &   repl_type,
    const storage_tier_type::type &repl_storage_type,
    bool also_accessing_other_part, double part_contention,
    double                                    other_part_contention,
    const std::vector<site_load_information> &site_loads,
    const std::vector<site_load_information> &new_site_loads,
    cached_ss_stat_holder &                   cached_stats,
    query_arrival_cached_stats &              cached_query_arrival_stats ) {
    auto future_change =
        compute_wr_score( part, correlated_write_part, score_adjustment,
                          destination, also_accessing_other_part );
    return get_change_in_remastering_cost(
        cost_model, data_loc_tab, pid, future_change, part_contention,
        part_contention, master, destination,
        std::get<1>( part->get_partition_type( part->master_location_ ) ),
        repl_type,
        std::get<1>( part->get_storage_type( part->master_location_ ) ),
        repl_storage_type, site_loads, new_site_loads, cached_stats,
        cached_query_arrival_stats );
}
double get_change_in_ww_remastering_cost(
    const std::shared_ptr<cost_modeller2> &                cost_model,
    const std::shared_ptr<partition_data_location_table> & data_loc_tab,
    const partition_column_identifier &                    pid,
    const std::shared_ptr<partition_location_information> &part,
    const std::shared_ptr<partition_location_information>
        &  correlated_write_part,
    double score_adjustment, int master, int destination,
    const partition_type::type &   repl_type,
    const storage_tier_type::type &repl_storage_type,
    bool also_accessing_other_part, double part_contention,
    double                                    other_part_contention,
    const std::vector<site_load_information> &site_loads,
    const std::vector<site_load_information> &new_site_loads,
    cached_ss_stat_holder &                   cached_stats,
    query_arrival_cached_stats &              cached_query_arrival_stats ) {
    auto future_change =
        compute_ww_score( part, correlated_write_part, score_adjustment,
                          destination, also_accessing_other_part );
    return get_change_in_remastering_cost(
        cost_model, data_loc_tab, pid, future_change, part_contention,
        part_contention + other_part_contention, master, destination,
        std::get<1>( part->get_partition_type( part->master_location_ ) ),
        repl_type,
        std::get<1>( part->get_storage_type( part->master_location_ ) ),
        repl_storage_type, site_loads, new_site_loads, cached_stats,
        cached_query_arrival_stats );
}

double get_change_in_replica_cost(
    const std::shared_ptr<cost_modeller2> &               cost_model,
    const std::shared_ptr<partition_data_location_table> &data_loc_tab,
    const partition_column_identifier &                   pid,
    const std::unordered_map<uint32_t, partition_type::type> &other_locations,
    const std::unordered_map<uint32_t, storage_tier_type::type> &other_storage,
    const std::unordered_map<uint32_t, partition_type::type> &   curr_locations,
    const std::unordered_map<uint32_t, partition_type::type> &future_locations,
    double contention, const std::vector<site_load_information> &site_loads,
    const std::vector<site_load_information> &new_site_loads,
    cached_ss_stat_holder &                   cached_stats,
    query_arrival_cached_stats &              cached_query_arrival_stats ) {

    uint32_t prev_count = 0;
    uint32_t future_count = 0;

    double prev_cost_accum = 0;
    double future_cost_accum = 0;

    std::vector<transaction_prediction_stats> txn_stats;

    std::vector<double> cell_widths =
        data_loc_tab->get_stats()->get_and_cache_cell_widths( pid,
                                                              cached_stats );
    uint32_t num_entries = get_number_of_rows( pid );
    double   scan_selectivity =
        data_loc_tab->get_stats()->get_and_cache_average_scan_selectivity(
            pid, cached_stats );
    uint32_t num_reads =
        data_loc_tab->get_stats()->get_and_cache_average_num_reads(
            pid, cached_stats );
    uint32_t num_updates =
        data_loc_tab->get_stats()->get_and_cache_average_num_updates(
            pid, cached_stats );
    uint32_t num_updates_needed = 0;

    DCHECK_EQ( other_locations.size(), other_storage.size() );

    for( auto entry : other_locations ) {
        txn_stats.clear();
        transaction_prediction_stats txn_stat(
            entry.second, other_storage.at( entry.first ), contention,
            num_updates_needed, cell_widths, num_entries, true,
            scan_selectivity, true, num_reads, true, num_updates );
        txn_stats.emplace_back( txn_stat );

        uint32_t site = entry.first;
        if( curr_locations.count( site ) == 1 ) {

            prev_count += 1;
            prev_cost_accum +=
                cost_model
                    ->predict_single_site_transaction_execution_time(
                        site_loads.at( site ).cpu_load_, txn_stats )
                    .get_prediction();
        }
        if( future_locations.count( site ) == 1 ) {
            future_count += 1;
            future_cost_accum +=
                cost_model
                    ->predict_single_site_transaction_execution_time(
                        new_site_loads.at( site ).cpu_load_, txn_stats )
                    .get_prediction();
        }
    }

    if( ( prev_count == 0 ) and ( future_count == 0 ) ) {
        // no change
        return 0;
    } else if( prev_count == 0 ) {
        // all gains
        return ( future_cost_accum / (double) future_count );
    } else if( future_count == 0 ) {
        // all losses
        return -( prev_cost_accum / (double) prev_count );
    } else {
        double prev_avg = prev_cost_accum / (double) prev_count;
        double future_avg = future_cost_accum / (double) future_count;

        return ( prev_avg - future_avg );
    }
    return 0;
}

double get_change_in_rw_remove_replica_cost(
    const std::shared_ptr<cost_modeller2> &                cost_model,
    const std::shared_ptr<partition_data_location_table> & data_loc_tab,
    const partition_column_identifier &                    pid,
    const std::shared_ptr<partition_location_information> &part,
    const std::shared_ptr<partition_location_information>
        &  correlated_write_part,
    double score_adjustment, int master, int destination,
    const partition_type::type &   repl_type,
    const storage_tier_type::type &repl_storage_type,
    bool also_accessing_other_part, double part_contention,
    double                                    other_part_contention,
    const std::vector<site_load_information> &site_loads,
    const std::vector<site_load_information> &new_site_loads,
    cached_ss_stat_holder &                   cached_stats,
    query_arrival_cached_stats &              cached_query_arrival_stats ) {

    std::unordered_map<uint32_t, partition_type::type>
        other_part_candidate_sites = {
            {correlated_write_part->master_location_,
             std::get<1>( correlated_write_part->get_partition_type(
                 correlated_write_part->master_location_ ) )}};
    std::unordered_map<uint32_t, storage_tier_type::type>
        other_part_storage_sites = {
            {correlated_write_part->master_location_,
             std::get<1>( correlated_write_part->get_storage_type(
                 correlated_write_part->master_location_ ) )}};

    std::unordered_map<uint32_t, partition_type::type> prev_cand_sites =
        part->partition_types_;

    std::unordered_map<uint32_t, partition_type::type> new_cand_sites =
        part->partition_types_;
    new_cand_sites.erase( destination );

    // benefit =
    return get_change_in_replica_cost(
        cost_model, data_loc_tab, pid, other_part_candidate_sites,
        other_part_storage_sites, prev_cand_sites, new_cand_sites,
        other_part_contention, site_loads, new_site_loads, cached_stats,
        cached_query_arrival_stats );
}

double get_change_in_rr_remove_replica_cost(
    const std::shared_ptr<cost_modeller2> &                cost_model,
    const std::shared_ptr<partition_data_location_table> & data_loc_tab,
    const partition_column_identifier &                    pid,
    const std::shared_ptr<partition_location_information> &part,
    const std::shared_ptr<partition_location_information> &correlated_read_part,
    double score_adjustment, int master, int destination,
    const partition_type::type &   repl_type,
    const storage_tier_type::type &repl_storage_type,
    bool also_accessing_other_part, double part_contention,
    double                                    other_part_contention,
    const std::vector<site_load_information> &site_loads,
    const std::vector<site_load_information> &new_site_loads,
    cached_ss_stat_holder &                   cached_stats,
    query_arrival_cached_stats &              cached_query_arrival_stats ) {

    std::unordered_map<uint32_t, partition_type::type>
        other_part_candidate_sites = correlated_read_part->partition_types_;
    std::unordered_map<uint32_t, storage_tier_type::type>
        other_part_storage_sites = correlated_read_part->storage_types_;

    std::unordered_map<uint32_t, partition_type::type> prev_cand_sites =
        part->partition_types_;

    std::unordered_map<uint32_t, partition_type::type> new_cand_sites =
        part->partition_types_;
    new_cand_sites.erase( destination );

    return get_change_in_replica_cost(
        cost_model, data_loc_tab, pid, other_part_candidate_sites,
        other_part_storage_sites, prev_cand_sites, new_cand_sites, 0,
        site_loads, new_site_loads, cached_stats, cached_query_arrival_stats );
}

double get_change_in_rw_add_replica_cost(
    const std::shared_ptr<cost_modeller2> &                cost_model,
    const std::shared_ptr<partition_data_location_table> & data_loc_tab,
    const partition_column_identifier &                    pid,
    const std::shared_ptr<partition_location_information> &part,
    const std::shared_ptr<partition_location_information>
        &  correlated_write_part,
    double score_adjustment, int master, int destination,
    const partition_type::type &   repl_type,
    const storage_tier_type::type &repl_storage_type,
    bool also_accessing_other_part, double part_contention,
    double                                    other_part_contention,
    const std::vector<site_load_information> &site_loads,
    const std::vector<site_load_information> &new_site_loads,
    cached_ss_stat_holder &                   cached_stats,
    query_arrival_cached_stats &              cached_query_arrival_stats ) {

    std::unordered_map<uint32_t, partition_type::type>
        other_part_candidate_sites = {
            {correlated_write_part->master_location_,
             std::get<1>( correlated_write_part->get_partition_type(
                 correlated_write_part->master_location_ ) )}};
    std::unordered_map<uint32_t, storage_tier_type::type>
        other_part_storage_sites = {
            {correlated_write_part->master_location_,
             std::get<1>( correlated_write_part->get_storage_type(
                 correlated_write_part->master_location_ ) )}};

    std::unordered_map<uint32_t, partition_type::type> prev_cand_sites =
        part->partition_types_;

    std::unordered_map<uint32_t, partition_type::type> new_cand_sites =
        part->partition_types_;
    new_cand_sites[destination] = repl_type;

    // benefit =
    return get_change_in_replica_cost(
        cost_model, data_loc_tab, pid, other_part_candidate_sites,
        other_part_storage_sites, prev_cand_sites, new_cand_sites,
        other_part_contention, site_loads, new_site_loads, cached_stats,
        cached_query_arrival_stats );
}

double get_change_in_rr_add_replica_cost(
    const std::shared_ptr<cost_modeller2> &                cost_model,
    const std::shared_ptr<partition_data_location_table> & data_loc_tab,
    const partition_column_identifier &                    pid,
    const std::shared_ptr<partition_location_information> &part,
    const std::shared_ptr<partition_location_information> &correlated_read_part,
    double score_adjustment, int master, int destination,
    const partition_type::type &   repl_type,
    const storage_tier_type::type &repl_storage_type,
    bool also_accessing_other_part, double part_contention,
    double                                    other_part_contention,
    const std::vector<site_load_information> &site_loads,
    const std::vector<site_load_information> &new_site_loads,
    cached_ss_stat_holder &                   cached_stats,
    query_arrival_cached_stats &              cached_query_arrival_stats ) {

    std::unordered_map<uint32_t, partition_type::type>
        other_part_candidate_sites = correlated_read_part->partition_types_;
    std::unordered_map<uint32_t, storage_tier_type::type>
        other_part_storage_sites = correlated_read_part->storage_types_;

    std::unordered_map<uint32_t, partition_type::type> prev_cand_sites =
        part->partition_types_;

    std::unordered_map<uint32_t, partition_type::type> new_cand_sites =
        part->partition_types_;
    new_cand_sites[destination] = repl_type;

    return get_change_in_replica_cost(
        cost_model, data_loc_tab, pid, other_part_candidate_sites,
        other_part_storage_sites, prev_cand_sites, new_cand_sites, 0,
        site_loads, new_site_loads, cached_stats, cached_query_arrival_stats );
}

template <double( get_change_in_cost )(
    const std::shared_ptr<cost_modeller2> &                cost_model,
    const std::shared_ptr<partition_data_location_table> & data_loc_tab,
    const partition_column_identifier &                    pid,
    const std::shared_ptr<partition_location_information> &part,
    const std::shared_ptr<partition_location_information>
        &  correlated_write_part,
    double score_adjustment, int master, int destination,
    const partition_type::type &   repl_type,
    const storage_tier_type::type &repl_storage_type,
    bool also_accessing_other_part, double part_contention,
    double                                    other_part_contention,
    const std::vector<site_load_information> &site_loads,
    const std::vector<site_load_information> &new_site_loads,
    cached_ss_stat_holder &                   cached_stats,
    query_arrival_cached_stats &              cached_query_arrival_stats )>
double heuristic_site_evaluator::compute_correlation_score(
    const partition_column_identifier &pid,
    const std::shared_ptr<partition_location_information>
        &  part_location_information,
    double part_contention, int total_partition_accesses,
    transition_statistics &stats, int master, int destination,
    const partition_type::type &   repl_type,
    const storage_tier_type::type &repl_storage_type,
    partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &other_write_locked_partitions,
    partition_column_identifier_map_t<std::shared_ptr<
        partition_location_information>> &partition_location_informations,
    const partition_column_identifier_unordered_set &original_partition_set,
    const std::vector<site_load_information> &       site_loads,
    const std::vector<site_load_information> &       new_site_loads,
    cached_ss_stat_holder &                          cached_stats,
    query_arrival_cached_stats &cached_query_arrival_stats ) {

    DVLOG( 40 ) << "Compute correlation score:" << pid;

    double remastering_op_score = 0.0;

    std::lock_guard<std::mutex>                       lk( stats.mutex_ );
    const partition_column_identifier_map_t<int64_t> &transitions =
        stats.transitions_;

    const partition_lock_mode lock_mode = partition_lock_mode::no_lock;

    if( total_partition_accesses == 0 ) {
        // don't do anything
        return 0;
    }

    for( const auto &correlated_part_count : transitions ) {
        const auto &corr_pid = correlated_part_count.first;
        int64_t     count = correlated_part_count.second;
        // Impossible for a correlation to have a higher traversal count
        // than the total number of accesses
        if( count > total_partition_accesses ) {
            DLOG( WARNING ) << "Access count:" << count
                            << ", greater than total partition accessess:"
                            << total_partition_accesses << ", partition:" << pid
                            << ", correlated pid:" << corr_pid
                            << ", rescale count";
            count = total_partition_accesses;
        }
        DCHECK_LE( count, total_partition_accesses );

        double score_adjustment =
            (double) count / (double) total_partition_accesses;

        auto part_search =
            get_partition_from_data_location_table_or_already_locked(
                corr_pid, other_write_locked_partitions, lock_mode );

        std::shared_ptr<partition_payload> correlated_part =
            std::get<0>( part_search );
        if( correlated_part == nullptr ) {
            // Couldn't get the partition
            continue;
        }

        auto location_information =
            get_location_information_for_partition_or_from_payload(
                correlated_part, partition_location_informations, lock_mode );
        if( location_information == nullptr ) {
            // we don't have it
            unlock_if_new( part_search, other_write_locked_partitions,
                           partition_location_informations, lock_mode );
            continue;
        }

        bool   accessed_with = original_partition_set.count( corr_pid ) == 1;
        double change_in_cost = get_change_in_cost(
            cost_model2_, data_loc_tab_, pid, part_location_information,
            location_information, score_adjustment, master, destination,
            repl_type, repl_storage_type, accessed_with, part_contention,
            cost_model2_->normalize_contention_by_time(
                correlated_part->get_contention() ),
            site_loads, new_site_loads, cached_stats,
            cached_query_arrival_stats );

        DVLOG( 40 ) << "Part:" << pid
                    << ", correlated part:" << correlated_part->identifier_
                    << ", get change in cost:" << change_in_cost;

        remastering_op_score += ( score_adjustment * change_in_cost );

        // Release the lock if we acquired it
        unlock_if_new( part_search, other_write_locked_partitions,
                       partition_location_informations, lock_mode );
    }
    DVLOG( 40 ) << "Compute correlation score:" << pid
                << ", score:" << remastering_op_score;

    return remastering_op_score;
}

storage_tier_type::type heuristic_site_evaluator::get_storage_type(
    const partition_column_identifier &pid, uint32_t destination,
    const std::shared_ptr<partition_location_information>
        &part_location_information,
    const partition_column_identifier_map_t<std::unordered_map<
        uint32_t, storage_tier_type::type>> &created_storage_types ) const {
    auto found_storage =
        part_location_information->get_storage_type( destination );
    storage_tier_type::type storage_type = std::get<1>( found_storage );
    if( !std::get<0>( found_storage ) ) {
        if( ( created_storage_types.count( pid ) == 1 ) and
            ( created_storage_types.at( pid ).count( destination ) ) ) {
            storage_type = created_storage_types.at( pid ).at( destination );
        } else {
            DLOG( FATAL ) << "Couldn't find storage type for pid:" << pid
                          << ", destination:" << destination;
        }
    }

    return storage_type;
}

partition_type::type heuristic_site_evaluator::get_partition_type(
    const partition_column_identifier &pid, uint32_t destination,
    const std::shared_ptr<partition_location_information>
        &part_location_information,
    const partition_column_identifier_map_t<std::unordered_map<
        uint32_t, partition_type::type>> &created_partition_types ) const {
    auto found_part =
        part_location_information->get_partition_type( destination );
    partition_type::type part_type = std::get<1>( found_part );
    if( !std::get<0>( found_part ) ) {
        if( ( created_partition_types.count( pid ) == 1 ) and
            ( created_partition_types.at( pid ).count( destination ) ) ) {
            part_type = created_partition_types.at( pid ).at( destination );
        } else {
            DLOG( FATAL ) << "Couldn't find partition type for pid:" << pid
                          << ", destination:" << destination;
        }
    }

    return part_type;
}

double heuristic_site_evaluator::compute_remastering_op_score(
    const partition_column_identifier & pid,
    std::shared_ptr<partition_payload> &payload,
    const std::shared_ptr<partition_location_information>
        &  part_location_information,
    double part_contention, int master, int destination,
    const partition_column_identifier_map_t<std::unordered_map<
        uint32_t, partition_type::type>> &created_partition_types,
    partition_column_identifier_map_t<std::unordered_map<
        uint32_t, storage_tier_type::type>> &created_storage_types,
    partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &other_write_locked_partitions,
    partition_column_identifier_map_t<std::shared_ptr<
        partition_location_information>> &partition_location_informations,
    const partition_column_identifier_unordered_set
        &original_write_partition_set,
    const partition_column_identifier_unordered_set
        &                                     original_read_partition_set,
    const std::vector<site_load_information> &site_loads,
    const std::vector<site_load_information> &new_site_loads,
    cached_ss_stat_holder &                   cached_stats,
    query_arrival_cached_stats &              cached_query_arrival_stats ) {

    DVLOG( 20 ) << "Compute remastering op score:" << pid;

    int64_t total_sample_based_partition_accesses =
        payload->sample_based_write_accesses_;

    // we will find the right type below
    partition_type::type repl_type = get_partition_type(
        payload->identifier_, destination, part_location_information,
        created_partition_types );
    auto storage_type =
        get_storage_type( payload->identifier_, destination,
                          part_location_information, created_storage_types );

    double query_arrival_score = get_upcoming_query_arrival_score(
        pid, true /* allow writes */, cached_query_arrival_stats );

    double within_wr_score =
        compute_correlation_score<get_change_in_wr_remastering_cost>(
            pid, part_location_information, part_contention,
            total_sample_based_partition_accesses,
            payload->within_wr_txn_statistics_, master, destination, repl_type,
            storage_type, other_write_locked_partitions,
            partition_location_informations, original_read_partition_set,
            site_loads, new_site_loads, cached_stats,
            cached_query_arrival_stats );

    double within_ww_score =
        compute_correlation_score<get_change_in_ww_remastering_cost>(
            pid, part_location_information, part_contention,
            total_sample_based_partition_accesses,
            payload->within_ww_txn_statistics_, master, destination, repl_type,
            storage_type, other_write_locked_partitions,
            partition_location_informations, original_write_partition_set,
            site_loads, new_site_loads, cached_stats,
            cached_query_arrival_stats );

    double across_wr_score =
        compute_correlation_score<get_change_in_wr_remastering_cost>(
            pid, part_location_information, part_contention,
            total_sample_based_partition_accesses,
            payload->across_wr_txn_statistics_, master, destination, repl_type,
            storage_type, other_write_locked_partitions,
            partition_location_informations, original_read_partition_set,
            site_loads, new_site_loads, cached_stats,
            cached_query_arrival_stats );

    double across_ww_score =
        compute_correlation_score<get_change_in_ww_remastering_cost>(
            pid, part_location_information, part_contention,
            total_sample_based_partition_accesses,
            payload->across_ww_txn_statistics_, master, destination, repl_type,
            storage_type, other_write_locked_partitions,
            partition_location_informations, original_write_partition_set,
            site_loads, new_site_loads, cached_stats,
            cached_query_arrival_stats );

    double score = ( within_wr_score + within_ww_score + across_wr_score +
                     across_ww_score ) *
                   query_arrival_score;

    DVLOG( 20 ) << "Compute remastering op score:" << pid
                << ", within_wr_score:" << within_wr_score
                << ", within_ww_score:" << within_ww_score
                << ", across_wr_score:" << across_wr_score
                << ", across_ww_score:" << across_ww_score
                << ", query_arrival_score:" << query_arrival_score
                << ", total score:" << score;
    return score;
}

double heuristic_site_evaluator::compute_add_replica_op_score(
    const partition_column_identifier & pid,
    std::shared_ptr<partition_payload> &payload,
    const std::shared_ptr<partition_location_information>
        &  part_location_information,
    double part_contention, int master, int destination,
    const partition_type::type &   repl_type,
    const storage_tier_type::type &repl_storage_type,
    partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &other_write_locked_partitions,
    partition_column_identifier_map_t<std::shared_ptr<
        partition_location_information>> &partition_location_informations,
    const partition_column_identifier_unordered_set
        &original_write_partition_set,
    const partition_column_identifier_unordered_set
        &                                     original_read_partition_set,
    const std::vector<site_load_information> &site_loads,
    const std::vector<site_load_information> &new_site_loads,
    cached_ss_stat_holder &                   cached_stats,
    query_arrival_cached_stats &              cached_query_arrival_stats ) {

    DVLOG( 20 ) << "Compute add replica op score:" << pid;

    int64_t total_sample_based_partition_accesses =
        payload->sample_based_read_accesses_;

    double query_arrival_score = get_upcoming_query_arrival_score(
        pid, false /* allow writes */, cached_query_arrival_stats );

    double within_rr_score =
        compute_correlation_score<get_change_in_rr_add_replica_cost>(
            pid, part_location_information, part_contention,
            total_sample_based_partition_accesses,
            payload->within_rr_txn_statistics_, master, destination, repl_type,
            repl_storage_type, other_write_locked_partitions,
            partition_location_informations, original_read_partition_set,
            site_loads, new_site_loads, cached_stats,
            cached_query_arrival_stats );

    double within_rw_score =
        compute_correlation_score<get_change_in_rw_add_replica_cost>(
            pid, part_location_information, part_contention,
            total_sample_based_partition_accesses,
            payload->within_rw_txn_statistics_, master, destination, repl_type,
            repl_storage_type, other_write_locked_partitions,
            partition_location_informations, original_write_partition_set,
            site_loads, new_site_loads, cached_stats,
            cached_query_arrival_stats );

    double across_rr_score =
        compute_correlation_score<get_change_in_rr_add_replica_cost>(
            pid, part_location_information, part_contention,
            total_sample_based_partition_accesses,
            payload->across_rr_txn_statistics_, master, destination, repl_type,
            repl_storage_type, other_write_locked_partitions,
            partition_location_informations, original_read_partition_set,
            site_loads, new_site_loads, cached_stats,
            cached_query_arrival_stats );

    double across_rw_score =
        compute_correlation_score<get_change_in_rw_add_replica_cost>(
            pid, part_location_information, part_contention,
            total_sample_based_partition_accesses,
            payload->across_rw_txn_statistics_, master, destination, repl_type,
            repl_storage_type, other_write_locked_partitions,
            partition_location_informations, original_write_partition_set,
            site_loads, new_site_loads, cached_stats,
            cached_query_arrival_stats );

    double score = ( within_rr_score + within_rw_score + across_rr_score +
                     across_rw_score ) *
                   query_arrival_score;

    DVLOG( 20 ) << "Compute add replica op score:" << pid
                << ", within_rr_score:" << within_rr_score
                << ", within_rw_score:" << within_rw_score
                << ", across_rr_score:" << across_rr_score
                << ", across_rw_score:" << across_rw_score
                << ", query_arrival_score:" << query_arrival_score
                << ", total score:" << score;
    return score;
}

double heuristic_site_evaluator::get_expected_benefit_of_removing_replicas(
    std::vector<std::shared_ptr<partition_payload>> &parts_to_remove,
    const std::vector<std::shared_ptr<partition_location_information>>
        &                                    parts_to_remove_locations,
    const std::vector<partition_type::type> &types_to_remove, int destination,
    partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &other_write_locked_partitions,
    partition_column_identifier_map_t<std::shared_ptr<
        partition_location_information>> &partition_location_informations,
    const partition_column_identifier_unordered_set
        &original_write_partition_set,
    const partition_column_identifier_unordered_set
        &                                     original_read_partition_set,
    const std::vector<site_load_information> &site_loads,
    cached_ss_stat_holder &                   cached_stats,
    query_arrival_cached_stats &              cached_query_arrival_stats ) {
    std::vector<site_load_information> new_site_loads =
        get_new_site_load_for_removing_replicas( parts_to_remove,
                                                 parts_to_remove_locations,
                                                 destination, site_loads );

    double default_benefit =
        get_default_benefit_of_changing_load( site_loads, new_site_loads );

    double sample_benefit = get_sample_benefit_of_removing_replica(
        parts_to_remove, parts_to_remove_locations, types_to_remove,
        destination, other_write_locked_partitions,
        partition_location_informations, original_write_partition_set,
        original_read_partition_set, site_loads, new_site_loads, cached_stats,
        cached_query_arrival_stats );

    double benefit =
        sample_benefit +
        ( default_benefit * configs_.default_horizon_benefit_weight_ );

    DVLOG( 40 ) << "Get expected benefit of removing replicas: default benefit:"
                << ", benefit:" << benefit;

    return benefit;
}

double heuristic_site_evaluator::compute_remove_replica_op_score(
    const partition_column_identifier & pid,
    std::shared_ptr<partition_payload> &payload,
    const std::shared_ptr<partition_location_information>
        &  part_location_information,
    double part_contention, int master, int destination,
    const partition_type::type &repl_type,
    partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &other_write_locked_partitions,
    partition_column_identifier_map_t<std::shared_ptr<
        partition_location_information>> &partition_location_informations,
    const partition_column_identifier_unordered_set
        &original_write_partition_set,
    const partition_column_identifier_unordered_set
        &                                     original_read_partition_set,
    const std::vector<site_load_information> &site_loads,
    const std::vector<site_load_information> &new_site_loads,
    cached_ss_stat_holder &                   cached_stats,
    query_arrival_cached_stats &              cached_query_arrival_stats ) {

    DVLOG( 20 ) << "Compute remove replica op score:" << pid;

    int64_t total_sample_based_partition_accesses =
        payload->sample_based_read_accesses_;

    auto repl_storage_type = std::get<1>(
        ( part_location_information->get_storage_type( destination ) ) );

    double query_arrival_score = get_upcoming_query_arrival_score(
        pid, false /* allow writes */, cached_query_arrival_stats );

    double within_rr_score =
        compute_correlation_score<get_change_in_rr_remove_replica_cost>(
            pid, part_location_information, part_contention,
            total_sample_based_partition_accesses,
            payload->within_rr_txn_statistics_, master, destination, repl_type,
            repl_storage_type, other_write_locked_partitions,
            partition_location_informations, original_read_partition_set,
            site_loads, new_site_loads, cached_stats,
            cached_query_arrival_stats );

    double within_rw_score =
        compute_correlation_score<get_change_in_rw_remove_replica_cost>(
            pid, part_location_information, part_contention,
            total_sample_based_partition_accesses,
            payload->within_rw_txn_statistics_, master, destination, repl_type,
            repl_storage_type, other_write_locked_partitions,
            partition_location_informations, original_write_partition_set,
            site_loads, new_site_loads, cached_stats,
            cached_query_arrival_stats );

    double across_rr_score =
        compute_correlation_score<get_change_in_rr_remove_replica_cost>(
            pid, part_location_information, part_contention,
            total_sample_based_partition_accesses,
            payload->across_rr_txn_statistics_, master, destination, repl_type,
            repl_storage_type, other_write_locked_partitions,
            partition_location_informations, original_read_partition_set,
            site_loads, new_site_loads, cached_stats,
            cached_query_arrival_stats );

    double across_rw_score =
        compute_correlation_score<get_change_in_rw_remove_replica_cost>(
            pid, part_location_information, part_contention,
            total_sample_based_partition_accesses,
            payload->across_rw_txn_statistics_, master, destination, repl_type,
            repl_storage_type, other_write_locked_partitions,
            partition_location_informations, original_write_partition_set,
            site_loads, new_site_loads, cached_stats,
            cached_query_arrival_stats );

    double score = ( within_rr_score + within_rw_score + across_rr_score +
                     across_rw_score ) *
                   query_arrival_score;

    DVLOG( 20 ) << "Compute remove replica op score:" << pid
                << ", within_rr_score:" << within_rr_score
                << ", within_rw_score:" << within_rw_score
                << ", across_rr_score:" << across_rr_score
                << ", across_rw_score:" << across_rw_score
                << ", query_arrival_score:" << query_arrival_score
                << ", total score:" << score;
    return score;
}

double heuristic_site_evaluator::get_expected_benefit_of_adding_replicas(
    std::vector<std::shared_ptr<partition_payload>> &parts_to_replicate,
    const std::vector<std::shared_ptr<partition_location_information>>
        &                                       parts_to_replicate_locations,
    const std::vector<partition_type::type> &   types_to_replicate,
    const std::vector<storage_tier_type::type> &storage_types_to_replicate,
    int master, int destination,
    partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &other_write_locked_partitions,
    partition_column_identifier_map_t<std::shared_ptr<
        partition_location_information>> &partition_location_informations,
    const partition_column_identifier_unordered_set
        &original_write_partition_set,
    const partition_column_identifier_unordered_set
        &                                     original_read_partition_set,
    const std::vector<site_load_information> &site_loads,
    const partition_column_identifier_map_t<
        std::vector<transaction_partition_accesses>> &write_sample_holder,
    const partition_column_identifier_map_t<
        std::vector<transaction_partition_accesses>> &read_sample_holder,
    cached_ss_stat_holder &                           cached_stats,
    query_arrival_cached_stats &cached_query_arrival_stats ) {

    std::vector<site_load_information> new_site_loads =
        get_new_site_load_for_adding_replicas(
            parts_to_replicate, parts_to_replicate_locations, master,
            destination, site_loads );

    double default_benefit =
        get_default_benefit_of_changing_load( site_loads, new_site_loads );

    double sample_benefit = get_sample_benefit_of_adding_replica(
        parts_to_replicate, parts_to_replicate_locations, types_to_replicate,
        storage_types_to_replicate, master, destination,
        other_write_locked_partitions, partition_location_informations,
        original_write_partition_set, original_read_partition_set, site_loads,
        new_site_loads, cached_stats, cached_query_arrival_stats );

    double benefit =
        sample_benefit +
        ( default_benefit * configs_.default_horizon_benefit_weight_ );

    DVLOG( 40 ) << "Get expected benefit of adding replicas: default benefit:"
                << default_benefit << ", sample benefit:" << sample_benefit
                << ", benefit:" << benefit;

    return benefit;
}

double heuristic_site_evaluator::get_expected_benefit_of_remastering(
    std::vector<std::shared_ptr<partition_payload>> &parts_to_remaster,
    std::vector<std::shared_ptr<partition_location_information>>
        &parts_to_remaster_locations,
    int master, int destination,
    const partition_column_identifier_map_t<std::unordered_map<
        uint32_t, partition_type::type>> &created_partition_types,
    partition_column_identifier_map_t<std::unordered_map<
        uint32_t, storage_tier_type::type>> &created_storage_types,
    partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &other_write_locked_partitions,
    partition_column_identifier_map_t<std::shared_ptr<
        partition_location_information>> &partition_location_informations,
    const partition_column_identifier_unordered_set
        &original_write_partition_set,
    const partition_column_identifier_unordered_set
        &                                     original_read_partition_set,
    const std::vector<site_load_information> &site_loads,
    cached_ss_stat_holder &                   cached_stats,
    query_arrival_cached_stats &              cached_query_arrival_stats ) {

    std::vector<site_load_information> new_site_loads =
        get_new_site_load_for_remastering( parts_to_remaster, master,
                                           destination, site_loads );

    double default_benefit =
        get_default_benefit_of_changing_load( site_loads, new_site_loads );

    double sample_benefit = get_sample_benefit_of_remastering(
        parts_to_remaster, parts_to_remaster_locations, master, destination,
        created_partition_types, created_storage_types,
        other_write_locked_partitions, partition_location_informations,
        original_write_partition_set, original_read_partition_set, site_loads,
        new_site_loads, cached_stats, cached_query_arrival_stats );

    double benefit =
        sample_benefit +
        ( default_benefit * configs_.default_horizon_benefit_weight_ );

    DVLOG( 40 ) << "Get expected benefit of remastering: default benefit:"
                << default_benefit << ", sample benefit:" << sample_benefit
                << ", benefit:" << benefit;

    return benefit;
}

void heuristic_site_evaluator::add_replica_to_plan(
    const partition_column_identifier &             pid,
    std::shared_ptr<partition_payload>              payload,
    std::shared_ptr<partition_location_information> part_location_information,
    const partition_type::type &p_type, const storage_tier_type::type &s_type,
    std::shared_ptr<pre_transaction_plan> plan, int destination,
    partition_column_identifier_map_t<std::shared_ptr<pre_action>> &pid_dep_map,
    partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &other_write_locked_partitions,
    const partition_column_identifier_unordered_set
        &original_write_partition_set,
    const partition_column_identifier_unordered_set
        &original_read_partition_set,
    partition_column_identifier_map_t<std::shared_ptr<
        partition_location_information>> &    partition_location_informations,
    const std::vector<site_load_information> &site_loads,
    const partition_column_identifier_map_t<
        std::vector<transaction_partition_accesses>> &write_sample_holder,
    const partition_column_identifier_map_t<
        std::vector<transaction_partition_accesses>> &read_sample_holder,
    cached_ss_stat_holder &                           cached_stats,
    query_arrival_cached_stats &cached_query_arrival_stats ) {

    std::vector<partition_column_identifier>        pids_to_replicate;
    std::vector<std::shared_ptr<partition_payload>> parts_to_replicate;
    std::vector<std::shared_ptr<partition_location_information>>
                                         parts_to_replicate_locations;
    std::vector<partition_type::type>    partition_types;
    std::vector<storage_tier_type::type> storage_types;

    pids_to_replicate.push_back( pid );
    parts_to_replicate.push_back( payload );
    parts_to_replicate_locations.push_back( part_location_information );
    partition_types.push_back( p_type );

    DCHECK_EQ(
        0, part_location_information->replica_locations_.count( destination ) );
    DCHECK_NE( destination, part_location_information->master_location_ );

    add_replicas_to_plan(
        pids_to_replicate, parts_to_replicate, parts_to_replicate_locations,
        partition_types, storage_types, plan,
        part_location_information->master_location_, destination, pid_dep_map,
        other_write_locked_partitions, original_write_partition_set,
        original_read_partition_set, partition_location_informations,
        site_loads, write_sample_holder, read_sample_holder, cached_stats,
        cached_query_arrival_stats );
}

void heuristic_site_evaluator::add_replicas_to_plan(
    const std::vector<partition_column_identifier> & pids_to_replicate,
    std::vector<std::shared_ptr<partition_payload>> &parts_to_replicate,
    std::vector<std::shared_ptr<partition_location_information>>
        &                                       parts_to_replicate_infos,
    const std::vector<partition_type::type> &   types_to_replicate,
    const std::vector<storage_tier_type::type> &storage_types_to_replicate,
    std::shared_ptr<pre_transaction_plan> plan, int master, int destination,
    partition_column_identifier_map_t<std::shared_ptr<pre_action>> &pid_dep_map,
    partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &other_write_locked_partitions,
    const partition_column_identifier_unordered_set
        &original_write_partition_set,
    const partition_column_identifier_unordered_set
        &original_read_partition_set,
    partition_column_identifier_map_t<std::shared_ptr<
        partition_location_information>> &    partition_location_informations,
    const std::vector<site_load_information> &site_loads,
    const partition_column_identifier_map_t<
        std::vector<transaction_partition_accesses>> &write_sample_holder,
    const partition_column_identifier_map_t<
        std::vector<transaction_partition_accesses>> &read_sample_holder,
    cached_ss_stat_holder &                           cached_stats,
    query_arrival_cached_stats &cached_query_arrival_stats ) {

    DVLOG( 40 ) << "Need to add replicas of:" << pids_to_replicate
                << ", to site:" << destination;

    DCHECK_EQ( pids_to_replicate.size(), parts_to_replicate_infos.size() );
    DCHECK_EQ( parts_to_replicate.size(), pids_to_replicate.size() );
    DCHECK_EQ( types_to_replicate.size(), pids_to_replicate.size() );
    DCHECK_EQ( storage_types_to_replicate.size(), pids_to_replicate.size() );

    std::vector<std::shared_ptr<pre_action>> deps;
    std::unordered_set<uint32_t>             add_deps;

    std::vector<add_replica_stats> add_stats;

    for( uint32_t pos = 0; pos < pids_to_replicate.size(); pos++ ) {
        const auto &pid = pids_to_replicate.at( pos );
        auto        payload = parts_to_replicate.at( pos );
        auto part_location_information = parts_to_replicate_infos.at( pos );

        DCHECK_EQ( 0, part_location_information->replica_locations_.count(
                          destination ) );
        DCHECK_NE( destination, part_location_information->master_location_ );
        DCHECK_EQ( master, part_location_information->master_location_ );

        add_to_deps( pid, pid_dep_map, deps, add_deps );

        add_replica_stats add_stat(
            std::get<1>( part_location_information->get_partition_type(
                part_location_information->master_location_ ) ),
            std::get<1>( part_location_information->get_storage_type(
                part_location_information->master_location_ ) ),
            types_to_replicate.at( pos ), storage_types_to_replicate.at( pos ),
            cost_model2_->normalize_contention_by_time(
                payload->get_contention() ),
            data_loc_tab_->get_stats()->get_and_cache_cell_widths(
                pid, cached_stats ),
            payload->get_num_rows() );

        add_stats.emplace_back( add_stat );
    }

    cost_model_prediction_holder model_holder =
        cost_model2_->predict_add_replica_execution_time(
            site_loads.at( master ).cpu_load_,
            site_loads.at( destination ).cpu_load_, add_stats );

    double expected_benefit = get_expected_benefit_of_adding_replicas(
        parts_to_replicate, parts_to_replicate_infos, types_to_replicate,
        storage_types_to_replicate, master, destination,
        other_write_locked_partitions, partition_location_informations,
        original_write_partition_set, original_read_partition_set, site_loads,
        write_sample_holder, read_sample_holder, cached_stats,
        cached_query_arrival_stats );

    double cost = -model_holder.get_prediction() +
                  ( configs_.horizon_weight_ * expected_benefit );

    auto add_plan = create_add_replica_partition_pre_action(
        destination, master, pids_to_replicate, types_to_replicate,
        storage_types_to_replicate, deps, cost, model_holder );

    DVLOG( k_heuristic_evaluator_log_level )
        << "PLAN: Add replica partitions: (" << pids_to_replicate
        << "), master:" << master << ", replica to add:" << destination;

    plan->add_pre_action_work_item( add_plan );

    DVLOG( 20 ) << "Adding replica " << pids_to_replicate
                << ": dep add action:" << add_plan->id_;

    DVLOG( k_heuristic_evaluator_log_level )
        << "PLAN: Add replica partitions: (" << pids_to_replicate
        << "), master:" << master << ", replica to add:" << destination
        << ", overall cost:" << cost
        << " ( upfront cost:" << model_holder.get_prediction()
        << ", expected_benefit:" << expected_benefit << " )";

    for( auto pid : pids_to_replicate ) {
        pid_dep_map[pid] = add_plan;
    }
}

void heuristic_site_evaluator::add_to_deps(
    const partition_column_identifier &pid,
    const partition_column_identifier_map_t<std::shared_ptr<pre_action>>
        &                                     pid_dep_map,
    std::vector<std::shared_ptr<pre_action>> &deps,
    std::unordered_set<uint32_t> &            add_deps ) {
    auto found_dep = pid_dep_map.find( pid );
    if( found_dep != pid_dep_map.end() ) {
        if( add_deps.count( found_dep->second->id_ ) == 0 ) {
            deps.push_back( found_dep->second );
            add_deps.insert( found_dep->second->id_ );
        }
    }
}

shared_left_right_merge_plan_state
    heuristic_site_evaluator::merge_partition_if_beneficial(
        std::shared_ptr<partition_payload>              partition_to_merge,
        std::shared_ptr<partition_location_information> location_information,
        double avg_write_count, double std_dev_write_count,
        partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>> &partition_location_informations,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &other_write_locked_partitions,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &write_sample_holder,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &read_sample_holder,
        std::unordered_map<storage_tier_type::type, std::vector<double>>
            &  storage_sizes,
        double ori_storage_imbalance, cached_ss_stat_holder &cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats ) {

    shared_left_right_merge_plan_state merge_res;

    merge_res.partition_ = partition_to_merge;
    merge_res.location_information_ = location_information;

    merge_res.merge_left_row_ = merge_partition_left_row_if_beneficial(
        partition_to_merge, location_information, avg_write_count,
        std_dev_write_count, partition_location_informations,
        other_write_locked_partitions, write_sample_holder, read_sample_holder,
        storage_sizes, ori_storage_imbalance, cached_stats,
        cached_query_arrival_stats );
    merge_res.merge_right_row_ = merge_partition_right_row_if_beneficial(
        partition_to_merge, location_information, avg_write_count,
        std_dev_write_count, partition_location_informations,
        other_write_locked_partitions, write_sample_holder, read_sample_holder,
        storage_sizes, ori_storage_imbalance, cached_stats,
        cached_query_arrival_stats );
    merge_res.merge_left_col_ = merge_partition_left_col_if_beneficial(
        partition_to_merge, location_information, avg_write_count,
        std_dev_write_count, partition_location_informations,
        other_write_locked_partitions, write_sample_holder, read_sample_holder,
        storage_sizes, ori_storage_imbalance, cached_stats,
        cached_query_arrival_stats );
    merge_res.merge_right_col_ = merge_partition_right_col_if_beneficial(
        partition_to_merge, location_information, avg_write_count,
        std_dev_write_count, partition_location_informations,
        other_write_locked_partitions, write_sample_holder, read_sample_holder,
        storage_sizes, ori_storage_imbalance, cached_stats,
        cached_query_arrival_stats );

    return merge_res;
}

shared_merge_plan_state
    heuristic_site_evaluator::merge_partition_left_row_if_beneficial(
        std::shared_ptr<partition_payload> partition_to_merge,
        std::shared_ptr<partition_location_information>
               to_merge_location_information,
        double avg_write_count, double std_dev_write_count,
        partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>> &partition_location_informations,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &other_write_locked_partitions,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &write_sample_holder,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &read_sample_holder,
        std::unordered_map<storage_tier_type::type, std::vector<double>>
            &  storage_sizes,
        double ori_storage_imbalance, cached_ss_stat_holder &cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats ) {

    shared_merge_plan_state merged_res;
    merged_res.mergeable_ = false;

    if( partition_to_merge->identifier_.partition_start == 0 ) {
        merged_res.mergeable_ = false;
        return merged_res;
    }

    if( !configs_.allow_horizontal_partitioning_ ) {
        return merged_res;
    }

    cell_key ck;
    ck.table_id = partition_to_merge->identifier_.table_id;
    ck.row_id = partition_to_merge->identifier_.partition_start - 1;
    ck.col_id = partition_to_merge->identifier_.column_start;

    return merge_partition_if_beneficial_with_ck(
        partition_to_merge, to_merge_location_information, avg_write_count,
        std_dev_write_count, ck, false /* is vertical split*/,
        false /* is left */, partition_location_informations,
        other_write_locked_partitions, write_sample_holder, read_sample_holder,
        storage_sizes, ori_storage_imbalance, cached_stats,
        cached_query_arrival_stats );
}
shared_merge_plan_state
    heuristic_site_evaluator::merge_partition_right_row_if_beneficial(
        std::shared_ptr<partition_payload> partition_to_merge,
        std::shared_ptr<partition_location_information>
               to_merge_location_information,
        double avg_write_count, double std_dev_write_count,
        partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>> &partition_location_informations,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &other_write_locked_partitions,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &write_sample_holder,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &read_sample_holder,
        std::unordered_map<storage_tier_type::type, std::vector<double>>
            &  storage_sizes,
        double ori_storage_imbalance, cached_ss_stat_holder &cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats ) {
    shared_merge_plan_state merged_res;
    merged_res.mergeable_ = false;

    if( partition_to_merge->identifier_.partition_end == INT64_MAX ) {
        merged_res.mergeable_ = false;
        return merged_res;
    }

    if( !configs_.allow_horizontal_partitioning_ ) {
        return merged_res;
    }

    cell_key ck;
    ck.table_id = partition_to_merge->identifier_.table_id;
    ck.row_id = partition_to_merge->identifier_.partition_end + 1;
    ck.row_id = partition_to_merge->identifier_.column_end;

    return merge_partition_if_beneficial_with_ck(
        partition_to_merge, to_merge_location_information, avg_write_count,
        std_dev_write_count, ck, false /* is vertical*/, true /* is left*/,
        partition_location_informations, other_write_locked_partitions,
        write_sample_holder, read_sample_holder, storage_sizes,
        ori_storage_imbalance, cached_stats, cached_query_arrival_stats );
}
shared_merge_plan_state
    heuristic_site_evaluator::merge_partition_left_col_if_beneficial(
        std::shared_ptr<partition_payload> partition_to_merge,
        std::shared_ptr<partition_location_information>
               to_merge_location_information,
        double avg_write_count, double std_dev_write_count,
        partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>> &partition_location_informations,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &other_write_locked_partitions,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &write_sample_holder,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &read_sample_holder,
        std::unordered_map<storage_tier_type::type, std::vector<double>>
            &  storage_sizes,
        double ori_storage_imbalance, cached_ss_stat_holder &cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats ) {

    shared_merge_plan_state merged_res;
    merged_res.mergeable_ = false;

    if( partition_to_merge->identifier_.column_start == 0 ) {
        merged_res.mergeable_ = false;
        return merged_res;
    }

    if( !configs_.allow_vertical_partitioning_ ) {
        return merged_res;
    }

    cell_key ck;
    ck.table_id = partition_to_merge->identifier_.table_id;
    ck.row_id = partition_to_merge->identifier_.partition_start;
    ck.col_id = partition_to_merge->identifier_.column_start - 1;

    return merge_partition_if_beneficial_with_ck(
        partition_to_merge, to_merge_location_information, avg_write_count,
        std_dev_write_count, ck, true /* vertical*/, false /* is left */,
        partition_location_informations, other_write_locked_partitions,
        write_sample_holder, read_sample_holder, storage_sizes,
        ori_storage_imbalance, cached_stats, cached_query_arrival_stats );
}
shared_merge_plan_state
    heuristic_site_evaluator::merge_partition_right_col_if_beneficial(
        std::shared_ptr<partition_payload> partition_to_merge,
        std::shared_ptr<partition_location_information>
               to_merge_location_information,
        double avg_write_count, double std_dev_write_count,
        partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>> &partition_location_informations,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &other_write_locked_partitions,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &write_sample_holder,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &read_sample_holder,
        std::unordered_map<storage_tier_type::type, std::vector<double>>
            &  storage_sizes,
        double ori_storage_imbalance, cached_ss_stat_holder &cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats ) {
    shared_merge_plan_state merged_res;
    merged_res.mergeable_ = false;

    uint32_t num_columns =
        data_loc_tab_
            ->get_table_metadata( partition_to_merge->identifier_.table_id )
            .num_columns_;
    if( (uint32_t) partition_to_merge->identifier_.column_end ==
        num_columns - 1 ) {
        merged_res.mergeable_ = false;
        return merged_res;
    }
    if( !configs_.allow_vertical_partitioning_ ) {
        return merged_res;
    }

    cell_key ck;
    ck.table_id = partition_to_merge->identifier_.table_id;
    ck.row_id = partition_to_merge->identifier_.partition_end;
    ck.row_id = partition_to_merge->identifier_.column_end + 1;

    return merge_partition_if_beneficial_with_ck(
        partition_to_merge, to_merge_location_information, avg_write_count,
        std_dev_write_count, ck, true /* is vertical*/, true /* is left*/,
        partition_location_informations, other_write_locked_partitions,
        write_sample_holder, read_sample_holder, storage_sizes,
        ori_storage_imbalance, cached_stats, cached_query_arrival_stats );
}

double heuristic_site_evaluator::get_merge_benefit_from_samples(
    const partition_column_identifier &low_pid,
    const partition_column_identifier &high_pid, const merge_stats &merge_stat,
    const partition_column_identifier_map_t<
        std::vector<transaction_partition_accesses>> &write_sample_holder,
    const partition_column_identifier_map_t<
        std::vector<transaction_partition_accesses>> &read_sample_holder,
    cached_ss_stat_holder &                           cached_stats,
    query_arrival_cached_stats &cached_query_arrival_stats ) {
    double merge_benefit = 0;

    partition_column_identifier combined_pid =
        create_partition_column_identifier(
            low_pid.table_id, low_pid.partition_start, high_pid.partition_end,
            low_pid.column_start, high_pid.column_end );

    merge_benefit +=
        get_partitioning_benefit_from_samples(
            low_pid, combined_pid, low_pid, high_pid, merge_stat.merge_type_,
            merge_stat.left_type_, merge_stat.right_type_,
            merge_stat.merge_storage_type_, merge_stat.left_storage_type_,
            merge_stat.right_storage_type_, merge_stat.left_contention_,
            merge_stat.right_contention_, write_sample_holder,
            read_sample_holder, cached_stats, cached_query_arrival_stats )
            .get_merge_benefit();
    merge_benefit +=
        get_partitioning_benefit_from_samples(
            high_pid, combined_pid, low_pid, high_pid, merge_stat.merge_type_,
            merge_stat.left_type_, merge_stat.right_type_,
            merge_stat.merge_storage_type_, merge_stat.left_storage_type_,
            merge_stat.right_storage_type_, merge_stat.left_contention_,
            merge_stat.right_contention_, write_sample_holder,
            read_sample_holder, cached_stats, cached_query_arrival_stats )
            .get_merge_benefit();
    merge_benefit +=
        get_partitioning_benefit_from_samples(
            combined_pid, combined_pid, low_pid, high_pid,
            merge_stat.merge_type_, merge_stat.left_type_,
            merge_stat.right_type_, merge_stat.merge_storage_type_,
            merge_stat.left_storage_type_, merge_stat.right_storage_type_,
            merge_stat.left_contention_, merge_stat.right_contention_,
            write_sample_holder, read_sample_holder, cached_stats,
            cached_query_arrival_stats )
            .get_merge_benefit();
    return merge_benefit;
}

double heuristic_site_evaluator::get_default_expected_merge_benefit(
    const partition_column_identifier &merged_pid,
    const partition_column_identifier &low_pid,
    const partition_column_identifier &high_pid, const merge_stats &merge_stat,
    cached_ss_stat_holder &     cached_stats,
    query_arrival_cached_stats &cached_query_arrival_stats ) {
    double total_contention =
        merge_stat.left_contention_ + merge_stat.right_contention_;

    double low_prob = 0;
    double high_prob = 0;

    if( total_contention > 0 ) {
        low_prob = merge_stat.left_contention_ / total_contention;
        high_prob = merge_stat.right_contention_ / total_contention;
    }

    return get_default_expected_partitioning_benefit(
               merged_pid, low_pid, high_pid, merge_stat.merge_type_,
               merge_stat.left_type_, merge_stat.right_type_,
               merge_stat.merge_storage_type_, merge_stat.left_storage_type_,
               merge_stat.right_storage_type_, merge_stat.left_contention_,
               low_prob, merge_stat.right_contention_, high_prob,
               true /*is_merge*/, cached_stats, cached_query_arrival_stats )
        .get_merge_benefit();
}

double heuristic_site_evaluator::get_expected_merge_benefit(
    const partition_column_identifier &left_pid,
    const partition_column_identifier &right_pid, const merge_stats &merge_stat,
    const partition_column_identifier_map_t<
        std::vector<transaction_partition_accesses>> &write_sample_holder,
    const partition_column_identifier_map_t<
        std::vector<transaction_partition_accesses>> &read_sample_holder,
    cached_ss_stat_holder &                           cached_stats,
    query_arrival_cached_stats &cached_query_arrival_stats ) {

    double default_benefit = get_default_expected_merge_benefit(
        create_partition_column_identifier(
            left_pid.table_id, left_pid.partition_start,
            right_pid.partition_end, left_pid.column_start,
            right_pid.column_end ),
        left_pid, right_pid, merge_stat, cached_stats,
        cached_query_arrival_stats );

    double merge_benefit = get_merge_benefit_from_samples(
        left_pid, right_pid, merge_stat, write_sample_holder,
        read_sample_holder, cached_stats, cached_query_arrival_stats );

    return merge_benefit +
           ( configs_.default_horizon_benefit_weight_ * default_benefit );
}

shared_merge_plan_state
    heuristic_site_evaluator::merge_partition_if_beneficial_with_ck(
        std::shared_ptr<partition_payload> partition_to_merge,
        std::shared_ptr<partition_location_information>
               to_merge_location_information,
        double avg_write_count, double std_dev_write_count,
        const ::cell_key other_ck, bool is_vertical_merge, bool ori_is_left,
        partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>> &partition_location_informations,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &other_write_locked_partitions,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &write_sample_holder,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &read_sample_holder,
        std::unordered_map<storage_tier_type::type, std::vector<double>>
            &  storage_sizes,
        double ori_storage_imbalance, cached_ss_stat_holder &cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats ) {

    partition_column_identifier_unordered_set no_merge_partitions;

    shared_merge_plan_state merge_res;
    merge_res.mergeable_ = false;
    merge_res.is_vertical_ = is_vertical_merge;

    partition_lock_mode lock_mode = partition_lock_mode::no_lock;

    auto partition_to_consider =
        data_loc_tab_->get_partition( other_ck, lock_mode );
    if( !eligible_for_merging( partition_to_consider, no_merge_partitions ) ) {
        return merge_res;
    }
    DCHECK( partition_to_consider );

    DVLOG( 40 ) << "Checking if we can merge:"
                << partition_to_merge->identifier_
                << ", with:" << partition_to_consider->identifier_;

    if( !can_merge_partition_identifiers( partition_to_consider->identifier_,
                                          partition_to_merge->identifier_ ) ) {
        return merge_res;
    }

    if( !within_merge_ratio( partition_to_consider, avg_write_count ) ) {
        return merge_res;
    }

    auto to_consider_location_information =
        get_location_information_for_partition_or_from_payload(
            partition_to_consider, partition_location_informations, lock_mode );
    if( to_consider_location_information == nullptr ) {
        return merge_res;
    }
    DCHECK( to_consider_location_information );
    DCHECK_NE( k_unassigned_master,
               to_consider_location_information->master_location_ );

    if( !can_merge_partition_location_informations(
            *to_consider_location_information,
            *to_merge_location_information ) ) {
        return merge_res;
    }

    merge_res.mergeable_ = true;
    merge_res.other_ck_ = other_ck;
    merge_res.other_part_ = partition_to_consider->identifier_;
    // this will be iterated over later
    merge_res.merge_type_ = partition_type::type::ROW;
    merge_res.merge_storage_type_ = storage_tier_type::type::MEMORY;

    auto left_part = partition_to_consider;
    auto left_loc_info = to_consider_location_information;
    auto right_part = partition_to_merge;
    auto right_loc_info = to_merge_location_information;

    if( !ori_is_left ) {
        left_part = partition_to_merge;
        left_loc_info = to_merge_location_information;
        right_part = partition_to_consider;
        left_loc_info = to_consider_location_information;
    }

    // we don't care for split / merge and it differs per site
    double site_load = 0;

    double low_partition_sizes = left_part->get_num_rows();
    double high_partition_sizes = right_part->get_num_rows();

    double low_contention = cost_model2_->normalize_contention_by_time(
        left_part->get_contention() );
    double high_contention = cost_model2_->normalize_contention_by_time(
        right_part->get_contention() );

    std::vector<double> left_cell_widths =
        data_loc_tab_->get_stats()->get_and_cache_cell_widths(
            left_part->identifier_, cached_stats );
    std::vector<double> right_cell_widths =
        data_loc_tab_->get_stats()->get_and_cache_cell_widths(
            right_part->identifier_, cached_stats );

    merge_stats merge_stat(
        std::get<1>( left_loc_info->get_partition_type(
            left_loc_info->master_location_ ) ),
        std::get<1>( right_loc_info->get_partition_type(
            right_loc_info->master_location_ ) ),
        merge_res.merge_type_, std::get<1>( left_loc_info->get_storage_type(
                                   left_loc_info->master_location_ ) ),
        std::get<1>( right_loc_info->get_storage_type(
            right_loc_info->master_location_ ) ),
        merge_res.merge_storage_type_, low_contention, high_contention,
        left_cell_widths, right_cell_widths, low_partition_sizes,
        high_partition_sizes );

    partition_type::type found_type =
        configs_.acceptable_partition_types_.at( 0 );
    storage_tier_type::type found_storage_type =
        configs_.acceptable_storage_types_.at( 0 );

    double                  max_cost = -DBL_MAX;
    shared_merge_plan_state max_plan;

    double left_storage_size =
        storage_stats_->get_partition_size( left_part->identifier_ );
    double right_storage_size =
        storage_stats_->get_partition_size( right_part->identifier_ );

    auto considered_types = enumerator_holder_->get_merge_type( merge_stat );

    for( const auto &merge_type : considered_types ) {
        shared_merge_plan_state loc_merge_res = merge_res;
        merge_stat.merge_type_ = std::get<0>( merge_type );
        merge_stat.merge_storage_type_ = std::get<1>( merge_type );

        loc_merge_res.merge_type_ = std::get<0>( merge_type );
        loc_merge_res.merge_storage_type_ = std::get<1>( merge_type );

        get_merge_cost( loc_merge_res, merge_stat, left_part->identifier_,
                        right_part->identifier_, site_load, write_sample_holder,
                        read_sample_holder, left_storage_size,
                        right_storage_size, ori_storage_imbalance,
                        storage_sizes,
                        to_merge_location_information->master_location_,
                        cached_stats, cached_query_arrival_stats );

        if( loc_merge_res.cost_ > max_cost ) {
            max_cost = loc_merge_res.cost_;
            found_type = std::get<0>( merge_type );
            found_storage_type = std::get<1>( merge_type );
            max_plan = loc_merge_res;
        }
    }
    merge_stat.merge_type_ = found_type;
    merge_stat.merge_storage_type_ = found_storage_type;

    merge_res.merge_stats_ = merge_stat;

    double outlier_write_count = std::max(
        0.0, avg_write_count - ( configs_.outlier_repartition_std_dev_ *
                                 std_dev_write_count ) );
    double write_accesses = (double) partition_to_merge->write_accesses_;

    DVLOG( 20 ) << "merge_res.cost_:" << merge_res.cost_
                << ", outlier_write_count:" << outlier_write_count
                << ", write_count:" << write_accesses;

    if( ( merge_res.cost_ < 0 ) and ( write_accesses > outlier_write_count ) ) {
        merge_res.mergeable_ = false;
    }

    DVLOG( k_heuristic_evaluator_log_level )
        << "PLAN: Merge partitions: (" << partition_to_consider->identifier_
        << ", " << partition_to_merge->identifier_
        << "), mergeable: " << merge_res.mergeable_
        << ", merge_ck:" << merge_res.other_ck_
        << ", overall cost:" << merge_res.cost_ << " ( upfront cost:"
        << merge_res.upfront_cost_prediction_.get_prediction()
        << " ), write_count:" << write_accesses
        << ", outlier_write_count:" << outlier_write_count
        << " ( avg_write_count:" << avg_write_count
        << ", std_dev_write_count:" << std_dev_write_count << " )";

    return merge_res;
}

void heuristic_site_evaluator::get_merge_cost(
    shared_merge_plan_state &merge_res, const merge_stats &merge_stat,
    const partition_column_identifier &left_pid,
    const partition_column_identifier &right_pid, double site_load,
    const partition_column_identifier_map_t<
        std::vector<transaction_partition_accesses>> &write_sample_holder,
    const partition_column_identifier_map_t<
        std::vector<transaction_partition_accesses>> &read_sample_holder,
    double left_storage_size, double right_storage_size,
    double ori_storage_imbalance,
    std::unordered_map<storage_tier_type::type, std::vector<double>>
        &    storage_sizes,
    uint32_t master_site, cached_ss_stat_holder &cached_stats,
    query_arrival_cached_stats &cached_query_arrival_stats ) {

    if( merge_stat.left_storage_type_ != merge_stat.merge_storage_type_ ) {
        storage_sizes.at( merge_stat.left_storage_type_ ).at( master_site ) -=
            left_storage_size;
        storage_sizes.at( merge_stat.merge_storage_type_ ).at( master_site ) +=
            left_storage_size;
    }
    if( merge_stat.right_storage_type_ != merge_stat.merge_storage_type_ ) {
        storage_sizes.at( merge_stat.right_storage_type_ ).at( master_site ) -=
            right_storage_size;
        storage_sizes.at( merge_stat.merge_storage_type_ ).at( master_site ) +=
            right_storage_size;
    }

    double storage_cost_change = get_change_in_storage_imbalance_costs(
        get_site_storage_imbalance_cost( storage_sizes ),
        ori_storage_imbalance );

    if( merge_stat.left_storage_type_ != merge_stat.merge_storage_type_ ) {
        storage_sizes.at( merge_stat.left_storage_type_ ).at( master_site ) +=
            left_storage_size;
        storage_sizes.at( merge_stat.merge_storage_type_ ).at( master_site ) -=
            left_storage_size;
    }
    if( merge_stat.right_storage_type_ != merge_stat.merge_storage_type_ ) {
        storage_sizes.at( merge_stat.right_storage_type_ ).at( master_site ) +=
            right_storage_size;
        storage_sizes.at( merge_stat.merge_storage_type_ ).at( master_site ) -=
            right_storage_size;
    }

    // update merge_point
    if( merge_res.is_vertical_ ) {
        merge_res.upfront_cost_prediction_ =
            cost_model2_->predict_vertical_merge_execution_time( site_load,
                                                                 merge_stat );
    } else {
        merge_res.upfront_cost_prediction_ =
            cost_model2_->predict_horizontal_merge_execution_time( site_load,
                                                                   merge_stat );
    }

    double expected_benefit = get_expected_merge_benefit(
        left_pid, right_pid, merge_stat, write_sample_holder,
        read_sample_holder, cached_stats, cached_query_arrival_stats );

    double expected_decrease_in_remastering =
        get_expected_amount_of_remastering_from_merge( merge_stat );

    merge_res.cost_ =
        ( configs_.upfront_cost_weight_ *
          -merge_res.upfront_cost_prediction_.get_prediction() ) +
        ( expected_decrease_in_remastering *
          configs_.repartition_remaster_horizon_weight_ ) +
        ( configs_.horizon_weight_ * expected_benefit ) +
        ( configs_.horizon_storage_weight_ * storage_cost_change );

    DVLOG( 20 ) << "Merge partitions: (" << left_pid << ", " << right_pid
                << ", merge_ck:" << merge_res.other_ck_
                << ", overall cost:" << merge_res.cost_ << " ( upfront cost:"
                << merge_res.upfront_cost_prediction_.get_prediction()
                << ", expected_benefit:" << expected_benefit
                << ", storage_cost_change:" << storage_cost_change
                << ", expected_decrease_in_remastering:"
                << expected_decrease_in_remastering << " )";
}

bool heuristic_site_evaluator::can_merge_partition_identifiers(
    const partition_column_identifier &a,
    const partition_column_identifier &b ) const {
    if( a.table_id != b.table_id ) {
        return false;
    }
    if( ( a.partition_start == b.partition_start ) and
        ( a.partition_end == b.partition_end ) ) {
        if( a.column_start == b.column_end + 1 ) {
            return true;
        } else if( a.column_end + 1 == b.column_start ) {
            return true;
        }
    } else if( ( a.column_start == b.column_start ) and
               ( a.column_end == b.column_end ) ) {
        if( a.partition_start == b.partition_end + 1 ) {
            return true;
        } else if( a.partition_end + 1 == b.partition_start ) {
            return true;
        }
    }

    return false;
}

bool heuristic_site_evaluator::eligible_for_merging(
    std::shared_ptr<partition_payload>               partition,
    const partition_column_identifier_unordered_set &no_merge_partitions ) {
    if( partition == nullptr ) {
        return false;
    }
    return ( no_merge_partitions.count( partition->identifier_ ) == 0 );
}

void heuristic_site_evaluator::merge_partitions_and_add_to_plan(
    int                                                    site,
    const std::vector<shared_left_right_merge_plan_state> &partitions_to_merge,
    partition_column_identifier_map_t<std::shared_ptr<
        partition_location_information>> &partition_location_informations,
    partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &write_partitions_map,
    partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &                                  read_partitions_map,
    std::shared_ptr<pre_transaction_plan> &plan,
    partition_column_identifier_map_t<std::shared_ptr<pre_action>> &pid_dep_map,
    partition_column_identifier_map_t<std::unordered_map<
        uint32_t, partition_type::type>> &created_partition_types,
    partition_column_identifier_map_t<std::unordered_map<
        uint32_t, storage_tier_type::type>> &created_storage_types,
    std::unordered_map<storage_tier_type::type, std::vector<double>>
        &                                      storage_sizes,
    partition_column_identifier_to_ckrs &      write_modifiable_pids_to_ckrs,
    partition_column_identifier_to_ckrs &      read_modifiable_pids_to_ckrs,
    partition_column_identifier_unordered_set &no_merge_partitions,
    partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &    other_write_locked_partitions,
    uint64_t avg_write_count ) {

    for( const auto &partition_to_merge_plan : partitions_to_merge ) {
        auto partition_to_merge = partition_to_merge_plan.partition_;
        auto location_information =
            partition_to_merge_plan.location_information_;

        if( !eligible_for_merging( partition_to_merge, no_merge_partitions ) ) {
            continue;
        }
        std::map<double, std::vector<shared_merge_plan_state>> found_merges;
        found_merges[partition_to_merge_plan.merge_left_row_.cost_]
            .emplace_back( partition_to_merge_plan.merge_left_row_ );
        found_merges[partition_to_merge_plan.merge_right_row_.cost_]
            .emplace_back( partition_to_merge_plan.merge_right_row_ );
        found_merges[partition_to_merge_plan.merge_left_col_.cost_]
            .emplace_back( partition_to_merge_plan.merge_left_col_ );
        found_merges[partition_to_merge_plan.merge_right_col_.cost_]
            .emplace_back( partition_to_merge_plan.merge_right_col_ );

        bool found = false;
        for( auto &merge_found : found_merges ) {
            for( auto &merge_plan : merge_found.second ) {
                bool merged = merge_if_possible(
                    site, partition_to_merge, location_information, merge_plan,
                    partition_location_informations, write_partitions_map,
                    read_partitions_map, plan, pid_dep_map,
                    created_partition_types, created_storage_types,
                    storage_sizes, write_modifiable_pids_to_ckrs,
                    read_modifiable_pids_to_ckrs, no_merge_partitions,
                    other_write_locked_partitions );
                if( merged ) {
                    found = true;
                    break;
                }
            }
            if( found ) {
                break;
            }
        }
    }
}

bool heuristic_site_evaluator::merge_if_possible(
    int site, std::shared_ptr<partition_payload> partition_to_merge,
    std::shared_ptr<partition_location_information>
                                   to_merge_location_information,
    const shared_merge_plan_state &merge_plan_state,
    partition_column_identifier_map_t<std::shared_ptr<
        partition_location_information>> &partition_location_informations,
    partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &write_partitions_map,
    partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &                                  read_partitions_map,
    std::shared_ptr<pre_transaction_plan> &plan,
    partition_column_identifier_map_t<std::shared_ptr<pre_action>> &pid_dep_map,
    partition_column_identifier_map_t<std::unordered_map<
        uint32_t, partition_type::type>> &created_partition_types,
    partition_column_identifier_map_t<std::unordered_map<
        uint32_t, storage_tier_type::type>> &created_storage_types,
    std::unordered_map<storage_tier_type::type, std::vector<double>>
        &                                      storage_sizes,
    partition_column_identifier_to_ckrs &      write_modifiable_pids_to_ckrs,
    partition_column_identifier_to_ckrs &      read_modifiable_pids_to_ckrs,
    partition_column_identifier_unordered_set &no_merge_partitions,
    partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &other_write_locked_partitions ) {

    if( !merge_plan_state.mergeable_ ) {
        return false;
    }

    partition_lock_mode lock_mode = partition_lock_mode::try_lock;

    auto partition_to_consider_lock_entry =
        get_partition_from_data_location_table_or_already_locked(
            merge_plan_state.other_part_, other_write_locked_partitions,
            lock_mode );
    auto partition_to_consider =
        std::get<0>( partition_to_consider_lock_entry );

    if( !eligible_for_merging( partition_to_consider, no_merge_partitions ) ) {
        unlock_if_new( partition_to_consider_lock_entry,
                       other_write_locked_partitions,
                       partition_location_informations, lock_mode );
        return false;
    }
    DCHECK( partition_to_consider );

    DVLOG( 40 ) << "Checking if we can merge:"
                << partition_to_merge->identifier_
                << ", with:" << partition_to_consider->identifier_;

    auto to_consider_location_information =
        get_location_information_for_partition_or_from_payload(
            partition_to_consider, partition_location_informations, lock_mode );
    if( to_consider_location_information == nullptr ) {
        unlock_if_new( partition_to_consider_lock_entry,
                       other_write_locked_partitions,
                       partition_location_informations, lock_mode );
        return false;
    }
    DCHECK( to_consider_location_information );
    DCHECK_NE( k_unassigned_master,
               to_consider_location_information->master_location_ );
    if( !can_merge_partition_identifiers(
            partition_to_merge->identifier_,
            partition_to_consider->identifier_ ) ) {
        unlock_if_new( partition_to_consider_lock_entry,
                       other_write_locked_partitions,
                       partition_location_informations, lock_mode );
        return false;
    }

    if( !can_merge_partition_location_informations(
            *to_consider_location_information,
            *to_merge_location_information ) ) {
        unlock_if_new( partition_to_consider_lock_entry,
                       other_write_locked_partitions,
                       partition_location_informations, lock_mode );
        return false;
    }

    add_merge_to_plan(
        site, partition_to_merge, partition_to_consider,
        to_merge_location_information, merge_plan_state.merge_type_,
        merge_plan_state.merge_storage_type_, merge_plan_state.cost_,
        merge_plan_state.upfront_cost_prediction_,
        merge_plan_state.merge_stats_, write_partitions_map,
        read_partitions_map, plan, pid_dep_map, created_partition_types,
        created_storage_types, storage_sizes, write_modifiable_pids_to_ckrs,
        read_modifiable_pids_to_ckrs, no_merge_partitions );

    return true;
}

void heuristic_site_evaluator::add_merge_to_plan(
    int site, std::shared_ptr<partition_payload> partition_to_merge,
    std::shared_ptr<partition_payload>              partition_to_consider,
    std::shared_ptr<partition_location_information> location_information,
    const partition_type::type &                    merge_type,
    const storage_tier_type::type &merge_storage_type, double cost,
    const cost_model_prediction_holder &model_holder,
    const merge_stats &                 merge_stat,
    partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &write_partitions_map,
    partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &                                  read_partitions_map,
    std::shared_ptr<pre_transaction_plan> &plan,
    partition_column_identifier_map_t<std::shared_ptr<pre_action>> &pid_dep_map,
    partition_column_identifier_map_t<std::unordered_map<
        uint32_t, partition_type::type>> &created_partition_types,
    partition_column_identifier_map_t<std::unordered_map<
        uint32_t, storage_tier_type::type>> &created_storage_types,
    std::unordered_map<storage_tier_type::type, std::vector<double>>
        &                                      storage_sizes,
    partition_column_identifier_to_ckrs &      write_modifiable_pids_to_ckrs,
    partition_column_identifier_to_ckrs &      read_modifiable_pids_to_ckrs,
    partition_column_identifier_unordered_set &no_merge_partitions ) {

    DCHECK_EQ( partition_to_merge->identifier_.table_id,
               partition_to_consider->identifier_.table_id );

    auto left_pid = partition_to_merge->identifier_;
    auto right_pid = partition_to_consider->identifier_;
    if( left_pid.partition_start > right_pid.partition_start ) {
        auto swap_pid = left_pid;
        left_pid = right_pid;
        right_pid = swap_pid;
    }
    if( left_pid.column_start > right_pid.column_start ) {
        auto swap_pid = left_pid;
        left_pid = right_pid;
        right_pid = swap_pid;
    }

    if( ( left_pid.partition_start == right_pid.partition_start ) and
        ( left_pid.partition_end == right_pid.partition_end ) ) {
        DCHECK_EQ( left_pid.column_end + 1, right_pid.column_start );
    } else if( ( left_pid.column_start == right_pid.column_start ) and
               ( left_pid.column_end == right_pid.column_end ) ) {
        DCHECK_EQ( left_pid.partition_end + 1, right_pid.partition_start );
    } else {
        DLOG( FATAL ) << "Inelegible merge";
    }

    partition_column_identifier merged_pid;
    merged_pid.table_id = left_pid.table_id;
    merged_pid.partition_start = left_pid.partition_start;
    merged_pid.partition_end = right_pid.partition_end;
    merged_pid.column_start = left_pid.column_start;
    merged_pid.column_end = right_pid.column_end;

    std::vector<std::shared_ptr<pre_action>> deps;

    std::vector<partition_column_identifier> pids = {left_pid, right_pid};

    std::unordered_set<uint32_t> add_deps;

    bool is_background =
        ( (uint32_t) site != location_information->master_location_ );

    for( auto pid : pids ) {
        add_to_deps( pid, pid_dep_map, deps, add_deps );

        no_merge_partitions.emplace( pid );
    }

    created_partition_types[merged_pid]
                           [location_information->master_location_] =
                               merge_type;
    created_storage_types[merged_pid][location_information->master_location_] =
        merge_storage_type;

    // update the storage type
    if( merge_storage_type != merge_stat.left_storage_type_ ) {
        double left_storage_size =
            storage_stats_->get_partition_size( left_pid );
        storage_sizes.at( merge_stat.left_storage_type_ )
            .at( location_information->master_location_ ) -= left_storage_size;
        storage_sizes.at( merge_storage_type )
            .at( location_information->master_location_ ) += left_storage_size;
    }
    if( merge_storage_type != merge_stat.right_storage_type_ ) {
        double right_storage_size =
            storage_stats_->get_partition_size( right_pid );
        storage_sizes.at( merge_stat.right_storage_type_ )
            .at( location_information->master_location_ ) -= right_storage_size;
        storage_sizes.at( merge_storage_type )
            .at( location_information->master_location_ ) += right_storage_size;
    }

    // MTODO-HTAP not sure which location info is the correct merge state here
    for( const auto &entry : location_information->replica_locations_ ) {
        created_partition_types[merged_pid][entry] =
            std::get<1>( location_information->get_partition_type( entry ) );
        created_storage_types[merged_pid][entry] =
            std::get<1>( location_information->get_storage_type( entry ) );
    }

    auto merge_action = create_merge_partition_pre_action(
        location_information->master_location_, left_pid, right_pid, merged_pid,
        merge_type, merge_storage_type, deps,
        cost /* 0.0 No cost, the heuristic says so */, model_holder,
        // just go with the partition part of our write set as the update prop
        location_information->update_destination_slot_ );

    DVLOG( 20 ) << "Added merge partition: " << left_pid << ", " << right_pid
                << " = " << merged_pid << " to candidate plan.";

    no_merge_partitions.emplace( merged_pid );

    plan->add_to_partition_mappings( partition_to_consider );
    if( is_background ) {
        DVLOG( k_heuristic_evaluator_log_level )
            << "BACKGROUND PLAN: Merge partitions: (" << left_pid << ", "
            << right_pid << "), merge_pid:" << merged_pid;

        plan->add_background_work_item( merge_action );
        plan->stat_decision_holder_->add_merge_type_decision(
            merge_stat, std::make_tuple<>( merge_type, merge_storage_type ) );

        return;
    }

    plan->add_pre_action_work_item( merge_action );
    plan->stat_decision_holder_->add_merge_type_decision(
        merge_stat, std::make_tuple<>( merge_type, merge_storage_type ) );

    update_partition_maps_from_merge( partition_to_merge, merged_pid, left_pid,
                                      right_pid, read_partitions_map,
                                      read_modifiable_pids_to_ckrs );
    update_partition_maps_from_merge( partition_to_merge, merged_pid, left_pid,
                                      right_pid, read_partitions_map,
                                      read_modifiable_pids_to_ckrs );

    pid_dep_map[merged_pid] = merge_action;
}

void heuristic_site_evaluator::update_partition_maps_from_merge(
    std::shared_ptr<partition_payload> &partition_to_merge,
    const partition_column_identifier & merged_pid,
    const partition_column_identifier & left_pid,
    const partition_column_identifier & right_pid,
    partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
                                         partitions_map,
    partition_column_identifier_to_ckrs &modifiable_pids_to_ckrs ) const {

    std::vector<cell_key_ranges>             ckrs;
    std::vector<partition_column_identifier> pids = {left_pid, right_pid};

    for( const auto pid : pids ) {
        auto found = modifiable_pids_to_ckrs.find( pid );
        if( found != modifiable_pids_to_ckrs.end() ) {
            for( const auto &ckr : found->second ) {
                DCHECK( is_ckr_partially_within_pcid( ckr, merged_pid ) );
                ckrs.emplace_back( ckr );
            }
        }
    }

    if( ckrs.size() > 0 ) {
        partitions_map[merged_pid] = partition_to_merge;
        modifiable_pids_to_ckrs[merged_pid] = ckrs;
    }

    partitions_map.erase( left_pid );
    partitions_map.erase( right_pid );

    modifiable_pids_to_ckrs.erase( left_pid );
    modifiable_pids_to_ckrs.erase( right_pid );
}

void heuristic_site_evaluator::split_partitions_and_add_to_plan(
    int site, const std::vector<shared_split_plan_state> &split_states,
    partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &write_partitions_map,
    partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &                                  read_partitions_map,
    std::shared_ptr<pre_transaction_plan> &plan,
    partition_column_identifier_map_t<std::shared_ptr<pre_action>> &pid_dep_map,
    partition_column_identifier_to_ckrs &write_modifiable_pids_to_ckrs,
    partition_column_identifier_to_ckrs &read_modifiable_pids_to_ckrs,
    partition_column_identifier_map_t<std::unordered_map<
        uint32_t, partition_type::type>> &created_partition_types,
    partition_column_identifier_map_t<std::unordered_map<
        uint32_t, storage_tier_type::type>> &created_storage_types,
    std::unordered_map<storage_tier_type::type, std::vector<double>>
        &                                      storage_sizes,
    partition_column_identifier_unordered_set &no_merge_partitions ) {

    for( const auto &split_state : split_states ) {

        std::map<double, std::vector<split_plan_state>> found_splits;
        found_splits[split_state.row_split_.cost_].emplace_back(
            split_state.row_split_ );
        found_splits[split_state.col_split_.cost_].emplace_back(
            split_state.col_split_ );

        bool found = false;
        for( auto &split_found : found_splits ) {
            for( auto &split_plan : split_found.second ) {
                bool split = add_split_to_plan_if_possible(
                    site, split_state, split_plan, write_partitions_map,
                    read_partitions_map, plan, pid_dep_map,
                    write_modifiable_pids_to_ckrs, read_modifiable_pids_to_ckrs,
                    created_partition_types, created_storage_types,
                    storage_sizes, no_merge_partitions );

                if( split ) {
                    found = true;
                    break;
                }
            }
            if( found ) {
                break;
            }
        }
    }
}

cell_key heuristic_site_evaluator::get_row_split_point(
    std::shared_ptr<partition_payload> partition_to_split ) {
    cell_key split_point;
    split_point.table_id = partition_to_split->identifier_.table_id;
    split_point.row_id = k_unassigned_key;
    split_point.col_id = k_unassigned_col;

    int partition_size = ( partition_to_split->identifier_.partition_end -
                           partition_to_split->identifier_.partition_start ) +
                         1;

    if( partition_size == 1 ) {
        return split_point;
    }
    // mid point
    split_point.row_id = partition_to_split->identifier_.partition_start +
                         ( ( partition_size ) / 2 );
    if( split_point.row_id ==
        partition_to_split->identifier_.partition_start ) {
        // advance by one
        split_point.row_id = split_point.row_id + 1;
    }
    if( split_point.row_id > partition_to_split->identifier_.partition_end ) {
        // if it surpasses the end, then we can't actually split this, so
        // just skip
        DVLOG( 20 ) << "Unable to generate split plan for partition:"
                    << partition_to_split->identifier_;
        split_point.row_id = k_unassigned_key;
    }
    return split_point;
}

cell_key heuristic_site_evaluator::get_col_split_point(
    std::shared_ptr<partition_payload> partition_to_split ) {
    cell_key split_point;
    split_point.table_id = partition_to_split->identifier_.table_id;
    split_point.row_id = k_unassigned_key;
    split_point.col_id = k_unassigned_col;

    int partition_size = ( partition_to_split->identifier_.column_end -
                           partition_to_split->identifier_.column_start ) +
                         1;

    if( partition_size == 1 ) {
        return split_point;
    }
    // mid point
    split_point.col_id = partition_to_split->identifier_.column_start +
                         ( ( partition_size ) / 2 );
    if( split_point.col_id == partition_to_split->identifier_.column_start ) {
        // advance by one
        split_point.col_id = split_point.col_id + 1;
    }
    if( split_point.col_id > partition_to_split->identifier_.column_end ) {
        // if it surpasses the end, then we can't actually split this, so
        // just skip
        DVLOG( 20 ) << "Unable to generate split plan for partition:"
                    << partition_to_split->identifier_;
        split_point.col_id = k_unassigned_col;
    }
    return split_point;
}
double heuristic_site_evaluator::get_expected_amount_of_remastering_from_merge(
    const merge_stats &merge_stat ) {
    return get_expected_amount_of_remastering(
               merge_stat.merge_type_, merge_stat.left_type_,
               merge_stat.right_type_, merge_stat.left_contention_,
               merge_stat.right_contention_, merge_stat.left_num_entries_,
               merge_stat.right_num_entries_ )
        .get_merge_benefit();
}
double heuristic_site_evaluator::get_expected_amount_of_remastering_from_split(
    const split_stats &split_stat, bool is_vertical ) {
    uint32_t num_entries = split_stat.num_entries_;
    if( !is_vertical ) {
        num_entries = num_entries / 2;
    }
    return get_expected_amount_of_remastering(
               split_stat.ori_type_, split_stat.left_type_,
               split_stat.right_type_, split_stat.contention_ / 2,
               split_stat.contention_ / 2, num_entries, num_entries )
        .get_split_benefit();
}

partitioning_cost_benefit
    heuristic_site_evaluator::get_expected_amount_of_remastering(
        const partition_type::type &merge_type,
        const partition_type::type &low_type,
        const partition_type::type &high_type, double low_contention,
        double high_contention, uint64_t low_part_size,
        uint64_t high_part_size ) {

    DVLOG( 40 ) << "Get expected amount of remastering: low_type: " << low_type
                << ", high_type:" << high_type
                << ", low_contention:" << low_contention
                << ", high_contention:" << high_contention
                << ", low_part_size:" << low_part_size
                << ", high_part_size:" << high_part_size;

    partitioning_cost_benefit res;

    remaster_stats merged_stats( merge_type, merge_type,
                                 low_part_size + high_part_size,
                                 low_contention + high_contention );
    remaster_stats low_stats( low_type, low_type, low_part_size,
                              low_contention );
    remaster_stats high_stats( high_type, high_type, high_part_size,
                               high_contention );

    std::vector<remaster_stats> remasters = {merged_stats};
    // MTODO-STRATEGIES do we need loads?
    double source_load = 0;
    double dest_load = 0;

    res.combined_benefit_ = cost_model2_
                                ->predict_remaster_execution_time(
                                    source_load, dest_load, remasters )
                                .get_prediction();

    std::vector<remaster_stats> low_remasters = {low_stats};
    std::vector<remaster_stats> high_remasters = {high_stats};

    res.split_benefit_ =
        2 * std::max( cost_model2_
                          ->predict_remaster_execution_time(
                              source_load, dest_load, low_remasters )
                          .get_prediction(),
                      cost_model2_
                          ->predict_remaster_execution_time(
                              source_load, dest_load, high_remasters )
                          .get_prediction() );

    DVLOG( 40 ) << "Get expected amount of remastering: low_type: " << low_type
                << ", high_type:" << high_type << low_contention
                << ", high_contention:" << high_contention
                << ", low_part_size:" << low_part_size
                << ", high_part_size:" << high_part_size
                << ", combined_benefit:" << res.combined_benefit_
                << ", split_benefit:" << res.split_benefit_;

    return res;
}

partitioning_cost_benefit
    heuristic_site_evaluator::get_default_expected_partitioning_benefit(
        const partition_column_identifier &combined_pid,
        const partition_column_identifier &low_pid,
        const partition_column_identifier &high_pid,
        const partition_type::type &       combined_type,
        const partition_type::type &       low_type,
        const partition_type::type &       high_type,
        const storage_tier_type::type &    combined_storage_type,
        const storage_tier_type::type &    low_storage_type,
        const storage_tier_type::type &high_storage_type, double low_cont,
        double low_prob, double high_cont, double high_prob, bool is_merge,
        cached_ss_stat_holder &     cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats ) {
    partitioning_cost_benefit cost_ben;

    DVLOG( 40 ) << "Get default expected partitioning benefit: low_contention:"
                << low_cont << ", low_prob:" << low_prob
                << ", high_contention:" << high_cont
                << ", high_prob:" << high_prob;

    DCHECK_GE( low_cont, 0 );
    DCHECK_GE( low_prob, 0 );
    DCHECK_GE( high_cont, 0 );
    DCHECK_GE( high_prob, 0 );

    // site_load doesn't matter here
    double site_load = 0;

    std::vector<double> cell_widths =
        data_loc_tab_->get_stats()->get_and_cache_cell_widths( combined_pid,
                                                               cached_stats );
    std::vector<double> left_cell_widths =
        data_loc_tab_->get_stats()->get_and_cache_cell_widths( low_pid,
                                                               cached_stats );
    std::vector<double> right_cell_widths =
        data_loc_tab_->get_stats()->get_and_cache_cell_widths( high_pid,
                                                               cached_stats );
    uint32_t num_entries = get_number_of_rows( combined_pid );
    uint32_t left_num_entries = get_number_of_rows( low_pid );
    uint32_t right_num_entries = get_number_of_rows( high_pid );

    double scan_selectivity =
        data_loc_tab_->get_stats()->get_and_cache_average_scan_selectivity(
            combined_pid, cached_stats );
    uint32_t num_reads =
        data_loc_tab_->get_stats()->get_and_cache_average_num_reads(
            combined_pid, cached_stats );
    uint32_t num_updates =
        data_loc_tab_->get_stats()->get_and_cache_average_num_updates(
            combined_pid, cached_stats );
    uint32_t num_updates_needed = 0;

    double combined_arrival_scalar = get_upcoming_query_arrival_score(
        combined_pid, true /* allow writes */, cached_query_arrival_stats );
    double low_arrival_scalar = get_upcoming_query_arrival_score(
        low_pid, true /* allow writes */, cached_query_arrival_stats );
    double high_arrival_scalar = get_upcoming_query_arrival_score(
        high_pid, true /* allow writes */, cached_query_arrival_stats );
    double split_arrival_scalar =
        std::max( low_arrival_scalar, high_arrival_scalar );

    std::vector<transaction_prediction_stats> txn_stats;
    transaction_prediction_stats              combined_stats(
        combined_type, combined_storage_type, low_cont + high_cont,
        num_updates_needed, cell_widths, num_entries, true, scan_selectivity,
        true, num_reads, true, num_updates );
    txn_stats.emplace_back( combined_stats );

    cost_ben.combined_benefit_ =
        configs_.repartition_prob_rescale_ * combined_arrival_scalar *
        cost_model2_
            ->predict_single_site_transaction_execution_time( site_load,
                                                              txn_stats )
            .get_prediction();

    DVLOG( 40 ) << "Default combined time:" << cost_ben.combined_benefit_;

    // assume writes are distributed uniformly, and hence  probablility p  =
    // split_ratio in the left partition. Thus chance falling both in left is
    // p*p, both in right is (1-p)(1-p). The remained is both prob
    double single_left_prob = low_prob;
    double single_right_prob = high_prob;
    double both_prob = 0;  // low_prob * high_prob;
    if( is_merge ) {
        both_prob = low_prob * high_prob;
    }

    double total_prob = single_left_prob + single_right_prob + both_prob;

    if( total_prob == 0 ) {
        single_left_prob = 0.5 * 0.5;
        single_right_prob = single_left_prob;
        both_prob = 1 - ( 2 * single_right_prob );
    } else {
        single_left_prob = single_left_prob / total_prob;
        single_right_prob = single_right_prob / total_prob;
        both_prob = both_prob / total_prob;
    }

    single_left_prob = single_left_prob * configs_.repartition_prob_rescale_ *
                       low_arrival_scalar;
    single_right_prob = single_right_prob * configs_.repartition_prob_rescale_ *
                        high_arrival_scalar;
    both_prob =
        both_prob * configs_.repartition_prob_rescale_ * split_arrival_scalar;

    transaction_prediction_stats left_stats(
        low_type, low_storage_type, low_cont, num_updates_needed,
        left_cell_widths, left_num_entries, true, scan_selectivity, true,
        num_reads / 2, true, num_updates / 2 );
    transaction_prediction_stats right_stats(
        high_type, high_storage_type, high_cont, num_updates_needed,
        right_cell_widths, right_num_entries, true, scan_selectivity, true,
        num_reads / 2, true, num_updates / 2 );
    txn_stats.at( 0 ) = left_stats;

    double single_left_part_time =
        cost_model2_
            ->predict_single_site_transaction_execution_time( site_load,
                                                              txn_stats )
            .get_prediction();
    txn_stats.at( 0 ) = right_stats;
    double single_right_part_time =
        cost_model2_
            ->predict_single_site_transaction_execution_time( site_load,
                                                              txn_stats )
            .get_prediction();
    txn_stats.emplace_back( left_stats );
    double both_part_time =
        cost_model2_
            ->predict_single_site_transaction_execution_time( site_load,
                                                              txn_stats )
            .get_prediction();

    cost_ben.split_benefit_ = ( single_left_prob * single_left_part_time ) +
                              ( single_right_prob * single_right_part_time ) +
                              ( both_prob * both_part_time );

    DVLOG( 40 ) << "Default split time:" << cost_ben.split_benefit_;

    return cost_ben;
}

partitioning_cost_benefit
    heuristic_site_evaluator::get_partitioning_benefit_from_samples(
        const partition_column_identifier &probe_pid,
        const partition_column_identifier &combined_pid,
        const partition_column_identifier &low_pid,
        const partition_column_identifier &high_pid,
        const partition_type::type &       combined_type,
        const partition_type::type &       low_type,
        const partition_type::type &       high_type,
        const storage_tier_type::type &    combined_storage_type,
        const storage_tier_type::type &    low_storage_type,
        const storage_tier_type::type &high_storage_type, double low_cont,
        double high_cont,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &write_sample_holder,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &read_sample_holder,
        cached_ss_stat_holder &                           cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats ) {
    // MTODO-HTAP consider read sample holder
    DVLOG( 40 ) << "Get partitioning benefit from samples: probe:" << probe_pid
                << ", low_pid:" << low_pid << ", high_pid:" << high_pid
                << ", low_contention:" << low_cont
                << ", high_contention:" << high_cont;

    partitioning_cost_benefit cost_ben;

    auto search = write_sample_holder.find( probe_pid );

    if( search == write_sample_holder.end() ) {
        DVLOG( 40 ) << "Probe not found, no cost";
        return cost_ben;
    }

    // site_load doesn't matter here
    double site_load = 0;

    std::vector<transaction_prediction_stats> txn_stats;

    uint32_t num_reads =
        data_loc_tab_->get_stats()->get_and_cache_average_num_reads(
            combined_pid, cached_stats );
    uint32_t num_updates =
        data_loc_tab_->get_stats()->get_and_cache_average_num_updates(
            combined_pid, cached_stats );
    uint32_t num_updates_needed = 0;
    double   scan_selectivity =
        data_loc_tab_->get_stats()->get_and_cache_average_scan_selectivity(
            combined_pid, cached_stats );

    std::vector<double> total_cell_widths =
        data_loc_tab_->get_stats()->get_and_cache_cell_widths( combined_pid,
                                                               cached_stats );
    std::vector<double> low_cell_widths =
        data_loc_tab_->get_stats()->get_and_cache_cell_widths( low_pid,
                                                               cached_stats );
    std::vector<double> high_cell_widths =
        data_loc_tab_->get_stats()->get_and_cache_cell_widths( high_pid,
                                                               cached_stats );
    uint32_t total_num_entries = get_number_of_rows( combined_pid );
    uint32_t low_num_entries = get_number_of_rows( low_pid );
    uint32_t high_num_entries = get_number_of_rows( high_pid );

    double combined_arrival_scalar = get_upcoming_query_arrival_score(
        combined_pid, true /* allow writes */, cached_query_arrival_stats );
    double low_arrival_scalar = get_upcoming_query_arrival_score(
        low_pid, true /* allow writes */, cached_query_arrival_stats );
    double high_arrival_scalar = get_upcoming_query_arrival_score(
        high_pid, true /* allow writes */, cached_query_arrival_stats );
    double split_arrival_scalar =
        std::max( low_arrival_scalar, high_arrival_scalar );

    for( transaction_partition_accesses part_accesses : search->second ) {
        txn_stats.clear();
        // look at rids in the pids
        bool low_accessed = false;
        bool high_accessed = false;
        for( const auto &rid : part_accesses.write_cids_ ) {
            low_accessed =
                low_accessed or is_ckr_partially_within_pcid( rid, low_pid );
            high_accessed =
                high_accessed or is_ckr_partially_within_pcid( rid, high_pid );

            if( low_accessed and high_accessed ) {
                break;
            }
        }
        double                                    combined_contention = 0;
        double                                    split_contention = 0;
        std::vector<transaction_prediction_stats> split_txn_stats;
        if( low_accessed ) {
            combined_contention = low_cont + high_cont;

            split_contention += ( low_cont );

            transaction_prediction_stats split_stats(
                low_type, low_storage_type, low_cont, num_updates_needed,
                low_cell_widths, low_num_entries, true, scan_selectivity, true,
                num_reads / 2, true, num_updates / 2 );
            split_txn_stats.emplace_back( split_stats );
        }
        if( high_accessed ) {
            combined_contention = low_cont + high_cont;

            split_contention += ( high_cont );

            transaction_prediction_stats split_stats(
                high_type, high_storage_type, high_cont, num_updates_needed,
                high_cell_widths, high_num_entries, true, scan_selectivity,
                true, num_reads / 2, true, num_updates / 2 );
            split_txn_stats.emplace_back( split_stats );
        }

        transaction_prediction_stats combined_stats(
            combined_type, combined_storage_type, combined_contention,
            num_updates_needed, total_cell_widths, total_num_entries, true,
            scan_selectivity, true, num_reads, true, num_updates );
        txn_stats.emplace_back( combined_stats );

        cost_ben.combined_benefit_ +=
            cost_model2_
                ->predict_single_site_transaction_execution_time( site_load,
                                                                  txn_stats )
                .get_prediction();

        cost_ben.split_benefit_ +=
            cost_model2_
                ->predict_single_site_transaction_execution_time(
                    site_load, split_txn_stats )
                .get_prediction();
    }

    DVLOG( 40 ) << "Sample Combined benefit:" << cost_ben.combined_benefit_;
    DVLOG( 40 ) << "Sample Split benefit:" << cost_ben.split_benefit_;

    cost_ben.split_benefit_ = cost_ben.split_benefit_ *
                              configs_.repartition_prob_rescale_ *
                              split_arrival_scalar;
    cost_ben.combined_benefit_ = cost_ben.combined_benefit_ *
                                 configs_.repartition_prob_rescale_ *
                                 combined_arrival_scalar;

    DVLOG( 40 ) << "Sample Combined benefit rescaled:"
                << cost_ben.combined_benefit_;
    DVLOG( 40 ) << "Sample Split benefit rescaled:" << cost_ben.split_benefit_;

    return cost_ben;
}

double heuristic_site_evaluator::get_default_expected_split_benefit(
    const partition_column_identifier &ori_pid,
    const partition_column_identifier &low_pid,
    const partition_column_identifier &high_pid, const split_stats &split_stat,
    double split_ratio, cached_ss_stat_holder &cached_stats,
    query_arrival_cached_stats &cached_query_arrival_stats ) {

    return get_default_expected_partitioning_benefit(
               ori_pid, low_pid, high_pid, split_stat.ori_type_,
               split_stat.left_type_, split_stat.right_type_,
               split_stat.ori_storage_type_, split_stat.left_storage_type_,
               split_stat.right_storage_type_,
               split_stat.contention_ * split_ratio, split_ratio,
               split_stat.contention_ * ( 1 - split_ratio ),
               ( 1 - split_ratio ), false /*is_merge*/, cached_stats,
               cached_query_arrival_stats )
        .get_split_benefit();
}

double heuristic_site_evaluator::get_split_benefit_from_samples(
    const partition_column_identifier &ori_pid,
    const partition_column_identifier &low_pid,
    const partition_column_identifier &high_pid, const split_stats &split_stat,
    double split_ratio,
    const partition_column_identifier_map_t<
        std::vector<transaction_partition_accesses>> &write_sample_holder,
    const partition_column_identifier_map_t<
        std::vector<transaction_partition_accesses>> &read_sample_holder,
    cached_ss_stat_holder &                           cached_stats,
    query_arrival_cached_stats &cached_query_arrival_stats ) {
    double split_benefit = 0;

    double total_contention = split_stat.contention_;
    double low_cont = split_ratio * total_contention;
    double high_cont = ( 1 - split_ratio ) * total_contention;

    split_benefit +=
        get_partitioning_benefit_from_samples(
            low_pid, ori_pid, low_pid, high_pid, split_stat.ori_type_,
            split_stat.left_type_, split_stat.right_type_,
            split_stat.ori_storage_type_, split_stat.left_storage_type_,
            split_stat.right_storage_type_, low_cont, high_cont,
            write_sample_holder, read_sample_holder, cached_stats,
            cached_query_arrival_stats )
            .get_split_benefit();
    split_benefit +=
        get_partitioning_benefit_from_samples(
            high_pid, ori_pid, low_pid, high_pid, split_stat.ori_type_,
            split_stat.left_type_, split_stat.right_type_,
            split_stat.ori_storage_type_, split_stat.left_storage_type_,
            split_stat.right_storage_type_, low_cont, high_cont,
            write_sample_holder, read_sample_holder, cached_stats,
            cached_query_arrival_stats )
            .get_split_benefit();
    split_benefit +=
        get_partitioning_benefit_from_samples(
            ori_pid, ori_pid, low_pid, high_pid, split_stat.ori_type_,
            split_stat.left_type_, split_stat.right_type_,
            split_stat.ori_storage_type_, split_stat.left_storage_type_,
            split_stat.right_storage_type_, low_cont, high_cont,
            write_sample_holder, read_sample_holder, cached_stats,
            cached_query_arrival_stats )
            .get_split_benefit();

    return split_benefit;
}

double heuristic_site_evaluator::get_expected_split_benefit(
    const partition_column_identifier &partition_to_split,
    const partition_column_identifier &low_pid,
    const partition_column_identifier &high_pid, const split_stats &split_stat,
    const split_plan_state &split_state,
    const partition_column_identifier_map_t<
        std::vector<transaction_partition_accesses>> &write_sample_holder,
    const partition_column_identifier_map_t<
        std::vector<transaction_partition_accesses>> &read_sample_holder,
    cached_ss_stat_holder &                           cached_stats,
    query_arrival_cached_stats &cached_query_arrival_stats ) {

    double split_ratio = 0;
    if( split_state.is_vertical_ ) {
        split_ratio =
            ( (double) ( 1 + low_pid.column_end - low_pid.column_start ) ) /
            ( (double) ( 1 + partition_to_split.column_end -
                         partition_to_split.column_start ) );
    } else {
        split_ratio = ( (double) ( 1 + low_pid.partition_end -
                                   low_pid.partition_start ) ) /
                      ( (double) ( 1 + partition_to_split.partition_end -
                                   partition_to_split.partition_start ) );
    }

    DVLOG( 40 ) << "Split ratio:" << split_ratio << ", low:" << low_pid
                << ", covering partition:" << partition_to_split;

    double default_benefit = get_default_expected_split_benefit(
        partition_to_split, low_pid, high_pid, split_stat, split_ratio,
        cached_stats, cached_query_arrival_stats );

    double split_benefit = get_split_benefit_from_samples(
        partition_to_split, low_pid, high_pid, split_stat, split_ratio,
        write_sample_holder, read_sample_holder, cached_stats,
        cached_query_arrival_stats );

    return split_benefit +
           ( configs_.default_horizon_benefit_weight_ * default_benefit );
}

split_plan_state
    heuristic_site_evaluator::split_partition_horizontally_if_beneficial(
        std::shared_ptr<partition_payload>              partition_to_split,
        std::shared_ptr<partition_location_information> location_information,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>>
            &sampled_partition_accesses_index_by_write_partition,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>>
            &  sampled_partition_accesses_index_by_read_partition,
        double avg_write_count, double std_dev_write_count,
        std::unordered_map<storage_tier_type::type, std::vector<double>>
            &  storage_sizes,
        double ori_storage_imbalance, cached_ss_stat_holder &cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats ) {
    split_plan_state split_res;
    split_res.is_vertical_ = false;
    split_res.splittable_ = false;

    split_res.split_point_ = get_row_split_point( partition_to_split );
    if( split_res.split_point_.row_id == k_unassigned_col ) {
        split_res.splittable_ = false;
        return split_res;
    }

    if( !configs_.allow_horizontal_partitioning_ ) {
        return split_res;
    }

    split_partition_if_beneficial(
        split_res, partition_to_split, location_information,
        sampled_partition_accesses_index_by_write_partition,
        sampled_partition_accesses_index_by_read_partition, avg_write_count,
        std_dev_write_count, storage_sizes, ori_storage_imbalance, cached_stats,
        cached_query_arrival_stats );

    return split_res;
}

split_plan_state
    heuristic_site_evaluator::split_partition_vertically_if_beneficial(
        std::shared_ptr<partition_payload>              partition_to_split,
        std::shared_ptr<partition_location_information> location_information,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>>
            &sampled_partition_accesses_index_by_write_partition,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>>
            &  sampled_partition_accesses_index_by_read_partition,
        double avg_write_count, double std_dev_write_count,
        std::unordered_map<storage_tier_type::type, std::vector<double>>
            &  storage_sizes,
        double ori_storage_imbalance, cached_ss_stat_holder &cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats ) {

    split_plan_state split_res;
    split_res.is_vertical_ = true;
    split_res.splittable_ = false;

    split_res.split_point_ = get_col_split_point( partition_to_split );
    if( split_res.split_point_.col_id == k_unassigned_col ) {
        split_res.splittable_ = false;
        return split_res;
    }

    if( !configs_.allow_vertical_partitioning_ ) {
        return split_res;
    }

    split_partition_if_beneficial(
        split_res, partition_to_split, location_information,
        sampled_partition_accesses_index_by_write_partition,
        sampled_partition_accesses_index_by_read_partition, avg_write_count,
        std_dev_write_count, storage_sizes, ori_storage_imbalance, cached_stats,
        cached_query_arrival_stats );

    return split_res;
}

void heuristic_site_evaluator::split_partition_if_beneficial(
    split_plan_state &                              split_res,
    std::shared_ptr<partition_payload>              partition_to_split,
    std::shared_ptr<partition_location_information> location_information,
    const partition_column_identifier_map_t<
        std::vector<transaction_partition_accesses>>
        &sampled_partition_accesses_index_by_write_partition,
    const partition_column_identifier_map_t<
        std::vector<transaction_partition_accesses>>
        &  sampled_partition_accesses_index_by_read_partition,
    double avg_write_count, double std_dev_write_count,
    std::unordered_map<storage_tier_type::type, std::vector<double>>
        &  storage_sizes,
    double ori_storage_imbalance, cached_ss_stat_holder &cached_stats,
    query_arrival_cached_stats &cached_query_arrival_stats ) {

    auto split_pids = construct_split_partition_column_identifiers(
        partition_to_split->identifier_, split_res.split_point_.row_id,
        split_res.split_point_.col_id );

    auto ori_pid = partition_to_split->identifier_;
    auto low_pid = std::get<0>( split_pids );
    auto high_pid = std::get<1>( split_pids );

    split_res.splittable_ = true;

    // we don't care for split / merge and it differs per site
    double site_load = 0;

    uint64_t partition_size = partition_to_split->get_num_cells();
    double   contention = cost_model2_->normalize_contention_by_time(
        partition_to_split->get_contention() );
    uint32_t normalized_split_point = 0;
    if( split_res.is_vertical_ ) {
        normalized_split_point =
            split_res.split_point_.col_id - ori_pid.column_start;
    } else {
        normalized_split_point = ( uint32_t )( split_res.split_point_.row_id -
                                               ori_pid.partition_start );
    }

    // we will iterate over these later
    split_res.low_type_ = partition_type::type::ROW;
    split_res.high_type_ = partition_type::type::ROW;
    split_res.low_storage_type_ = storage_tier_type::type::MEMORY;
    split_res.high_storage_type_ = storage_tier_type::type::MEMORY;

    split_stats split_stat(
        std::get<1>( location_information->get_partition_type(
            location_information->master_location_ ) ),
        split_res.low_type_, split_res.high_type_,
        std::get<1>( location_information->get_storage_type(
            location_information->master_location_ ) ),
        split_res.low_storage_type_, split_res.high_storage_type_, contention,
        data_loc_tab_->get_stats()->get_and_cache_cell_widths(
            partition_to_split->identifier_, cached_stats ),
        partition_to_split->get_num_rows(), normalized_split_point );

    double           max_cost = -DBL_MAX;
    split_plan_state max_plan = split_res;

    double left_storage_size = storage_stats_->get_partition_size( low_pid );
    double right_storage_size = storage_stats_->get_partition_size( high_pid );

    auto considered_types = enumerator_holder_->get_split_types( split_stat );

    for( const auto &considered_type : considered_types ) {
        partition_type::type    left_type = std::get<0>( considered_type );
        partition_type::type    right_type = std::get<1>( considered_type );
        storage_tier_type::type left_storage_type =
            std::get<2>( considered_type );
        storage_tier_type::type right_storage_type =
            std::get<3>( considered_type );

        split_plan_state loc_split_res = split_res;

        split_stat.left_type_ = left_type;
        split_stat.right_type_ = right_type;
        split_stat.left_storage_type_ = left_storage_type;
        split_stat.right_storage_type_ = right_storage_type;

        loc_split_res.low_type_ = left_type;
        loc_split_res.high_type_ = right_type;
        loc_split_res.low_storage_type_ = left_storage_type;
        loc_split_res.high_storage_type_ = right_storage_type;

        get_split_cost(
            loc_split_res, split_stat, ori_pid, low_pid, high_pid, site_load,
            sampled_partition_accesses_index_by_write_partition,
            sampled_partition_accesses_index_by_read_partition,
            left_storage_size, right_storage_size, ori_storage_imbalance,
            storage_sizes, location_information->master_location_, cached_stats,
            cached_query_arrival_stats );

        if( loc_split_res.cost_ > max_cost ) {
            max_cost = loc_split_res.cost_;
            max_plan = loc_split_res;
        }
    }

    split_stat.left_type_ = max_plan.low_type_;
    split_stat.right_type_ = max_plan.high_type_;
    split_stat.left_storage_type_ = max_plan.low_storage_type_;
    split_stat.right_storage_type_ = max_plan.high_storage_type_;

    split_res.split_stats_ = split_stat;

    double std_dev_mult = configs_.outlier_repartition_std_dev_;
    double min_part_size =
        min_part_sizes_.at( partition_to_split->identifier_.table_id );
    if( partition_size < min_part_size ) {
        double rescaled_mult =
            std_dev_mult * ( min_part_size / (double) partition_size );
        std_dev_mult = sqrt( rescaled_mult );
        DVLOG( 40 ) << "Split rescaling std_dev_mult, partition size:"
                    << partition_size << ", min_part_size:" << min_part_size
                    << ", default std_dev_mult:"
                    << configs_.outlier_repartition_std_dev_
                    << ", rescaled_mult:" << rescaled_mult
                    << ", sqrt(rescaled_mult):" << std_dev_mult;
    }

    double outlier_write_count =
        avg_write_count + ( std_dev_mult * std_dev_write_count );

    double write_accesses = (double) partition_to_split->write_accesses_;
    DVLOG( 20 ) << "split_res.cost_:" << split_res.cost_
                << ", outlier_write_count:" << outlier_write_count
                << ", write_count:" << write_accesses;

    if( ( split_res.cost_ < 0 ) and ( write_accesses < outlier_write_count ) ) {
        split_res.splittable_ = false;
    }

    DVLOG( k_heuristic_evaluator_log_level )
        << "PLAN: Split partition: (" << partition_to_split->identifier_
        << "), splittable: " << split_res.splittable_
        << ", split_ck:" << split_res.split_point_
        << ", overall cost:" << split_res.cost_ << " ( upfront cost:"
        << split_res.upfront_cost_prediction_.get_prediction()
        << " ), write_count:" << write_accesses
        << ", outlier_write_count:" << outlier_write_count
        << " ( avg_write_count:" << avg_write_count
        << ", std_dev_write_count:" << std_dev_write_count
        << ", std_dev_mult:" << std_dev_mult << " )";
}

void heuristic_site_evaluator::get_split_cost(
    split_plan_state &split_res, const split_stats &split_stat,
    const partition_column_identifier &partition_to_split,
    const partition_column_identifier &low_pid,
    const partition_column_identifier &high_pid, double site_load,
    const partition_column_identifier_map_t<
        std::vector<transaction_partition_accesses>>
        &sampled_partition_accesses_index_by_write_partition,
    const partition_column_identifier_map_t<
        std::vector<transaction_partition_accesses>>
        &  sampled_partition_accesses_index_by_read_partition,
    double left_storage_size, double right_storage_size,
    double ori_storage_imbalance,
    std::unordered_map<storage_tier_type::type, std::vector<double>>
        &    storage_sizes,
    uint32_t site, cached_ss_stat_holder &cached_stats,
    query_arrival_cached_stats &cached_query_arrival_stats ) {

    if( split_res.is_vertical_ ) {
        split_res.upfront_cost_prediction_ =
            cost_model2_->predict_vertical_split_execution_time( site_load,
                                                                 split_stat );
    } else {
        split_res.upfront_cost_prediction_ =
            cost_model2_->predict_horizontal_split_execution_time( site_load,
                                                                   split_stat );
    }

    if( split_stat.ori_storage_type_ != split_stat.left_storage_type_ ) {
        storage_sizes.at( split_stat.ori_storage_type_ ).at( site ) -=
            left_storage_size;
        storage_sizes.at( split_stat.left_storage_type_ ).at( site ) +=
            left_storage_size;
    }
    if( split_stat.ori_storage_type_ != split_stat.right_storage_type_ ) {
        storage_sizes.at( split_stat.ori_storage_type_ ).at( site ) -=
            right_storage_size;
        storage_sizes.at( split_stat.right_storage_type_ ).at( site ) +=
            right_storage_size;
    }

    double storage_cost_change = get_change_in_storage_imbalance_costs(
        get_site_storage_imbalance_cost( storage_sizes ),
        ori_storage_imbalance );

    if( split_stat.ori_storage_type_ != split_stat.left_storage_type_ ) {
        storage_sizes.at( split_stat.ori_storage_type_ ).at( site ) +=
            left_storage_size;
        storage_sizes.at( split_stat.left_storage_type_ ).at( site ) -=
            left_storage_size;
    }
    if( split_stat.ori_storage_type_ != split_stat.right_storage_type_ ) {
        storage_sizes.at( split_stat.ori_storage_type_ ).at( site ) +=
            right_storage_size;
        storage_sizes.at( split_stat.right_storage_type_ ).at( site ) -=
            right_storage_size;
    }

    double expected_benefit = get_expected_split_benefit(
        partition_to_split, low_pid, high_pid, split_stat, split_res,
        sampled_partition_accesses_index_by_write_partition,
        sampled_partition_accesses_index_by_read_partition, cached_stats,
        cached_query_arrival_stats );

    double expected_increase_in_remastering =
        get_expected_amount_of_remastering_from_split( split_stat,
                                                       split_res.is_vertical_ );

    split_res.cost_ =
        ( configs_.upfront_cost_weight_ *
          -split_res.upfront_cost_prediction_.get_prediction() ) +
        ( expected_increase_in_remastering *
          configs_.repartition_remaster_horizon_weight_ ) +
        ( configs_.horizon_weight_ * expected_benefit ) +
        ( configs_.horizon_storage_weight_ * storage_cost_change );

    DVLOG( 10 ) << "Split partition: (" << partition_to_split
                << "), splittable: " << split_res.splittable_
                << ", split_ck:" << split_res.split_point_
                << ", overall cost:" << split_res.cost_ << " ( upfront cost:"
                << split_res.upfront_cost_prediction_.get_prediction()
                << ", expected_benefit:" << expected_benefit
                << ", expected_increase_in_remastering:"
                << expected_increase_in_remastering << " )";
}

bool heuristic_site_evaluator::add_split_to_plan_if_possible(
    int site, const shared_split_plan_state &shared_split_res,
    const split_plan_state &split_res,
    partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &write_partitions_map,
    partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &                                  read_partitions_map,
    std::shared_ptr<pre_transaction_plan> &plan,
    partition_column_identifier_map_t<std::shared_ptr<pre_action>> &pid_dep_map,
    partition_column_identifier_to_ckrs &write_modifiable_pids_to_ckrs,
    partition_column_identifier_to_ckrs &read_modifiable_pids_to_ckrs,
    partition_column_identifier_map_t<std::unordered_map<
        uint32_t, partition_type::type>> &created_partition_types,
    partition_column_identifier_map_t<std::unordered_map<
        uint32_t, storage_tier_type::type>> &created_storage_types,
    std::unordered_map<storage_tier_type::type, std::vector<double>>
        &                                      storage_sizes,
    partition_column_identifier_unordered_set &no_merge_partitions ) {
    auto     partition_to_split = shared_split_res.partition_;
    auto     location_information = shared_split_res.location_information_;
    cell_key split_point = split_res.split_point_;
    double   cost = split_res.cost_;
    if( !split_res.splittable_ ) {
        return false;
    }

    // Since we split using the heuristic, there's no explicit cost for
    // doing a split
    // In the future, we will properly cost this
    auto split_action = create_split_partition_pre_action(
        location_information->master_location_, partition_to_split, split_point,
        cost /* 0.0  No cost, told to do this by the heuristic*/,
        split_res.upfront_cost_prediction_,
        // split the partition to the same topic, to avoid unecessary
        // seeking
        split_res.low_type_, split_res.high_type_, split_res.low_storage_type_,
        split_res.high_storage_type_,
        location_information->update_destination_slot_,
        location_information->update_destination_slot_ );

    DVLOG( 20 ) << "Added split partition: " << partition_to_split->identifier_
                << " to candidate plan.";

    if( (uint32_t) site != location_information->master_location_ ) {
        DVLOG( k_heuristic_evaluator_log_level )
            << "BACKGROUND PLAN: Split partition: ("
            << partition_to_split->identifier_
            << "), split_point:" << split_point;

        plan->add_background_work_item( split_action );
        plan->stat_decision_holder_->add_split_types_decision(
            split_res.split_stats_,
            std::make_tuple<>( split_res.low_type_, split_res.high_type_,
                               split_res.low_storage_type_,
                               split_res.high_storage_type_ ) );

        return true;
    }

    plan->add_pre_action_work_item( split_action );
    plan->stat_decision_holder_->add_split_types_decision(
        split_res.split_stats_,
        std::make_tuple<>( split_res.low_type_, split_res.high_type_,
                           split_res.low_storage_type_,
                           split_res.high_storage_type_ ) );

    auto split_pids = construct_split_partition_column_identifiers(
        partition_to_split->identifier_, split_point.row_id,
        split_point.col_id );

    auto low_pid = std::get<0>( split_pids );
    auto high_pid = std::get<1>( split_pids );

    created_partition_types[low_pid][site] = split_res.low_type_;
    created_partition_types[high_pid][site] = split_res.high_type_;
    created_storage_types[low_pid][site] = split_res.low_storage_type_;
    created_storage_types[high_pid][site] = split_res.high_storage_type_;

    // update storage sizes
    auto ori_storage_tier = split_res.split_stats_.ori_storage_type_;
    auto destination = location_information->master_location_;
    if( ori_storage_tier != split_res.low_storage_type_ ) {
        double low_storage_size = storage_stats_->get_partition_size( low_pid );
        storage_sizes.at( ori_storage_tier ).at( destination ) -=
            low_storage_size;
        storage_sizes.at( split_res.low_storage_type_ ).at( destination ) +=
            low_storage_size;
    }
    if( ori_storage_tier != split_res.high_storage_type_ ) {
        double high_storage_size =
            storage_stats_->get_partition_size( low_pid );
        storage_sizes.at( ori_storage_tier ).at( destination ) -=
            high_storage_size;
        storage_sizes.at( split_res.high_storage_type_ ).at( destination ) +=
            high_storage_size;
    }

    for( const auto &entry : location_information->replica_locations_ ) {
        created_partition_types[low_pid][entry] =
            std::get<1>( location_information->get_partition_type( entry ) );
        created_partition_types[high_pid][entry] =
            std::get<1>( location_information->get_partition_type( entry ) );
        created_storage_types[low_pid][entry] =
            std::get<1>( location_information->get_storage_type( entry ) );
        created_storage_types[high_pid][entry] =
            std::get<1>( location_information->get_storage_type( entry ) );
    }

    pid_dep_map[low_pid] = split_action;
    pid_dep_map[high_pid] = split_action;

    no_merge_partitions.emplace( partition_to_split->identifier_ );

    update_partition_maps_from_split( partition_to_split, low_pid, high_pid,
                                      write_partitions_map,
                                      write_modifiable_pids_to_ckrs );
    update_partition_maps_from_split( partition_to_split, low_pid, high_pid,
                                      read_partitions_map,
                                      read_modifiable_pids_to_ckrs );

    return true;
}

void heuristic_site_evaluator::update_partition_maps_from_split(
    std::shared_ptr<partition_payload> &partition_to_split,
    const partition_column_identifier & low_pid,
    const partition_column_identifier & high_pid,
    partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &                                partitions_map,
    partition_column_identifier_to_ckrs &modifiable_pids_to_ckrs ) const {

    const auto &ckrs = modifiable_pids_to_ckrs[partition_to_split->identifier_];
    std::vector<cell_key_ranges> low_ckrs;
    std::vector<cell_key_ranges> high_ckrs;

    for( const auto &ckr : ckrs ) {
        if( is_ckr_fully_within_pcid( ckr, low_pid ) ) {
            low_ckrs.push_back( ckr );
            DCHECK( !is_ckr_partially_within_pcid( ckr, high_pid ) );
        } else if( is_ckr_fully_within_pcid( ckr, high_pid ) ) {
            high_ckrs.push_back( ckr );
            DCHECK( !is_ckr_partially_within_pcid( ckr, low_pid ) );
        } else {
            DCHECK( is_ckr_partially_within_pcid( ckr, high_pid ) );
            DCHECK( is_ckr_partially_within_pcid( ckr, low_pid ) );
            low_ckrs.push_back( shape_ckr_to_pcid( ckr, low_pid ) );
            high_ckrs.push_back( shape_ckr_to_pcid( ckr, high_pid ) );
        }
    }

    modifiable_pids_to_ckrs.erase( partition_to_split->identifier_ );
    partitions_map.erase( partition_to_split->identifier_ );

    if( low_ckrs.size() > 0 ) {
        modifiable_pids_to_ckrs[low_pid] = std::move( low_ckrs );
        partitions_map[low_pid] = partition_to_split;
    }
    if( high_ckrs.size() > 0 ) {
        modifiable_pids_to_ckrs[high_pid] = std::move( high_ckrs );
        partitions_map[high_pid] = partition_to_split;
    }
}

std::shared_ptr<partition_location_information>
    heuristic_site_evaluator::get_location_information_for_partition(
        std::shared_ptr<partition_payload> &partition,
        const partition_column_identifier_map_t<
            std::shared_ptr<partition_location_information>>
            &partition_location_informations ) {
    const auto &pid = partition->identifier_;
    auto        found = partition_location_informations.find( pid );
    if( found == partition_location_informations.end() ) {
        return nullptr;
    }
    return found->second;
}

std::shared_ptr<partition_location_information> heuristic_site_evaluator::
    get_location_information_for_partition_or_from_payload(
        std::shared_ptr<partition_payload> &partition,
        partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>> &partition_location_informations,
        const partition_lock_mode &           lock_mode ) {
    auto location_information = get_location_information_for_partition(
        partition, partition_location_informations );
    if( location_information ) {
        return location_information;
    }
    location_information = partition->get_location_information( lock_mode );
    if( ( location_information == nullptr ) or
        ( location_information->master_location_ == k_unassigned_master ) ) {
        return nullptr;
    }
    partition_location_informations[partition->identifier_] =
        location_information;
    return location_information;
}

std::tuple<std::shared_ptr<partition_payload>, bool> heuristic_site_evaluator::
    get_partition_from_data_location_table_or_already_locked(
        const partition_column_identifier &pid,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &                      other_write_locked_partitions,
        const partition_lock_mode &lock_mode ) {
    std::shared_ptr<partition_payload> part = nullptr;
    auto found = other_write_locked_partitions.find( pid );
    if( found != other_write_locked_partitions.end() ) {
        part = found->second;
        return std::make_tuple<std::shared_ptr<partition_payload>, bool>(
            std::move( part ), false );
    }
    part = data_loc_tab_->get_partition( pid, lock_mode );
    if( part == nullptr ) {
        return std::make_tuple<std::shared_ptr<partition_payload>, bool>(
            nullptr, false );
    }

    if( lock_mode != partition_lock_mode::no_lock ) {
        other_write_locked_partitions[part->identifier_] = part;
    }
    return std::make_tuple<std::shared_ptr<partition_payload>, bool>(
        std::move( part ), true );
}

void heuristic_site_evaluator::unlock_if_new(
    std::tuple<std::shared_ptr<partition_payload>, bool> &entry,
    partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &other_write_locked_partitions,
    partition_column_identifier_map_t<std::shared_ptr<
        partition_location_information>> &partition_location_informations,
    const partition_lock_mode &           lock_mode ) {
    if( !std::get<1>( entry ) ) {
        return;
    }
    auto part = std::get<0>( entry );
    if( part == nullptr ) {
        return;
    }
    other_write_locked_partitions.erase( part->identifier_ );
    partition_location_informations.erase( part->identifier_ );
    part->unlock( lock_mode );
}

int heuristic_site_evaluator::generate_insert_only_partition_destination(
    std::vector<std::shared_ptr<partition_payload>> &insert_partitions ) {

    start_timer( SS_GENERATE_INSERT_ONLY_PARTITION_DESTINATION_TIMER_ID );

    int site = rand_dist_.get_uniform_int( 0, configs_.num_sites_ );
    if( configs_.ss_mastering_type_ ==
        ss_mastering_type::SINGLE_MASTER_MULTI_SLAVE ) {
        site = 0;
    }

    stop_timer( SS_GENERATE_INSERT_ONLY_PARTITION_DESTINATION_TIMER_ID );

    return site;
}

uint32_t heuristic_site_evaluator::get_new_partition_update_destination_slot(
    int                                              destination,
    std::vector<std::shared_ptr<partition_payload>> &insert_partitions ) {
    // randomly assign to all the partitions in the insertion
    // this should keep load balanced, and we will try to keep this locality
    uint32_t slot = rand_dist_.get_uniform_int(
        0, configs_.num_update_destinations_per_site_ );
    return slot;
}

std::tuple<std::vector<partition_type::type>,
           std::vector<storage_tier_type::type>>
    heuristic_site_evaluator::get_new_partition_types(
        int destination, std::shared_ptr<pre_transaction_plan> &plan,
        const site_load_information &site_load_info,
        const std::unordered_map<storage_tier_type::type, std::vector<double>>
            &                                            storage_sizes,
        std::vector<std::shared_ptr<partition_payload>> &insert_partitions ) {
    cached_ss_stat_holder      cached_stats;
    query_arrival_cached_stats cached_query_arrival_stats;

    double ori_storage_imbalance =
        get_site_storage_imbalance_cost( storage_sizes );

    auto loc_storage_sizes = storage_sizes;

    return internal_get_new_partition_types(
        destination, plan, site_load_info, insert_partitions, loc_storage_sizes,
        ori_storage_imbalance, cached_stats, cached_query_arrival_stats );
}

std::tuple<std::vector<partition_type::type>,
           std::vector<storage_tier_type::type>>
    heuristic_site_evaluator::internal_get_new_partition_types(
        int destination, std::shared_ptr<pre_transaction_plan> &plan,
        const site_load_information &                    site_load_info,
        std::vector<std::shared_ptr<partition_payload>> &insert_partitions,
        std::unordered_map<storage_tier_type::type, std::vector<double>>
            &  storage_sizes,
        double ori_storage_imbalance, cached_ss_stat_holder &cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats ) {
    std::vector<partition_type::type> part_types(
        insert_partitions.size(),
        configs_.acceptable_new_partition_types_.at( 0 ) );
    std::vector<storage_tier_type::type> storage_types(
        insert_partitions.size(),
        configs_.acceptable_new_storage_types_.at( 0 ) );

    partition_column_identifier_map_t<partition_type::type> observations;
    partition_column_identifier_map_t<storage_tier_type::type>
        observations_storage;

    for( uint32_t pos = 0; pos < insert_partitions.size(); pos++ ) {
        auto new_types = get_new_partition_type(
            destination, plan, site_load_info, insert_partitions.at( pos ),
            observations, observations_storage, storage_sizes,
            ori_storage_imbalance, cached_stats, cached_query_arrival_stats );
        part_types.at( pos ) = std::get<0>( new_types );
        storage_types.at( pos ) = std::get<1>( new_types );
    }
    return std::make_tuple<>( part_types, storage_types );
}

std::tuple<partition_type::type, storage_tier_type::type>
    heuristic_site_evaluator::get_new_partition_type(
        int destination, std::shared_ptr<pre_transaction_plan> &plan,
        const site_load_information &                            site_load_info,
        std::shared_ptr<partition_payload> &                     payload,
        partition_column_identifier_map_t<partition_type::type> &observed_types,
        partition_column_identifier_map_t<storage_tier_type::type>
            &observed_storage_types,
        std::unordered_map<storage_tier_type::type, std::vector<double>>
            &  storage_sizes,
        double ori_storage_imbalance, cached_ss_stat_holder &cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats ) {
    auto pid = payload->identifier_;
    auto generic_pid = create_partition_column_identifier(
        pid.table_id, 0, 0, pid.column_start, pid.column_end );
    auto partition_found = observed_types.find( generic_pid );
    auto storage_found = observed_storage_types.find( generic_pid );
    if( ( partition_found != observed_types.end() ) and
        ( storage_found != observed_storage_types.end() ) ) {
        return std::make_tuple<>( partition_found->second,
                                  storage_found->second );
    }
    partition_type::type found_type =
        configs_.acceptable_new_partition_types_.at( 0 );
    storage_tier_type::type found_storage_type =
        configs_.acceptable_new_storage_types_.at( 0 );

    double max_cost = DBL_MAX;

    // build a txn that accesses it
    transaction_prediction_stats txn_stat(
        found_type, found_storage_type,
        data_loc_tab_->get_stats()->get_and_cache_average_contention(
            pid, cached_stats ),
        (double) 0 /* num_updates needed*/,
        data_loc_tab_->get_stats()->get_and_cache_cell_widths( pid,
                                                               cached_stats ),
        get_number_of_rows( pid ), true,
        data_loc_tab_->get_stats()->get_and_cache_average_scan_selectivity(
            pid, cached_stats ),
        true, data_loc_tab_->get_stats()->get_and_cache_average_num_reads(
                  pid, cached_stats ),
        true, data_loc_tab_->get_stats()->get_and_cache_average_num_updates(
                  pid, cached_stats ) );
    double site_load = site_load_info.cpu_load_;

    double partition_storage_size = storage_stats_->get_partition_size( pid );

    auto considered_types = enumerator_holder_->get_new_types( txn_stat );

    // cost it out
    for( const auto cons_type : considered_types ) {
        txn_stat.part_type_ = std::get<0>( cons_type );
        txn_stat.storage_type_ = std::get<1>( cons_type );
        std::vector<transaction_prediction_stats> txn_stats = {txn_stat};
        double                                    txn_cost = cost_model2_
                              ->predict_single_site_transaction_execution_time(
                                  site_load, txn_stats )
                              .get_prediction();

        storage_sizes.at( txn_stat.storage_type_ ).at( destination ) +=
            partition_storage_size;

        double storage_cost_change = get_change_in_storage_imbalance_costs(
            get_site_storage_imbalance_cost( storage_sizes ),
            ori_storage_imbalance );

        storage_sizes.at( txn_stat.storage_type_ ).at( destination ) -=
            partition_storage_size;

        double cost =
            ( configs_.horizon_weight_ * txn_cost ) +
            ( configs_.horizon_storage_weight_ * storage_cost_change );

        if( cost < max_cost ) {
            found_type = std::get<0>( cons_type );
            found_storage_type = std::get<1>( cons_type );
            max_cost = cost;
        }
    }
    plan->stat_decision_holder_->add_new_types_decision(
        txn_stat, std::make_tuple<>( found_type, found_storage_type ) );

    return std::make_tuple<>( found_type, found_storage_type );
}

int heuristic_site_evaluator::get_num_sites() { return configs_.num_sites_; }
uint32_t heuristic_site_evaluator::get_num_update_destinations_per_site() {
    return configs_.num_update_destinations_per_site_;
}

void heuristic_site_evaluator::set_samplers(
    std::vector<std::unique_ptr<adaptive_reservoir_sampler>> *samplers ) {
    samplers_ = samplers;
}

std::shared_ptr<pre_transaction_plan>
    heuristic_site_evaluator::get_plan_from_current_planners(
        const grouped_partition_information &    grouped_info,
        std::shared_ptr<multi_query_plan_entry> &mq_plan_entry,
        ::snapshot_vector &                      svv ) {

    DVLOG( 20 ) << "Trying to get plan from current planners";

    std::shared_ptr<pre_transaction_plan> plan = nullptr;

    if( !configs_.enable_wait_for_plan_reuse_ ) {
        return plan;
    }
    transaction_query_entry query_entry = build_transaction_query_entry(
        grouped_info.existing_write_partitions_,
        grouped_info.existing_read_partitions_,
        grouped_info.existing_write_partitions_set_,
        grouped_info.existing_read_partitions_set_ );

    std::vector<
        std::tuple<double, uint64_t, std::shared_ptr<multi_query_plan_entry>>>
        sorted_plans = query_entry.get_sorted_plans();

    int dest = -1;

    for( const auto &entry : sorted_plans ) {
        if( std::get<0>( entry ) < configs_.plan_reuse_threshold_ ) {
            break;
        }
        auto plan_entry = std::get<2>( entry );
        auto dest_site = plan_entry->get_plan_destination();
        if( !std::get<0>( dest_site ) ) {
            plan_entry->wait_for_plan_to_be_generated();
            dest_site = plan_entry->get_plan_destination();
        }
        DCHECK( std::get<0>( dest_site ) );
        bool can_exec =
            can_plan_execute_at_site( grouped_info, std::get<1>( dest_site ) );
        if( can_exec ) {
            dest = std::get<1>( dest_site );
            break;
        }
    }

    DVLOG( 20 ) << "Trying to get plan from current planners, destination:"
                << dest;
    if( dest >= 0 ) {
        plan = build_no_change_plan( dest, grouped_info, mq_plan_entry, svv );

        DVLOG( 10 )
            << "Trying to get plan from current planners, got destination:"
            << dest << ", plan:" << plan;
    }

    return plan;
}
bool heuristic_site_evaluator::can_plan_execute_at_site(
    const grouped_partition_information &grouped_info, int site ) const {
    for( const auto &payload : grouped_info.existing_write_partitions_ ) {
        auto loc_info = payload->get_location_information();
        if( (int) loc_info->master_location_ != site ) {
            return false;
        }
    }
    for( const auto &payload : grouped_info.existing_read_partitions_ ) {
        auto loc_info = payload->get_location_information();
        if( ( (int) loc_info->master_location_ != site ) or
            ( loc_info->replica_locations_.count( (uint32_t) site ) == 0 ) ) {
            return false;
        }
    }
    return true;
}

std::shared_ptr<pre_transaction_plan>
    heuristic_site_evaluator::build_no_change_plan(
        int destination, const grouped_partition_information &grouped_info,
        std::shared_ptr<multi_query_plan_entry> &mq_plan_entry,
        ::snapshot_vector &                      svv ) const {
    auto plan = std::make_shared<pre_transaction_plan>(
        enumerator_holder_->construct_decision_holder_ptr() );
    plan->destination_ = destination;
    plan->read_pids_ =
        payloads_to_identifiers( grouped_info.existing_read_partitions_ );
    plan->write_pids_ =
        payloads_to_identifiers( grouped_info.existing_write_partitions_ );

    DVLOG( 10 ) << "Generate no change plan:"
                << ", write_partitions_set:" << plan->write_pids_
                << ", read_partitions_set:" << plan->read_pids_;

    build_multi_query_plan_entry( mq_plan_entry,
                                  grouped_info.existing_write_partitions_set_,
                                  grouped_info.existing_read_partitions_set_ );
    plan->mq_plan_ = mq_plan_entry;
    mq_plan_entry->set_plan_destination( plan->destination_,
                                         multi_query_plan_type::PLANNING );

    merge_snapshot_versions_if_larger( svv,
                                       grouped_info.inflight_snapshot_vector_ );

    double read_wait_for =
        site_partition_version_info_
            ->estimate_number_of_updates_need_to_wait_for(
                grouped_info.existing_read_partitions_,
                grouped_info.partition_location_informations_,
                plan->destination_, svv );

    double inflight_wait_for = 0;
    if( !grouped_info.inflight_snapshot_vector_.empty() ) {
        plan->inflight_pids_ = grouped_info.inflight_pids_;
        DVLOG( 10 ) << "In flight changes:"
                    << grouped_info.inflight_snapshot_vector_
                    << ", pids:" << grouped_info.inflight_pids_;

        inflight_wait_for =
            site_partition_version_info_
                ->estimate_number_of_updates_need_to_wait_for(
                    grouped_info.existing_write_partitions_,
                    grouped_info.partition_location_informations_,
                    plan->destination_, svv );
    }

    plan->estimated_number_of_updates_to_wait_for_ =
        std::max( read_wait_for, inflight_wait_for );
    return plan;
}

std::shared_ptr<multi_site_pre_transaction_plan>
    heuristic_site_evaluator::build_no_change_scan_plan(
        const ::clientid id, const grouped_partition_information &grouped_info,
        const std::vector<scan_arguments> &scan_args,
        const std::unordered_map<uint32_t /* label*/,
                                 partition_column_identifier_to_ckrs>
            &labels_to_pids_and_ckrs,
        const partition_column_identifier_map_t<std::unordered_map<
            uint32_t /* label */, std::vector<cell_key_ranges>>>
            &pids_to_labels_and_ckrs,
        const std::unordered_map<uint32_t /* label */, uint32_t /* pos */>
            &                                     label_to_pos,
        const std::vector<site_load_information> &site_load_infos,
        const std::unordered_map<storage_tier_type::type, std::vector<double>>
            &                                    storage_sizes,
        std::shared_ptr<multi_query_plan_entry> &mq_plan_entry,
        ::snapshot_vector &                      svv ) {

    DVLOG( 10 ) << "Generate no change scan plan:"
                << ", write_partitions_set:"
                << grouped_info.existing_write_partitions_set_
                << ", read_partitions_set:"
                << grouped_info.existing_read_partitions_set_;

    std::vector<
        std::tuple<double, uint64_t, std::shared_ptr<multi_query_plan_entry>>>
        sorted_plans;

    return generate_scan_plan_from_sorted_planners(
        id, grouped_info, scan_args, labels_to_pids_and_ckrs,
        pids_to_labels_and_ckrs, label_to_pos, site_load_infos, storage_sizes,
        sorted_plans, mq_plan_entry, svv );
}

void heuristic_site_evaluator::finalize_scan_plan(
    const ::clientid id, std::shared_ptr<multi_site_pre_transaction_plan> &plan,
    const grouped_partition_information &         grouped_info,
    const partition_column_identifier_map_t<int> &pids_to_site,
    std::shared_ptr<multi_query_plan_entry> &     mq_plan_entry,
    snapshot_vector &                             session ) {

    if( mq_plan_entry ) {

        build_multi_query_plan_entry(
            mq_plan_entry, grouped_info.existing_write_partitions_set_,
            grouped_info.existing_read_partitions_set_ );
        plan->plan_->mq_plan_ = mq_plan_entry;
        if( plan->per_site_parts_.size() == 1 ) {
            mq_plan_entry->set_plan_destination(
                plan->per_site_parts_.begin()->first,
                multi_query_plan_type::PLANNING );
        } else {
            mq_plan_entry->set_scan_plan_destination(
                pids_to_site, multi_query_plan_type::PLANNING );
        }
    }

    merge_snapshot_versions_if_larger( session,
                                       grouped_info.inflight_snapshot_vector_ );


    if( !grouped_info.inflight_snapshot_vector_.empty() ) {
        plan->plan_->inflight_pids_ = grouped_info.inflight_pids_;

        for( const auto &pid : grouped_info.inflight_pids_ ) {
            DCHECK_EQ( pids_to_site.count( pid ), 1 );
            int site = pids_to_site.at( pid );
            if( plan->inflight_pids_.count( site ) == 0 ) {
                plan->inflight_pids_.emplace(
                    site, std::vector<partition_column_identifier>() );
            }
            plan->inflight_pids_.at( site ).emplace_back( pid );
        }

        DVLOG( 10 ) << "In flight changes:"
                    << grouped_info.inflight_snapshot_vector_
                    << ", pids:" << grouped_info.inflight_pids_;
    }
    double inflight_wait_for = 0;
    double read_wait_for = 0;
    for( const auto &entry : plan->per_site_parts_ ) {
        int         dest = entry.first;
        double      loc_read_wait_for = 0;

        DCHECK_EQ( 1, plan->per_site_part_locs_.count( dest ) );
        const auto &locs = plan->per_site_part_locs_.at( dest );

        site_partition_version_info_
            ->estimate_number_of_updates_need_to_wait_for( entry.second, locs,
                                                           session, dest );

        read_wait_for = std::max( read_wait_for, loc_read_wait_for );

        inflight_wait_for = 0;
    }

    plan->plan_->estimated_number_of_updates_to_wait_for_ =
        std::max( read_wait_for, inflight_wait_for );

}

bool heuristic_site_evaluator::has_converged(
    const std::vector<double> &changes ) const {
    if( changes.size() < 2 ) {
        return false;
    }

    double prev = changes.at( 0 );
    for( uint32_t pos = 1; pos < changes.size(); pos++ ) {
        double cur = changes.at( pos );
        double slope = ( cur - prev ) / (double) pos;
        DVLOG( 40 ) << "Changes at pos:" << pos << " = " << cur
                    << ", prev=" << prev << ",slope:" << slope;
        prev = cur;

        if( slope <= configs_.plan_scan_limit_ratio_ ) {
            DVLOG( 40 ) << "Changes at pos:" << pos << " = " << cur
                        << ", prev=" << prev << ",slope:" << slope
                        << ", has converged!";
            return true;
        }
    }

    return false;
}

std::shared_ptr<multi_site_pre_transaction_plan>
    heuristic_site_evaluator::generate_scan_plan(
        const ::clientid id, const grouped_partition_information &grouped_info,
        const std::vector<scan_arguments> &scan_args,
        const std::unordered_map<uint32_t /* label*/,
                                 partition_column_identifier_to_ckrs>
            &labels_to_pids_and_ckrs,
        const partition_column_identifier_map_t<std::unordered_map<
            uint32_t /* label */, std::vector<cell_key_ranges>>>
            &pids_to_labels_and_ckrs,
        const std::unordered_map<uint32_t /* label */, uint32_t /* pos */>
            &                                     label_to_pos,
        const std::vector<site_load_information> &site_load_infos,
        const std::unordered_map<storage_tier_type::type, std::vector<double>>
            &                                    storage_sizes,
        std::shared_ptr<multi_query_plan_entry> &mq_plan_entry,
        snapshot_vector &                        session ) {

    std::shared_ptr<multi_site_pre_transaction_plan> plan =
        std::make_shared<multi_site_pre_transaction_plan>(
            enumerator_holder_->construct_decision_holder_ptr() );

    DVLOG( 10 )
        << "Generate scan plan:"
        << ", write_partitions_set:"
        << payloads_to_identifiers( grouped_info.existing_write_partitions_ )
        << ", read_partitions_set:"
        << payloads_to_identifiers( grouped_info.existing_read_partitions_ );

    /* plan */
    partition_column_identifier_map_t<int> pids_to_site;
    std::unordered_map<int /*site */, std::unordered_map<uint32_t /* label */,
                                                         uint32_t /* pos */>>
        site_label_positions;
    std::unordered_map<int /* site */, double> costs_per_site;

    cached_ss_stat_holder      cached_stats;
    query_arrival_cached_stats cached_query_arrival_stats;

    partition_column_identifier_map_t<std::shared_ptr<pre_action>> pid_dep_map;

    partition_column_identifier_map_t<
        std::unordered_map<uint32_t, partition_type::type>>
        created_partition_types;

    partition_column_identifier_map_t<
        std::unordered_map<uint32_t, storage_tier_type::type>>
        created_storage_types;


    partition_column_identifier_map_t<
        std::vector<transaction_partition_accesses>>
        sampled_partition_accesses_index_by_write_partition;
    partition_column_identifier_map_t<
        std::vector<transaction_partition_accesses>>
        sampled_partition_accesses_index_by_read_partition;

    build_sampled_transactions(
        id, sampled_partition_accesses_index_by_write_partition,
        sampled_partition_accesses_index_by_write_partition );

    partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        other_write_locked_partitions;

    DCHECK( grouped_info.existing_write_partitions_.empty() );

    master_destination_partition_entries to_add_replicas;
    master_destination_partition_entries to_change_types;

    partition_column_identifier_unordered_set ori_write_set(
        grouped_info.existing_write_partitions_set_.begin(),
        grouped_info.existing_write_partitions_set_.end() );
    partition_column_identifier_unordered_set ori_read_set(
        grouped_info.existing_read_partitions_set_.begin(),
        grouped_info.existing_read_partitions_set_.end() );
    partition_column_identifier_map_t<
        std::shared_ptr<partition_location_information>>
        partition_location_informations(
            grouped_info.partition_location_informations_.begin(),
            grouped_info.partition_location_informations_.end() );

    partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        read_partitions_map;
    partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        write_partitions_map;

    std::unordered_map<storage_tier_type::type, std::vector<double>>
        loc_storage_sizes = storage_sizes;

    std::vector<std::shared_ptr<partition_payload>> parts_in_order;
    if( configs_.plan_scan_in_cost_order_ ) {
        DVLOG( 40 ) << "Build no change scan cost to get costs";
        std::shared_ptr<multi_query_plan_entry> empty_mq_plan_entry;
        auto no_change_plan = build_no_change_scan_plan(
            id, grouped_info, scan_args, labels_to_pids_and_ckrs,
            pids_to_labels_and_ckrs, label_to_pos, site_load_infos,
            storage_sizes, empty_mq_plan_entry, session );
        if( no_change_plan ) {
            std::sort( no_change_plan->part_costs_.begin(),
                       no_change_plan->part_costs_.end(),
                       std::greater<double>() );
            double last_seen = DBL_MAX;
            for( double d : no_change_plan->part_costs_ ) {
                if( d == last_seen ) {
                    continue;
                }
                last_seen = d;
                auto found = no_change_plan->per_part_costs_.find( d );
                DCHECK( found != no_change_plan->per_part_costs_.end() );
                for( const auto &part : found->second ) {
                    DVLOG( 40 ) << "Add part in order:" << part->identifier_
                                << ", cost:" << d;
                    parts_in_order.emplace_back( part );
                }
            }
        }
    }
    if( parts_in_order.empty() ) {
        for( const auto &part : grouped_info.existing_read_partitions_ ) {
            parts_in_order.emplace_back( part );
        }
    }

    bool allow_more_changes = true;

    double total_part_cost = 0;
    double total_no_change_part_cost = 0;

    std::vector<double> change_gaps;

    for( const auto &part : parts_in_order ) {
        DCHECK( part );
        const auto &pid = part->identifier_;

        DVLOG( 40 ) << "Considering pid:" << pid;

        auto        part_loc_info =
            grouped_info.partition_location_informations_.at( pid );
        const auto found_pid_entry = pids_to_labels_and_ckrs.find( pid );
        if( found_pid_entry == pids_to_labels_and_ckrs.end() ) {
            continue;
        }

        read_partitions_map[pid] = part;
        const auto &label_to_ckrs = found_pid_entry->second;

        auto costs = add_partition_to_scan_plan(
            plan, part, pid, part_loc_info, label_to_ckrs, label_to_pos,
            scan_args, site_load_infos, loc_storage_sizes, session,
            pids_to_site, site_label_positions, costs_per_site, pid_dep_map,
            created_partition_types, created_storage_types, allow_more_changes,
            other_write_locked_partitions, partition_location_informations,
            ori_write_set, ori_read_set,
            sampled_partition_accesses_index_by_write_partition,
            sampled_partition_accesses_index_by_read_partition, to_add_replicas,
            to_change_types, cached_stats, cached_query_arrival_stats );

        if( configs_.plan_scan_limit_changes_ and allow_more_changes ) {
            total_part_cost += std::get<0>( costs );
            total_no_change_part_cost += std::get<1>( costs );
            double change_gap = total_no_change_part_cost - total_part_cost;
            change_gaps.emplace_back( change_gap );
            DVLOG( 40 ) << "change gaps:" << change_gaps;
            allow_more_changes = !has_converged( change_gaps );
            DVLOG( 40 ) << "Allow more changes:" << allow_more_changes;
#if 0
            double ratio =
                ( total_part_cost + 1 ) / ( total_no_change_part_cost + 1 );
            allow_more_changes = ( ratio <= configs_.plan_scan_limit_ratio_ );
            DVLOG( 40 ) << "Plan scan ratio:" << ratio
                        << ", config ratio:" << configs_.plan_scan_limit_ratio_
                        << ", allow_more_changes:" << allow_more_changes
                        << ", total_part_cost:" << total_part_cost
                        << ", total_no_change_part_cost:"
                        << total_no_change_part_cost;
#endif
        }
    }  // pid

    // do all the modifications
    DVLOG( 40 ) << "Consider adding replicas";

    for( auto &add_replica_entry : to_add_replicas ) {
        const auto &master_dest = add_replica_entry.first;
        auto &      parts = add_replica_entry.second;
        add_replicas_to_plan(
            parts.pids_, parts.payloads_, parts.location_infos_,
            parts.partition_types_, parts.storage_types_, plan->plan_,
            master_dest.master_, master_dest.destination_, pid_dep_map,
            other_write_locked_partitions, ori_write_set, ori_read_set,
            partition_location_informations, site_load_infos,
            sampled_partition_accesses_index_by_write_partition,
            sampled_partition_accesses_index_by_read_partition, cached_stats,
            cached_query_arrival_stats );
    }

    std::unordered_map<uint32_t, partition_column_identifier_unordered_set>
        remove_replica_sets;

    DVLOG( 40 ) << "Consider removing replicas";
    for( const auto& entry : plan->scan_arg_sites_ ) {
        int site = entry.first;
        for( const auto &tier : configs_.acceptable_storage_types_ ) {
            if( storage_stats_->compute_storage_ratio(
                    loc_storage_sizes.at( tier ).at( site ),
                    storage_stats_->get_storage_limit( site, tier ) ) >=
                configs_.storage_removal_threshold_ ) {
                remove_replicas_or_change_types_for_capacity(
                    site, tier, remove_replica_sets, to_change_types,
                    partition_location_informations, write_partitions_map,
                    read_partitions_map, pid_dep_map,
                    other_write_locked_partitions, created_partition_types,
                    created_storage_types, ori_write_set, ori_read_set,
                    site_load_infos, loc_storage_sizes,
                    sampled_partition_accesses_index_by_write_partition,
                    sampled_partition_accesses_index_by_read_partition,
                    cached_stats, cached_query_arrival_stats );
            }
        }
    }

    for( auto &change_type_entry : to_change_types ) {
        const auto &master_dest = change_type_entry.first;
        auto &      parts = change_type_entry.second;
        add_change_types_to_plan(
            parts.pids_, parts.payloads_, parts.location_infos_,
            parts.partition_types_, parts.storage_types_, plan->plan_,
            master_dest.destination_, pid_dep_map,
            other_write_locked_partitions, ori_write_set, ori_read_set,
            partition_location_informations, site_load_infos,
            sampled_partition_accesses_index_by_write_partition,
            sampled_partition_accesses_index_by_read_partition, cached_stats,
            cached_query_arrival_stats );
    }
    add_remove_replicas_to_plan(
        plan->plan_, remove_replica_sets, pid_dep_map, read_partitions_map,
        write_partitions_map, other_write_locked_partitions,
        partition_location_informations, ori_write_set, ori_read_set,
        site_load_infos, loc_storage_sizes, cached_stats,
        cached_query_arrival_stats );

    finalize_scan_plan( id, plan, grouped_info, pids_to_site, mq_plan_entry,
                        session );

    return plan;
}

std::tuple<double, double> heuristic_site_evaluator::add_partition_to_scan_plan(
    std::shared_ptr<multi_site_pre_transaction_plan> &     plan,
    const std::shared_ptr<partition_payload> &             payload,
    const partition_column_identifier &                    pid,
    const std::shared_ptr<partition_location_information> &part_loc_info,
    const std::unordered_map<uint32_t /* label */, std::vector<cell_key_ranges>>
        &labels_to_ckrs,
    const std::unordered_map<uint32_t /* label */, uint32_t /* pos */>
        &                                     label_to_pos,
    const std::vector<scan_arguments> &       scan_args,
    const std::vector<site_load_information> &site_load_infos,
    std::unordered_map<storage_tier_type::type, std::vector<double>>
        &                                   storage_sizes,
    const snapshot_vector &                 svv,
    partition_column_identifier_map_t<int> &pids_to_site,
    std::unordered_map<int /*site */, std::unordered_map<uint32_t /* label */,
                                                         uint32_t /* pos */>>
        &site_label_positions,
    std::unordered_map<int /* site */, double> &costs_per_site,
    partition_column_identifier_map_t<std::shared_ptr<pre_action>> &pid_dep_map,
    partition_column_identifier_map_t<std::unordered_map<
        uint32_t, partition_type::type>> &created_partition_types,
    partition_column_identifier_map_t<std::unordered_map<
        uint32_t, storage_tier_type::type>> &created_storage_types,
    bool allow_changes,
    partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &other_write_locked_partitions,
    partition_column_identifier_map_t<std::shared_ptr<
        partition_location_information>> &partition_location_informations,
    const partition_column_identifier_unordered_set
        &original_write_partition_set,
    const partition_column_identifier_unordered_set
        &original_read_partition_set,
    const partition_column_identifier_map_t<
        std::vector<transaction_partition_accesses>>
        &sampled_partition_accesses_index_by_write_partition,
    const partition_column_identifier_map_t<
        std::vector<transaction_partition_accesses>>
        &sampled_partition_accesses_index_by_read_partition,
    master_destination_partition_entries &to_add_replicas,
    master_destination_partition_entries &to_change_types,
    cached_ss_stat_holder &               cached_stats,
    query_arrival_cached_stats &          cached_query_arrival_stats ) {

    int found_destination = -1;

    double total_part_cost = 0;
    double total_part_no_change_cost = 0;

    for( const auto &label_entry : labels_to_ckrs ) {
        uint32_t label = label_entry.first;
        DCHECK_EQ( label_to_pos.count( label ), 1 );
        const auto &scan_arg = scan_args.at( label_to_pos.at( label ) );

        const std::vector<cell_key_ranges> &ckrs = label_entry.second;

        DVLOG( 40 ) << "Considering label:" << label << ", for pid:" << pid
                    << ", ckrs:" << ckrs;

        auto plan_change = add_change_scan_destination_to_plan(
            plan, found_destination, payload, pid, part_loc_info, ckrs, label,
            scan_arg, pids_to_site, site_label_positions, site_load_infos,
            storage_sizes, svv, costs_per_site, pid_dep_map,
            created_partition_types, created_storage_types, allow_changes,
            other_write_locked_partitions, partition_location_informations,
            original_write_partition_set, original_read_partition_set,
            sampled_partition_accesses_index_by_write_partition,
            sampled_partition_accesses_index_by_read_partition, to_add_replicas,
            to_change_types, cached_stats, cached_query_arrival_stats );

        found_destination = std::get<0>( plan_change );
        double loc_part_cost = std::get<1>( plan_change );
        double loc_part_no_change_cost = std::get<2>( plan_change );

        total_part_cost += loc_part_cost;
        total_part_no_change_cost += loc_part_no_change_cost;
    }

    DVLOG( 40 ) << "Total part cost:" << total_part_cost
                << ", no change cost:" << total_part_no_change_cost;

    return std::make_tuple<>( total_part_cost, total_part_no_change_cost );
}

std::tuple<int, double /*cost */, double /*best no change */>
    heuristic_site_evaluator::add_change_scan_destination_to_plan(
        std::shared_ptr<multi_site_pre_transaction_plan> &plan,
        int prev_destination, const std::shared_ptr<partition_payload> &part,
        const partition_column_identifier &                    pid,
        const std::shared_ptr<partition_location_information> &part_loc_info,
        const std::vector<cell_key_ranges> &ckrs, int label,
        const scan_arguments &                  scan_arg,
        partition_column_identifier_map_t<int> &pids_to_site,
        std::unordered_map<
            int /*site */,
            std::unordered_map<uint32_t /* label */, uint32_t /* pos */>>
            &                                     site_label_positions,
        const std::vector<site_load_information> &site_load_infos,
        std::unordered_map<storage_tier_type::type, std::vector<double>>
            &                  storage_sizes,
        const snapshot_vector &svv,
        std::unordered_map<int /* site */, double> &costs_per_site,
        partition_column_identifier_map_t<std::shared_ptr<pre_action>>
            &pid_dep_map,
        partition_column_identifier_map_t<std::unordered_map<
            uint32_t, partition_type::type>> &created_partition_types,
        partition_column_identifier_map_t<std::unordered_map<
            uint32_t, storage_tier_type::type>> &created_storage_types,
        bool allow_changes,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &other_write_locked_partitions,
        partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>> &partition_location_informations,
        const partition_column_identifier_unordered_set
            &original_write_partition_set,
        const partition_column_identifier_unordered_set
            &original_read_partition_set,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>>
            &sampled_partition_accesses_index_by_write_partition,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>>
            &sampled_partition_accesses_index_by_read_partition,
        master_destination_partition_entries &to_add_replicas,
        master_destination_partition_entries &to_change_types,
        cached_ss_stat_holder &               cached_stats,
        query_arrival_cached_stats &          cached_query_arrival_stats ) {
    DCHECK( part );
    DCHECK( part_loc_info );

    double contention =
        cost_model2_->normalize_contention_by_time( part->get_contention() );
    std::vector<double> avg_cell_widths =
        query_stats_->get_and_cache_cell_widths( part->identifier_,
                                                 cached_stats );
    uint32_t num_entries = get_number_of_rows( part->identifier_ );
    double   scan_selectivity =
        get_scan_selectivity( part, scan_arg, cached_stats );

    transaction_prediction_stats txn_stat(
        partition_type::type::ROW, storage_tier_type::type::MEMORY, contention,
        0 /* num_updates_needed */, avg_cell_widths, num_entries,
        true /* is_scan */, scan_selectivity, false /* is_point_read */,
        0 /* num_point_reads */, false /* is_point_update */,
        0 /* num_point_updates */ );

    change_scan_plan best_change_scan;
    best_change_scan.cost_ = DBL_MAX;
    best_change_scan.destination_ = part_loc_info->master_location_;
    double best_no_change = DBL_MAX;

    int num_sites = get_num_sites();
    DVLOG( 40 ) << "Num sites:" << num_sites;

    int dest_end = num_sites - 1;
    int dest_start = 0;

    if( prev_destination >= 0 ) {
        dest_end = prev_destination;
        dest_start = prev_destination;
    }

    for( int destination = dest_start; destination <= dest_end;
         destination++ ) {
        DVLOG( 40 ) << "Considering destination:" << destination;

        partition_type::type    loc_part_type;
        storage_tier_type::type loc_storage_type;

        bool can_do_no_change = false;

        bool allow_change = allow_changes;
        if( ( created_storage_types.count( pid ) == 1 ) and
            ( created_partition_types.count( pid ) == 1 ) and
            ( created_storage_types.at( pid ).count( destination ) == 1 ) and
            ( created_partition_types.at( pid ).count( destination ) == 1 ) ) {
            allow_change = false;

            loc_part_type = created_partition_types.at( pid ).at( destination );
            loc_storage_type =
                created_storage_types.at( pid ).at( destination );

            can_do_no_change = true;

        } else if( ( part_loc_info->replica_locations_.count( destination ) ==
                     1 ) or
                   ( (int) part_loc_info->master_location_ == destination ) ) {
            loc_part_type =
                std::get<1>( part->get_partition_type( destination ) );
            loc_storage_type =
                std::get<1>( part->get_storage_type( destination ) );

            can_do_no_change = true;
        }

        if( can_do_no_change ) {
            DVLOG( 40 ) << "Considering no change cost";
            double no_change_cost = get_scan_destination_cost(
                plan, destination, loc_part_type, loc_storage_type, txn_stat,
                label, scan_arg, site_load_infos, storage_sizes, svv,
                costs_per_site, cached_stats, cached_query_arrival_stats );

            if( no_change_cost < best_change_scan.cost_ ) {
                best_change_scan.cost_ = no_change_cost;
                best_change_scan.destination_ = destination;
                best_change_scan.add_replica_ = false;
                best_change_scan.change_type_ = false;

                best_change_scan.part_type_ = loc_part_type;
                best_change_scan.storage_type_ = loc_storage_type;

            }
            if ( no_change_cost < best_no_change) {
                best_no_change = no_change_cost;
            }
        }

        if( allow_change ) {
            DVLOG( 40 ) << "Considering allow change cost";
            auto change_res = get_change_scan_destination_and_cost(
                plan, destination, part, pid, part_loc_info, ckrs, label,
                scan_arg, txn_stat, pids_to_site, site_label_positions,
                site_load_infos, storage_sizes, svv, costs_per_site,
                pid_dep_map, created_partition_types, created_storage_types,
                other_write_locked_partitions, partition_location_informations,
                original_write_partition_set, original_read_partition_set,
                sampled_partition_accesses_index_by_write_partition,
                sampled_partition_accesses_index_by_read_partition,
                cached_stats, cached_query_arrival_stats );

            if( change_res.cost_ < best_change_scan.cost_ ) {
                best_change_scan = change_res;
            }
        }
    }

    DVLOG( 40 ) << "Got best destination:" << best_change_scan.destination_;

    txn_stat.part_type_ = best_change_scan.part_type_;
    txn_stat.storage_type_ = best_change_scan.storage_type_;

    add_to_scan_plan( plan, best_change_scan.destination_, pid, part,
                      part_loc_info, ckrs, txn_stat, scan_arg, pids_to_site,
                      site_label_positions, site_load_infos, costs_per_site );

    if( best_change_scan.add_replica_ ) {
        created_storage_types[pid][best_change_scan.destination_] =
            best_change_scan.storage_type_;
        created_partition_types[pid][best_change_scan.destination_] =
            best_change_scan.part_type_;
        master_destination_dependency_version master_dest(
            part_loc_info->master_location_, best_change_scan.destination_, 0 );
        add_to_partition_operation_tracking(
            to_add_replicas, master_dest, pid, part, part_loc_info,
            best_change_scan.part_type_, best_change_scan.storage_type_ );
    } else if( best_change_scan.change_type_ ) {
        created_storage_types[pid][best_change_scan.destination_] =
            best_change_scan.storage_type_;
        created_partition_types[pid][best_change_scan.destination_] =
            best_change_scan.part_type_;
        master_destination_dependency_version master_dest(
            best_change_scan.destination_, best_change_scan.destination_, 0 );
        add_to_partition_operation_tracking(
            to_change_types, master_dest, pid, part, part_loc_info,
            best_change_scan.part_type_, best_change_scan.storage_type_ );
    }

    return std::make_tuple<>( best_change_scan.destination_,
                              best_change_scan.cost_, best_no_change );
}

change_scan_plan heuristic_site_evaluator::get_change_scan_destination_and_cost(
    std::shared_ptr<multi_site_pre_transaction_plan> &plan, int destination,
    const std::shared_ptr<partition_payload> &             part,
    const partition_column_identifier &                    pid,
    const std::shared_ptr<partition_location_information> &part_loc_info,
    const std::vector<cell_key_ranges> &ckrs, int label,
    const scan_arguments &scan_arg, transaction_prediction_stats &txn_stat,
    partition_column_identifier_map_t<int> &pids_to_site,
    std::unordered_map<int /*site */, std::unordered_map<uint32_t /* label */,
                                                         uint32_t /* pos */>>
        &                                     site_label_positions,
    const std::vector<site_load_information> &site_load_infos,
    std::unordered_map<storage_tier_type::type, std::vector<double>>
        &                  storage_sizes,
    const snapshot_vector &svv,
    std::unordered_map<int /* site */, double> &costs_per_site,
    partition_column_identifier_map_t<std::shared_ptr<pre_action>> &pid_dep_map,
    partition_column_identifier_map_t<std::unordered_map<
        uint32_t, partition_type::type>> &created_partition_types,
    partition_column_identifier_map_t<std::unordered_map<
        uint32_t, storage_tier_type::type>> &created_storage_types,
    partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        &other_write_locked_partitions,
    partition_column_identifier_map_t<std::shared_ptr<
        partition_location_information>> &partition_location_informations,
    const partition_column_identifier_unordered_set
        &original_write_partition_set,
    const partition_column_identifier_unordered_set
        &original_read_partition_set,
    const partition_column_identifier_map_t<
        std::vector<transaction_partition_accesses>>
        &sampled_partition_accesses_index_by_write_partition,
    const partition_column_identifier_map_t<
        std::vector<transaction_partition_accesses>>
        &                  sampled_partition_accesses_index_by_read_partition,
    cached_ss_stat_holder &cached_stats,
    query_arrival_cached_stats &cached_query_arrival_stats ) {

    change_scan_plan change_scan;
    change_scan.cost_ = DBL_MAX;
    change_scan.destination_ = destination;
    change_scan.add_replica_ = false;
    change_scan.change_type_ = false;

    // make sure we haven't already created it
    if( ( created_storage_types.count( pid ) == 1 and
          ( created_storage_types.at( pid ).count( destination ) == 1 ) ) or
        ( created_partition_types.count( pid ) == 1 and
          ( created_partition_types.at( pid ).count( destination ) == 1 ) ) ) {

        return change_scan;
    }

    double upfront_cost = 0;

    if( ( destination == (int) part_loc_info->master_location_ ) or
        ( part_loc_info->replica_locations_.count( destination ) == 1 ) ) {
        auto cur_part_type =
            std::get<1>( part_loc_info->get_partition_type( destination ) );
        auto cur_storage_type =
            std::get<1>( part_loc_info->get_storage_type( destination ) );

        auto new_types = get_change_type(
            part, part_loc_info, cur_part_type, cur_storage_type, destination,
            plan->plan_, other_write_locked_partitions,
            partition_location_informations, original_write_partition_set,
            original_read_partition_set, site_load_infos, storage_sizes,
            sampled_partition_accesses_index_by_write_partition,
            sampled_partition_accesses_index_by_read_partition, cached_stats,
            cached_query_arrival_stats, false /* is not write */ );
        partition_type::type    new_type = std::get<0>( ( new_types ) );
        storage_tier_type::type new_storage_type = std::get<1>( ( new_types ) );

        if( ( cur_part_type == new_type ) and
            ( cur_storage_type == new_storage_type ) ) {
            return change_scan;
        }

        change_scan.change_type_ = true;
        change_scan.add_replica_ = false;
        change_scan.part_type_ = new_type;
        change_scan.storage_type_ = new_storage_type;

        change_types_stats change_stat(
            cur_part_type, new_type, cur_storage_type, new_storage_type,
            cost_model2_->normalize_contention_by_time(
                part->get_contention() ),
            query_stats_->get_and_cache_cell_widths( pid, cached_stats ),
            part->get_num_rows() );
        std::vector<change_types_stats> change_stats = {change_stat};

        cost_model_prediction_holder model_holder =
            cost_model2_->predict_changing_type_execution_time(
                site_load_infos.at( destination ).cpu_load_, change_stats );

        std::vector<std::shared_ptr<partition_payload>> parts_to_change = {
            part};
        std::vector<std::shared_ptr<partition_location_information>>
                                             parts_to_change_infos = {part_loc_info};
        std::vector<partition_type::type>    old_types = {cur_part_type};
        std::vector<storage_tier_type::type> old_storage_types = {
            cur_storage_type};
        std::vector<partition_type::type>    types_to_change = {new_type};
        std::vector<storage_tier_type::type> storage_to_change = {
            new_storage_type};

        double expected_benefit = get_expected_benefit_of_changing_types(
            parts_to_change, parts_to_change_infos, types_to_change,
            storage_to_change, old_types, old_storage_types, change_stats,
            destination, other_write_locked_partitions,
            partition_location_informations, original_write_partition_set,
            original_read_partition_set, site_load_infos,
            sampled_partition_accesses_index_by_write_partition,
            sampled_partition_accesses_index_by_read_partition, cached_stats,
            cached_query_arrival_stats );

        upfront_cost = model_holder.get_prediction() +
                       ( configs_.horizon_weight_ * -expected_benefit );

        // change type
    } else {
        auto found_types = get_replica_type(
            part, part_loc_info, part_loc_info->master_location_, destination,
            plan->plan_, other_write_locked_partitions,
            partition_location_informations, original_write_partition_set,
            original_read_partition_set, site_load_infos, storage_sizes,
            sampled_partition_accesses_index_by_write_partition,
            sampled_partition_accesses_index_by_read_partition, cached_stats,
            cached_query_arrival_stats );
        partition_type::type    dest_type = std::get<0>( found_types );
        storage_tier_type::type dest_storage_type = std::get<1>( found_types );

        add_replica_stats add_replica(
            std::get<1>( part_loc_info->get_partition_type(
                part_loc_info->master_location_ ) ),
            std::get<1>( part_loc_info->get_storage_type(
                part_loc_info->master_location_ ) ),
            dest_type, dest_storage_type,
            cost_model2_->normalize_contention_by_time(
                part->get_contention() ),
            query_stats_->get_and_cache_cell_widths( part->identifier_,
                                                     cached_stats ),
            part->get_num_rows() );

        // add replica
        double dest_load =
            site_load_infos.at( part_loc_info->master_location_ ).cpu_load_;
        double source_load = site_load_infos.at( destination ).cpu_load_;

        std::vector<add_replica_stats> add_stats = {add_replica};

        cost_model_prediction_holder model_holder =
            cost_model2_->predict_add_replica_execution_time(
                source_load, dest_load, add_stats );

        std::vector<std::shared_ptr<partition_payload>> parts_to_replicate = {
            part};
        std::vector<std::shared_ptr<partition_location_information>>
                                             part_locations_to_replicate = {part_loc_info};
        std::vector<partition_type::type>    partition_types = {dest_type};
        std::vector<storage_tier_type::type> storage_types = {
            dest_storage_type};

        double expected_benefit = get_expected_benefit_of_adding_replicas(
            parts_to_replicate, part_locations_to_replicate, partition_types,
            storage_types, part_loc_info->master_location_, destination,
            other_write_locked_partitions, partition_location_informations,
            original_write_partition_set, original_read_partition_set,
            site_load_infos,
            sampled_partition_accesses_index_by_write_partition,
            sampled_partition_accesses_index_by_read_partition, cached_stats,
            cached_query_arrival_stats );

        upfront_cost =
            ( configs_.upfront_cost_weight_ * model_holder.get_prediction() ) +
            ( configs_.horizon_weight_ * -expected_benefit );

        change_scan.part_type_ = dest_type;
        change_scan.storage_type_ = dest_storage_type;
        change_scan.change_type_ = false;
        change_scan.add_replica_ = true;
    }

    double scan_cost = get_scan_destination_cost(
        plan, destination, change_scan.part_type_, change_scan.storage_type_,
        txn_stat, label, scan_arg, site_load_infos, storage_sizes, svv,
        costs_per_site, cached_stats, cached_query_arrival_stats );

    change_scan.cost_ =
        scan_cost + ( configs_.scan_upfront_cost_weight_ * upfront_cost );

    return change_scan;
}

void heuristic_site_evaluator::add_to_partition_operation_tracking(
    master_destination_partition_entries &                 to_add,
    const master_destination_dependency_version &          master_dest,
    const partition_column_identifier &                    pid,
    const std::shared_ptr<partition_payload> &             part,
    const std::shared_ptr<partition_location_information> &part_loc_info,
    const partition_type::type &                           p_type,
    const storage_tier_type::type &                        s_type ) const {
    auto entry_search = to_add.find( master_dest );
    if( entry_search == to_add.end() ) {
        partitions_entry part_entry;
        part_entry.add( pid, part, part_loc_info, p_type, s_type );
        to_add.emplace( master_dest, part_entry );

    } else {
        entry_search->second.add( pid, part, part_loc_info, p_type, s_type );
    }
}
void heuristic_site_evaluator::add_to_partition_operation_tracking(
    master_destination_partition_entries &                 to_add,
    const master_destination_dependency_version &          master_dest,
    const partition_column_identifier &                    pid,
    const std::shared_ptr<partition_payload> &             part,
    const std::shared_ptr<partition_location_information> &part_loc_info )
    const {
    auto entry_search = to_add.find( master_dest );
    if( entry_search == to_add.end() ) {
        partitions_entry part_entry;
        part_entry.add( pid, part, part_loc_info );
        to_add.emplace( master_dest, part_entry );

    } else {
        entry_search->second.add( pid, part, part_loc_info );
    }
}


double internal_get_upcoming_query_arrival_score(
    const partition_column_identifier &pid, bool allow_updates,
    query_arrival_cached_stats &             cached_arrival_stats,
    std::shared_ptr<query_arrival_predictor> query_predictor,
    bool                                     use_query_arrival_predictor,
    double arrival_time_no_prediction_default_score,
    double arrival_time_score_scalar ) {
    DVLOG( 40 ) << "get_upcoming_query_arrival_score:" << pid
                << ", allow_updates:" << allow_updates;
    double upcoming_access_score = 1;
    if( use_query_arrival_predictor ) {
        upcoming_access_score = arrival_time_no_prediction_default_score;
        auto prediction = query_predictor->get_next_access_from_cached_stats(
            pid, allow_updates, cached_arrival_stats );
        if( prediction.found_ ) {
            double denom =
                (double) translate_to_epoch_time( prediction.offset_time_ );
            const std::time_t t_c = std::chrono::system_clock::to_time_t(
                prediction.predicted_time_ );
            DVLOG( 40 ) << "Found prediction, offset time:"
                        << std::chrono::duration_cast<std::chrono::seconds>(
                               prediction.offset_time_.time_since_epoch() )
                               .count()
                        << " (seconds), predicted time:"
                        << std::put_time( std::localtime( &t_c ), "%F %T" )
                        << ", in epoch:" << denom
                        << ", count:" << prediction.count_;
            if( denom < DBL_MAX - 1 ) {
                denom += 1;
            }
            upcoming_access_score =
                // further access ==> smaller score ==> more likely to be
                // removed
                ( upcoming_access_score / denom ) * arrival_time_score_scalar *
                ( prediction.count_ + 1 );
        }
    }
    DVLOG( 40 ) << "get_upcoming_query_arrival_score:" << pid
                << ", allow_updates:" << allow_updates
                << ", upcoming_access_score:" << upcoming_access_score;
    return upcoming_access_score;
}

double heuristic_site_evaluator::get_upcoming_query_arrival_score(
    const partition_column_identifier &pid, bool allow_updates,
    query_arrival_cached_stats &cached_arrival_stats ) {
    return internal_get_upcoming_query_arrival_score(
        pid, allow_updates, cached_arrival_stats, query_predictor_,
        configs_.use_query_arrival_predictor_,
        configs_.arrival_time_no_prediction_default_score_,
        configs_.arrival_time_score_scalar_ );
}

int heuristic_site_evaluator::get_scan_destination(
    std::shared_ptr<multi_site_pre_transaction_plan> &     plan,
    const std::shared_ptr<partition_payload> &             part,
    const std::shared_ptr<partition_location_information> &part_loc_info,
    const std::vector<cell_key_ranges> &ckrs, uint32_t label,
    const scan_arguments &                    scan_arg,
    const std::vector<site_load_information> &site_load_infos,
    const std::unordered_map<storage_tier_type::type, std::vector<double>>
        &                    storage_sizes,
    const ::snapshot_vector &svv,
    std::unordered_map<int /* site */, double> &costs_per_site,
    cached_ss_stat_holder &     cached_stats,
    query_arrival_cached_stats &cached_arrival_stats ) {
    DCHECK( part );
    DCHECK( part_loc_info );

    double contention =
        cost_model2_->normalize_contention_by_time( part->get_contention() );
    std::vector<double> avg_cell_widths =
        query_stats_->get_and_cache_cell_widths( part->identifier_,
                                                 cached_stats );
    uint32_t num_entries = get_number_of_rows( part->identifier_ );
    double   scan_selectivity =
        get_scan_selectivity( part, scan_arg, cached_stats );

    transaction_prediction_stats txn_stat(
        partition_type::type::ROW, storage_tier_type::type::MEMORY, contention,
        0 /* num_updates_needed */, avg_cell_widths, num_entries,
        true /* is_scan */, scan_selectivity, false /* is_point_read */,
        0 /* num_point_reads */, false /* is_point_update */,
        0 /* num_point_updates */ );

    int    best_site = part_loc_info->master_location_;
    int    destination = part_loc_info->master_location_;
    double best_cost = get_scan_destination_cost(
        plan, destination,
        std::get<1>( part->get_partition_type( destination ) ),
        std::get<1>( part->get_storage_type( destination ) ), txn_stat, label,
        scan_arg, site_load_infos, storage_sizes, svv, costs_per_site,
        cached_stats, cached_arrival_stats );

    for( int destination : part_loc_info->replica_locations_ ) {
        double cost = get_scan_destination_cost(
            plan, destination,
            std::get<1>( part->get_partition_type( destination ) ),
            std::get<1>( part->get_storage_type( destination ) ), txn_stat,
            label, scan_arg, site_load_infos, storage_sizes, svv,
            costs_per_site, cached_stats, cached_arrival_stats );
        if( cost < best_cost ) {
            best_cost = cost;
            best_site = destination;
        }
    }

    return best_site;
}

double heuristic_site_evaluator::get_scan_destination_cost(
    std::shared_ptr<multi_site_pre_transaction_plan> &plan, int destination,
    const partition_type::type &   part_type,
    const storage_tier_type::type &storage_type,
    transaction_prediction_stats &txn_stat, uint32_t label,
    const scan_arguments &                    scan_arg,
    const std::vector<site_load_information> &site_load_infos,
    const std::unordered_map<storage_tier_type::type, std::vector<double>>
        &                    storage_sizes,
    const ::snapshot_vector &svv,
    std::unordered_map<int /* site */, double> &costs_per_site,
    cached_ss_stat_holder &     cached_stats,
    query_arrival_cached_stats &cached_query_arrival_stats ) {

    txn_stat.storage_type_ = storage_type;
    txn_stat.part_type_ = part_type;

    std::vector<transaction_prediction_stats> txn_stats;
    auto                                      empty_exec_at_site_cost =
        cost_model2_->predict_single_site_transaction_execution_time(
            site_load_infos.at( destination ).cpu_load_, txn_stats );

    txn_stats.emplace_back( txn_stat );
    auto exec_at_site_cost =
        cost_model2_->predict_single_site_transaction_execution_time(
            site_load_infos.at( destination ).cpu_load_, txn_stats );

    bool is_insert_to_site = false;
    if( costs_per_site.count( destination ) == 0 ) {
        is_insert_to_site = true;
        costs_per_site[destination] = exec_at_site_cost.get_prediction();
    } else {
        costs_per_site[destination] +=
            ( exec_at_site_cost.get_prediction() -
              empty_exec_at_site_cost.get_prediction() );
    }

    auto scan_latency_pred =
        cost_model2_->predict_distributed_scan_execution_time( costs_per_site );
    double scan_latency = scan_latency_pred.get_prediction();

    if( is_insert_to_site ) {
        costs_per_site.erase( destination );
    }

    return scan_latency;
}

double heuristic_site_evaluator::add_to_scan_plan(
    std::shared_ptr<multi_site_pre_transaction_plan> &plan, int destination,
    const std::shared_ptr<partition_payload> &             payload,
    const std::shared_ptr<partition_location_information> &part_loc_info,
    const partition_column_identifier &                    pid,
    const std::vector<cell_key_ranges> &ckrs, const scan_arguments &scan_arg,
    partition_column_identifier_map_t<int> &pids_to_site,
    std::unordered_map<int /*site */, std::unordered_map<uint32_t /* label */,
                                                         uint32_t /* pos */>>
        &                                     site_label_positions,
    const std::vector<site_load_information> &site_load_infos,
    std::unordered_map<int /* site */, double> &costs_per_site,
    cached_ss_stat_holder &cached_stats ) {

    partition_type::type part_type =
        std::get<1>( part_loc_info->get_partition_type( destination ) );
    storage_tier_type::type storage_type =
        std::get<1>( part_loc_info->get_storage_type( destination ) );
    double contention =
        cost_model2_->normalize_contention_by_time( payload->get_contention() );
    std::vector<double> avg_cell_widths =
        query_stats_->get_and_cache_cell_widths( pid, cached_stats );
    uint32_t num_entries = get_number_of_rows( pid );
    double   scan_selectivity =
        get_scan_selectivity( payload, scan_arg, cached_stats );

    transaction_prediction_stats txn_stat(
        part_type, storage_type, contention, 0 /* num_updates_needed */,
        avg_cell_widths, num_entries, true /* is_scan */, scan_selectivity,
        false /* is_point_read */, 0 /* num_point_reads */,
        false /* is_point_update */, 0 /* num_point_updates */ );
    return add_to_scan_plan( plan, destination, pid, payload, part_loc_info,
                             ckrs, txn_stat, scan_arg, pids_to_site,
                             site_label_positions, site_load_infos,
                             costs_per_site );
}
double heuristic_site_evaluator::add_to_scan_plan(
    std::shared_ptr<multi_site_pre_transaction_plan> &plan, int destination,
    const partition_column_identifier &                    pid,
    const std::shared_ptr<partition_payload> &             part,
    const std::shared_ptr<partition_location_information> &part_loc_info,
    const std::vector<cell_key_ranges> &                   ckrs,
    const transaction_prediction_stats &                   txn_stat,
    const scan_arguments &                                 scan_arg,
    partition_column_identifier_map_t<int> &               pids_to_site,
    std::unordered_map<int /*site */, std::unordered_map<uint32_t /* label */,
                                                         uint32_t /* pos */>>
        &                                     site_label_positions,
    const std::vector<site_load_information> &site_load_infos,
    std::unordered_map<int /* site */, double> &costs_per_site ) {

    std::vector<transaction_prediction_stats> txn_stats;
    auto                                      empty_exec_at_site_cost =
        cost_model2_->predict_single_site_transaction_execution_time(
            site_load_infos.at( destination ).cpu_load_, txn_stats );

    if( plan->scan_arg_sites_.count( destination ) == 0 ) {
        DCHECK_EQ( 0, plan->read_pids_.count( destination ) );
        DCHECK_EQ( 0, plan->per_site_parts_.count( destination ) );
        DCHECK_EQ( 0, plan->per_site_part_locs_.count( destination ) );
        DCHECK_EQ( 0, plan->inflight_pids_.count( destination ) );
        DCHECK_EQ( 0, plan->txn_stats_.count( destination ) );
        DCHECK_EQ( 0, site_label_positions.count( destination ) );
        DCHECK_EQ( 0, costs_per_site.count( destination ) );

        plan->scan_arg_sites_.emplace( destination,
                                       std::vector<scan_arguments>() );
        plan->read_pids_.emplace( destination,
                                  std::vector<partition_column_identifier>() );
        plan->inflight_pids_.emplace(
            destination, std::vector<partition_column_identifier>() );
        plan->txn_stats_.emplace( destination,
                                  std::vector<transaction_prediction_stats>() );
        plan->per_site_parts_.emplace(
            destination, std::vector<std::shared_ptr<partition_payload>>() );
        plan->per_site_part_locs_.emplace(
            destination,
            std::vector<std::shared_ptr<partition_location_information>>() );

        site_label_positions.emplace(
            destination, std::unordered_map<uint32_t, uint32_t>() );

        costs_per_site.emplace( destination,
                                empty_exec_at_site_cost.get_prediction() );
    }

    if( pids_to_site.count( pid ) == 0 ) {
        pids_to_site[pid] = destination;

        // add to the global read pids
        plan->plan_->read_pids_.emplace_back( pid );

        plan->read_pids_.at( destination ).emplace_back( pid );
        plan->per_site_parts_.at( destination ).emplace_back( part );
        plan->per_site_part_locs_.at( destination )
            .emplace_back( part_loc_info );
    }
    // inflight pids gets filled in later
    uint32_t label = scan_arg.label;
    if( site_label_positions.at( destination ).count( label ) == 0 ) {
        site_label_positions.at( destination )
            .emplace( label, plan->scan_arg_sites_.at( destination ).size() );

        scan_arguments loc_scan_arg;
        loc_scan_arg.label = label;
        loc_scan_arg.predicate = scan_arg.predicate;
        plan->scan_arg_sites_.at( destination ).emplace_back( loc_scan_arg );
    }


    txn_stats.emplace_back( txn_stat );
    auto exec_at_site_cost =
        cost_model2_->predict_single_site_transaction_execution_time(
            site_load_infos.at( destination ).cpu_load_, txn_stats );

    uint32_t pos = site_label_positions.at( destination ).at( label );
    DCHECK_LT( pos, plan->scan_arg_sites_.at( destination ).size() );
    scan_arguments &scan_arg_to_edit =
        plan->scan_arg_sites_.at( destination ).at( pos );

    double scan_lat = ( exec_at_site_cost.get_prediction() -
                        empty_exec_at_site_cost.get_prediction() );

    scan_arg_to_edit.read_ckrs.insert( scan_arg_to_edit.read_ckrs.end(),
                                       ckrs.begin(), ckrs.end() );

    plan->txn_stats_.at( destination ).emplace_back( txn_stat );
    costs_per_site.at( destination ) += scan_lat;

    return scan_lat;
}

double heuristic_site_evaluator::get_scan_selectivity(
    const std::shared_ptr<partition_payload> &payload,
    const scan_arguments &                    scan_arg,
    cached_ss_stat_holder &                   cached_stats ) const {
    // MTODO be more accurate
    return query_stats_->get_and_cache_average_scan_selectivity(
        payload->identifier_, cached_stats );
}

std::shared_ptr<multi_site_pre_transaction_plan>
    heuristic_site_evaluator::generate_scan_plan_from_current_planners(
        const ::clientid id, const grouped_partition_information &grouped_info,
        const std::vector<scan_arguments> &scan_args,
        const std::unordered_map<uint32_t /* label*/,
                                 partition_column_identifier_to_ckrs>
            &labels_to_pids_and_ckrs,
        const partition_column_identifier_map_t<std::unordered_map<
            uint32_t /* label */, std::vector<cell_key_ranges>>>
            &pids_to_labels_and_ckrs,
        const std::unordered_map<uint32_t /* label */, uint32_t /* pos */>
            &                                     label_to_pos,
        const std::vector<site_load_information> &site_load_infos,
        const std::unordered_map<storage_tier_type::type, std::vector<double>>
            &                                    storage_sizes,
        std::shared_ptr<multi_query_plan_entry> &mq_plan_entry,
        snapshot_vector &                        session ) {
    DVLOG( 10 ) << "Generate no change scan plan from current planners:"
                << ", write_partitions_set:"
                << grouped_info.existing_write_partitions_set_
                << ", read_partitions_set:"
                << grouped_info.existing_read_partitions_set_;
    if( !configs_.enable_wait_for_plan_reuse_ ) {
        return nullptr;
    }

    transaction_query_entry query_entry = build_transaction_query_entry(
        grouped_info.existing_write_partitions_,
        grouped_info.existing_read_partitions_,
        grouped_info.existing_write_partitions_set_,
        grouped_info.existing_read_partitions_set_ );

    std::vector<
        std::tuple<double, uint64_t, std::shared_ptr<multi_query_plan_entry>>>
        sorted_plans = query_entry.get_sorted_plans();

    return generate_scan_plan_from_sorted_planners(
        id, grouped_info, scan_args, labels_to_pids_and_ckrs,
        pids_to_labels_and_ckrs, label_to_pos, site_load_infos, storage_sizes,
        sorted_plans, mq_plan_entry, session );
}
std::shared_ptr<multi_site_pre_transaction_plan>
    heuristic_site_evaluator::generate_scan_plan_from_sorted_planners(
        const ::clientid id, const grouped_partition_information &grouped_info,
        const std::vector<scan_arguments> &scan_args,
        const std::unordered_map<uint32_t /* label*/,
                                 partition_column_identifier_to_ckrs>
            &labels_to_pids_and_ckrs,
        const partition_column_identifier_map_t<std::unordered_map<
            uint32_t /* label */, std::vector<cell_key_ranges>>>
            &pids_to_labels_and_ckrs,
        const std::unordered_map<uint32_t /* label */, uint32_t /* pos */>
            &                                     label_to_pos,
        const std::vector<site_load_information> &site_load_infos,
        const std::unordered_map<storage_tier_type::type, std::vector<double>>
            &storage_sizes,
        const std::vector<std::tuple<double, uint64_t,
                                     std::shared_ptr<multi_query_plan_entry>>>
                                                 sorted_plans,
        std::shared_ptr<multi_query_plan_entry> &mq_plan_entry,
        snapshot_vector &                        session ) {

    std::shared_ptr<multi_site_pre_transaction_plan> plan =
        std::make_shared<multi_site_pre_transaction_plan>(
            enumerator_holder_->construct_decision_holder_ptr() );


    /* plan */
    partition_column_identifier_map_t<int> pids_to_site;
    std::unordered_map<int /*site */, std::unordered_map<uint32_t /* label */,
                                                         uint32_t /* pos */>>
        site_label_positions;

    std::unordered_map<int /* site */, double> costs_per_site;

    cached_ss_stat_holder      cached_stats;
    query_arrival_cached_stats cached_query_arrival_stats;

    DCHECK( grouped_info.existing_write_partitions_.empty() );
    for( const auto &part : grouped_info.existing_read_partitions_ ) {
        DCHECK( part );
        const auto &pid = part->identifier_;
        DVLOG( 40 ) << "Considering part:" << pid;

        auto part_loc_info =
            grouped_info.partition_location_informations_.at( pid );
        const auto found_pid_entry = pids_to_labels_and_ckrs.find( pid );
        if( found_pid_entry == pids_to_labels_and_ckrs.end() ) {
            continue;
        }

        int         found_destination = -1;
        const auto &label_to_ckrs = found_pid_entry->second;

        // look at them from previous plans
        for( const auto &entry : sorted_plans ) {
            if( std::get<0>( entry ) < configs_.plan_reuse_threshold_ ) {
                break;
            }
            auto plan_entry = std::get<2>( entry );
            auto dest_scan = plan_entry->get_scan_plan_destination();
            if( std::get<0>( dest_scan ) ) {
                auto part_dest = std::get<1>( dest_scan );
                auto found = part_dest.find( pid );
                if( found != part_dest.end() ) {
                    int cand_site = found->second;
                    if( ( cand_site ==
                          (int) part_loc_info->master_location_ ) or
                        ( part_loc_info->replica_locations_.count(
                              cand_site ) == 1 ) ) {
                        found_destination = cand_site;
                        DVLOG( 40 ) << "Part destination found from previous "
                                       "scan plans:"
                                    << found_destination;
                        break;
                    }
                }
            }
            auto dest_site = plan_entry->get_plan_destination();
            if( std::get<0>( dest_site ) ) {
                int cand_site = std::get<1>( dest_site );
                if( ( cand_site == (int) part_loc_info->master_location_ ) or
                    ( part_loc_info->replica_locations_.count( cand_site ) ==
                      1 ) ) {
                    found_destination = cand_site;
                    DVLOG( 40 ) << "Part destination found from previous plans:"
                                << found_destination;

                    break;
                }
            }
        }
        DVLOG( 40 ) << "Part destination from previous plans:"
                    << found_destination;

        double total_part_cost = 0;
        // add it to the plan
        for( const auto &label_entry : label_to_ckrs ) {
            uint32_t label = label_entry.first;
            DCHECK_EQ( label_to_pos.count( label ), 1 );
            const auto &scan_arg = scan_args.at( label_to_pos.at( label ) );

            const std::vector<cell_key_ranges> &ckrs = label_entry.second;

            DVLOG( 40 ) << "Considering label:" << label << ", for pid:" << pid
                        << ", ckrs:" << ckrs;
            int destination;

            if( found_destination < 0 ) {

                destination = get_scan_destination(
                    plan, part, part_loc_info, ckrs, label, scan_arg,
                    site_load_infos, storage_sizes, session, costs_per_site,
                    cached_stats, cached_query_arrival_stats );
                DCHECK_GE( destination, 0 );

                DVLOG( 40 ) << "Considered label:" << label
                            << ", for pid:" << pid << ", ckrs:" << ckrs
                            << ", got destination:" << destination;
                found_destination = destination;
            } else {
                destination = found_destination;
                DVLOG( 40 ) << "Considered label:" << label
                            << ", for pid:" << pid << ", ckrs:" << ckrs
                            << ", reusing destination:" << destination;
            }

            double loc_cost = add_to_scan_plan(
                plan, destination, part, part_loc_info, pid, ckrs, scan_arg,
                pids_to_site, site_label_positions, site_load_infos,
                costs_per_site, cached_stats );
            total_part_cost += loc_cost;
        }
        DVLOG( 40 ) << "Total part cost for pid:" << pid
                    << ", cost:" << total_part_cost;
        plan->part_costs_.emplace_back( total_part_cost );
        plan->per_part_costs_[total_part_cost].emplace_back( part );
    }  // pid

    finalize_scan_plan( id, plan, grouped_info, pids_to_site, mq_plan_entry,
                        session );
    return plan;
}
