#include "transaction_preparer.h"

#include <glog/logging.h>

#include "../common/constants.h"
#include "../common/hw.h"
#include "../common/perf_tracking.h"
#include "../common/scan_results.h"
#include "../common/thread_utils.h"
#include "../concurrency/async.h"
#include "../persistence/site_selector_persistence_manager.h"
#include "client_exec.h"
#include "multi_query_optimization.h"
#include "partition_access.h"

// we want to log unordered_maps
#ifndef GLOG_STL_LOGGING_FOR_UNORDERED
#define GLOG_STL_LOGGING_FOR_UNORDERED
#endif
#include <glog/stl_logging.h>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>

transaction_preparer::transaction_preparer(
    std::shared_ptr<client_conn_pools>             conn_pool,
    std::shared_ptr<partition_data_location_table> data_loc_tab,
    std::shared_ptr<cost_modeller2>                cost_model2,
    std::shared_ptr<sites_partition_version_information>
                                                    site_partition_version_info,
    std::unique_ptr<abstract_site_evaluator>        evaluator,
    std::shared_ptr<query_arrival_predictor>        query_predictor,
    std::shared_ptr<stat_tracked_enumerator_holder> stat_enumerator_holder,
    std::shared_ptr<partition_tier_tracking>        tier_tracking,
    const transaction_preparer_configs&             configs )
    : conn_pool_( conn_pool ),
      data_loc_tab_( data_loc_tab ),
      cost_model2_( cost_model2 ),
      site_partition_version_info_( site_partition_version_info ),
      evaluator_( std::move( evaluator ) ),
      query_predictor_( query_predictor ),
      stat_enumerator_holder_( stat_enumerator_holder ),
      tier_tracking_( tier_tracking ),
      configs_( configs ),
      mastering_type_configs_( configs_.ss_mastering_type_ ),
      samplers_(),
      access_streams_(),
      last_sampled_txn_start_points_(),
      tracking_duration_(
          std::chrono::milliseconds( configs.astream_tracking_interval_ms_ ) ),
      periodic_site_selector_operations_(
          conn_pool, cost_model2, site_partition_version_info, data_loc_tab_,
          data_loc_tab_->get_stats(), tier_tracking_, query_predictor_,
          data_loc_tab_->get_site_read_access_counts(),
          data_loc_tab_->get_site_write_access_counts(),
          evaluator_->get_num_sites(), configs.periodic_operations_cid_,
          configs_.periodic_site_selector_operations_configs_ ),
      action_executor_( conn_pool_, data_loc_tab_, cost_model2_,
                        &periodic_site_selector_operations_,
                        site_partition_version_info, tier_tracking_,
                        configs_ ) {
    samplers_.reserve( configs_.num_clients_ );
    DVLOG( 40 ) << "Starting transaction preparer";
    high_res_time current_time = std::chrono::high_resolution_clock::now();
    for( int32_t i = 0; i < (int32_t) configs_.num_clients_; i++ ) {
        init_client_stream( i, current_time,
                            ( i % configs_.astream_mod_ == 0 ) );
    }

    evaluator_->set_samplers( &samplers_ );

    DVLOG( 40 ) << "Starting site poller";
    site_partition_version_info_->init_partition_states(
        evaluator_->get_num_sites(), data_loc_tab_->get_num_tables() );
    periodic_site_selector_operations_.start_polling();

    action_executor_.start_background_executor_threads();
}

void transaction_preparer::init_client_stream( int32_t id, high_res_time t,
                                               bool add_access_stream ) {
    // Use i1 and i2 b/c it insists on std::moving them in to match the
    // template...
    auto eval_configs = evaluator_->get_configs();
    std::unique_ptr<adaptive_reservoir_sampler> sampler =
        std::make_unique<adaptive_reservoir_sampler>(
            eval_configs.sample_reservoir_size_,
            eval_configs.sample_reservoir_initial_multiplier_,
            eval_configs.sample_reservoir_weight_increment_,
            eval_configs.sample_reservoir_decay_rate_, data_loc_tab_ );

    DCHECK_EQ( samplers_.size(), id );
    samplers_.push_back( std::move( sampler ) );
    if( add_access_stream ) {
        access_streams_.push_back( std::make_unique<access_stream>(
            configs_.astream_max_queue_size_, samplers_.at( id ).get(),
            std::chrono::milliseconds( configs_.astream_empty_wait_ms_ ),
            std::chrono::milliseconds(
                configs_.astream_tracking_interval_ms_ ) ) );
    }
    last_sampled_txn_start_points_.push_back( t );
}

transaction_preparer::~transaction_preparer() {
    action_executor_.stop_background_executor_threads();
    periodic_site_selector_operations_.stop_polling();
}

#if 0  // HDB-TODO
static void merge_svv_results(::site_version_vector &my_svv,
                              ::site_version_vector &output_svv,
                              std::mutex &           mut ) {
    std::lock_guard<std::mutex> guard( mut );
    unsigned int                loop_iters = my_svv.size();
    if( output_svv.size() != my_svv.size() ) {
        output_svv.resize( my_svv.size(), 0 );
    }

    __SIMD_LOOP__
    for( unsigned i = 0; i < loop_iters; i++ ) {
        output_svv[i] =
            ( my_svv[i] > output_svv[i] ) ? my_svv[i] : output_svv[i];
    }
}

static void move_data_thread(
    const sm_client_ptr &client, const sm_client_ptr &destination_site_client,
    std::shared_ptr<location_dao> data_loc_tab, const ::clientid id, int source,
    int destination, std::shared_ptr<std::vector<::bucket_key>> keys,
    const ::site_version_vector &svv, ::site_version_vector &result_svv,
    std::mutex &mutex, std::shared_ptr<ThreadPtrs> downgrade_threads,
    unsigned int downgrade_pos ) {

    DVLOG( 7 ) << "Releasing mastership at:" << source;
    // Release at remote site
    release_result rr;
    client->rpc_release_mastership_by_buckets( rr, id, *keys, destination,
                                               svv );
    // make sure it succeeds
    DCHECK_EQ( rr.status, exec_status_type::MASTER_CHANGE_OK );

    // Grant at destination site
    DVLOG( 7 ) << "Granting mastership at:" << destination;
    grant_result gr;
    destination_site_client->rpc_grant_mastership_by_buckets(
        gr, id, *keys, rr.session_version_vector );
    // make sure it succeeds
    DCHECK_EQ( gr.status, exec_status_type::MASTER_CHANGE_OK );

    // Downgrade our held locks to CMT
    std::unique_ptr<std::thread> thread_ptr =
        std::make_unique<std::thread>( &location_dao::downgrade_movement_locks,
                                       data_loc_tab, id, keys, destination );

    DVLOG( 20 ) << "Downgrade for:" << id
                << " on thread:" << thread_ptr->get_id();
    downgrade_threads->at( downgrade_pos ) = std::move( thread_ptr );

    // Merge our svv results with other move threads' svvs.
    merge_svv_results( gr.session_version_vector, result_svv, mutex );
}
#endif

#define execute_transaction_preparation(                                       \
    _method, _ckr_write_set, _ckr_read_set, _force_change, _args... )          \
    std::vector<cell_key_ranges> _sorted_ckr_write_set(                        \
        _ckr_write_set.begin(), _ckr_write_set.end() );                        \
    std::vector<cell_key_ranges> _sorted_ckr_read_set( _ckr_read_set.begin(),  \
                                                       _ckr_read_set.end() );  \
    std::sort( _sorted_ckr_write_set.begin(), _sorted_ckr_write_set.end() );   \
    std::sort( _sorted_ckr_read_set.begin(), _sorted_ckr_read_set.end() );     \
    execution_result prepare_ret;                                              \
    int              location = -1;                                            \
    if( _force_change ) {                                                      \
        prepare_ret = _method( _sorted_ckr_write_set, _sorted_ckr_read_set,    \
                               _args, plan_strategy::FORCE_CHANGE );           \
        location = std::get<0>( prepare_ret );                                 \
        if( location >= 0 ) {                                                  \
            return location;                                                   \
        }                                                                      \
    }                                                                          \
    prepare_ret = _method( _sorted_ckr_write_set, _sorted_ckr_read_set, _args, \
                           plan_strategy::ANY );                               \
    location = std::get<0>( prepare_ret );                                     \
    if( location >= 0 ) {                                                      \
        return location;                                                       \
    }                                                                          \
    plan_strategy executed_strategy = std::get<1>( prepare_ret );              \
    if( executed_strategy != plan_strategy::NO_CHANGE ) {                      \
        return location;                                                       \
    }                                                                          \
    prepare_ret = _method( _sorted_ckr_write_set, _sorted_ckr_read_set, _args, \
                           plan_strategy::CHANGE_POSSIBLE );                   \
    location = std::get<0>( prepare_ret );                                     \
    return location;

int transaction_preparer::prepare_transaction(
    const ::clientid id, const std::vector<::cell_key_ranges>& ckr_write_set,
    const std::vector<::cell_key_ranges>& ckr_read_set,
    const ::snapshot_vector&              svv ) {

    execute_transaction_preparation( prepare_transaction_with_strategy,
                                     ckr_write_set, ckr_read_set,
                                     false /*force change*/, id, svv );
}

std::unordered_map<int, std::vector<::cell_key_ranges>>
    transaction_preparer::prepare_multi_site_transaction(
        const ::clientid                      id,
        const std::vector<::cell_key_ranges>& ckr_write_set,
        const std::vector<::cell_key_ranges>& ckr_read_set,
        const std::vector<::cell_key_ranges>& ckr_full_replica_set,
        const ::snapshot_vector&              svv ) {
    bool is_fully_replicated = false;
    if( ckr_full_replica_set.size() > 0 ) {
        DCHECK_EQ( 0, ckr_write_set.size() + ckr_read_set.size() );
        is_fully_replicated = true;
    }

    high_res_time time_of_txn_exec = std::chrono::high_resolution_clock::now();

    snapshot_vector begin_svv = svv;
    std::unordered_map<int, std::vector<::cell_key_ranges>> destinations;
    std::unordered_map<int, std::vector<::cell_key_ranges>> write_destinations;
    std::unordered_map<int, std::vector<::cell_key_ranges>> read_destinations;

    partition_lock_mode           lock_mode = partition_lock_mode::lock;
    grouped_partition_information group_info =
        get_partitions_and_create_if_necessary_for_multi_site_transactions(
            id, ckr_write_set, ckr_read_set, ckr_full_replica_set,
            is_fully_replicated, begin_svv, lock_mode );

    std::unordered_map<int, per_site_grouped_partition_information>
                                   per_site_groupings;
    std::vector<::cell_key_ranges> full_replica_read_ckrs;
    group_data_items_for_multi_site_transaction(
        id, group_info, per_site_groupings, destinations, write_destinations,
        read_destinations, full_replica_read_ckrs, is_fully_replicated );

    start_timer( BEGIN_FOR_DEST_TIMER_ID );
    // std::vector<std::thread> begin_threads;
    exec_status_type::type status = exec_status_type::COMMAND_OK;
    for( auto& dest_entry : destinations ) {
        uint32_t site = dest_entry.first;
        auto&    dest_keys = dest_entry.second;

        if( full_replica_read_ckrs.size() > 0 ) {
            dest_keys.insert( dest_keys.end(), full_replica_read_ckrs.begin(),
                              full_replica_read_ckrs.end() );
        }

        auto& site_entry = per_site_groupings.at( site );

        DCHECK_EQ( site_entry.site_, site );

        if( status != exec_status_type::COMMAND_OK ) {
            break;
            continue;
        }

        // svv and downgrade threads
        status = execute_multi_site_begin( id, site, site_entry.write_pids_,
                                           site_entry.read_pids_, begin_svv,
                                           lock_mode );
    }
    // unlock payloads
    unlock_payloads( group_info.existing_write_partitions_, lock_mode );
    unlock_payloads( group_info.existing_read_partitions_, lock_mode );

    // MTODO

    if( status != exec_status_type::COMMAND_OK ) {
        destinations.clear();
    } else if( !is_fully_replicated ) {
        add_tracking_for_multi_site( id, group_info, per_site_groupings );
        sample_transaction_accesses( id, per_site_groupings, ckr_write_set,
                                     ckr_read_set, time_of_txn_exec );
    }

    // join_threads( begin_threads );
    stop_timer( BEGIN_FOR_DEST_TIMER_ID );

    return destinations;
}

void transaction_preparer::add_tracking_for_multi_site(
    const ::clientid id, const grouped_partition_information& group_info,
    const std::unordered_map<int, per_site_grouped_partition_information>&
        per_site_groupings ) {
    for( const auto& per_site_entry : per_site_groupings ) {
        uint32_t site = per_site_entry.first;
        auto&    site_entry = per_site_entry.second;

        data_loc_tab_->modify_site_read_access_count(
            site, site_entry.read_pids_.size() );
        data_loc_tab_->modify_site_write_access_count(
            site, site_entry.write_pids_.size() );
    }
    for( auto& read_part : group_info.existing_read_partitions_ ) {
        read_part->read_accesses_++;
    }
    for( auto& write_part : group_info.existing_write_partitions_ ) {
        write_part->write_accesses_++;
    }
}

exec_status_type::type transaction_preparer::execute_multi_site_begin(
    const ::clientid id, int site,
    const std::vector<partition_column_identifier>& write_pids,
    const std::vector<partition_column_identifier>& read_pids,
    const snapshot_vector&                          transaction_begin_svv,
    const partition_lock_mode&                      lock_mode ) {
    DVLOG( k_rpc_handler_log_level )
        << "Opening multi site transaction on behalf of the client" << id
        << " at " << site;
    DVLOG( 20 ) << "Begin:" << id << " svv:" << transaction_begin_svv;

    DVLOG( 20 ) << "Begin:" << id << " write pids:" << write_pids
                << " read pids:" << read_pids;

    start_timer( BEGIN_FOR_WRITE_TIMER_ID );
    const sm_client_ptr& client = conn_pool_->getClient( site, id, 0 );
    DVLOG( 30 ) << "Calling begin...";

    DVLOG( 40 ) << "Calling begin transaction:" << id << ", at:" << site
                << ", write_pids:" << write_pids << ", read_pids:" << read_pids;

    begin_result br;
    make_rpc_and_get_service_timer( rpc_begin_transaction, client, br, id,
                                    transaction_begin_svv, write_pids,
                                    read_pids, {} /* inflight_pids*/ );
    if( br.status != exec_status_type::COMMAND_OK ) {
        LOG( WARNING ) << "Unable to prepare transaction for local execution"
                       << br.status << ", client" << id << ", at site:" << site;
    }

    stop_timer( BEGIN_FOR_WRITE_TIMER_ID );

    return br.status;
}

void transaction_preparer::group_data_items_for_multi_site_transaction(
    const ::clientid id, const grouped_partition_information& group_info,
    std::unordered_map<int, per_site_grouped_partition_information>&
        per_site_groupings,
    std::unordered_map<int, std::vector<::cell_key_ranges>>& destinations,
    std::unordered_map<int, std::vector<::cell_key_ranges>>& write_destinations,
    std::unordered_map<int, std::vector<::cell_key_ranges>>& read_destinations,
    std::vector<::cell_key_ranges>& full_replica_read_ckrs,
    bool                            is_fully_replicated ) {

    DVLOG( 30 ) << "group_data_items_for_multi_site_transaction: for client:"
                << id << ", write partitions size:"
                << group_info.existing_write_partitions_.size()
                << ", read partitions size:"
                << group_info.existing_read_partitions_.size();

    DCHECK_EQ( group_info.new_partitions_.size(), 0 );
    if( configs_.ss_mastering_type_ == ss_mastering_type::ADR ) {
        generate_per_site_adr_grouping(
            group_info, per_site_groupings, destinations, write_destinations,
            read_destinations, full_replica_read_ckrs, is_fully_replicated );
    } else {
        for( auto write_part : group_info.existing_write_partitions_ ) {
            add_to_per_site_group_data_items(
                write_part, group_info, per_site_groupings, destinations,
                write_destinations, full_replica_read_ckrs, true /*is write*/,
                is_fully_replicated );
        }
        for( auto read_part : group_info.existing_read_partitions_ ) {
            add_to_per_site_group_data_items(
                read_part, group_info, per_site_groupings, destinations,
                read_destinations, full_replica_read_ckrs, false /*is write*/,
                is_fully_replicated );
        }
    }
    if( per_site_groupings.empty() and destinations.empty() ) {
        DVLOG( 30 ) << "Empty per site groupings, and destinations, picking a "
                       "site at random";
        // pick a random site
        std::vector<int> all_sites;
        for( int site = 0; site < evaluator_->get_num_sites(); site++ ) {
            all_sites.push_back( site );
        }
        snapshot_vector svv;
        int             site = evaluator_->get_no_change_destination(
            all_sites, group_info.existing_write_partitions_,
            group_info.existing_read_partitions_,
            group_info.existing_write_partitions_set_,
            group_info.existing_read_partitions_set_, svv );

        DVLOG( 30 )
            << "Empty per site groupings, and destinations, picked site:"
            << site;

        per_site_groupings[site] =
            per_site_grouped_partition_information( site );
        destinations[site] = std::vector<::cell_key_ranges>();
        write_destinations[site] = std::vector<::cell_key_ranges>();
        read_destinations[site] = std::vector<::cell_key_ranges>();
    }
    DVLOG( 30 ) << "group_data_items_for_multi_site_transaction: for client:"
                << id << ", okay!";
}

void transaction_preparer::generate_per_site_adr_grouping(
    const grouped_partition_information& group_info,
    std::unordered_map<int, per_site_grouped_partition_information>&
        per_site_groupings,
    std::unordered_map<int, std::vector<::cell_key_ranges>>& destinations,
    std::unordered_map<int, std::vector<::cell_key_ranges>>&
        write_destinations_ckr,
    std::unordered_map<int, std::vector<::cell_key_ranges>>&
                                    read_destinations_ckr,
    std::vector<::cell_key_ranges>& full_replica_read_ckrs,
    bool                            is_fully_replicated ) {

    std::unordered_map<int, partition_column_identifier_unordered_set>
        write_destinations;
    std::unordered_map<int, partition_column_identifier_unordered_set>
        read_destinations;

    partition_column_identifier_map_t<std::unordered_set<int>>
        read_part_locations;
    partition_column_identifier_map_t<std::unordered_set<int>>
        write_part_locations;
    partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        pids_to_parts;

    for( auto write_part : group_info.existing_write_partitions_ ) {
        add_to_adr_per_site_group_data_items(
            write_part, group_info, write_destinations, write_part_locations,
            full_replica_read_ckrs, true /*is write*/, is_fully_replicated );
        pids_to_parts[write_part->identifier_] = write_part;
    }
    for( auto read_part : group_info.existing_read_partitions_ ) {
        add_to_adr_per_site_group_data_items(
            read_part, group_info, read_destinations, read_part_locations,
            full_replica_read_ckrs, false /*is write*/, is_fully_replicated );
        pids_to_parts[read_part->identifier_] = read_part;
    }

    // now see if can reduce destinations, by doing a set cover over the read
    // set
    std::unordered_map<int, partition_column_identifier_unordered_set>
        actual_read_destinations;
    while( read_part_locations.size() > 0 ) {
        auto first_entry = read_destinations.begin();
        int  greedy_site = first_entry->first;
        int  greedy_count = first_entry->second.size();

        // pick the greedy site
        for( const auto& read_entry : read_destinations ) {
            int site = read_entry.first;
            int count = read_entry.second.size();
            if( count > greedy_count ) {
                greedy_site = site;
                greedy_count = count;
            }
        }

        // now remove the parts from those sites
        auto parts = read_destinations[greedy_site];
        for( const auto& pid : parts ) {
            actual_read_destinations[greedy_site].emplace( pid );
            std::unordered_set<int> read_loc = read_part_locations[pid];
            for( int loc : read_loc ) {
                read_destinations[loc].erase( pid );
                if( read_destinations[loc].size() == 0 ) {
                    read_destinations.erase( loc );
                }
            }
            read_part_locations.erase( pid );
        }
    }

    for( const auto& write_entry : write_destinations ) {
        int  site = write_entry.first;
        auto pids = write_entry.second;
        for( auto pid : pids ) {
            add_to_adr_destinations( site, pid, group_info, per_site_groupings,
                                     destinations, write_destinations_ckr,
                                     pids_to_parts, true /*is_write*/ );
        }
    }
    for( const auto& read_entry : actual_read_destinations ) {
        int  site = read_entry.first;
        auto pids = read_entry.second;
        for( auto pid : pids ) {
            add_to_adr_destinations( site, pid, group_info, per_site_groupings,
                                     destinations, read_destinations_ckr,
                                     pids_to_parts, false /*is write*/ );
        }
    }
}

void transaction_preparer::add_to_adr_destinations(
    int site, const partition_column_identifier& pid,
    const grouped_partition_information& group_info,
    std::unordered_map<int, per_site_grouped_partition_information>&
        per_site_groupings,
    std::unordered_map<int, std::vector<::cell_key_ranges>>& destinations,
    std::unordered_map<int, std::vector<::cell_key_ranges>>& type_destinations,
    partition_column_identifier_map_t<std::shared_ptr<partition_payload>>&
         pids_to_parts,
    bool is_write ) {

    auto& per_site_group = per_site_groupings[site];
    auto  part = pids_to_parts[pid];
    per_site_group.site_ = site;
    per_site_group.payloads_.push_back( part );
    if( is_write ) {
        per_site_group.write_pids_.push_back( pid );
        per_site_group.write_payloads_.push_back( part );
    } else {
        per_site_group.read_pids_.push_back( pid );
        per_site_group.read_payloads_.push_back( part );
    }

    auto& per_site_keys = destinations[site];
    auto& per_site_type_keys = type_destinations[site];

    auto write_found = group_info.write_pids_to_shaped_ckrs_.find( pid );
    if( write_found != group_info.write_pids_to_shaped_ckrs_.end() ) {
        for( const auto& ckr : write_found->second ) {
            per_site_keys.push_back( ckr );
            per_site_type_keys.push_back( ckr );
        }
    }
    auto read_found = group_info.read_pids_to_shaped_ckrs_.find( pid );
    if( read_found != group_info.read_pids_to_shaped_ckrs_.end() ) {
        for( const auto& ckr : read_found->second ) {
            per_site_keys.push_back( ckr );
            per_site_type_keys.push_back( ckr );
        }
    }
}

void transaction_preparer::add_to_adr_per_site_group_data_items(
    std::shared_ptr<partition_payload>&  part,
    const grouped_partition_information& group_info,
    std::unordered_map<int, partition_column_identifier_unordered_set>&
                                                                destinations,
    partition_column_identifier_map_t<std::unordered_set<int>>& part_locations,
    std::vector<::cell_key_ranges>& full_replica_read_ckrs, bool is_write,
    bool is_fully_replicated ) {

    auto        pid = part->identifier_;
    const auto& part_loc =
        group_info.partition_location_informations_.at( pid );
    DCHECK( part_loc );

    auto master = part_loc->master_location_;

    DVLOG( 40 ) << "Add part:" << pid << ", to per site grouping:" << master
                << ", is write:" << is_write;

    std::unordered_set<int> sites_to_run_at;
    sites_to_run_at.emplace( master );

    if( master == K_DATA_AT_ALL_SITES ) {
        if( !is_fully_replicated ) {
            DCHECK( !is_write );
        } else {
            DCHECK( is_fully_replicated );
        }
        sites_to_run_at.clear();
        int num_sites = evaluator_->get_num_sites();
        for( int site = 0; site < num_sites; site++ ) {
            sites_to_run_at.emplace( site );
        }
    } else if( !is_write ) {
        for( const auto& rep_site : part_loc->replica_locations_ ) {
            sites_to_run_at.emplace( rep_site );
        }
    }

    part_locations[pid] = sites_to_run_at;
    for( int site : sites_to_run_at ) {
        auto& per_site_pids = destinations[site];
        per_site_pids.emplace( pid );
    }

    if( ( master == K_DATA_AT_ALL_SITES ) and ( !is_write ) ) {
        const auto& write_ckrs =
            group_info.write_pids_to_shaped_ckrs_.at( pid );
        for( const auto& ckr : write_ckrs ) {
            full_replica_read_ckrs.push_back( ckr );
        }
        const auto& read_ckrs = group_info.read_pids_to_shaped_ckrs_.at( pid );
        for( const auto& ckr : read_ckrs ) {
            full_replica_read_ckrs.push_back( ckr );
        }
    }
}

void transaction_preparer::add_to_per_site_group_data_items(
    std::shared_ptr<partition_payload>&  part,
    const grouped_partition_information& group_info,
    std::unordered_map<int, per_site_grouped_partition_information>&
        per_site_groupings,
    std::unordered_map<int, std::vector<::cell_key_ranges>>& destinations,
    std::unordered_map<int, std::vector<::cell_key_ranges>>& type_destinations,
    std::vector<::cell_key_ranges>& full_replica_read_ckrs, bool is_write,
    bool is_fully_replicated ) {

    auto        pid = part->identifier_;
    const auto& part_loc =
        group_info.partition_location_informations_.at( pid );
    DCHECK( part_loc );

    auto master = part_loc->master_location_;

    DVLOG( 40 ) << "Add part:" << pid << ", to per site grouping:" << master
                << ", is write:" << is_write;

    std::vector<int> sites_to_run_at;
    sites_to_run_at.push_back( master );

    if( master == K_DATA_AT_ALL_SITES ) {
        if( !is_fully_replicated ) {
            DCHECK( !is_write );
        } else {
            DCHECK( is_fully_replicated );
        }
        sites_to_run_at.clear();
        int num_sites = evaluator_->get_num_sites();
        for( int site = 0; site < num_sites; site++ ) {
            sites_to_run_at.push_back( site );
        }
    }

    for( int site : sites_to_run_at ) {
        auto& per_site_group = per_site_groupings[site];
        per_site_group.site_ = site;
        per_site_group.payloads_.push_back( part );
        if( is_write ) {
            per_site_group.write_pids_.push_back( pid );
            per_site_group.write_payloads_.push_back( part );
        } else {
            per_site_group.read_pids_.push_back( pid );
            per_site_group.read_payloads_.push_back( part );
        }
        if( not( ( master == K_DATA_AT_ALL_SITES ) and ( !is_write ) ) ) {
            auto& per_site_keys = destinations[site];
            auto& per_site_type_keys = type_destinations[site];
            auto  write_found =
                group_info.write_pids_to_shaped_ckrs_.find( pid );
            if( write_found != group_info.write_pids_to_shaped_ckrs_.end() ) {
                for( const auto& ckr : write_found->second ) {
                    per_site_keys.push_back( ckr );
                    per_site_type_keys.push_back( ckr );
                }
            }
            auto read_found = group_info.read_pids_to_shaped_ckrs_.find( pid );
            if( read_found != group_info.read_pids_to_shaped_ckrs_.end() ) {
                for( const auto& ckr : read_found->second ) {
                    per_site_keys.push_back( ckr );
                    per_site_type_keys.push_back( ckr );
                }
            }
        }
    }
    if( ( master == K_DATA_AT_ALL_SITES ) and ( !is_write ) ) {
        auto write_found = group_info.write_pids_to_shaped_ckrs_.find( pid );
        if( write_found != group_info.write_pids_to_shaped_ckrs_.end() ) {
            for( const auto& ckr : write_found->second ) {
                full_replica_read_ckrs.push_back( ckr );
            }
        }
        auto read_found = group_info.read_pids_to_shaped_ckrs_.find( pid );
        if( read_found != group_info.read_pids_to_shaped_ckrs_.end() ) {
            for( const auto& ckr : read_found->second ) {
                full_replica_read_ckrs.push_back( ckr );
            }
        }
    }
}

grouped_partition_information transaction_preparer::
    get_partitions_and_create_if_necessary_for_multi_site_transactions(
        const ::clientid                      id,
        const std::vector<::cell_key_ranges>& ckr_write_set,
        const std::vector<::cell_key_ranges>& ckr_read_set,
        const std::vector<::cell_key_ranges>& ckr_full_replica_set,
        bool is_fully_replicated, snapshot_vector& svv,
        const partition_lock_mode& lock_mode ) {

    std::vector<cell_key_ranges> sorted_ckr_write_set( ckr_write_set.begin(),
                                                       ckr_write_set.end() );
    std::vector<cell_key_ranges> sorted_ckr_read_set( ckr_read_set.begin(),
                                                      ckr_read_set.end() );

    if( is_fully_replicated ) {
        DCHECK_EQ( sorted_ckr_read_set.size(), 0 );
        DCHECK_EQ( sorted_ckr_write_set.size(), 0 );
        sorted_ckr_write_set.assign( ckr_full_replica_set.begin(),
                                     ckr_full_replica_set.end() );
    }

    std::sort( sorted_ckr_write_set.begin(), sorted_ckr_write_set.end() );
    std::sort( sorted_ckr_read_set.begin(), sorted_ckr_read_set.end() );

    uint32_t num_sites = evaluator_->get_num_sites();

    // first look up to see if we need to create
    auto grouped_info = data_loc_tab_->get_partitions_and_group(
        sorted_ckr_write_set, sorted_ckr_read_set, partition_lock_mode::no_lock,
        num_sites, false /* don't allow missing */ );
    if( grouped_info.payloads_.empty() and
        !( sorted_ckr_write_set.empty() and sorted_ckr_read_set.empty() ) ) {
        DVLOG( 20 ) << "Could not find partitions, for multi site transaction, "
                       "must create them";
        DVLOG( 40 ) << "Client id:" << id
                    << ", sorted_ckr_write_set:" << sorted_ckr_write_set
                    << ", sorted_ckr_read_set:" << sorted_ckr_read_set;
        create_multi_site_partitions( id, sorted_ckr_write_set,
                                      sorted_ckr_read_set, svv,
                                      is_fully_replicated );
    }
    // then actually look up
    grouped_info = data_loc_tab_->get_partitions_and_group(
        sorted_ckr_write_set, sorted_ckr_read_set, lock_mode, num_sites,
        false /* don't allow missing */ );

    return grouped_info;
}

void transaction_preparer::create_multi_site_partitions(
    const ::clientid                      id,
    const std::vector<::cell_key_ranges>& sorted_ckr_write_set,
    const std::vector<::cell_key_ranges>& sorted_ckr_read_set,
    snapshot_vector& svv, bool is_fully_replicated ) {
    DVLOG( 40 ) << "Creating multi site partitions";
    partition_lock_mode lock_mode = partition_lock_mode::lock;
    auto grouped_info = data_loc_tab_->get_or_create_partitions_and_group(
        sorted_ckr_write_set, sorted_ckr_read_set, lock_mode,
        evaluator_->get_num_sites() );

    unlock_payloads( grouped_info.existing_write_partitions_, lock_mode );
    unlock_payloads( grouped_info.existing_read_partitions_, lock_mode );

    if( grouped_info.new_partitions_.size() > 0 ) {
        auto plan = evaluator_->generate_multi_site_insert_only_plan(
            id, periodic_site_selector_operations_.get_site_load_information(),
            data_loc_tab_->get_storage_stats()->get_storage_tier_sizes(),
            grouped_info.new_partitions_, is_fully_replicated );
        DCHECK( plan );

        plan->add_to_partition_mappings( grouped_info.new_partitions_ );
        plan->add_to_location_information_mappings(
            std::move( grouped_info.partition_location_informations_ ) );
        plan->sort_and_finalize_pids();
        plan->build_work_queue();

        DCHECK_GT( plan->get_num_work_items(), 0 );

        action_outcome outcome = execute_pre_transaction_plan( plan, id, svv );
        if( outcome == action_outcome::FAILED ) {
            LOG( ERROR ) << "Unable to execute pre transaction plan with "
                            "physical adjustment for client:"
                         << id << ", as actions failed!";
            // unlock plan
            plan->unlock_payloads_because_of_failure(
                partition_lock_mode::lock );
        } else {
            plan->unlock_remaining_payloads( lock_mode );
        }
    }
}

int transaction_preparer::execute_one_shot_sproc_for_multi_site_system(
    one_shot_sproc_result& _return, const ::clientid id,
    const ::snapshot_vector&              client_session_version_vector,
    const std::vector<::cell_key_ranges>& ckr_write_set,
    const std::vector<::cell_key_ranges>& ckr_read_set, const std::string& name,
    const std::string& sproc_args ) {

    high_res_time time_of_txn_exec = std::chrono::high_resolution_clock::now();

    snapshot_vector begin_svv = client_session_version_vector;

    std::vector<::cell_key_ranges> ckr_full_replica_set;
    bool                           is_fully_replicated = false;

    partition_lock_mode           lock_mode = partition_lock_mode::no_lock;
    grouped_partition_information group_info =
        get_partitions_and_create_if_necessary_for_multi_site_transactions(
            id, ckr_write_set, ckr_read_set, ckr_full_replica_set,
            is_fully_replicated, begin_svv, lock_mode );

    std::unordered_map<int, per_site_grouped_partition_information>
        per_site_groupings;
    std::unordered_map<int, std::vector<::cell_key_ranges>> write_destinations;
    std::unordered_map<int, std::vector<::cell_key_ranges>> read_destinations;
    std::unordered_map<int, std::vector<::cell_key_ranges>> destinations;
    std::vector<::cell_key_ranges> full_replica_read_ckrs;
    group_data_items_for_multi_site_transaction(
        id, group_info, per_site_groupings, destinations, write_destinations,
        read_destinations, full_replica_read_ckrs, is_fully_replicated );

    int ret_dest = -1;
    if( destinations.size() == 1 ) {
        // execute the sproc
        const auto& per_site_dest = destinations.cbegin();
        int         site = per_site_dest->first;

        const auto& site_entry = per_site_groupings.at( site );

        ret_dest = execute_one_shot_sproc_at_site(
            id, site, _return, begin_svv, site_entry.write_pids_,
            site_entry.read_pids_, {} /*in flight pids*/, name,
            write_destinations[site], read_destinations[site], sproc_args,
            plan_strategy::NO_CHANGE );
    } else {
        DLOG( WARNING ) << "Cannot execute for client:" << id
                        << ", one shot sproc:" << name
                        << ", as data located at more than one site, num sites:"
                        << per_site_groupings.size();
    }

    // unlock parts
    unlock_payloads( group_info.existing_write_partitions_, lock_mode );
    unlock_payloads( group_info.existing_read_partitions_, lock_mode );

    if( ret_dest >= 0 ) {
        add_tracking_for_multi_site( id, group_info, per_site_groupings );
        sample_transaction_accesses( id, per_site_groupings, ckr_write_set,
                                     ckr_read_set, time_of_txn_exec );
    }

    return ret_dest;
}

int transaction_preparer::execute_one_shot_sproc_at_site(
    const ::clientid id, int destination, one_shot_sproc_result& _return,
    const snapshot_vector&                          svv,
    const std::vector<partition_column_identifier>& write_pids,
    const std::vector<partition_column_identifier>& read_pids,
    const std::vector<partition_column_identifier>& inflight_pids,
    const std::string& name, const std::vector<cell_key_ranges>& write_ckrs,
    const std::vector<cell_key_ranges>& read_ckrs,
    const std::string& sproc_args, const plan_strategy& actual_strategy ) {
    DCHECK_GE( destination, 0 );

    DVLOG( k_rpc_handler_log_level )
        << "Opening one shot sproc transaction: " << name
        << " behalf of the client" << id << " at " << destination;

    DVLOG( 20 ) << "One shot sproc:" << id << " svv:" << svv;

    DVLOG( 7 ) << "Calling one shot sproc transaction:" << name
               << " behalf of the client" << id << " at " << destination
               << ", write_pids:" << write_pids << ", read_pids:" << read_pids;

    start_timer( SS_CALL_RPC_ONE_SHOT_SPROC_TIMER_ID );
    // call the sproc at the site manager
    make_rpc_and_get_service_timer(
        rpc_one_shot_sproc, conn_pool_->getClient( destination, id, 0 ),
        _return, id, svv, write_pids, read_pids, inflight_pids, name,
        write_ckrs, read_ckrs, sproc_args );
    stop_timer( SS_CALL_RPC_ONE_SHOT_SPROC_TIMER_ID );

    int ret_dest = destination;
    if( _return.status != exec_status_type::COMMAND_OK ) {
        ret_dest = -1;
        DLOG( WARNING ) << "Error with one shot sproc, overwriting status:"
                        << _return.status << ", to:"
                        << exec_status_type::DATA_NOT_LOCATED_AT_ONE_SITE
                        << ", client" << id << ", at:" << destination
                        << ", sproc name:" << name
                        << ", plan strategy:" << actual_strategy;
        _return.status = exec_status_type::DATA_NOT_LOCATED_AT_ONE_SITE;
    }

    return ret_dest;
}

/*
        id, _return, transaction_begin_svv, name, plan, sorted_ckr_write_set,
        sorted_ckr_read_set, sproc_args, actual_strategy );
        */

std::vector<int> transaction_preparer::execute_one_shot_scan_at_sites(
    const ::clientid id, one_shot_scan_result& _return,
    const snapshot_vector&                            svv,
    std::shared_ptr<multi_site_pre_transaction_plan>& plan,
    const std::string& name, const std::vector<cell_key_ranges>& read_ckrs,
    const std::vector<scan_arguments>& scan_args, const std::string& sproc_args,
    const plan_strategy& actual_strategy ) {

    start_timer( SS_EXECUTE_ONE_SHOT_SCAN_AT_SITES_TIMER_ID );
    std::chrono::high_resolution_clock::time_point
        start_ss_one_shot_scan_timer =
            std::chrono::high_resolution_clock::now();

    DCHECK( plan );
    DCHECK_GT( plan->scan_arg_sites_.size(), 0 );
    DCHECK_GE( plan->scan_arg_sites_.size(), plan->inflight_pids_.size() );
    DCHECK_GE( plan->scan_arg_sites_.size(), plan->read_pids_.size() );

    std::vector<one_shot_scan_result> scan_results(
        plan->scan_arg_sites_.size() );

    DVLOG( k_rpc_handler_log_level )
        << "Opening one shot scan transaction: " << name
        << " behalf of the client" << id << " at " << plan->scan_arg_sites_
        << ", sites";

    DVLOG( 20 ) << "Calling one shot scan transaction:" << name
                << " behalf of the client" << id << ", read_ckrs:" << read_ckrs;

    std::vector<int> destinations;
    for( const auto& scan_entry : plan->scan_arg_sites_ ) {
        destinations.emplace_back( scan_entry.first );
        if( plan->inflight_pids_.count( scan_entry.first ) == 0 ) {
            plan->inflight_pids_[scan_entry.first] =
                std::vector<partition_column_identifier>();
        }
        if( plan->read_pids_.count( scan_entry.first ) == 0 ) {
            plan->read_pids_[scan_entry.first] =
                std::vector<partition_column_identifier>();
        }
    }

    std::vector<std::thread> execute_threads;
    for( uint32_t pos = 0; pos < destinations.size(); pos++ ) {
        // svv and downgrade threads
        execute_threads.push_back( std::thread(
            &transaction_preparer::execute_one_shot_scan_at_site, this, id,
            destinations.at( pos ), std::ref( scan_results.at( pos ) ),
            std::cref( svv ),
            std::cref( plan->read_pids_.at( destinations.at( pos ) ) ),
            std::cref( plan->inflight_pids_.at( destinations.at( pos ) ) ),
            std::cref( name ),
            std::cref( plan->scan_arg_sites_.at( destinations.at( pos ) ) ),
            std::cref( sproc_args ) ) );
    }
    join_threads( execute_threads );

    join_scan_results( _return, scan_results, scan_args );

    std::chrono::high_resolution_clock::time_point stop_ss_one_shot_scan_timer =
        std::chrono::high_resolution_clock::now();

    stop_timer( SS_EXECUTE_ONE_SHOT_SCAN_AT_SITES_TIMER_ID );

    context_timer scan_context_timer;
    scan_context_timer.counter_id = SS_EXECUTE_ONE_SHOT_SCAN_AT_SITES_TIMER_ID;
    scan_context_timer.counter_seen_count = 1;
    std::chrono::duration<double, std::micro> elapsed_one_shot_scan_count =
        stop_ss_one_shot_scan_timer - start_ss_one_shot_scan_timer;

    scan_context_timer.timer_us_average = elapsed_one_shot_scan_count.count();
    _return.timers.emplace_back( scan_context_timer );

    for( uint32_t pos = 0; pos < destinations.size(); pos++ ) {
        int         dest = destinations.at( pos );
        const auto& txn_stat = plan->txn_stats_.at( dest );
        const auto& res = scan_results.at( pos );

        auto prediction_holder =
            cost_model2_->predict_single_site_transaction_execution_time(
                periodic_site_selector_operations_.get_site_load( dest ),
                txn_stat );
        prediction_holder.add_real_timers( res.timers );
        cost_model2_->add_results( prediction_holder );
    }

    return destinations;
}

void transaction_preparer::execute_one_shot_scan_at_site(
    const ::clientid id, int destination, one_shot_scan_result& _return,
    const snapshot_vector&                          svv,
    const std::vector<partition_column_identifier>& read_pids,
    const std::vector<partition_column_identifier>& inflight_pids,
    const std::string& name, const std::vector<scan_arguments>& scan_args,
    const std::string& sproc_args ) {

    DVLOG( k_rpc_handler_log_level )
        << "Opening one shot scan transaction: " << name
        << " behalf of the client" << id << " at " << destination;

    DVLOG( 20 ) << "One shot scan:" << id << " svv:" << svv;

    DVLOG( 7 ) << "Calling one shot scan transaction:" << name
               << " behalf of the client" << id << " at " << destination
               << ", read_pids:" << read_pids;

    start_timer( SS_CALL_RPC_ONE_SHOT_SCAN_TIMER_ID );
    // call the sproc at the site manager
    make_rpc_and_get_service_timer(
        rpc_one_shot_scan, conn_pool_->getClient( destination, id, 0 ), _return,
        id, svv, read_pids, inflight_pids, name, scan_args, sproc_args );
    stop_timer( SS_CALL_RPC_ONE_SHOT_SCAN_TIMER_ID );

    if( _return.status != exec_status_type::COMMAND_OK ) {
        DLOG( WARNING ) << "Error with one shot scan, overwriting status:"
                        << _return.status << ", to:"
                        << exec_status_type::DATA_NOT_LOCATED_AT_ONE_SITE
                        << ", client" << id << ", at:" << destination
                        << ", scan name:" << name;
        _return.status = exec_status_type::DATA_NOT_LOCATED_AT_ONE_SITE;
    }
}

int transaction_preparer::execute_one_shot_sproc(
    one_shot_sproc_result& _return, const ::clientid id,
    const ::snapshot_vector&              svv,
    const std::vector<::cell_key_ranges>& ckr_write_set,
    const std::vector<::cell_key_ranges>& ckr_read_set, const std::string& name,
    const std::string& sproc_args, bool allow_force_change ) {
    bool force_change =
        allow_force_change and
        evaluator_->get_should_force_change( id, ckr_write_set, ckr_read_set );
    DVLOG( 7 ) << "client:" << id << " force_change:" << force_change
               << ", allow_force_change:" << allow_force_change;
    execute_transaction_preparation( execute_one_shot_sproc_with_strategy,
                                     ckr_write_set, ckr_read_set, force_change,
                                     _return, id, svv, name, sproc_args );
}

std::shared_ptr<multi_site_pre_transaction_plan>
    transaction_preparer::prepare_transaction_data_items_for_scan_execution(
        const ::clientid id, ::snapshot_vector& svv,
        const std::vector<::cell_key_ranges>& sorted_ckr_write_set,
        const std::vector<::cell_key_ranges>& sorted_ckr_read_set,
        const std::vector<scan_arguments>&    scan_args,
        bool allow_missing,
        plan_strategy&                        strategy ) {
    start_timer(
        SS_PREPARE_TRANSACTION_DATA_ITEMS_FOR_SCAN_EXECUTION_TIMER_ID );

    std::shared_ptr<multi_site_pre_transaction_plan> plan = nullptr;
    bool                                             need_new_parts = false;

    auto mq_plan_entry = std::make_shared<multi_query_plan_entry>();

    if( ( strategy == plan_strategy::ANY ) or
        ( strategy == plan_strategy::NO_CHANGE ) ) {
        auto optimistic_ret = generate_optimistic_no_change_scan_plan(
            id, sorted_ckr_write_set, sorted_ckr_read_set, scan_args,
            mq_plan_entry, allow_missing, svv );
        plan = std::get<0>( optimistic_ret );
        need_new_parts = std::get<1>( optimistic_ret );
    }
    if( plan ) {
        strategy = plan_strategy::NO_CHANGE;
        DCHECK_EQ( 0, plan->plan_->get_num_work_items() );
    } else if( strategy != plan_strategy::NO_CHANGE ) {
        plan = generate_scan_plan_with_physical_adjustments(
            id, sorted_ckr_write_set, sorted_ckr_read_set, scan_args,
            mq_plan_entry, svv, strategy, need_new_parts, allow_missing );
        strategy = plan_strategy::CHANGE_POSSIBLE;
        if( plan == nullptr ) {
            LOG( ERROR )
                << "Unable to generate scan plan with physical adjustments "
                   "for client:"
                << id << ", as could not find partitions!";
            stop_timer(
                SS_PREPARE_TRANSACTION_DATA_ITEMS_FOR_SCAN_EXECUTION_TIMER_ID );

            return nullptr;
        }
        plan->plan_->unlock_background_partitions();
        if( plan->plan_->get_num_work_items() > 0 ) {
            action_outcome outcome =
                execute_pre_transaction_plan( plan->plan_, id, svv );
            if( outcome == action_outcome::FAILED ) {
                LOG( ERROR )
                    << "Unable to execute pre transaction scan plan with "
                       "physical adjustment for client:"
                    << id << ", as actions failed!";
                // unlock plan
                plan->plan_->unlock_payloads_because_of_failure(
                    partition_lock_mode::lock );

                stop_timer(
                    SS_PREPARE_TRANSACTION_DATA_ITEMS_FOR_SCAN_EXECUTION_TIMER_ID );

                return nullptr;
            }
        }
        if( plan->plan_->get_background_work_items().size() > 0 ) {
            action_executor_.add_background_work_items(
                plan->plan_->get_background_work_items() );
        }
        plan->plan_->unlock_remaining_payloads( partition_lock_mode::lock );
    }
    DCHECK( plan );

    DVLOG( 40 ) << "Scan Plan for id:" << id
                << ", has write_pids: " << plan->plan_->write_pids_
                << ", and read_pids:" << plan->plan_->read_pids_;

    stop_timer( SS_PREPARE_TRANSACTION_DATA_ITEMS_FOR_SCAN_EXECUTION_TIMER_ID );

    return plan;
}

std::vector<int> transaction_preparer::execute_one_shot_scan(
    one_shot_scan_result& _return, const ::clientid id,
    const ::snapshot_vector& svv, const std::string& name,
    const std::vector<scan_arguments>& scan_args, const std::string& sproc_args,
    bool allow_missing_data, bool allow_force_change ) {

    std::vector<int> ret;

    std::vector<::cell_key_ranges> sorted_ckr_write_set;
    std::vector<::cell_key_ranges> sorted_ckr_read_set;

    for( const auto& scan_arg : scan_args ) {
        sorted_ckr_read_set.insert( sorted_ckr_read_set.end(),
                                    scan_arg.read_ckrs.begin(),
                                    scan_arg.read_ckrs.end() );
    }

    std::sort( sorted_ckr_write_set.begin(), sorted_ckr_write_set.end() );
    std::sort( sorted_ckr_read_set.begin(), sorted_ckr_read_set.end() );

    if( sorted_ckr_read_set.empty() ) {
        _return.status = exec_status_type::COMMAND_OK;
        return ret;
    }

    bool force_change = allow_force_change and
                        evaluator_->get_should_force_change(
                            id, sorted_ckr_write_set, sorted_ckr_read_set );
    DVLOG( 7 ) << "client:" << id << " force_change:" << force_change
               << ", allow_force_change:" << allow_force_change;

    /*
     * 1. Get the locations
     * 2. Make a plan
     * 3. Execute plan
     * 4. Send out the queries
     * 5. Record all the stats and stuff
     */

    high_res_time time_of_txn_exec = std::chrono::high_resolution_clock::now();

    snapshot_vector transaction_begin_svv = svv;
    plan_strategy   actual_strategy = plan_strategy::ANY;
    if( force_change ) {
        actual_strategy = plan_strategy::FORCE_CHANGE;
    }

    // 1-3
    std::shared_ptr<multi_site_pre_transaction_plan> plan =
        prepare_transaction_data_items_for_scan_execution(
            id, transaction_begin_svv, sorted_ckr_write_set,
            sorted_ckr_read_set, scan_args, allow_missing_data,
            actual_strategy );
    if( !plan ) {
        actual_strategy = plan_strategy::CHANGE_POSSIBLE;
        plan = prepare_transaction_data_items_for_scan_execution(
            id, transaction_begin_svv, sorted_ckr_write_set,
            sorted_ckr_read_set, scan_args, allow_missing_data,
            actual_strategy );
    }

    if( plan == nullptr ) {
        LOG( WARNING ) << "Unable to execute_one_shot_scan: with strategy:"
                       << actual_strategy << ", client" << id
                       << ", name:" << name;

        _return.status = exec_status_type::DATA_NOT_LOCATED_AT_ONE_SITE;
        return ret;
    }

    // 4
    DCHECK( plan->plan_->mq_plan_ );
    plan->plan_->mq_plan_->set_plan_type( multi_query_plan_type::EXECUTING );

    ret = execute_one_shot_scan_at_sites(
        id, _return, transaction_begin_svv, plan, name, sorted_ckr_read_set,
        scan_args, sproc_args, actual_strategy );

    plan->plan_->mq_plan_->set_plan_type( multi_query_plan_type::CACHED );
    DCHECK( plan->plan_->stat_decision_holder_ );
    stat_enumerator_holder_->add_decisions(
        plan->plan_->stat_decision_holder_ );

    // 5
    auto prediction_holder =
        cost_model2_->predict_distributed_scan_execution_time(
            periodic_site_selector_operations_.get_site_loads(),
            plan->txn_stats_ );
    prediction_holder.add_real_timers( _return.timers );
    cost_model2_->add_results( prediction_holder );

    DVLOG( 40 ) << "One shot sproc:" << id
                << ", return code:" << _return.status;

    increase_tracking_counts( plan );
    // MTODO-PAR decide if this should be based on scans
    // MTODO-STRATEGIES decide if we want to sample this
    data_loc_tab_->get_stats()->add_transaction_accesses( sorted_ckr_write_set,
                                                          sorted_ckr_read_set );
    sample_transaction_accesses( id, plan, sorted_ckr_write_set,
                                 sorted_ckr_read_set, time_of_txn_exec );

    return ret;
}

std::tuple<std::shared_ptr<multi_site_pre_transaction_plan>, bool>
    transaction_preparer::generate_optimistic_no_change_scan_plan(
        const ::clientid                         id,
        const std::vector<::cell_key_ranges>&    ckr_write_set,
        const std::vector<::cell_key_ranges>&    ckr_read_set,
        const std::vector<scan_arguments>&       scan_args,
        std::shared_ptr<multi_query_plan_entry>& mq_plan_entry,
        bool allow_missing, ::snapshot_vector& svv ) {
    std::shared_ptr<multi_site_pre_transaction_plan> plan = nullptr;

    start_timer( SS_GENERATE_OPTIMISTIC_NO_CHANGE_SCAN_PLAN_TIMER_ID );

    DVLOG( 40 ) << "Trying to generate optimistic no change scan plan";

    partition_lock_mode lock_mode =
        configs_.optimistic_lock_mode_;  // partition_lock_mode::no_lock;

    grouped_partition_information group_info =
        data_loc_tab_->get_partitions_and_group(
            ckr_write_set, ckr_read_set, lock_mode, evaluator_->get_num_sites(),
            allow_missing );
    if( group_info.payloads_.empty() ) {
        DVLOG( 40 ) << "Could not generate optimistic no change scan plan, as could "
                       "not find partitions";
        // couldn't find a key
        stop_timer( SS_GENERATE_OPTIMISTIC_NO_CHANGE_SCAN_PLAN_TIMER_ID );
        return std::make_tuple<>( nullptr, true );
    }

    if( !group_info.new_partitions_.empty() ) {
        // did an insert so can't do the work
        DVLOG( 40 ) << "Could not generate optimistic no change scan plan, as some "
                       "partitions are new ";
        unlock_payloads( group_info.payloads_, lock_mode );
        stop_timer( SS_GENERATE_OPTIMISTIC_NO_CHANGE_SCAN_PLAN_TIMER_ID );
        return std::make_tuple<>( nullptr, true );
    }

    DCHECK_EQ( 0, group_info.new_write_partitions_.size() );
    DCHECK_EQ( 0, group_info.new_read_partitions_.size() );

    std::unordered_map<uint32_t /* label*/, partition_column_identifier_to_ckrs>
        labels_to_pids_and_ckrs;
    partition_column_identifier_map_t<
        std::unordered_map<uint32_t /* label */, std::vector<cell_key_ranges>>>
        pids_to_labels_and_ckrs;
    std::unordered_map<uint32_t /* label */, uint32_t /* pos */> label_to_pos;
    build_scan_argument_mappings( scan_args, group_info,
                                  labels_to_pids_and_ckrs,
                                  pids_to_labels_and_ckrs, label_to_pos );

    plan =
        generate_no_change_destination_scan_plan_if_possible_or_wait_for_plan_reuse(
            id, group_info, scan_args, labels_to_pids_and_ckrs,
            pids_to_labels_and_ckrs, label_to_pos, mq_plan_entry, svv );

    if( plan ) {
        start_timer( SS_SORT_FINALIZE_AND_ADD_METADATA_TO_SCAN_PLAN_TIMER_ID );
        plan->plan_->add_to_partition_mappings( group_info.payloads_ );
        plan->plan_->add_to_location_information_mappings(
            std::move( group_info.partition_location_informations_ ) );

        plan->plan_->sort_and_finalize_pids();
        stop_timer( SS_SORT_FINALIZE_AND_ADD_METADATA_TO_SCAN_PLAN_TIMER_ID );

        DVLOG( 40 ) << "Generates optimistic no change scan plan to # sites:"
                    << plan->scan_arg_sites_.size();

    } else {
        DVLOG( 40 ) << "Could not generate optimistic no change scan plan";
    }
    // unlock it all anyhow here, so we can keep up the appearance that there is
    // no lock later
    unlock_payloads( group_info.payloads_, lock_mode );

    stop_timer( SS_GENERATE_OPTIMISTIC_NO_CHANGE_SCAN_PLAN_TIMER_ID );

    return std::make_tuple<>( plan, false );
}

std::shared_ptr<multi_site_pre_transaction_plan> transaction_preparer::
    generate_no_change_destination_scan_plan_if_possible_or_wait_for_plan_reuse(
        const ::clientid id, const grouped_partition_information& grouped_info,
        const std::vector<scan_arguments>& scan_args,
        const std::unordered_map<uint32_t /* label*/,
                                 partition_column_identifier_to_ckrs>&
            labels_to_pids_and_ckrs,
        const partition_column_identifier_map_t<std::unordered_map<
            uint32_t /* label */, std::vector<cell_key_ranges>>>&
            pids_to_labels_and_ckrs,
        const std::unordered_map<uint32_t /* label */, uint32_t /* pos */>&
                                                 label_to_pos,
        std::shared_ptr<multi_query_plan_entry>& mq_plan_entry,
        ::snapshot_vector&                       svv ) {

    std::shared_ptr<multi_site_pre_transaction_plan> plan =
        generate_no_change_destination_scan_plan_if_possible(
            id, grouped_info, scan_args, labels_to_pids_and_ckrs,
            pids_to_labels_and_ckrs, label_to_pos, mq_plan_entry, svv );

    if( plan ) {
        return plan;
    } else if( configs_.enable_wait_for_plan_reuse_ ) {
        plan = evaluator_->generate_scan_plan_from_current_planners(
            id, grouped_info, scan_args, labels_to_pids_and_ckrs,
            pids_to_labels_and_ckrs, label_to_pos,
            periodic_site_selector_operations_.get_site_load_information(),
            data_loc_tab_->get_storage_stats()->get_storage_tier_sizes(),
            mq_plan_entry, svv );
    }

    return plan;
}

void transaction_preparer::build_scan_argument_mappings(
    const std::vector<scan_arguments>&   scan_args,
    const grouped_partition_information& grouped_info,
    std::unordered_map<uint32_t /* label*/,
                       partition_column_identifier_to_ckrs>&
        labels_to_pids_and_ckrs,
    partition_column_identifier_map_t<
        std::unordered_map<uint32_t /* label */, std::vector<cell_key_ranges>>>&
        pids_to_labels_and_ckrs,
    std::unordered_map<uint32_t /* label */, uint32_t /* pos */>& label_to_pos )
    const {

    // build a no change plan

    for( uint32_t pos = 0; pos < scan_args.size(); pos++ ) {
        const auto&                         scan_arg = scan_args.at( pos );
        partition_column_identifier_to_ckrs pids_to_ckrs;
        for( const auto& pid_entry : grouped_info.read_pids_to_shaped_ckrs_ ) {
            std::vector<cell_key_ranges> pid_ckrs;
            const auto&                  pid = pid_entry.first;
            for( const auto& scan_ckr : scan_arg.read_ckrs ) {
                if( is_ckr_partially_within_pcid( scan_ckr, pid ) ) {
                    pid_ckrs.emplace_back( shape_ckr_to_pcid( scan_ckr, pid ) );
                }
            }
            if( pid_ckrs.size() > 0 ) {
                pids_to_ckrs[pid] = pid_ckrs;
                if( pids_to_labels_and_ckrs.count( pid ) == 1 ) {
                    DCHECK_EQ( pids_to_labels_and_ckrs.at( pid ).count(
                                   scan_arg.label ),
                               0 );
                }
                pids_to_labels_and_ckrs[pid][scan_arg.label] = pid_ckrs;
            }
        }
        DCHECK_EQ( labels_to_pids_and_ckrs.count( scan_arg.label ), 0 );
        DCHECK_EQ( label_to_pos.count( scan_arg.label ), 0 );
        label_to_pos[scan_arg.label] = pos;
        labels_to_pids_and_ckrs[scan_arg.label] = pids_to_ckrs;
    }
}

std::shared_ptr<multi_site_pre_transaction_plan>
    transaction_preparer::generate_no_change_destination_scan_plan_if_possible(
        const ::clientid id, const grouped_partition_information& grouped_info,
        const std::vector<scan_arguments>& scan_args,
        const std::unordered_map<uint32_t /* label*/,
                                 partition_column_identifier_to_ckrs>&
            labels_to_pids_and_ckrs,
        const partition_column_identifier_map_t<std::unordered_map<
            uint32_t /* label */, std::vector<cell_key_ranges>>>&
            pids_to_labels_and_ckrs,
        const std::unordered_map<uint32_t /* label */, uint32_t /* pos */>&
                                                 label_to_pos,
        std::shared_ptr<multi_query_plan_entry>& mq_plan_entry,
        ::snapshot_vector&                       svv ) {

    start_timer(
        SS_GENERATE_NO_CHANGE_DESTINATION_SCAN_PLAN_IF_POSSIBLE_TIMER_ID );

    std::shared_ptr<multi_site_pre_transaction_plan> plan =
        evaluator_->build_no_change_scan_plan(
            id, grouped_info, scan_args, labels_to_pids_and_ckrs,
            pids_to_labels_and_ckrs, label_to_pos,
            periodic_site_selector_operations_.get_site_load_information(),
            data_loc_tab_->get_storage_stats()->get_storage_tier_sizes(),
            mq_plan_entry, svv );

    DVLOG( 40 ) << "Generated plan:" << plan;

    stop_timer(
        SS_GENERATE_NO_CHANGE_DESTINATION_SCAN_PLAN_IF_POSSIBLE_TIMER_ID );

    return plan;
}

std::shared_ptr<multi_site_pre_transaction_plan>
    transaction_preparer::generate_scan_plan_with_physical_adjustments(
        const ::clientid                         id,
        const std::vector<::cell_key_ranges>&    sorted_ckr_write_set,
        const std::vector<::cell_key_ranges>&    sorted_ckr_read_set,
        const std::vector<scan_arguments>&       scan_args,
        std::shared_ptr<multi_query_plan_entry>& mq_plan_entry,
        ::snapshot_vector& svv, const plan_strategy& strategy,
        bool need_new_parts, bool allow_missing ) {
    std::shared_ptr<multi_site_pre_transaction_plan> plan = nullptr;
    DVLOG( 40 ) << "Generating scan plan with physical adjustments";

    start_timer( SS_GENERATE_SCAN_PLAN_WITH_PHYSICAL_ADJUSTMENTS_TIMER_ID );


    partition_lock_mode lock_mode =
        configs_.physical_adjustment_lock_mode_;  // lock_mode::try_lock;
    if( strategy == plan_strategy::FORCE_CHANGE ) {
        lock_mode =
            configs_.force_change_lock_mode_;  // partition_lock_mode::try_lock;
    }
    if( need_new_parts ) {
        lock_mode = partition_lock_mode::lock;
    }

    int32_t num_sites = evaluator_->get_num_sites();

    grouped_partition_information grouped_info( num_sites );
    grouped_info = data_loc_tab_->get_partitions_and_group(
        sorted_ckr_write_set, sorted_ckr_read_set, lock_mode, num_sites,
        allow_missing );

    if( grouped_info.payloads_.empty() ) {
        // even getting or creating failed, so return a nullptr
        stop_timer( SS_GENERATE_SCAN_PLAN_WITH_PHYSICAL_ADJUSTMENTS_TIMER_ID );
        return plan;
    }

    // we grab a lock so this should always be true
    DCHECK_EQ( 0, grouped_info.inflight_snapshot_vector_.size() );
    DCHECK_EQ( 0, grouped_info.inflight_pids_.size() );

    DCHECK_EQ( grouped_info.new_partitions_.size(),
               grouped_info.new_read_partitions_.size() +
                   grouped_info.new_write_partitions_.size() );
    DCHECK_EQ( grouped_info.new_partitions_.size(), 0 );

    std::unordered_map<uint32_t /* label*/, partition_column_identifier_to_ckrs>
        labels_to_pids_and_ckrs;
    partition_column_identifier_map_t<
        std::unordered_map<uint32_t /* label */, std::vector<cell_key_ranges>>>
        pids_to_labels_and_ckrs;
    std::unordered_map<uint32_t /* label */, uint32_t /* pos */> label_to_pos;

    build_scan_argument_mappings( scan_args, grouped_info,
                                  labels_to_pids_and_ckrs,
                                  pids_to_labels_and_ckrs, label_to_pos );

    plan = get_pre_transaction_execution_scan_plan(
        id, grouped_info, scan_args, labels_to_pids_and_ckrs,
        pids_to_labels_and_ckrs, label_to_pos, mq_plan_entry, svv, strategy );

    DCHECK( plan );

    DVLOG( 40 )
        << "Generating scan_plan with physical adjustments going to # sites:"
        << plan->scan_arg_sites_.size();

    start_timer( SS_SORT_FINALIZE_AND_ADD_METADATA_TO_SCAN_PLAN_TIMER_ID );

    plan->plan_->sort_and_finalize_pids();
    plan->plan_->add_to_partition_mappings( grouped_info.payloads_ );
    plan->plan_->add_to_location_information_mappings(
        std::move( grouped_info.partition_location_informations_ ) );

    stop_timer( SS_SORT_FINALIZE_AND_ADD_METADATA_TO_SCAN_PLAN_TIMER_ID );

    stop_timer( SS_GENERATE_SCAN_PLAN_WITH_PHYSICAL_ADJUSTMENTS_TIMER_ID );
    return plan;
}

std::shared_ptr<multi_site_pre_transaction_plan>
    transaction_preparer::get_pre_transaction_execution_scan_plan(
        const ::clientid id, grouped_partition_information& grouped_info,
        const std::vector<scan_arguments>& scan_args,
        const std::unordered_map<uint32_t /* label*/,
                                 partition_column_identifier_to_ckrs>&
            labels_to_pids_and_ckrs,
        const partition_column_identifier_map_t<std::unordered_map<
            uint32_t /* label */, std::vector<cell_key_ranges>>>&
            pids_to_labels_and_ckrs,
        const std::unordered_map<uint32_t /* label */, uint32_t /* pos */>&
                                                 label_to_pos,
        std::shared_ptr<multi_query_plan_entry>& mq_plan_entry,
        ::snapshot_vector& svv, const plan_strategy& strategy ) {
    DVLOG( 40 ) << "Getting pre transaction execution plan";

    start_timer( SS_GET_PRE_TRANSACTION_EXECUTION_SCAN_PLAN_TIMER_ID );

    std::shared_ptr<multi_site_pre_transaction_plan> plan = nullptr;
    if( strategy == plan_strategy::CHANGE_POSSIBLE ) {
        plan = generate_no_change_destination_scan_plan_if_possible(
            id, grouped_info, scan_args, labels_to_pids_and_ckrs,
            pids_to_labels_and_ckrs, label_to_pos, mq_plan_entry, svv );
    }
    if( !plan ) {
        plan = evaluator_->generate_scan_plan(
            id, grouped_info, scan_args, labels_to_pids_and_ckrs,
            pids_to_labels_and_ckrs, label_to_pos,
            periodic_site_selector_operations_.get_site_load_information(),
            data_loc_tab_->get_storage_stats()->get_storage_tier_sizes(),
            mq_plan_entry, svv );
    }

    DVLOG( 40 ) << "Going to sites:" << plan->scan_arg_sites_.size();

    plan->plan_->build_work_queue();

    stop_timer( SS_GET_PRE_TRANSACTION_EXECUTION_SCAN_PLAN_TIMER_ID );

    return plan;
}

execution_result transaction_preparer::prepare_transaction_with_strategy(
    const std::vector<::cell_key_ranges>& sorted_ckr_write_set,
    const std::vector<::cell_key_ranges>& sorted_ckr_read_set,
    const ::clientid id, const ::snapshot_vector& svv,
    const plan_strategy& strategy ) {

    snapshot_vector                       transaction_begin_svv = svv;
    plan_strategy                         actual_strategy = strategy;
    std::shared_ptr<pre_transaction_plan> plan =
        prepare_transaction_data_items_for_single_site_execution(
            id, transaction_begin_svv, sorted_ckr_write_set,
            sorted_ckr_read_set, actual_strategy );

    if( plan == nullptr ) {
        DLOG( WARNING ) << "Unable to prepare_transaction_with_strategy:"
                        << actual_strategy << ", for client:" << id;

        return std::make_tuple( -1, actual_strategy );
    }

    DVLOG( k_rpc_handler_log_level )
        << "Opening transaction on behalf of the client" << id << " at "
        << plan->destination_;
    DVLOG( 20 ) << "Begin:" << id << " svv:" << transaction_begin_svv;

    DVLOG( 20 ) << "Begin:" << id << " ws:" << sorted_ckr_write_set
                << " rs:" << sorted_ckr_read_set;

    start_timer( BEGIN_FOR_WRITE_TIMER_ID );
    const sm_client_ptr& client =
        conn_pool_->getClient( plan->destination_, id, 0 );
    DVLOG( 30 ) << "Calling begin...";

    auto prediction_holder =
        cost_model2_->predict_single_site_transaction_execution_time(
            periodic_site_selector_operations_.get_site_load(
                plan->destination_ ),
            plan->txn_stats_ );

    DVLOG( 40 ) << "Calling begin transaction:" << id
                << ", at:" << plan->destination_
                << ", write_pids:" << plan->write_pids_
                << ", read_pids:" << plan->read_pids_;

    DCHECK( plan->mq_plan_ );
    plan->mq_plan_->set_plan_type( multi_query_plan_type::EXECUTING );

    begin_result br;
    make_rpc_and_get_service_timer( rpc_begin_transaction, client, br, id,
                                    transaction_begin_svv, plan->write_pids_,
                                    plan->read_pids_, plan->inflight_pids_ );
    int ret_dest = plan->destination_;
    if( br.status != exec_status_type::COMMAND_OK ) {
        LOG( WARNING ) << "Unable to prepare transaction for local execution"
                       << br.status << ", client" << id
                       << ", at site:" << ret_dest;
        ret_dest = -1;
    }

    stop_timer( BEGIN_FOR_WRITE_TIMER_ID );

    prediction_holder.add_real_timers( br.timers );
    cost_model2_->add_results( prediction_holder );

#if 0
    wait_for_downgrade_to_complete_and_release_partitions( id, plan,
                                                           actual_strategy );
#endif
    plan->mq_plan_->set_plan_type( multi_query_plan_type::CACHED );
    DCHECK( plan->stat_decision_holder_ );
    stat_enumerator_holder_->add_decisions( plan->stat_decision_holder_  );

    DVLOG( 40 ) << "Calling begin transaction:" << id
                << ", at:" << plan->destination_
                << ", write_pids:" << plan->write_pids_
                << ", read_pids:" << plan->read_pids_
                << ", done. Status:" << br.status;

    return std::make_tuple( ret_dest, actual_strategy );
}

std::shared_ptr<pre_transaction_plan> transaction_preparer::
    prepare_transaction_data_items_for_single_site_execution(
        const ::clientid id, ::snapshot_vector& svv,
        const std::vector<::cell_key_ranges>& sorted_ckr_write_set,
        const std::vector<::cell_key_ranges>& sorted_ckr_read_set,
        plan_strategy&                        strategy ) {
    start_timer(
        SS_PREPARE_TRANSACTION_DATA_ITEMS_FOR_SINGLE_SITE_EXECUTION_TIMER_ID );

    std::shared_ptr<pre_transaction_plan> plan = nullptr;
    bool                                  need_new_parts = false;

    auto mq_plan_entry = std::make_shared<multi_query_plan_entry>();

    if( ( strategy == plan_strategy::ANY ) or
        ( strategy == plan_strategy::NO_CHANGE ) ) {
        auto optimistic_ret = generate_optimistic_no_change_plan(
            sorted_ckr_write_set, sorted_ckr_read_set, mq_plan_entry, svv );
        plan = std::get<0>( optimistic_ret );
        need_new_parts = std::get<1>( optimistic_ret );
    }
    if( plan ) {
        strategy = plan_strategy::NO_CHANGE;
        DCHECK_EQ( 0, plan->get_num_work_items() );
    } else if( strategy != plan_strategy::NO_CHANGE ) {
        plan = generate_plan_with_physical_adjustments(
            id, sorted_ckr_write_set, sorted_ckr_read_set, mq_plan_entry, svv,
            strategy, need_new_parts );
        strategy = plan_strategy::CHANGE_POSSIBLE;
        if( plan == nullptr ) {
            LOG( ERROR ) << "Unable to generate plan with physical adjustments "
                            "for client:"
                         << id << ", as could not find partitions!";
            stop_timer(
                SS_PREPARE_TRANSACTION_DATA_ITEMS_FOR_SINGLE_SITE_EXECUTION_TIMER_ID );

            return nullptr;
        }
        plan->unlock_background_partitions();
        if( plan->get_num_work_items() > 0 ) {
            action_outcome outcome =
                execute_pre_transaction_plan( plan, id, svv );
            if( outcome == action_outcome::FAILED ) {
                LOG( ERROR ) << "Unable to execute pre transaction plan with "
                                "physical adjustment for client:"
                             << id << ", as actions failed!";
                // unlock plan
                plan->unlock_payloads_because_of_failure(
                    partition_lock_mode::lock );

                stop_timer(
                    SS_PREPARE_TRANSACTION_DATA_ITEMS_FOR_SINGLE_SITE_EXECUTION_TIMER_ID );

                return nullptr;
            }
        }
        if( plan->get_background_work_items().size() > 0 ) {
            action_executor_.add_background_work_items(
                plan->get_background_work_items() );
        }
        plan->unlock_remaining_payloads( partition_lock_mode::lock );
    }
    DCHECK( plan );

    DVLOG( 40 ) << "Plan for id:" << id
                << ", has write_pids: " << plan->write_pids_
                << ", and read_pids:" << plan->read_pids_;

    plan->build_and_get_transaction_prediction_stats(
        sorted_ckr_write_set, sorted_ckr_read_set, data_loc_tab_->get_stats(),
        cost_model2_ );

    stop_timer(
        SS_PREPARE_TRANSACTION_DATA_ITEMS_FOR_SINGLE_SITE_EXECUTION_TIMER_ID );

    return plan;
}

std::tuple<std::shared_ptr<pre_transaction_plan>, bool>
    transaction_preparer::generate_optimistic_no_change_plan(
        const std::vector<::cell_key_ranges>&    ckr_write_set,
        const std::vector<::cell_key_ranges>&    ckr_read_set,
        std::shared_ptr<multi_query_plan_entry>& mq_plan_entry,
        ::snapshot_vector&                       svv ) {

    start_timer( SS_GENERATE_OPTIMISTIC_NO_CHANGE_PLAN_TIMER_ID );

    DVLOG( 40 ) << "Trying to generate optimistic no change plan";
    std::shared_ptr<pre_transaction_plan> plan = nullptr;

    partition_lock_mode lock_mode =
        configs_.optimistic_lock_mode_;  // partition_lock_mode::no_lock;

    grouped_partition_information group_info =
        data_loc_tab_->get_partitions_and_group(
            ckr_write_set, ckr_read_set, lock_mode, evaluator_->get_num_sites(),
            false /* don't allow missing */ );
    if( group_info.payloads_.empty() ) {
        DVLOG( 40 ) << "Could not generate optimistic no change plan, as could "
                       "not find partitions";
        // couldn't find a key
        stop_timer( SS_GENERATE_OPTIMISTIC_NO_CHANGE_PLAN_TIMER_ID );
        return std::make_tuple<>( nullptr, true );
    }

    if( !group_info.new_partitions_.empty() ) {
        // did an insert so can't do the work
        DVLOG( 40 ) << "Could not generate optimistic no change plan, as some "
                       "partitions are new ";
        unlock_payloads( group_info.payloads_, lock_mode );
        stop_timer( SS_GENERATE_OPTIMISTIC_NO_CHANGE_PLAN_TIMER_ID );
        return std::make_tuple<>( nullptr, true );
    }

    DCHECK_EQ( 0, group_info.new_write_partitions_.size() );
    DCHECK_EQ( 0, group_info.new_read_partitions_.size() );

    plan =
        generate_no_change_destination_plan_if_possible_or_wait_for_plan_reuse(
            group_info, mq_plan_entry, svv );

    if( plan ) {
        start_timer( SS_SORT_FINALIZE_AND_ADD_METADATA_TO_PLAN_TIMER_ID );
        plan->add_to_partition_mappings( group_info.payloads_ );
        plan->add_to_location_information_mappings(
            std::move( group_info.partition_location_informations_ ) );

        plan->sort_and_finalize_pids();
        stop_timer( SS_SORT_FINALIZE_AND_ADD_METADATA_TO_PLAN_TIMER_ID );

        DVLOG( 40 ) << "Generates optimistic no change plan to site:"
                    << plan->destination_;

    } else {
        DVLOG( 40 ) << "Could not generate optimistic no change plan, as "
                       "partitions are at multiple sites";
    }
    // unlock it all anyhow here, so we can keep up the appearance that there is
    // no lock later
    unlock_payloads( group_info.payloads_, lock_mode );

    stop_timer( SS_GENERATE_OPTIMISTIC_NO_CHANGE_PLAN_TIMER_ID );

    return std::make_tuple<>( plan, false );
}

std::shared_ptr<pre_transaction_plan>
    transaction_preparer::generate_plan_with_physical_adjustments(
        const ::clientid                         id,
        const std::vector<::cell_key_ranges>&    sorted_ckr_write_set,
        const std::vector<::cell_key_ranges>&    sorted_ckr_read_set,
        std::shared_ptr<multi_query_plan_entry>& mq_plan_entry,
        ::snapshot_vector& svv, const plan_strategy& strategy,
        bool need_new_parts ) {
    DVLOG( 40 ) << "Generating plan with physical adjustments";

    start_timer( SS_GENERATE_PLAN_WITH_PHYSICAL_ADJUSTMENTS_TIMER_ID );

    std::shared_ptr<pre_transaction_plan> plan = nullptr;

    partition_lock_mode lock_mode =
        configs_.physical_adjustment_lock_mode_;  // lock_mode::try_lock;
    if( strategy == plan_strategy::FORCE_CHANGE ) {
        lock_mode =
            configs_.force_change_lock_mode_;  // partition_lock_mode::try_lock;
    }
    if( need_new_parts ) {
        lock_mode = partition_lock_mode::lock;
    }

    int32_t num_sites = evaluator_->get_num_sites();

    grouped_partition_information grouped_info( num_sites );
    if( need_new_parts ) {
        DVLOG( 40 ) << "Generating plan with physical adjustments must create "
                       "partitions";
        grouped_info = data_loc_tab_->get_or_create_partitions_and_group(
            sorted_ckr_write_set, sorted_ckr_read_set, lock_mode, num_sites );
    } else {
        grouped_info = data_loc_tab_->get_partitions_and_group(
            sorted_ckr_write_set, sorted_ckr_read_set, lock_mode, num_sites,
            false /* don't allow missing */ );
    }

    if( grouped_info.payloads_.empty() and
        !( sorted_ckr_write_set.empty() and sorted_ckr_read_set.empty() ) ) {
        // even getting or creating failed, so return a nullptr
        stop_timer( SS_GENERATE_PLAN_WITH_PHYSICAL_ADJUSTMENTS_TIMER_ID );
        return plan;
    }

    // we grab a lock so this should always be true
    DCHECK_EQ( 0, grouped_info.inflight_snapshot_vector_.size() );
    DCHECK_EQ( 0, grouped_info.inflight_pids_.size() );

    DCHECK_EQ( grouped_info.new_partitions_.size(),
               grouped_info.new_read_partitions_.size() +
                   grouped_info.new_write_partitions_.size() );
    if( !need_new_parts ) {
        DCHECK_EQ( 0, grouped_info.new_partitions_.size() );
    }

    plan = get_pre_transaction_execution_plan( id, grouped_info, mq_plan_entry,
                                               svv, strategy );
    DCHECK( plan );

    DVLOG( 40 ) << "Generating plan with physical adjustments going to site:"
                << plan->destination_;

    start_timer( SS_SORT_FINALIZE_AND_ADD_METADATA_TO_PLAN_TIMER_ID );

    plan->sort_and_finalize_pids();
    plan->add_to_partition_mappings( grouped_info.payloads_ );
    plan->add_to_location_information_mappings(
        std::move( grouped_info.partition_location_informations_ ) );

    stop_timer( SS_SORT_FINALIZE_AND_ADD_METADATA_TO_PLAN_TIMER_ID );

    stop_timer( SS_GENERATE_PLAN_WITH_PHYSICAL_ADJUSTMENTS_TIMER_ID );

    return plan;
}

void transaction_preparer::add_replica_to_all_sites(
    std::shared_ptr<pre_transaction_plan>& plan, int master,
    const std::vector<std::shared_ptr<partition_payload>>& new_partitions,
    const std::shared_ptr<pre_action>&                     add_action ) {

    DCHECK_EQ( pre_action_type::ADD_PARTITIONS, add_action->type_ );

    add_partition_pre_action* add_pre_action =
        (add_partition_pre_action*) add_action->args_;


    std::vector<std::shared_ptr<pre_action>> deps;
    deps.push_back( add_action );

    const std::vector<partition_column_identifier> add_pids =
        payloads_to_identifiers( new_partitions );

    for( int site = 0; site < evaluator_->get_num_sites(); site++ ) {
        if( site != master ) {
            cost_model_prediction_holder model_holder;
            std::shared_ptr<pre_action>  replica_action =
                create_add_replica_partition_pre_action(
                    site, master, add_pids, add_pre_action->partition_types_,
                    add_pre_action->storage_types_, deps, 0 /* cost*/,
                    model_holder );
            plan->add_pre_action_work_item( replica_action );
            deps.push_back( replica_action );
        }
    }
}

std::shared_ptr<pre_transaction_plan>
    transaction_preparer::get_pre_transaction_execution_plan(
        const ::clientid id, grouped_partition_information& grouped_info,
        std::shared_ptr<multi_query_plan_entry>& mq_plan_entry,
        ::snapshot_vector& svv, const plan_strategy& strategy ) {
    DVLOG( 40 ) << "Getting pre transaction execution plan";

    start_timer( SS_GET_PRE_TRANSACTION_EXECUTION_PLAN_TIMER_ID );

    int                                   destination = -1;
    std::shared_ptr<pre_transaction_plan> plan = nullptr;
    if( grouped_info.existing_read_partitions_.empty() and
        grouped_info.existing_write_partitions_.empty() ) {
        plan = std::make_shared<pre_transaction_plan>(
            stat_enumerator_holder_->construct_decision_holder_ptr() );
        plan->destination_ =
            evaluator_->generate_insert_only_partition_destination(
                grouped_info.new_partitions_ );
        plan->estimated_number_of_updates_to_wait_for_ =
            cost_model2_->get_default_write_num_updates_required_count();
        build_multi_query_plan_entry( mq_plan_entry,
                                      grouped_info.new_write_partitions_set_,
                                      grouped_info.new_read_partitions_set_ );
        plan->mq_plan_ = mq_plan_entry;
        mq_plan_entry->set_plan_destination( plan->destination_,
                                             multi_query_plan_type::PLANNING );

    } else {
        if( strategy == plan_strategy::CHANGE_POSSIBLE ) {
            plan = generate_no_change_destination_plan_if_possible(
                grouped_info, mq_plan_entry, svv );
        }
        if( !plan ) {
            plan = evaluator_->generate_plan(
                id, grouped_info.site_locations_, grouped_info.new_partitions_,
                grouped_info.existing_write_partitions_,
                grouped_info.existing_read_partitions_,
                grouped_info.existing_write_partitions_set_,
                grouped_info.existing_read_partitions_set_,
                grouped_info.partition_location_informations_,
                grouped_info.write_pids_to_shaped_ckrs_,
                grouped_info.read_pids_to_shaped_ckrs_,
                periodic_site_selector_operations_.get_site_load_information(),
                data_loc_tab_->get_storage_stats()->get_storage_tier_sizes(),
                mq_plan_entry, svv );
        }
    }
    destination = plan->destination_;

    DVLOG( 40 ) << "Going to site:" << destination;

    if( grouped_info.new_partitions_.size() > 0 ) {
        DVLOG( 40 ) << "insert_partitions, at site:" << destination
                    << ", identifiers:"
                    << payloads_to_identifiers( grouped_info.new_partitions_ );

        auto new_partition_types = evaluator_->get_new_partition_types(
            destination, plan,
            periodic_site_selector_operations_.get_site_load_information().at(
                destination ),
            data_loc_tab_->get_storage_stats()->get_storage_tier_sizes(),
            grouped_info.new_partitions_ );

        auto add_action = create_add_partition_pre_action(
            destination, destination, grouped_info.new_partitions_,
            std::get<0>( new_partition_types ),
            std::get<1>( new_partition_types ),
            evaluator_->get_new_partition_update_destination_slot(
                destination, grouped_info.new_partitions_ ) );
        plan->add_pre_action_work_item( add_action );

        if( mastering_type_configs_.is_mastering_type_fully_replicated_ ) {
            // add a replica to every site
            add_replica_to_all_sites(
                plan, destination, grouped_info.new_partitions_, add_action );
        }

        for( auto payload : grouped_info.new_write_partitions_ ) {
            plan->write_pids_.push_back( payload->identifier_ );
        }
        for( auto payload : grouped_info.new_read_partitions_ ) {
            plan->read_pids_.push_back( payload->identifier_ );
        }
    }

    plan->build_work_queue();

    stop_timer( SS_GET_PRE_TRANSACTION_EXECUTION_PLAN_TIMER_ID );

    return plan;
}

std::shared_ptr<pre_transaction_plan> transaction_preparer::
    generate_no_change_destination_plan_if_possible_or_wait_for_plan_reuse(
        const grouped_partition_information&     grouped_info,
        std::shared_ptr<multi_query_plan_entry>& mq_plan_entry,
        ::snapshot_vector&                       svv ) {

    std::shared_ptr<pre_transaction_plan> plan =
        generate_no_change_destination_plan_if_possible( grouped_info,
                                                         mq_plan_entry, svv );

    if( plan ) {
        return plan;
    } else if( configs_.enable_wait_for_plan_reuse_ ) {
        plan = evaluator_->get_plan_from_current_planners( grouped_info,
                                                           mq_plan_entry, svv );
    }

    return plan;
}

std::shared_ptr<pre_transaction_plan>
    transaction_preparer::generate_no_change_destination_plan_if_possible(
        const grouped_partition_information&     grouped_info,
        std::shared_ptr<multi_query_plan_entry>& mq_plan_entry,
        ::snapshot_vector&                       svv ) {

    start_timer( SS_GENERATE_NO_CHANGE_DESTINATION_PLAN_IF_POSSIBLE_TIMER_ID );

    std::shared_ptr<pre_transaction_plan> plan = nullptr;

    std::vector<int> no_change_destinations =
        grouped_info.get_no_change_destinations();

    DVLOG( 40 ) << "Generate no change destination:" << no_change_destinations;

    if( no_change_destinations.size() > 0 ) {
        int destination = evaluator_->get_no_change_destination(
            no_change_destinations, grouped_info.existing_write_partitions_,
            grouped_info.existing_read_partitions_,
            grouped_info.existing_write_partitions_set_,
            grouped_info.existing_read_partitions_set_, svv );

        plan = evaluator_->build_no_change_plan( destination, grouped_info,
                                                 mq_plan_entry, svv );
    }

    DVLOG( 40 ) << "Generated plan:" << plan;

    stop_timer( SS_GENERATE_NO_CHANGE_DESTINATION_PLAN_IF_POSSIBLE_TIMER_ID );

    return plan;
}

action_outcome transaction_preparer::execute_pre_transaction_plan(
    std::shared_ptr<pre_transaction_plan> plan, const ::clientid id,
    ::snapshot_vector& svv ) {

    uint32_t num_threads = std::min( plan->get_num_work_items(),
                                     configs_.num_worker_threads_per_client_ );

    start_timer( SS_EXECUTE_PRE_TRANSACTION_PLAN_TIMER_ID );

    DVLOG( 10 ) << "Execute pre transaction plan actions:" << id
                << ", dest:" << plan->destination_
                << ", parent on num threads:" << num_threads;

    std::vector<std::thread> execute_threads;
    for( uint32_t tid = 0; tid < num_threads; tid++ ) {
        // svv and downgrade threads
        execute_threads.push_back( std::thread(
            &transaction_preparer::execute_pre_transaction_plan_actions, this,
            id, tid, conn_pool_->translate_client_id( id, tid ), plan ) );
    }
    join_threads( execute_threads );

    action_outcome outcome = plan->get_plan_outcome();

    DVLOG( 10 ) << "Execute pre transaction plan actions:" << id
                << ", dest:" << plan->destination_ << ", parent done!"
                << " Outcome:" << outcome;

    stop_timer( SS_EXECUTE_PRE_TRANSACTION_PLAN_TIMER_ID );

    return outcome;
}

void transaction_preparer::execute_pre_transaction_plan_actions(
    const ::clientid id, const int32_t tid, const ::clientid translated_id,
    std::shared_ptr<pre_transaction_plan> plan ) {
    start_timer( SS_EXECUTE_PRE_TRANSACTION_PLAN_ACTIONS_WORKER_TIMER_ID );

    DVLOG( k_ss_action_executor_log_level )
        << "Execute pre transaction plan actions:" << id << ", tid:" << tid
        << ", translated_id:" << translated_id
        << ", dest:" << plan->destination_ << ", worker";

    std::shared_ptr<pre_action> action = plan->get_next_pre_action_work_item();
    while( action ) {
        action_outcome outcome =
            action_executor_.execute_pre_transaction_plan_action(
                plan, action, id, tid, translated_id );
        if( outcome == action_outcome::FAILED ) {
            break;
        }

        action = plan->get_next_pre_action_work_item();
    }

    DVLOG( k_ss_action_executor_log_level )
        << "Execute pre transaction plan actions:" << id << ", tid:" << tid
        << ", dest:" << plan->destination_ << ", worker done!";

    stop_timer( SS_EXECUTE_PRE_TRANSACTION_PLAN_ACTIONS_WORKER_TIMER_ID );
}

void transaction_preparer::
    wait_for_downgrade_to_complete_and_release_partitions(
        const ::clientid id, std::shared_ptr<pre_transaction_plan> plan,
        const plan_strategy& actual_strategy ) {

    if( actual_strategy == plan_strategy::NO_CHANGE ) {
        return;
    }

    start_timer(
        SS_WAIT_FOR_DOWNGRADE_TO_COMPLETE_AND_RELEASE_PARTITIONS_TIMER_ID );

    // this will only have an affect if we pack in the partitions, which we
    // don't do for plans that require no action. MV-TODO is generalize this
    plan->unlock_remaining_payloads( partition_lock_mode::lock );

    stop_timer(
        SS_WAIT_FOR_DOWNGRADE_TO_COMPLETE_AND_RELEASE_PARTITIONS_TIMER_ID );
}

void transaction_preparer::increase_tracking_counts(
    std::shared_ptr<pre_transaction_plan>& plan ) {
    increase_tracking_counts( plan->destination_, plan, plan->read_pids_,
                              plan->write_pids_ );
}
void transaction_preparer::increase_tracking_counts(
    std::shared_ptr<multi_site_pre_transaction_plan>& plan ) {
    std::vector<partition_column_identifier> empty_pids;
    for( const auto& entry : plan->read_pids_ ) {
        increase_tracking_counts( entry.first, plan->plan_, entry.second,
                                  empty_pids );
    }
}

void transaction_preparer::increase_tracking_counts(
    int destination, std::shared_ptr<pre_transaction_plan>& plan,
    const std::vector<partition_column_identifier>& read_pids,
    const std::vector<partition_column_identifier>& write_pids ) {
    start_timer( SS_INCREASE_TRACKING_COUNTS_TIMER_ID );

    data_loc_tab_->modify_site_read_access_count( destination,
                                                  read_pids.size() );
    for( const auto& pid : read_pids ) {
        auto partition = plan->get_partition( nullptr, pid );
        partition->read_accesses_++;
    }
    data_loc_tab_->modify_site_write_access_count( destination,
                                                   write_pids.size() );
    int64_t max_write_accesses = 0;
    for( const auto& pid : plan->write_pids_ ) {
        auto    partition = plan->get_partition( nullptr, pid );
        int64_t count = partition->write_accesses_++;
        if( count > max_write_accesses ) {
            max_write_accesses = count;
        }
    }

    data_loc_tab_->set_max_write_accesses_if_greater( max_write_accesses );

    stop_timer( SS_INCREASE_TRACKING_COUNTS_TIMER_ID );
}

void transaction_preparer::add_plan_to_multi_query_tracking(
    std::shared_ptr<pre_transaction_plan>& plan ) const {

    DCHECK( plan->mq_plan_ );
    auto mq_plan_entry = plan->mq_plan_;

    for( const auto& pid : plan->write_pids_ ) {
        auto partition = plan->get_partition( nullptr, pid );
        partition->multi_query_entry_->add_entry( mq_plan_entry );
    }
    for( const auto& pid : plan->read_pids_ ) {
        auto partition = plan->get_partition( nullptr, pid );
        partition->multi_query_entry_->add_entry( mq_plan_entry );
    }
}

void transaction_preparer::add_scan_plan_to_multi_query_tracking(
    std::shared_ptr<multi_site_pre_transaction_plan>& plan ) const {

    DCHECK( plan->plan_->mq_plan_ );
    auto mq_plan_entry = plan->plan_->mq_plan_;

    for( const auto& pid : plan->plan_->read_pids_ ) {
        auto partition = plan->plan_->get_partition( nullptr, pid );
        if( partition ) {
            partition->multi_query_entry_->add_entry( mq_plan_entry );
        }
    }
}

void transaction_preparer::sample_transaction_accesses(
    ::clientid id, std::shared_ptr<multi_site_pre_transaction_plan>& plan,
    const std::vector<::cell_key_ranges>& ckr_write_set,
    const std::vector<::cell_key_ranges>& ckr_read_set,
    const high_res_time&                  time_of_exec ) {

    if( id % configs_.astream_mod_ != 0 ) {
        // we don't care
        return;
    }


    start_timer( SS_SAMPLE_TRANSACTION_ACCESSES_TIMER_ID );

    high_res_time time_after_txn_exec =
        std::chrono::high_resolution_clock::now();

    // add to query predictor
    query_predictor_->add_query_observation( ckr_write_set, ckr_read_set,
                                             time_of_exec );

    transaction_partition_accesses txn_partition_accesses =
        generate_txn_partition_accesses_from_plan( plan, ckr_write_set,
                                                   ckr_read_set );

    std::unique_ptr<adaptive_reservoir_sampler>& sampler = samplers_.at( id );
    high_res_time last_start_point = last_sampled_txn_start_points_.at( id );

    std::unique_ptr<access_stream>& access_stream =
        access_streams_.at( id / configs_.astream_mod_ );

    bool should_sample = sampler->should_sample();
    if( should_sample ) {
        add_scan_plan_to_multi_query_tracking( plan );
    }

    // If we are told to sample it, then write it to the stream as a start point
    if( should_sample and
        !( ckr_write_set.empty() and ckr_read_set.empty() ) ) {
        for( const auto& entry : plan->read_pids_ ) {
            data_loc_tab_->increase_sample_based_site_read_access_count(
                entry.first, entry.second.size() );
            for( const auto& pid : entry.second ) {
                auto partition = plan->plan_->get_partition( nullptr, pid );
                partition->sample_based_read_accesses_++;
            }
        }
        // New start point!
        access_stream->write_access_to_stream( txn_partition_accesses,
                                               time_after_txn_exec, true );
        last_sampled_txn_start_points_[id] = time_after_txn_exec;
    }

    std::chrono::milliseconds elapsed_time_since_last_start_point =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            time_after_txn_exec - last_start_point );

    // else if we are within the tracking interval of the last exec time for a
    // start point, location_information us
    //(but not as a start point )
    // N.B. There is a race condition here that the astream wakes up and
    // computes the tracking_summary.
    // Currently, this would not be included and just skipped over until they
    // grab a start point.
    // This is a trade-off we make for having less locking
    if( elapsed_time_since_last_start_point <= tracking_duration_ ) {
        access_stream->write_access_to_stream( txn_partition_accesses,
                                               time_after_txn_exec, false );
    }

    stop_timer( SS_SAMPLE_TRANSACTION_ACCESSES_TIMER_ID );
}

void transaction_preparer::sample_transaction_accesses(
    ::clientid id, std::shared_ptr<pre_transaction_plan>& plan,
    const std::vector<::cell_key_ranges>& ckr_write_set,
    const std::vector<::cell_key_ranges>& ckr_read_set,
    const high_res_time&                  time_of_exec ) {

    if( id % configs_.astream_mod_ != 0 ) {
        // we don't care
        return;
    }


    start_timer( SS_SAMPLE_TRANSACTION_ACCESSES_TIMER_ID );

    high_res_time time_after_txn_exec =
        std::chrono::high_resolution_clock::now();

    // add to query predictor
    query_predictor_->add_query_observation( ckr_write_set, ckr_read_set,
                                             time_of_exec );

    transaction_partition_accesses txn_partition_accesses =
        generate_txn_partition_accesses_from_plan( plan, ckr_write_set,
                                                   ckr_read_set );

    std::unique_ptr<adaptive_reservoir_sampler>& sampler = samplers_.at( id );
    high_res_time last_start_point = last_sampled_txn_start_points_.at( id );

    std::unique_ptr<access_stream>& access_stream =
        access_streams_.at( id / configs_.astream_mod_ );

    bool should_sample = sampler->should_sample();
    if( should_sample ) {
        add_plan_to_multi_query_tracking( plan );
    }

    // If we are told to sample it, then write it to the stream as a start point
    if( should_sample and
        !( ckr_write_set.empty() and ckr_read_set.empty() ) ) {
        // add the thing
        data_loc_tab_->increase_sample_based_site_read_access_count(
            plan->destination_, plan->read_pids_.size() );
        for( const auto& pid : plan->read_pids_ ) {
            auto partition = plan->get_partition( nullptr, pid );
            partition->sample_based_read_accesses_++;
        }
        data_loc_tab_->increase_sample_based_site_write_access_count(
            plan->destination_, plan->write_pids_.size() );
        for( const auto& pid : plan->write_pids_ ) {
            auto partition = plan->get_partition( nullptr, pid );
            partition->sample_based_write_accesses_++;
        }

        // New start point!
        access_stream->write_access_to_stream( txn_partition_accesses,
                                               time_after_txn_exec, true );
        last_sampled_txn_start_points_[id] = time_after_txn_exec;
    }

    std::chrono::milliseconds elapsed_time_since_last_start_point =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            time_after_txn_exec - last_start_point );

    // else if we are within the tracking interval of the last exec time for a
    // start point, location_information us
    //(but not as a start point )
    // N.B. There is a race condition here that the astream wakes up and
    // computes the tracking_summary.
    // Currently, this would not be included and just skipped over until they
    // grab a start point.
    // This is a trade-off we make for having less locking
    if( elapsed_time_since_last_start_point <= tracking_duration_ ) {
        access_stream->write_access_to_stream( txn_partition_accesses,
                                               time_after_txn_exec, false );
    }

    stop_timer( SS_SAMPLE_TRANSACTION_ACCESSES_TIMER_ID );
}

void transaction_preparer::sample_transaction_accesses(
    ::clientid id,
    const std::unordered_map<int, per_site_grouped_partition_information>&
                                          per_site_groupings,
    const std::vector<::cell_key_ranges>& ckr_write_set,
    const std::vector<::cell_key_ranges>& ckr_read_set,
    const high_res_time&                  time_of_exec ) {

    if( id % configs_.astream_mod_ != 0 ) {
        // we don't care
        return;
    }

    start_timer( SS_SAMPLE_TRANSACTION_ACCESSES_TIMER_ID );

    // add to query predictor
    query_predictor_->add_query_observation( ckr_write_set, ckr_read_set,
                                             time_of_exec );


    partition_column_identifier_map_t<int>   pid_to_site;
    std::vector<partition_column_identifier> write_pids;
    std::vector<partition_column_identifier> read_pids;

    for( auto& per_site_entry : per_site_groupings ) {
        uint32_t site = per_site_entry.first;
        auto&    site_entry = per_site_entry.second;

        for( const auto& pid : site_entry.read_pids_ ) {
            read_pids.push_back( pid );
            pid_to_site[pid] = site;
        }
        for( const auto& pid : site_entry.write_pids_ ) {
            write_pids.push_back( pid );
            pid_to_site[pid] = site;
        }
    }

    high_res_time time_after_txn_exec =
        std::chrono::high_resolution_clock::now();

    transaction_partition_accesses txn_partition_accesses =
        generate_txn_partition_accesses( write_pids, read_pids, -1 /*dest*/,
                                         pid_to_site, ckr_write_set,
                                         ckr_read_set );

    std::unique_ptr<adaptive_reservoir_sampler>& sampler = samplers_.at( id );
    high_res_time last_start_point = last_sampled_txn_start_points_.at( id );

    std::unique_ptr<access_stream>& access_stream =
        access_streams_.at( id / configs_.astream_mod_ );

    if( txn_partition_accesses.partition_accesses_.empty() ) {
        stop_timer( SS_SAMPLE_TRANSACTION_ACCESSES_TIMER_ID );
        return;
    }

    // If we are told to sample it, then write it to the stream as a start point
    if( sampler->should_sample() and
        !( ckr_write_set.empty() and ckr_read_set.empty() ) ) {
        for( auto& per_site_entry : per_site_groupings ) {
            uint32_t site = per_site_entry.first;
            auto&    site_entry = per_site_entry.second;

            data_loc_tab_->increase_sample_based_site_read_access_count(
                site, site_entry.read_pids_.size() );
            data_loc_tab_->increase_sample_based_site_write_access_count(
                site, site_entry.write_pids_.size() );

            // add the thing
            for( auto& part : site_entry.read_payloads_ ) {
                part->sample_based_read_accesses_++;
            }
            for( auto& part : site_entry.write_payloads_ ) {
                part->sample_based_write_accesses_++;
            }
        }

        // New start point!
        access_stream->write_access_to_stream( txn_partition_accesses,
                                               time_after_txn_exec, true );
        last_sampled_txn_start_points_[id] = time_after_txn_exec;
    }

    std::chrono::milliseconds elapsed_time_since_last_start_point =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            time_after_txn_exec - last_start_point );

    // else if we are within the tracking interval of the last exec time for a
    // start point, location_information us
    //(but not as a start point )
    // N.B. There is a race condition here that the astream wakes up and
    // computes the tracking_summary.
    // Currently, this would not be included and just skipped over until they
    // grab a start point.
    // This is a trade-off we make for having less locking
    if( elapsed_time_since_last_start_point <= tracking_duration_ ) {
        access_stream->write_access_to_stream( txn_partition_accesses,
                                               time_after_txn_exec, false );
    }

    stop_timer( SS_SAMPLE_TRANSACTION_ACCESSES_TIMER_ID );
}

execution_result transaction_preparer::execute_one_shot_sproc_with_strategy(
    const std::vector<::cell_key_ranges>& ckr_write_set,
    const std::vector<::cell_key_ranges>& ckr_read_set,
    one_shot_sproc_result& _return, const ::clientid id,
    const ::snapshot_vector& svv, const std::string& name,
    const std::string& sproc_args, const plan_strategy& strategy ) {
    start_timer( SS_EXECUTE_ONE_SHOT_SPROC_WITH_STRATEGY_TIMER_ID );
    start_timer( SS_EXECUTE_ONE_SHOT_SPROC_WITH_FORCE_STRATEGY_TIMER_ID );

    DVLOG( 40 ) << "Executing one shot sproc with strategy:" << id;

    high_res_time time_of_txn_exec = std::chrono::high_resolution_clock::now();

    snapshot_vector transaction_begin_svv = svv;
    plan_strategy   actual_strategy = strategy;

    std::shared_ptr<pre_transaction_plan> plan =
        prepare_transaction_data_items_for_single_site_execution(
            id, transaction_begin_svv, ckr_write_set, ckr_read_set,
            actual_strategy );

    if( plan == nullptr ) {
        LOG( WARNING ) << "Unable to execute_one_shot_sproc_with_strategy:"
                       << actual_strategy << ", client" << id
                       << ", name:" << name;

        _return.status = exec_status_type::DATA_NOT_LOCATED_AT_ONE_SITE;

        stop_timer( SS_EXECUTE_ONE_SHOT_SPROC_WITH_STRATEGY_TIMER_ID );
        if( strategy == plan_strategy::FORCE_CHANGE ) {
            stop_timer(
                SS_EXECUTE_ONE_SHOT_SPROC_WITH_FORCE_STRATEGY_TIMER_ID );
        }

        // MV-TODO
        return std::make_tuple( -1, actual_strategy );
    }

    DCHECK_GE( plan->destination_, 0 );

    DCHECK( plan->mq_plan_ );
    plan->mq_plan_->set_plan_type( multi_query_plan_type::EXECUTING );

    int ret_dest = execute_one_shot_sproc_at_site(
        id, plan->destination_, _return, transaction_begin_svv,
        plan->write_pids_, plan->read_pids_, plan->inflight_pids_, name,
        ckr_write_set, ckr_read_set, sproc_args, actual_strategy );

#if 0
    wait_for_downgrade_to_complete_and_release_partitions( id, plan,
                                                           actual_strategy );
#endif
    plan->mq_plan_->set_plan_type( multi_query_plan_type::CACHED );
    DCHECK( plan->stat_decision_holder_ );
    stat_enumerator_holder_->add_decisions( plan->stat_decision_holder_  );

    auto prediction_holder =
        cost_model2_->predict_single_site_transaction_execution_time(
            periodic_site_selector_operations_.get_site_load(
                plan->destination_ ),
            plan->txn_stats_ );

    prediction_holder.add_real_timers( _return.timers );
    cost_model2_->add_results( prediction_holder );

    DVLOG( 40 ) << "One shot sproc:" << id
                << ", return code:" << _return.status;

    increase_tracking_counts( plan );
    // MTODO-STRATEGIES decide if we want to sample this
    data_loc_tab_->get_stats()->add_transaction_accesses(ckr_write_set, ckr_read_set);
    sample_transaction_accesses( id, plan, ckr_write_set, ckr_read_set,
                                 time_of_txn_exec );

#if 0

    if( _return.status == exec_status_type::COMMAND_OK ) {
        start_timer( UPDATE_SVV_FROM_RESULT_SET_TIMER_ID );
        // Use result of this sproc to update what we know about site version
        // vectors
        evaluator_->update_svv_from_result_set(
            destination, _return.session_version_vector );
        stop_timer( UPDATE_SVV_FROM_RESULT_SET_TIMER_ID );
    }
#endif
    if( strategy == plan_strategy::FORCE_CHANGE ) {
        stop_timer( SS_EXECUTE_ONE_SHOT_SPROC_WITH_FORCE_STRATEGY_TIMER_ID );
    }

    stop_timer( SS_EXECUTE_ONE_SHOT_SPROC_WITH_STRATEGY_TIMER_ID );
    return std::make_tuple( ret_dest, actual_strategy );
}

std::vector<tracking_summary>& transaction_preparer::generate_txn_sample(
    ::clientid id ) {
    if( id != -1 ) {
        // real id, only sample from one client
        // TODO: Throw for now, put in later
        DCHECK( false );
    }
    // sample from all clients
    // HACK: for ADAPT, all clients are equal, so just sample from *any* client
    std::unique_ptr<adaptive_reservoir_sampler>& sampler = samplers_.at( id );
    return sampler->get_reservoir_ref();
}

transaction_partition_accesses
    transaction_preparer::generate_txn_partition_accesses_from_plan(
        std::shared_ptr<pre_transaction_plan>& plan,
        const std::vector<::cell_key_ranges>   write_ckrs,
        const std::vector<::cell_key_ranges>   read_ckrs ) {
    return generate_txn_partition_accesses( plan->write_pids_, plan->read_pids_,
                                            plan->destination_, {}, write_ckrs,
                                            read_ckrs );
}
transaction_partition_accesses
    transaction_preparer::generate_txn_partition_accesses_from_plan(
        std::shared_ptr<multi_site_pre_transaction_plan>& plan,
        const std::vector<::cell_key_ranges>              write_ckrs,
        const std::vector<::cell_key_ranges>              read_ckrs ) {

    std::vector<partition_column_identifier>       read_pids;
    partition_column_identifier_map_t<int>         destinations;

    for( const auto& entry : plan->read_pids_ ) {
        for( const auto& pid : entry.second ) {
            if( destinations.count( pid ) == 0 ) {
                destinations[pid] = entry.first;
                read_pids.emplace_back( pid );
            }
        }
    }

    return generate_txn_partition_accesses( {} /* write_pids */, read_pids, -1,
                                            destinations, write_ckrs,
                                            read_ckrs );
}
transaction_partition_accesses
    transaction_preparer::generate_txn_partition_accesses(
        const std::vector<partition_column_identifier>& write_pids,
        const std::vector<partition_column_identifier>& read_pids,
        int                                             destination,
        const partition_column_identifier_map_t<int>&   destinations,
        const std::vector<::cell_key_ranges>            write_ckrs,
        const std::vector<::cell_key_ranges>            read_ckrs ) {
    std::vector<partition_access> partition_accesses;
    partition_accesses.reserve( write_pids.size() + read_pids.size() );

    for( const auto& partition_id : write_pids ) {
        partition_access access;
        access.site_ = destination;
        access.partition_id_ = partition_id;
        if( destination < 0 ) {
            access.site_ = destinations.at( partition_id );
        }
        access.is_write_ = true;
        partition_accesses.emplace_back( std::move( access ) );
    }

    for( const auto& partition_id : read_pids ) {
        partition_access access;
        access.site_ = destination;
        if( destination < 0 ) {
            access.site_ = destinations.at( partition_id );
        }
        access.partition_id_ = partition_id;
        access.is_write_ = false;
        partition_accesses.emplace_back( std::move( access ) );
    }

    transaction_partition_accesses txn_partition_accesses(
        std::move( partition_accesses ), write_ckrs, read_ckrs );
    return txn_partition_accesses;
}

void transaction_preparer::restore_state_from_files(
    const ::clientid id, const std::string& in_ss_name,
    const std::string& in_db_name, const std::string& in_part_name ) {
    // start a thread to call the db restore
    std::vector<std::thread> restore_threads;

    std::vector<std::vector<propagation_configuration>> prop_configs =
        periodic_site_selector_operations_
            .get_all_site_propagation_configurations();

    for( int site = 0; site < evaluator_->get_num_sites(); site++ ) {
        std::thread restore_thread = std::thread(
            [&in_db_name, &in_part_name, &prop_configs, site, id, this]() {
                std::string site_in_db_name =
                    in_db_name + "/" + std::to_string( site );
                const sm_client_ptr& client =
                    conn_pool_->getClient( site, id, 0 );
                persistence_result pr;
                client->rpc_restore_db_from_file( pr, id, site_in_db_name,
                                                  in_part_name, prop_configs );
                DCHECK_EQ( pr.status, exec_status_type::COMMAND_OK );
            } );
        restore_threads.push_back( std::move( restore_thread ) );
    }
    restore_site_selector( id, in_ss_name, prop_configs );
    join_threads( restore_threads );
}

void transaction_preparer::persist_state_to_files(
    const ::clientid id, const std::string& out_ss_name,
    const std::string& out_db_name, const std::string& out_part_name ) {
    // start a thread to call the db restore
    std::vector<std::thread> persist_threads;

    std::vector<std::vector<propagation_configuration>> prop_configs =
        periodic_site_selector_operations_
            .get_all_site_propagation_configurations();

    // evaluator_->wait_for_stable_update_state();
    for( int site = 0; site < evaluator_->get_num_sites(); site++ ) {
        std::thread restore_thread = std::thread(
            [&out_db_name, &out_part_name, &prop_configs, site, id, this]() {
                std::string site_out_db_name =
                    out_db_name + "/" + std::to_string( site );

                const sm_client_ptr& client =
                    conn_pool_->getClient( site, id, 0 );
                persistence_result pr;
                client->rpc_persist_db_to_file( pr, id, site_out_db_name,
                                                out_part_name, prop_configs );
                DCHECK_EQ( pr.status, exec_status_type::COMMAND_OK );
            } );
        persist_threads.push_back( std::move( restore_thread ) );
    }
    persist_site_selector( id, out_ss_name, prop_configs );
    join_threads( persist_threads );
}

void transaction_preparer::add_to_stats(
    const ::clientid id, const uint32_t site,
    const std::vector<cell_key_ranges>& ckrs,
    const std::vector<int32_t>& ckr_counts, bool is_write,
    std::vector<transaction_prediction_stats>& txn_stats ) const {

    cached_ss_stat_holder cached_stats;

    DCHECK_EQ( ckrs.size(), ckr_counts.size() );

    for( uint32_t i = 0; i < ckrs.size(); i++ ) {
        auto parts = data_loc_tab_->get_partition(
            ckrs.at( i ), partition_lock_mode::no_lock );
        for( const auto& part : parts ) {
            double contention = cost_model2_->normalize_contention_by_time(
                part->get_contention() );
            auto loc_info =
                part->get_location_information( partition_lock_mode::no_lock );

            auto shaped_ckr =
                shape_ckr_to_pcid( ckrs.at( i ), part->identifier_ );
            uint32_t op_counts =
                get_number_of_rows( shaped_ckr ) * ckr_counts.at( i );

            std::tuple<bool, partition_type::type> part_type_info =
                loc_info->get_partition_type( site );
            partition_type::type part_type = std::get<1>( part_type_info );
            std::tuple<bool, storage_tier_type::type> storage_type_info =
                loc_info->get_storage_type( site );
            storage_tier_type::type storage_type =
                std::get<1>( storage_type_info );

            transaction_prediction_stats txn_stat(
                part_type, storage_type, contention, 0 /* num updates needed */,
                data_loc_tab_->get_stats()->get_and_cache_cell_widths(
                    part->identifier_, cached_stats ),
                part->get_num_rows(), false /* is scan*/, 0, !is_write,
                op_counts, is_write, op_counts );
            txn_stats.emplace_back( txn_stat );
        }
    }
}

std::vector<transaction_prediction_stats>
    transaction_preparer::get_previous_requests_stats(
        const ::clientid id, const uint32_t site,
        const std::vector<cell_key_ranges>& write_ckrs,
        const std::vector<int32_t>&         write_ckr_counts,
        const std::vector<cell_key_ranges>& read_ckrs,
        const std::vector<int32_t>&         read_ckr_counts ) const {
    std::vector<transaction_prediction_stats> txn_stats;

    add_to_stats( id, site, write_ckrs, write_ckr_counts, true /*is write*/,
                  txn_stats );
    add_to_stats( id, site, read_ckrs, read_ckr_counts, false /*is write */,
                  txn_stats );

    return txn_stats;
}

void transaction_preparer::add_previous_context_timer_information(
    const ::clientid                          id,
    const previous_context_timer_information& previous_contexts ) {
    if( ( !previous_contexts.is_present ) or
        ( previous_contexts.num_requests == 0 ) ) {
        return;
    }
    start_timer( SS_ADD_PREVIOUS_CONTEXT_TIMER_INFORMATION_TIMER_ID );

    std::vector<context_timer> normalized_timers;
    for( unsigned int pos = 0; pos < previous_contexts.timers.size(); pos++ ) {
        normalized_timers.push_back( previous_contexts.timers.at( pos ) );
        normalized_timers.at( pos ).counter_seen_count =
            previous_contexts.timers.at( pos ).counter_seen_count /
            previous_contexts.num_requests;
    }

    std::vector<transaction_prediction_stats> txn_stats =
        get_previous_requests_stats(
            id, previous_contexts.site, previous_contexts.write_ckrs,
            previous_contexts.write_ckr_counts, previous_contexts.read_ckrs,
            previous_contexts.read_ckr_counts );

    auto prediction_holder =
        cost_model2_->predict_single_site_transaction_execution_time(
            periodic_site_selector_operations_.get_site_load(
                previous_contexts.site ),
            txn_stats );

    prediction_holder.add_real_timers( normalized_timers );
    cost_model2_->add_results( prediction_holder );

    stop_timer( SS_ADD_PREVIOUS_CONTEXT_TIMER_INFORMATION_TIMER_ID );
}

void transaction_preparer::persist_site_selector(
    const ::clientid id, const std::string& dir,
    const std::vector<std::vector<propagation_configuration>>& prop_configs ) {
    DVLOG( 7 ) << "Persisting site selector to directory:" << dir;

    site_selector_persistence_manager manager(
        dir, data_loc_tab_->get_partition_location_table(), prop_configs,
        construct_persistence_configs() );
    manager.persist();

    DVLOG( 7 ) << "Persisting site selector to directory:" << dir << " okay!";
}
void transaction_preparer::restore_site_selector(
    const ::clientid id, const std::string& dir,
    const std::vector<std::vector<propagation_configuration>>& prop_configs ) {
    DVLOG( 7 ) << "Restoring site selector from directory:" << dir;

    site_selector_persistence_manager manager(
        dir, data_loc_tab_->get_partition_location_table(), prop_configs,
        construct_persistence_configs() );
    manager.load();

    DVLOG( 7 ) << "Restoring site selector from directory:" << dir << " okay!";
}
abstract_site_evaluator* transaction_preparer::get_evaluator() {
    return evaluator_.get();
}
adaptive_reservoir_sampler* transaction_preparer::get_sampler(::clientid id ) {
    return samplers_.at( id ).get();
}

bool transaction_preparer::is_single_site_transaction_system() const {
    return mastering_type_configs_.is_single_site_transaction_system_;
}
