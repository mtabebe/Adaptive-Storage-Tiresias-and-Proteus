#include "periodic_site_selector_operations.h"

#include <chrono>
#include <glog/logging.h>
#include <unordered_map>

// the periodic_site_selector_operations has a number of threads, that
// periodically polls the site
// managers, for their statistics
//
// The site selector can get this state, as a low watermark, such as where to
// begin subscribing

periodic_site_selector_operations::periodic_site_selector_operations(
    std::shared_ptr<client_conn_pools> conn_pool,
    std::shared_ptr<cost_modeller2>    cost_model2,
    std::shared_ptr<sites_partition_version_information>
                                                   site_partition_version_info,
    std::shared_ptr<partition_data_location_table> data_loc_tab,
    site_selector_query_stats*                     stats,
    std::shared_ptr<partition_tier_tracking>       tier_tracking,
    std::shared_ptr<query_arrival_predictor>       query_predictor,
    std::vector<std::atomic<int64_t>>*             site_read_counts,
    std::vector<std::atomic<int64_t>>* site_write_counts, uint32_t num_sites,
    uint32_t                                         client_id,
    const periodic_site_selector_operations_configs& configs )
    : conn_pool_( conn_pool ),
      cost_model2_( cost_model2 ),
      site_partition_version_info_( site_partition_version_info ),
      data_loc_tab_( data_loc_tab ),
      stats_( stats ),
      tier_tracking_( tier_tracking ),
      query_predictor_( query_predictor ),
      site_read_counts_( site_read_counts ),
      site_write_counts_( site_write_counts ),
      old_read_counts_( site_read_counts_->size(), 0 ),
      old_write_counts_( site_write_counts_->size(), 0 ),
      num_sites_( num_sites ),
      client_id_( client_id ),
      configs_( configs ),
      hasher_(),
      site_propagation_configs_(),
      prop_offsets_(),
      site_mach_stats_(),
      site_selectivities_(),
      site_col_widths_(),
      timers_(),
      site_load_infos_( num_sites ),
      shutdown_( false ),
      polling_threads_(),
      wait_counter_( 0 ),
      guard_(),
      cv_() {

    DCHECK_EQ( num_sites, site_read_counts_->size() );
    DCHECK_EQ( num_sites, site_write_counts_->size() );

    DVLOG( 40 ) << "Constructing site poller";
}

periodic_site_selector_operations::~periodic_site_selector_operations() {
    stop_polling();
}

void periodic_site_selector_operations::start_polling() {
    build_initial_state();

    // divide up the sites per thread, in a relatively even way
    // do this by repeatedly dividing the remaining sites with the remaining
    // threads
    uint32_t num_remaining_threads = configs_.num_poller_threads_;
    uint32_t num_remaining_sites = num_sites_ + 1;
    uint32_t start_site = 0;
    uint32_t end_site = 0;

    uint32_t t_id = 0;

    DVLOG( 40 ) << "Starting periodic operations:" << configs_;

    while( num_remaining_threads > 0 ) {
        uint32_t sites_per_thread = num_remaining_sites / num_remaining_threads;
        end_site = start_site + sites_per_thread;
        end_site = std::min( end_site, num_sites_ );

        bool do_cost_model = false;
        if( end_site == num_sites_ ) {
            do_cost_model = true;
        }

        polling_threads_.push_back(
            std::thread( &periodic_site_selector_operations::run_poller, this,
                         t_id, start_site, end_site - 1, do_cost_model ) );

        start_site = end_site;
        num_remaining_sites = num_remaining_sites - sites_per_thread;
        num_remaining_threads = num_remaining_threads - 1;
        t_id += 1;
    }
    DCHECK_EQ( end_site, num_sites_ );
}

void periodic_site_selector_operations::run_poller( uint32_t t_id,
                                                    uint32_t start_site,
                                                    uint32_t end_site,
                                                    bool     do_cost_model ) {
    // run from start to end (inclusive)
    DVLOG( k_periodic_cost_model_log_level )
        << "Running poller:" << t_id << ", site:" << start_site << " to "
        << end_site << ", do_cost_model:" << do_cost_model;
    uint32_t num_sites = ( 1 + end_site - start_site );
    if( do_cost_model ) {
        num_sites += 1;
    }
    std::chrono::duration<double, std::milli> base_sleep_time(
        configs_.update_stats_sleep_delay_ms_ / num_sites );
    DVLOG( 40 ) << "Base sleep time:" << base_sleep_time.count();
    for( ;; ) {
        if( shutdown_ ) {
            break;
        }

        for( uint32_t site = start_site; site <= end_site; site++ ) {
            std::chrono::high_resolution_clock::time_point start_time =
                std::chrono::high_resolution_clock::now();

            poll_site( t_id, site );

            std::chrono::high_resolution_clock::time_point end_time =
                std::chrono::high_resolution_clock::now();
            std::chrono::duration<double, std::milli> elapsed =
                end_time - start_time;

            if( elapsed < base_sleep_time ) {
                std::chrono::duration<double, std::milli> sleep_time =
                    base_sleep_time - elapsed;
                std::this_thread::sleep_for( sleep_time );
            }
        }

        wait_for_all_pollers( do_cost_model );

        if( do_cost_model ) {
            update_table_stats();

            query_predictor_->train_model();
            cost_model2_->update_model();
            tier_tracking_->update_statistics();

            std::this_thread::sleep_for( base_sleep_time );

            release_pollers( do_cost_model );
        }
    }
    DVLOG( 40 ) << "Done running poller";
}

void periodic_site_selector_operations::update_table_stats() {
    if( !stats_ ) {
        return;
    }

    DCHECK_EQ( site_selectivities_.size(), site_col_widths_.size() );
    DCHECK_GT( site_selectivities_.size(), 0 );
    uint32_t num_sites = site_selectivities_.size();

    uint32_t num_tables = site_selectivities_.at( 0 ).size();
    DCHECK_EQ( num_tables, site_col_widths_.at( 0 ).size() );

    for( uint32_t table_id = 0; table_id < num_tables; table_id++ ) {
        uint32_t num_cols = site_selectivities_.at( 0 ).at( table_id ).size();
        DCHECK_EQ( num_cols, site_col_widths_.at( 0 ).at( table_id ).size() );
        for ( uint32_t col_id = 0; col_id < num_cols; col_id++) {
            double sel_sum = 0;
            double sel_count = 0;

            double width_sum = 0;
            double width_count = 0;

            for( uint32_t site_id = 0; site_id < num_sites; site_id++ ) {
                DCHECK_LT( table_id, site_selectivities_.at( site_id ).size() );
                DCHECK_LT( table_id, site_col_widths_.at( site_id ).size() );

                DCHECK_LT(
                    col_id,
                    site_selectivities_.at( site_id ).at( table_id ).size() );
                DCHECK_LT(
                    col_id,
                    site_col_widths_.at( site_id ).at( table_id ).size() );

                double local_avg_width = site_col_widths_.at( site_id )
                                             .at( table_id )
                                             .at( col_id )
                                             .avg_width;
                double local_width_count = site_col_widths_.at( site_id )
                                               .at( table_id )
                                               .at( col_id )
                                               .num_observations;

                double local_avg_sel = site_selectivities_.at( site_id )
                                           .at( table_id )
                                           .at( col_id )
                                           .avg_selectivity;
                double local_sel_count = site_selectivities_.at( site_id )
                                             .at( table_id )
                                             .at( col_id )
                                             .num_observations;
                width_sum += ( local_avg_width * local_width_count );
                sel_sum += ( local_avg_sel * local_sel_count );

                width_count += local_width_count;
                sel_count += local_sel_count;

            }  // site

            double avg_sel = 1.0;
            double avg_width = 0.0;

            if( sel_count != 0 ) {
                avg_sel = sel_sum / sel_count;
            }
            if( width_count != 0 ) {
                avg_width = width_sum / width_count;
            }

            DVLOG( 5 ) << "Set stats for table_id:" << table_id
                       << ", col_id:" << col_id << ", selectivity:" << avg_sel
                       << ", width:" << avg_width;

            stats_->set_column_selectivity( table_id, col_id, avg_sel );
            stats_->set_cell_width( table_id, col_id, avg_width );
        } // col
    } // table

    auto storage_stats = data_loc_tab_->get_storage_stats();
    if( storage_stats ) {
        storage_stats->update_table_widths_from_stats( *stats_ );
    }
}

void periodic_site_selector_operations::wait_for_all_pollers(
    bool do_cost_model ) {
    // we increment this counter every num threads + 1 time every iteration
    uint64_t num_counts_per_iter = configs_.num_poller_threads_ + 1;
    uint64_t wait_limit = num_counts_per_iter;

    if( do_cost_model ) {
        wait_limit = wait_limit - 1;
    }

    DVLOG( 40 ) << "Wait for all pollers:" << do_cost_model
                << " , wait limit:" << wait_limit;

    {
        std::unique_lock<std::mutex> lock( guard_ );
        // get the counter before we increment, and see what the base would be
        uint64_t wait_count_base =
            ( wait_counter_ / num_counts_per_iter ) * num_counts_per_iter;

        wait_counter_ += 1;
        cv_.notify_all();

        DVLOG( 40 ) << "Wait counter:" << wait_counter_
                    << ", wait count base:" << wait_count_base;

        while( wait_counter_ < wait_count_base + wait_limit ) {
            cv_.wait( lock );
        }
    }
}

void periodic_site_selector_operations::release_pollers( bool do_cost_model ) {
    DCHECK( do_cost_model );
    {
        std::unique_lock<std::mutex> lock( guard_ );
        wait_counter_ += 1;
        DVLOG( 40 ) << "Release poller, Wait counter:" << wait_counter_;
    }
    cv_.notify_all();
}

void periodic_site_selector_operations::make_load_prediction(
    uint32_t t_id, uint32_t site_id,
    const std::vector<context_timer>& timers ) {
    // build the new_timers

    std::vector<double> load_inputs = {0, 0, 0};
    std::vector<double> normalized_load =
        cost_model2_->get_load_normalization();

    // stats are in us
    uint64_t time_pass_in_ms =
        site_mach_stats_.at( site_id ).average_interval / 1000;
    if( time_pass_in_ms == 0 ) {
        time_pass_in_ms = 1;
    }

    int64_t read_count = site_read_counts_->at( site_id );
    int64_t write_count = site_write_counts_->at( site_id );

    int64_t old_read_count = old_read_counts_.at( site_id );
    int64_t old_write_count = old_write_counts_.at( site_id );

    int64_t read_change = read_count - old_read_count;
    int64_t write_change = write_count - old_write_count;

#if 0
    int64_t read_change = read_count ;
    int64_t write_change = write_count;
#endif

    int64_t site_count_change = read_change + write_change;
    int64_t observed_change = 0;

    for( context_timer timer : timers ) {
        // change
        context_timer old_timer = timers_.at( site_id )[timer.counter_id];
        int64_t       op_count_change =
            timer.counter_seen_count - old_timer.counter_seen_count;
        timers_.at( site_id )[timer.counter_id] = timer;

        int32_t load_timer_pos =
            transform_timer_id_to_load_input_position( timer.counter_id );

        if( load_timer_pos >= 0 ) {
            DVLOG( 10 ) << "Make load prediction:" << site_id
                        << ", timer_id:" << timer.counter_id
                        << ", op_count_change:" << op_count_change
                        << ", time_pass_in_ms:" << time_pass_in_ms
                        << ", read_change:" << read_change
                        << ", write_change:" << write_change;

            load_inputs.at( load_timer_pos ) =
                (double) op_count_change / (double) time_pass_in_ms;
            load_inputs.at( load_timer_pos ) =
                load_inputs.at( load_timer_pos ) /
                normalized_load.at( load_timer_pos );

            if( ( timer.counter_id ==
                  PARTITION_BEGIN_WRITE_ACQUIRE_LOCKS_TIMER_ID ) or
                ( timer.counter_id ==
                  PARTITION_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID ) ) {
                observed_change += op_count_change;
            }
        }
    }

    double normalized_counts =
        cost_model2_->normalize_contention_by_time( read_count + write_count );

    DVLOG( 10 ) << "Site change:" << site_id
                << ", observed change:" << observed_change
                << ", site_count_change:" << site_count_change
                << ", total counts:" << read_count + write_count
                << ", normalized counts:" << normalized_counts
                << ", time_pass_in_ms:" << time_pass_in_ms;

    cost_model_prediction_holder pred_holder =
        cost_model2_->predict_site_operation_count( normalized_counts );
    DCHECK_EQ( 1, pred_holder.component_predictions_.size() );
    auto pred_res = pred_holder.component_predictions_.at( 0 );
    pred_res.add_actual_output(
        double( observed_change / (double) time_pass_in_ms ) );
    cost_model2_->add_result( cost_model_component_type::SITE_OPERATION_COUNT,
                              pred_res );

    old_read_counts_.at( site_id ) = read_count;
    old_write_counts_.at( site_id ) = write_count;

    // now make the prediction
    site_load_information load_info(
        load_inputs.at( 0 ) * normalized_load.at( 0 ),
        load_inputs.at( 1 ) * normalized_load.at( 1 ),
        load_inputs.at( 2 ) * normalized_load.at( 2 ),
        site_mach_stats_.at( site_id ).average_cpu_load );
    DVLOG( k_periodic_cost_model_log_level )
        << "Real Site load information:" << site_id << ", " << load_info;

    site_load_infos_.at( site_id ) = load_info;
}

void periodic_site_selector_operations::build_initial_state() {

    // poll each site once to get the subscriptions
    uint32_t num_configs = 0;
    for( uint32_t site = 0; site < num_sites_; site++ ) {
        const sm_client_ptr& client =
            conn_pool_->getClient( site, client_id_, 0 );

        site_statistics_results result;
        client->rpc_get_site_statistics( result );
        DCHECK_EQ( result.status, exec_status_type::COMMAND_OK );

        site_propagation_configs_.push_back( result.prop_configs );
        site_mach_stats_.push_back( result.mach_stats );

        site_selectivities_.push_back( result.selectivities );
        site_col_widths_.push_back( result.col_widths );

        site_partition_version_info_->set_version_of_partitions(
            site, result.partition_col_versions );

        num_configs += result.prop_configs.size();

        std::unordered_map<int, context_timer> timer_map;
        for( const auto timer : result.timers ) {
            timer_map[timer.counter_id] = timer;
        }
        timers_.push_back( timer_map );
    }
    prop_offsets_ = std::vector<std::atomic<int64_t>>( num_configs );
    site_partition_version_info_->init_topics( num_configs );

    update_table_stats();

    uint32_t renamed_offset = 0;
    // store the offset as an atomic separately
    for( uint32_t site = 0; site < num_sites_; site++ ) {
        for( uint32_t config_pos = 0;
             config_pos < site_propagation_configs_.at( site ).size();
             config_pos++ ) {
            prop_offsets_.at( renamed_offset ) =
                site_propagation_configs_.at( site ).at( config_pos ).offset;
            site_propagation_configs_.at( site ).at( config_pos ).offset =
                renamed_offset;
            site_partition_version_info_->set_site_topic_translation(
                site, config_pos, renamed_offset );
            site_partition_version_info_->set_topic_update_count(
                renamed_offset, 0 );

            renamed_offset += 1;
        }
    }
}

bool periodic_site_selector_operations::check_configs_match(
    const propagation_configuration& a, const propagation_configuration& b ) {
    if( a.type != b.type ) {
        return false;
    }
    if( a.type != propagation_type::NO_OP ) {
        if( a.partition != b.partition ) {
            return false;
        }
        if( a.type == propagation_type::KAFKA ) {
            if( a.topic.compare( b.topic ) != 0 ) {
                return false;
            }
        }
    }
    return true;
}

void periodic_site_selector_operations::poll_site( uint32_t t_id,
                                                   uint32_t site ) {
    DVLOG( 20 ) << "Polling site:" << site;

    const sm_client_ptr& client =
        conn_pool_->getClient( site, client_id_, t_id );

    site_statistics_results result;
    client->rpc_get_site_statistics( result );

    site_mach_stats_.at( site ) = result.mach_stats;

    site_selectivities_.at( site ) = result.selectivities;
    site_col_widths_.at( site ) = result.col_widths;

    make_load_prediction( t_id, site, result.timers );

    site_partition_version_info_->set_version_of_partitions(
        site, result.partition_col_versions );

    DCHECK_EQ( result.prop_configs.size(),
               site_propagation_configs_.at( site ).size() );
    DCHECK_EQ( result.prop_counts.size(), result.prop_configs.size() );

    for( uint32_t pos = 0; pos < result.prop_configs.size(); pos++ ) {
        const auto& prop_config = result.prop_configs.at( pos );
        const auto& existing_config =
            site_propagation_configs_.at( site ).at( pos );

        uint32_t offset_pos = existing_config.offset;

        DCHECK( check_configs_match( existing_config, prop_config ) );

        prop_offsets_.at( offset_pos ) = prop_config.offset;
        site_partition_version_info_->set_topic_update_count(
            offset_pos, result.prop_counts.at( pos ) );

        DVLOG( 10 ) << "Polled site:" << site << ", topic:" << offset_pos
                    << ", kafka offset:" << prop_config.offset
                    << ", update count:" << result.prop_counts.at( pos );
    }

    update_storage_tier_state( result.storage_changes );
}

void periodic_site_selector_operations::update_storage_tier_state(
    const std::vector<storage_tier_change>& storage_changes ) {
    auto storage_stats = data_loc_tab_->get_storage_stats();
    for( const auto& change : storage_changes ) {
        auto part = data_loc_tab_->get_partition(
            change.pid, partition_lock_mode::no_lock );
        if( !part ) {
            continue;
        }
        auto loc_info =
            part->get_location_information( partition_lock_mode::lock );
        if( !loc_info ) {
            continue;
        }
        auto new_loc_info = std::make_shared<partition_location_information>();
        new_loc_info
            ->set_from_existing_location_information_and_increment_version(
                loc_info );

        auto storage_loc = new_loc_info->get_storage_type( change.site );

        if( std::get<0>( storage_loc ) and
            ( std::get<1>( storage_loc ) != change.change ) ) {
            new_loc_info->storage_types_[change.site] = change.change;

            part->compare_and_swap_location_information( new_loc_info,
                                                         loc_info );

            storage_stats->change_partition_tier( change.site,
                                                  std::get<1>( storage_loc ),
                                                  change.change, change.pid );
            tier_tracking_->change_storage_tier(
                part, change.site, std::get<1>( storage_loc ), change.change );
        }
    }
}

void periodic_site_selector_operations::stop_polling() {
    shutdown_ = true;
    join_threads( polling_threads_ );
    polling_threads_.clear();
}

propagation_configuration periodic_site_selector_operations::
    get_site_propagation_configuration_by_position( uint32_t site_id,
                                                    uint32_t pos ) const {
    // get the config, by filling in the offset from the atomics
    propagation_configuration p =
        site_propagation_configs_.at( site_id ).at( pos );
    p.offset = prop_offsets_.at( p.offset );
    return p;
}

std::vector<std::vector<propagation_configuration>>
    periodic_site_selector_operations::get_all_site_propagation_configurations()
        const {
    std::vector<std::vector<propagation_configuration>> site_prop_configs =
        site_propagation_configs_;
    for( uint32_t site = 0; site < site_prop_configs.size(); site++ ) {
        for( uint32_t pos = 0; pos < site_prop_configs.at( site ).size();
             pos++ ) {
            auto& p = site_prop_configs.at( site ).at( pos );
            p.offset = prop_offsets_.at( p.offset );
        }
    }
    return site_prop_configs;
}

propagation_configuration periodic_site_selector_operations::
    get_site_propagation_configuration_by_hashing_partition_column_identifier(
        uint32_t site_id, const partition_column_identifier& pid ) const {
    uint32_t pos =
        hasher_( pid ) % site_propagation_configs_.at( site_id ).size();
    return get_site_propagation_configuration_by_position( site_id, pos );
}

std::vector<propagation_configuration>
    periodic_site_selector_operations::get_site_propagation_configurations(
        uint32_t                                        site_id,
        const std::vector<partition_column_identifier>& pids ) const {
    std::unordered_map<uint32_t, propagation_configuration> seen_results;
    std::vector<propagation_configuration> found_configs;
    found_configs.reserve( pids.size() );

    for( const auto& pid : pids ) {
        uint32_t pos =
            hasher_( pid ) % site_propagation_configs_.at( site_id ).size();
        auto found = seen_results.find( pos );
        if( found != seen_results.end() ) {
            found_configs.push_back( found->second );
        } else {
            auto config =
                get_site_propagation_configuration_by_position( site_id, pos );
            seen_results.emplace( pos, config );
            found_configs.push_back( config );
        }
        DVLOG( 10 ) << "Site propagation configs:" << pid << ", "
                    << found_configs.at( found_configs.size() - 1 );
    }

    return found_configs;
}

std::vector<propagation_configuration>
    periodic_site_selector_operations::get_site_propagation_configurations(
        uint32_t site_id, const std::vector<uint32_t>& positions ) const {
    std::unordered_map<uint32_t, propagation_configuration> seen_results;
    std::vector<propagation_configuration> found_configs;
    found_configs.reserve( positions.size() );

    for( uint32_t pos : positions ) {
        auto found = seen_results.find( pos );
        if( found != seen_results.end() ) {
            found_configs.push_back( found->second );
        } else {
            auto config =
                get_site_propagation_configuration_by_position( site_id, pos );
            seen_results.emplace( pos, config );
            found_configs.push_back( config );
        }
        DVLOG( 10 ) << "Site propagation configs:" << pos << ", "
                    << found_configs.at( found_configs.size() - 1 );
    }

    return found_configs;
}

std::vector<site_load_information>
    periodic_site_selector_operations::get_site_load_information() const {
    return site_load_infos_;
}

std::vector<double> periodic_site_selector_operations::get_site_loads() const {
    std::vector<double> ret;
    for( const auto& site_load : site_load_infos_ ) {
        ret.emplace_back( site_load.cpu_load_ );
    }
    return ret;
}

double periodic_site_selector_operations::get_site_load(
    uint32_t site_id ) const {
    return site_load_infos_.at( site_id ).cpu_load_;
}

int32_t periodic_site_selector_operations::get_num_sites() const {
    return conn_pool_->get_size();
}

