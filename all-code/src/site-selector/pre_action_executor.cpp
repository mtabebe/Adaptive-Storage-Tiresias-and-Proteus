#include "pre_action_executor.h"

#include <glog/logging.h>

#include "client_exec.h"
#include "../data-site/db/cell_identifier.h"

pre_action_executor::pre_action_executor(
    std::shared_ptr<client_conn_pools>             conn_pool,
    std::shared_ptr<partition_data_location_table> data_loc_tab,
    std::shared_ptr<cost_modeller2>                cost_model2,
    periodic_site_selector_operations* periodic_site_selector_operations,
    std::shared_ptr<sites_partition_version_information>
                                             site_partition_version_info,
    std::shared_ptr<partition_tier_tracking> tier_tracking,
    const transaction_preparer_configs&      configs )
    : conn_pool_( conn_pool ),
      data_loc_tab_( data_loc_tab ),
      storage_stats_( data_loc_tab_->get_storage_stats() ),
      cost_model2_( cost_model2 ),
      periodic_site_selector_operations_( periodic_site_selector_operations ),
      site_partition_version_info_( site_partition_version_info ),
      tier_tracking_( tier_tracking ),
      configs_( configs ),
      background_actions_(),
      background_lock_(),
      background_cv_(),
      shutdown_( false ),
      background_worker_threads_(),
      pid_sorter_() {}

pre_action_executor::~pre_action_executor() {
    stop_background_executor_threads();
}

void pre_action_executor::add_background_work_items(
    const std::vector<std::shared_ptr<pre_action>>& actions ) {
    if( ( actions.size() == 0 ) or !( configs_.run_background_workers_ ) ) {
        return;
    }
    {
        std::unique_lock<std::mutex> guard( background_lock_ );
        for( const auto& action : actions ) {
            background_actions_.push( action );
        }
    }
    background_cv_.notify_all();
}

void pre_action_executor::start_background_executor_threads() {
    if( !configs_.run_background_workers_ ) {
        return;
    }

    if( background_worker_threads_.size() > 0 ) {
        return;
    }
    for( uint32_t tid = 0; tid < configs_.num_worker_threads_per_client_;
         tid++ ) {
        background_worker_threads_.push_back( std::thread(
            &pre_action_executor::run_background_executor, this, tid ) );
    }
}

void pre_action_executor::stop_background_executor_threads() {
    {
        std::unique_lock<std::mutex> guard( background_lock_ );
        shutdown_ = true;
    }
    background_cv_.notify_all();
    join_threads( background_worker_threads_ );
    background_worker_threads_.clear();
}

void pre_action_executor::run_background_executor( uint32_t tid ) {
    DVLOG( 7 ) << "Running background executor:" << tid;
    if( ( configs_.ss_mastering_type_ == ss_mastering_type::E_STORE ) or
        ( configs_.ss_mastering_type_ == ss_mastering_type::CLAY ) or
        ( configs_.ss_mastering_type_ == ss_mastering_type::ADR ) ) {
#if 0 // MTODO-ESTORE
        execute_background_adr_or_estore_or_clay_worker( tid );
#endif
    } else {
        execute_background_work_items( tid );
    }
    DVLOG( 7 ) << "Done running background executor:" << tid;
}

void pre_action_executor::execute_background_work_items( uint32_t tid ) {
    std::vector<std::shared_ptr<pre_action>> actions;
    actions.reserve( configs_.num_background_work_items_to_execute_ );
    for( ;; ) {

        if( shutdown_ ) {
            break;
        }

        {
            // get the lock
            std::unique_lock<std::mutex> guard( background_lock_ );
            if( shutdown_ ) {
                break;
            }
            if( background_actions_.size() == 0 ) {
              // we need to wait
                background_cv_.wait( guard );
            } else {
                // get a set of actions
                while( ( actions.size() <
                         configs_.num_background_work_items_to_execute_ ) and
                       ( background_actions_.size() > 0 ) ) {
                    auto action = background_actions_.top();
                    actions.push_back( action );
                    background_actions_.pop();
                }
            }
        }

        if( actions.size() > 0 ) {
            // do the actions
            execute_background_actions( tid, actions );
            actions.clear();
        }
    }
}

#if 0 // MTODO-ESTORE
void pre_action_executor::execute_background_adr_or_estore_or_clay_worker(
    uint32_t tid ) {
    std::chrono::milliseconds sleep_time(
        configs_.estore_clay_periodic_interval_ms_ );
    for( ;; ) {

        if( shutdown_ ) {
            break;
        }

        {
            // get the lock
            std::unique_lock<std::mutex> guard( background_lock_ );
            if( shutdown_ ) {
                break;
            }

            execute_adr_estore_clay_work( tid );

            background_cv_.wait_for( guard, sleep_time );
        }
    }
}

void pre_action_executor::execute_adr_estore_clay_work( uint32_t tid ) {
    DVLOG( 7 ) << "Executing adr/estore/clay work:" << tid;

    auto planner = std::make_shared<adr_estore_clay_plan_evaluator>(
        data_loc_tab_, configs_.ss_mastering_type_,
        configs_.estore_load_epsilon_threshold_,
        configs_.estore_hot_partition_pct_thresholds_,
        configs_.adr_multiplier_ );

    auto plan = planner->build_rebalance_plan();
    plan->sort_and_finalize_pids();
    plan->build_work_queue();

    uint32_t translated_id = conn_pool_->translate_client_id(
        configs_.background_executor_cid_, tid );
    std::vector<site_load_information> site_loads =
        periodic_site_selector_operations_->get_site_load_information();

    std::shared_ptr<pre_action> action = plan->get_next_pre_action_work_item();
    while( action ) {
        background_execute_pre_transaction_plan_action(
            configs_.background_executor_cid_, tid, translated_id, site_loads,
            action, partition_lock_mode::lock );
        action = plan->get_next_pre_action_work_item();
    }

    DVLOG( 7 ) << "Done executing adr/estore/clay work:" << tid;
}
#endif


void pre_action_executor::execute_background_actions(
    uint32_t tid, std::vector<std::shared_ptr<pre_action>>& actions ) {

    uint32_t translated_id = conn_pool_->translate_client_id(
        configs_.background_executor_cid_, tid );
    std::vector<site_load_information> site_loads =
        periodic_site_selector_operations_->get_site_load_information();

    for( auto action : actions ) {
        background_execute_pre_transaction_plan_action(
            configs_.background_executor_cid_, tid, translated_id, site_loads,
            action, partition_lock_mode::try_lock );
    }
}

action_outcome
    pre_action_executor::background_execute_pre_transaction_plan_action(
        const ::clientid id, const int32_t tid, const ::clientid translated_id,
        const std::vector<site_load_information>& site_loads,
        std::shared_ptr<pre_action>               action,
        const partition_lock_mode&                lock_mode ) {
    DCHECK( action );

    DVLOG( 20 ) << "Beginning executing background action:" << id
                << ", tid:" << tid << ", translated_id:" << translated_id
                << ", action id:" << action->id_ << ", type:" << action->type_
                << ", cost:" << action->cost_;

    action_outcome outcome = action_outcome::FAILED;


    switch( action->type_ ) {
        case NONE: {
            break;
        }
        case REMASTER: {
            outcome = background_execute_remaster_partitions_action(
                action, id, tid, translated_id, site_loads, lock_mode );
            break;
        }
        case ADD_PARTITIONS: {
            // HDB-BACKGROUND-NONE
            break;
        }
        case ADD_REPLICA_PARTITIONS: {
            outcome = background_execute_add_replica_partitions_action(
                action, id, tid, translated_id, site_loads, lock_mode );
            break;
        }
        case REMOVE_PARTITIONS: {
            outcome = background_execute_remove_partitions_action(
                action, id, tid, translated_id, site_loads, lock_mode );
            break;
        }
        case SPLIT_PARTITION: {
            outcome = background_execute_split_partition_action(
                action, id, tid, translated_id, site_loads, lock_mode );
            break;
        }
        case MERGE_PARTITION: {
            outcome = background_execute_merge_partition_action(
                action, id, tid, translated_id, site_loads, lock_mode );
            break;
        }
        case CHANGE_OUTPUT_DESTINATION: {
            outcome = background_execute_change_partition_output_destination(
                action, id, tid, translated_id, site_loads, lock_mode );
            break;
        }
        case CHANGE_PARTITION_TYPE: {
            outcome = background_execute_change_partition_type(
                action, id, tid, translated_id, site_loads, lock_mode );
            break;
        }
        case TRANSFER_REMASTER: {
            outcome = background_execute_transfer_remaster(
                action, id, tid, translated_id, site_loads, lock_mode );
            break;
        }
    }

    DVLOG( 20 ) << "Done executing background action:" << id << ", tid:" << tid
                << ", translated_id:" << translated_id
                << ", type:" << action->type_ << ", outcome:" << outcome;

    return outcome;
}
action_outcome pre_action_executor::background_execute_remove_partitions_action(
    std::shared_ptr<pre_action> action, const ::clientid id, const int32_t tid,
    const ::clientid                          translated_id,
    const std::vector<site_load_information>& site_loads,
    const partition_lock_mode&                lock_mode ) {

    DCHECK( action );
    DCHECK_EQ( pre_action_type::REMOVE_PARTITIONS, action->type_ );

    remove_partition_action* remove_action =
        (remove_partition_action*) action->args_;

    int site = action->site_;

    std::vector<std::shared_ptr<partition_payload>> found_payloads;
    try_to_get_payloads( remove_action->partition_ids_, found_payloads,
                         lock_mode );

    std::vector<std::shared_ptr<partition_payload>> remove_payloads;
    for( auto& payload : found_payloads ) {
        auto loc_info = payload->get_location_information();
        DCHECK( loc_info );
        bool do_remove = false;
        if( loc_info->replica_locations_.count( site ) == 1 ) {
            do_remove = true;
            DVLOG( 40 ) << "Will remove partition:" << payload->identifier_
                        << ", replica at site:" << site;
        }

        if( do_remove ) {
            remove_payloads.push_back( payload );
        } else {
            DVLOG( 40 ) << "Not removing partition:" << payload->identifier_
                        << ", site:" << site << ", is not a replica anymore";
            payload->unlock( lock_mode );
        }
    }

    if( remove_payloads.size() == 0 ) {
        DVLOG( 10 ) << "No partitions to remove, failing action";
        // nothing to do
        return action_outcome::FAILED;
    }

    cost_model_prediction_holder model_holder;

    std::vector<std::shared_ptr<pre_action>> deps;
    std::shared_ptr<pre_action> new_action = create_remove_partition_action(
        site, remove_payloads, deps, 0 /*cost*/, model_holder );

    snapshot_vector svv;
    auto            ret = internal_execute_remove_partitions_action(
        new_action, id, tid, translated_id, svv, remove_payloads );

    unlock_payloads( remove_payloads, lock_mode );

    add_result_timers( new_action, std::get<1>( ret ) );

    return std::get<0>( ret );
}

action_outcome
    pre_action_executor::background_execute_remaster_partitions_action(
        std::shared_ptr<pre_action> action, const ::clientid id,
        const int32_t tid, const ::clientid translated_id,
        const std::vector<site_load_information>& site_loads,
        const partition_lock_mode&                lock_mode ) {

    DCHECK( action );
    DCHECK_EQ( pre_action_type::REMASTER, action->type_ );

    remaster_partition_pre_action* remaster_action =
        (remaster_partition_pre_action*) action->args_;

    int old_master = remaster_action->old_master_;
    int new_master = remaster_action->new_master_;

    if( ( old_master == K_DATA_AT_ALL_SITES ) or
        ( new_master == K_DATA_AT_ALL_SITES ) ) {
        DVLOG( 10 ) << "Remaster, old_master or new_master destination is "
                       "K_DATA_AT_ALL_SITES";
        return action_outcome::FAILED;

    }

    std::vector<std::shared_ptr<partition_payload>> parts_to_remaster;
    try_to_get_payloads( remaster_action->partition_ids_, parts_to_remaster,
                         lock_mode );

    if( parts_to_remaster.size() != remaster_action->partition_ids_.size() ) {
        unlock_payloads( parts_to_remaster, lock_mode );
        DVLOG( 10 ) << "Not enough partitions to remaster, so failing action. "
                       "Number of desired partitions:"
                    << remaster_action->partition_ids_.size()
                    << ", found partitions:" << parts_to_remaster.size();
        return action_outcome::FAILED;
    }

    snapshot_vector remaster_session;
    std::vector<std::shared_ptr<partition_location_information>>
                          parts_to_remaster_infos;
    std::vector<uint32_t> update_destination_slots;

    std::vector<remaster_stats> remasters;

    bool do_remaster = true;
    for( auto& payload : parts_to_remaster ) {
        auto loc_info = payload->get_location_information();
        DCHECK( loc_info );
        if( ( loc_info->master_location_ == (uint32_t) old_master ) and
            ( loc_info->replica_locations_.count( new_master ) == 1 ) ) {
            DVLOG( 40 ) << "Able to background remaster partition:"
                        << payload->identifier_ << ", from site:" << old_master
                        << ", to new master:" << new_master;

            parts_to_remaster_infos.push_back( loc_info );
            update_destination_slots.push_back(
                // MTODO-BACKGROUND decide if want to change this?
                loc_info->update_destination_slot_ );

            uint64_t master_version =
                site_partition_version_info_->get_version_of_partition(
                    old_master, payload->identifier_ );
            if( master_version == K_NOT_COMMITTED ) {
                master_version = 0;
            }

            // wait for the current master's version
            set_snapshot_version( remaster_session, payload->identifier_,
                                  master_version );

            std::vector<std::shared_ptr<partition_location_information>>
                                                           remaster_infos = {loc_info};
            std::vector<std::shared_ptr<partition_payload>> to_remaster = {
                payload};

            double num_updates_need =
                site_partition_version_info_
                    ->estimate_number_of_updates_need_to_wait_for(
                        to_remaster, remaster_infos, remaster_session,
                        new_master );

            if( num_updates_need == 0 ) {
                num_updates_need =
                    cost_model2_
                        ->get_default_remaster_num_updates_required_count();
            }

            remasters.emplace_back( remaster_stats(
                std::get<1>( loc_info->get_partition_type(
                    loc_info->master_location_ ) ),
                std::get<1>( loc_info->get_partition_type( new_master ) ),
                num_updates_need, cost_model2_->normalize_contention_by_time(
                                      payload->get_contention() ) ) );

        } else {
            DVLOG( 40 ) << "Unable to background remaster partition:"
                        << payload->identifier_ << ", from site:" << old_master
                        << ", to new master:" << new_master;

            do_remaster = false;
            break;
        }
    }

    if( !do_remaster ) {
        unlock_payloads( parts_to_remaster, lock_mode );
        DVLOG( 10 ) << "Unable to background remaster partitions";

        return action_outcome::FAILED;
    }

    cost_model_prediction_holder model_holder =
        cost_model2_->predict_remaster_execution_time(
            site_loads.at( old_master ).cpu_load_,
            site_loads.at( new_master ).cpu_load_, remasters );

    std::shared_ptr<pre_action> new_action =
        create_remaster_partition_pre_action(
            old_master, new_master, parts_to_remaster, 0 /* cost*/,
            model_holder, update_destination_slots );

    snapshot_vector svv;
    auto            ret = internal_execute_remaster_partitions_action(
        new_action, id, tid, translated_id, svv, parts_to_remaster );

    unlock_payloads( parts_to_remaster, lock_mode );

    add_result_timers( new_action, std::get<1>( ret ) );

    return std::get<0>( ret );
}

action_outcome
    pre_action_executor::background_execute_add_replica_partitions_action(
        std::shared_ptr<pre_action> action, const ::clientid id,
        const int32_t tid, const ::clientid translated_id,
        const std::vector<site_load_information>& site_loads,
        const partition_lock_mode&                lock_mode ) {

    DCHECK( action );
    DCHECK_EQ( pre_action_type::ADD_REPLICA_PARTITIONS, action->type_ );


    add_replica_partition_pre_action* add_action =
        (add_replica_partition_pre_action*) action->args_;

    int master_site = add_action->master_site_;
    int destination = action->site_;

    if( ( master_site == K_DATA_AT_ALL_SITES ) or
        ( destination == K_DATA_AT_ALL_SITES ) ) {
        DVLOG( 10 ) << "Add replica partition, destination or master site is "
                       "K_DATA_AT_ALL_SITES";
        // nothing to do
        return action_outcome::FAILED;
    }

    std::vector<std::shared_ptr<partition_payload>> found_payloads;
    try_to_get_payloads( add_action->partition_ids_, found_payloads,
                         lock_mode );

    std::vector<partition_type::type>    partition_types;
    std::vector<storage_tier_type::type> storage_types;

    DCHECK_EQ( add_action->partition_ids_.size(),
               add_action->partition_types_.size() );
    DCHECK_EQ( add_action->storage_types_.size(),
               add_action->partition_types_.size() );
    DCHECK_LE( found_payloads.size(), add_action->partition_ids_.size() );

    std::vector<std::shared_ptr<partition_payload>> add_replica_payloads;

    std::vector<add_replica_stats> replicas;

    uint32_t                                       ori_pos = 0;
    for( uint32_t pos = 0; pos < found_payloads.size(); pos++ ) {
        auto payload = found_payloads.at( pos );
        while( add_action->partition_ids_.at( ori_pos ) !=
               payload->identifier_ ) {
            ori_pos += 1;
            DCHECK_LT( ori_pos, add_action->partition_ids_.size() );
        }

        auto loc_info = payload->get_location_information();
        DCHECK( loc_info );
        bool do_add = false;
        if( loc_info->master_location_ == (uint32_t) master_site ) {
            if( loc_info->replica_locations_.count( destination ) == 0 ) {
                do_add = true;
                DVLOG( 40 ) << "Able to background add replica of partition:"
                            << payload->identifier_
                            << ", from master site:" << master_site
                            << " to:" << destination;
            }
        }

        if( do_add ) {
            add_replica_payloads.push_back( payload );
            partition_types.push_back(
                add_action->partition_types_.at( ori_pos ) );
            storage_types.push_back( add_action->storage_types_.at( ori_pos ) );

            replicas.emplace_back( std::get<1>( loc_info->get_partition_type(
                                       loc_info->master_location_ ) ),
                                   std::get<1>( loc_info->get_storage_type(
                                       loc_info->master_location_ ) ),
                                   add_action->partition_types_.at( ori_pos ),
                                   add_action->storage_types_.at( ori_pos ),
                                   cost_model2_->normalize_contention_by_time(
                                       payload->get_contention() ),
                                   data_loc_tab_->get_stats()->get_cell_widths(
                                       payload->identifier_ ),
                                   payload->get_num_rows() );

        } else {
            payload->unlock( lock_mode );
            DVLOG( 40 ) << "Unable to background add replica of partition:"
                        << payload->identifier_
                        << ", from master site:" << master_site
                        << " to:" << destination;
        }
    }

    if( add_replica_payloads.size() == 0 ) {
        DVLOG( 10 )
            << "No partitions to background add replica, from master site:"
            << master_site << " to:" << destination;

        // nothing to do
        return action_outcome::FAILED;
    }

    cost_model_prediction_holder model_holder =
        cost_model2_->predict_add_replica_execution_time(
            site_loads.at( master_site ).cpu_load_,
            site_loads.at( destination ).cpu_load_, replicas );

    std::shared_ptr<pre_action> new_action =
        create_add_replica_partition_pre_action(
            destination, master_site, add_replica_payloads, partition_types,
            storage_types, 0 /* cost*/, model_holder );

    auto ret = internal_execute_add_replica_partitions_action(
        new_action, id, tid, translated_id, add_replica_payloads );

    unlock_payloads( add_replica_payloads, lock_mode );

    add_result_timers( new_action, std::get<1>( ret ) );

    return std::get<0>( ret );
}

action_outcome pre_action_executor::background_execute_split_partition_action(
    std::shared_ptr<pre_action> action, const ::clientid id, const int32_t tid,
    const ::clientid                          translated_id,
    const std::vector<site_load_information>& site_loads,
    const partition_lock_mode&                lock_mode ) {

    DCHECK( action );
    DCHECK_EQ( pre_action_type::SPLIT_PARTITION, action->type_ );

    split_partition_pre_action* split_action =
        (split_partition_pre_action*) action->args_;

    auto pid = split_action->partition_id_;
    auto payload = data_loc_tab_->get_partition( pid, lock_mode );
    if( !payload ) {
        DVLOG( 10 ) << "No partition to background split, pid:" << pid;

        // no part
        return action_outcome::FAILED;
    }
    auto cur_location_information =
        payload->get_location_information( partition_lock_mode::lock );
    DCHECK( cur_location_information );

    int master = cur_location_information->master_location_;
    if( master == K_DATA_AT_ALL_SITES ) {
        DVLOG( 10 ) << "Split partition, master is K_DATA_AT_ALL_SITES:" << pid;
        // no part
        return action_outcome::FAILED;
    }


    double   site_load = site_loads.at( master ).cpu_load_;
    double   contention =
        cost_model2_->normalize_contention_by_time( payload->get_contention() );

    std::vector<std::shared_ptr<partition_payload>> split_payloads;
    split_payloads.push_back( payload );

    uint32_t normalized_split_point = 0;
    cost_model_prediction_holder pred_holder;
    auto split_point = split_action->split_point_;
    auto ori_type = std::get<1>( cur_location_information->get_partition_type(
        cur_location_information->master_location_ ) );
    auto ori_storage_type =
        std::get<1>( cur_location_information->get_storage_type(
            cur_location_information->master_location_ ) );
    auto ori_pid = payload->identifier_;

    if( split_point.row_id != k_unassigned_key ) {
        normalized_split_point =
            ( uint32_t )( split_point.row_id - ori_pid.partition_start );
        split_stats split(
            ori_type, split_action->low_type_, split_action->high_type_,
            ori_storage_type, split_action->low_storage_type_,
            split_action->high_storage_type_, contention,
            data_loc_tab_->get_stats()->get_cell_widths( payload->identifier_ ),
            payload->get_num_rows(), normalized_split_point );

        pred_holder = cost_model2_->predict_horizontal_split_execution_time(
            site_load, split );

    } else {
        DCHECK_NE( split_point.col_id, k_unassigned_col );

        normalized_split_point = split_point.col_id - ori_pid.column_start;

        split_stats split(
            ori_type, split_action->low_type_, split_action->high_type_,
            ori_storage_type, split_action->low_storage_type_,
            split_action->high_storage_type_, contention,
            data_loc_tab_->get_stats()->get_cell_widths( payload->identifier_ ),
            payload->get_num_rows(), normalized_split_point );

        pred_holder = cost_model2_->predict_vertical_split_execution_time(
            site_load, split );

    }

    auto new_action = create_split_partition_pre_action(
        master, payload, split_action->split_point_, 0 /*cost*/, pred_holder,
        split_action->low_type_, split_action->high_type_,
        split_action->low_storage_type_, split_action->high_storage_type_,
        split_action->low_update_destination_slot_,
        split_action->high_update_destination_slot_ );

    snapshot_vector svv;

    auto ret = internal_execute_split_partition_action(
        new_action, id, tid, translated_id, svv, split_payloads );

    unlock_payloads( split_payloads, lock_mode );

    add_result_timers( new_action, std::get<1>( ret ) );

    return std::get<0>( ret );
}
action_outcome pre_action_executor::background_execute_merge_partition_action(
    std::shared_ptr<pre_action> action, const ::clientid id, const int32_t tid,
    const ::clientid                          translated_id,
    const std::vector<site_load_information>& site_loads,
    const partition_lock_mode                 lock_mode ) {

    DCHECK( action );
    DCHECK_EQ( pre_action_type::MERGE_PARTITION, action->type_ );

    merge_partition_pre_action* merge_action =
        (merge_partition_pre_action*) action->args_;

    std::vector<partition_column_identifier> pids;
    pids.push_back( merge_action->left_pid_);
    pids.push_back( merge_action->right_pid_ );

    std::vector<std::shared_ptr<partition_payload>> merge_payloads;
    try_to_get_payloads( pids, merge_payloads, lock_mode );

    if ( merge_payloads.size() != 2) {
        DVLOG( 10 ) << "Missing partitions to background merge, pid:" << pids;
        unlock_payloads( merge_payloads, lock_mode );
        return action_outcome::FAILED;
    }
    DCHECK_EQ( merge_payloads.size(), 2 );

    auto left_part = merge_payloads.at( 0 );
    auto right_part = merge_payloads.at( 1 );

    auto left_loc =
        left_part->get_location_information( partition_lock_mode::lock );
    auto right_loc =
        right_part->get_location_information( partition_lock_mode::lock );
    DCHECK( left_loc );
    DCHECK( right_loc );

    bool can_merge =
        can_merge_partition_location_informations( *left_loc, *right_loc );
    if( !can_merge ) {
        DVLOG( 10 ) << "Can not background merge:" << pids;
        unlock_payloads( merge_payloads, lock_mode );
        return action_outcome::FAILED;
    }

    int master = left_loc->master_location_;
    if( master == K_DATA_AT_ALL_SITES ) {
        DVLOG( 10 ) << "Merge partition, master is K_DATA_AT_ALL_SITES";
        // nothing to do
        return action_outcome::FAILED;
    }

    double   site_load = site_loads.at( master ).cpu_load_;

    double low_partition_sizes = left_part->get_num_rows();
    double high_partition_sizes = right_part->get_num_rows();

    double low_contention = cost_model2_->normalize_contention_by_time(
        left_part->get_contention() );
    double high_contention = cost_model2_->normalize_contention_by_time(
        right_part->get_contention() );

    std::vector<double> left_cell_widths =
        data_loc_tab_->get_stats()->get_cell_widths( left_part->identifier_ );
    std::vector<double> right_cell_widths =
        data_loc_tab_->get_stats()->get_cell_widths( right_part->identifier_ );

    partition_column_identifier merge_pid;
    merge_pid.table_id = merge_action->left_pid_.table_id;
    merge_pid.partition_start = merge_action->left_pid_.partition_start;
    merge_pid.partition_end = merge_action->right_pid_.partition_end;
    merge_pid.column_start = merge_action->left_pid_.column_start;
    merge_pid.column_end = merge_action->right_pid_.column_end;

    std::vector<std::shared_ptr<pre_action>> deps;

    cost_model_prediction_holder pred_holder;

    merge_stats merge_stat(
        std::get<1>(
            left_loc->get_partition_type( left_loc->master_location_ ) ),
        std::get<1>(
            right_loc->get_partition_type( right_loc->master_location_ ) ),
        merge_action->merge_type_,
        std::get<1>( left_loc->get_storage_type( left_loc->master_location_ ) ),
        std::get<1>(
            right_loc->get_storage_type( right_loc->master_location_ ) ),
        merge_action->merge_storage_type_, low_contention, high_contention,
        left_cell_widths, right_cell_widths, low_partition_sizes,
        high_partition_sizes );

    bool is_vertical = ( ( merge_pid.partition_start ==
                           merge_action->left_pid_.partition_start ) and
                         ( merge_pid.partition_end ==
                           merge_action->left_pid_.partition_end ) );

    if( is_vertical ) {
        pred_holder = cost_model2_->predict_vertical_merge_execution_time(
            site_load, merge_stat );
    } else {
        DCHECK_EQ( merge_pid.column_end, merge_action->left_pid_.column_end );

        pred_holder = cost_model2_->predict_horizontal_merge_execution_time(
            site_load, merge_stat );
    }

    auto new_action = create_merge_partition_pre_action(
        master, merge_action->left_pid_, merge_action->right_pid_, merge_pid,
        merge_action->merge_type_, merge_action->merge_storage_type_, deps,
        0 /*cost*/, pred_holder, merge_action->update_destination_slot_ );

    snapshot_vector svv;
    auto ret = internal_execute_merge_partition_action(
        new_action, id, tid, translated_id, svv, merge_payloads );

    unlock_payloads( merge_payloads, lock_mode );

    add_result_timers( new_action, std::get<1>( ret ) );

    return std::get<0>( ret );
}
action_outcome
    pre_action_executor::background_execute_change_partition_output_destination(
        std::shared_ptr<pre_action> action, const ::clientid id,
        const int32_t tid, const ::clientid translated_id,
        const std::vector<site_load_information>& site_loads,
        const partition_lock_mode&                lock_mode ) {

    DCHECK( action );
    DCHECK_EQ( pre_action_type::CHANGE_OUTPUT_DESTINATION, action->type_ );

    change_output_destination_action* change_action =
        (change_output_destination_action*) action->args_;

    int master_site = action->site_;
    uint32_t slot = change_action->update_destination_slot_;

    if( master_site == K_DATA_AT_ALL_SITES ) {
        DVLOG( 10 )
            << "Change output destination, master is K_DATA_AT_ALL_SITES";
        // nothing to do
        return action_outcome::FAILED;
    }

    std::vector<std::shared_ptr<partition_payload>> found_payloads;
    try_to_get_payloads( change_action->partition_ids_, found_payloads,
                         lock_mode );

    std::vector<std::shared_ptr<partition_payload>> change_payloads;
    for( auto& payload : found_payloads ) {
        auto loc_info = payload->get_location_information();
        DCHECK( loc_info );
        bool do_add = false;
        if( ( loc_info->master_location_ == (uint32_t) master_site ) and
            ( loc_info->update_destination_slot_ != slot ) ) {

            DVLOG( 40 ) << "Can background change update destination:"
                        << payload->identifier_ << ", slot:" << slot
                        << ", master:" << master_site;
            do_add = true;
        }

        if( do_add ) {
            change_payloads.push_back( payload );
        } else {
            DVLOG( 40 ) << "Cannot background change update destination:"
                        << payload->identifier_ << ", slot:" << slot
                        << ", master:" << master_site;

            payload->unlock( lock_mode );
        }
    }

    if( change_payloads.size() == 0 ) {
        DVLOG( 10 )
            << "No partitions to background change update destination slot"
            << slot << ", at master:" << master_site;

        // nothing to do
        return action_outcome::FAILED;
    }

    cost_model_prediction_holder model_prediction;
    std::vector<std::shared_ptr<pre_action>> deps;

    auto new_action = create_change_output_destination_action(
        master_site, change_payloads, deps, 0 /*cost*/, model_prediction,
        slot );

    snapshot_vector svv;
    auto ret = internal_execute_change_partition_output_destination(
        new_action, id, tid, translated_id, svv, change_payloads );
    unlock_payloads( change_payloads, lock_mode );

    add_result_timers( new_action, std::get<1>( ret ) );

    return std::get<0>( ret );
}

action_outcome pre_action_executor::background_execute_change_partition_type(
    std::shared_ptr<pre_action> action, const ::clientid id, const int32_t tid,
    const ::clientid                          translated_id,
    const std::vector<site_load_information>& site_loads,
    const partition_lock_mode&                lock_mode ) {

    DCHECK( action );
    DCHECK_EQ( pre_action_type::CHANGE_PARTITION_TYPE, action->type_ );

    change_partition_type_action* change_action =
        (change_partition_type_action*) action->args_;

    int site = action->site_;

    std::vector<std::shared_ptr<partition_payload>> parts_to_change;
    try_to_get_payloads( change_action->partition_ids_, parts_to_change,
                         lock_mode );

    if( parts_to_change.size() != change_action->partition_ids_.size() ) {
        unlock_payloads( parts_to_change, lock_mode );
        DVLOG( 10 ) << "Not enough partitions to change, so failing action. "
                       "Number of desired partitions:"
                    << change_action->partition_ids_.size()
                    << ", found partitions:" << parts_to_change.size();
        return action_outcome::FAILED;
    }

    snapshot_vector change_session;
    std::vector<std::shared_ptr<partition_location_information>>
                          parts_to_change_infos;
    std::vector<partition_column_identifier> change_pids;

    std::vector<partition_type::type> partition_types;
    std::vector<storage_tier_type::type> storage_types;
    DCHECK_EQ( change_action->partition_types_.size(), parts_to_change.size() );
    DCHECK_EQ( change_action->storage_types_.size(), parts_to_change.size() );

    std::vector<change_types_stats> changes;

    for( uint32_t pos = 0; pos < parts_to_change.size(); pos++ ) {
        auto& payload = parts_to_change.at( pos );
        const auto& p_type = change_action->partition_types_.at( pos );
        const auto& s_type = change_action->storage_types_.at( pos );

        auto loc_info = payload->get_location_information();
        DCHECK( loc_info );
        auto found_type = loc_info->get_partition_type( site );
        auto found_part_type = std::get<1>( found_type );
        auto found_store_type = loc_info->get_storage_type( site );
        auto found_storage_type = std::get<1>( found_store_type );
        if( ( ( loc_info->master_location_ == (uint32_t) site ) or
              ( loc_info->replica_locations_.count( site ) == 1 ) ) and
            ( std::get<0>( found_type ) ) and
            ( std::get<0>( found_store_type ) ) ) {
            if( ( found_storage_type != s_type ) or
                ( found_part_type != p_type ) ) {
                DVLOG( 40 ) << "Able to background change partition type:"
                            << payload->identifier_
                            << ", from:" << found_part_type
                            << ", to :" << p_type
                            << ", and storage type from:" << found_storage_type
                            << ", to:" << s_type;
                parts_to_change_infos.push_back( loc_info );
                change_pids.push_back( payload->identifier_ );
                partition_types.push_back( p_type );
                storage_types.push_back( s_type );

                changes.emplace_back( change_types_stats(
                    found_part_type, p_type,
                    std::get<1>( loc_info->get_storage_type( site ) ), s_type,
                    cost_model2_->normalize_contention_by_time(
                        payload->get_contention() ),
                    data_loc_tab_->get_stats()->get_cell_widths(
                        payload->identifier_ ),
                    payload->get_num_rows() ) );
            }
        }
    }

    DCHECK_EQ( partition_types.size(), storage_types.size() );

    if( partition_types.empty() ) {
        unlock_payloads( parts_to_change, lock_mode );
        DVLOG( 10 ) << "Unable to background change partition types";

        return action_outcome::FAILED;
    }

    cost_model_prediction_holder model_holder =
        cost_model2_->predict_changing_type_execution_time(
            site_loads.at( site ).cpu_load_, changes );

    std::shared_ptr<pre_action> new_action =
        create_change_partition_type_action(
            site, parts_to_change, partition_types, storage_types, 0 /* cost */,
            model_holder, {} /* deps */ );

    snapshot_vector svv;
    auto            ret = internal_execute_change_partition_type_action(
        new_action, id, tid, translated_id, svv, parts_to_change );

    unlock_payloads( parts_to_change, lock_mode );

    add_result_timers( new_action, std::get<1>( ret ) );

    return std::get<0>( ret );
}

action_outcome pre_action_executor::background_execute_transfer_remaster(
    std::shared_ptr<pre_action> action, const ::clientid id, const int32_t tid,
    const ::clientid                          translated_id,
    const std::vector<site_load_information>& site_loads,
    const partition_lock_mode&                lock_mode ) {

    DCHECK( action );
    DCHECK_EQ( pre_action_type::TRANSFER_REMASTER, action->type_ );

    transfer_remaster_action* transfer_action =
        (transfer_remaster_action*) action->args_;

    int old_master = transfer_action->old_master_;
    int new_master = transfer_action->new_master_;

    if( ( old_master == K_DATA_AT_ALL_SITES ) or
        ( new_master == K_DATA_AT_ALL_SITES ) ) {
        DVLOG( 10 ) << "Transfer remaster, old_master or new_master is "
                       "K_DATA_AT_ALL_SITES";
        // nothing to do
        return action_outcome::FAILED;
    }

    std::vector<std::shared_ptr<partition_payload>> parts_to_remaster;
    try_to_get_payloads( transfer_action->partition_ids_, parts_to_remaster,
                         lock_mode );

    if( parts_to_remaster.size() != transfer_action->partition_ids_.size() ) {
        unlock_payloads( parts_to_remaster, lock_mode );
        DVLOG( 10 ) << "Not enough partitions to remaster, so failing action. "
                       "Number of desired partitions:"
                    << transfer_action->partition_ids_.size()
                    << ", found partitions:" << parts_to_remaster.size();
        return action_outcome::FAILED;
    }

    snapshot_vector remaster_session;
    std::vector<std::shared_ptr<partition_location_information>>
                          parts_to_remaster_infos;
    std::vector<partition_column_identifier> remaster_pids;
    bool do_remaster = true;

    std::vector<partition_type::type>    partition_types;
    std::vector<storage_tier_type::type> storage_types;

    DCHECK_EQ( transfer_action->partition_types_.size(),
               parts_to_remaster.size() );
    DCHECK_EQ( transfer_action->storage_types_.size(),
               parts_to_remaster.size() );

    for( uint32_t pos = 0; pos < parts_to_remaster.size(); pos++ ) {
        auto& payload = parts_to_remaster.at( pos );

        auto loc_info = payload->get_location_information();
        DCHECK( loc_info );
        if( loc_info->master_location_ == (uint32_t) old_master ) {
            DVLOG( 40 ) << "Able to background transfer remaster partition:"
                        << payload->identifier_ << ", from site:" << old_master
                        << ", to new master:" << new_master;

            parts_to_remaster_infos.push_back( loc_info );
            remaster_pids.push_back( payload->identifier_ );
            partition_types.push_back(
                transfer_action->partition_types_.at( pos ) );
            storage_types.push_back(
                transfer_action->storage_types_.at( pos ) );

        } else {
            DVLOG( 40 ) << "Unable to background transfer remaster partition:"
                        << payload->identifier_ << ", from site:" << old_master
                        << ", to new master:" << new_master;

            do_remaster = false;
            break;
        }
    }

    if( !do_remaster ) {
        unlock_payloads( parts_to_remaster, lock_mode );
        DVLOG( 10 ) << "Unable to background transfer remaster partitions";

        return action_outcome::FAILED;
    }

    cost_model_prediction_holder model_holder;

    std::shared_ptr<pre_action> new_action =
        create_transfer_remaster_partitions_action(
            old_master, new_master, remaster_pids, parts_to_remaster,
            partition_types, storage_types, {} /* deps */ );

    snapshot_vector svv;
    auto            ret = internal_execute_transfer_remaster_partitions_action(
        new_action, id, tid, translated_id, svv, parts_to_remaster );

    unlock_payloads( parts_to_remaster, lock_mode );

    add_result_timers( new_action, std::get<1>( ret ) );

    return std::get<0>( ret );
}

std::tuple<action_outcome, std::vector<context_timer>>
    pre_action_executor::execute_add_partitions_action(
        std::shared_ptr<pre_transaction_plan> plan,
        std::shared_ptr<pre_action> action, const ::clientid id,
        const int32_t tid, const ::clientid translated_id ) {

    start_timer( SS_EXECUTE_ADD_PARTITIONS_ACTION_TIMER_ID );

    DVLOG( 20 ) << "Execute add partitions:" << id
                << ", translated_id:" << translated_id;
    DCHECK( action );
    DCHECK_EQ( pre_action_type::ADD_PARTITIONS, action->type_ );
    int destination = action->site_;

    add_partition_pre_action* add_action =
        (add_partition_pre_action*) action->args_;
    int master_site = add_action->master_site_;

    DVLOG( k_ss_action_executor_log_level )
        << "SS EXECUTE: Add partitions:" << id << ", at:" << destination
        << ", partitions:" << add_action->partition_ids_;

    DCHECK_EQ( master_site, destination );

    std::vector<std::shared_ptr<partition_payload>> partitions =
        plan->get_partitions( add_action->partitions_,
                              add_action->partition_ids_ );
    DCHECK_EQ( partitions.size(), add_action->partition_types_.size() );
    DCHECK_EQ( add_action->storage_types_.size(),
               add_action->partition_types_.size() );

    auto outcome = add_partitions_to_data_site(
        add_action->partition_ids_, add_action->partition_types_,
        add_action->storage_types_, id, tid, translated_id, master_site,
        plan->svv_, add_action->update_destination_slot_ );

    if( std::get<0>( outcome ) == action_outcome::OK ) {

        for( uint32_t pos = 0; pos < partitions.size(); pos++ ) {
            auto payload = partitions.at( pos );
            auto p_type = add_action->partition_types_.at( pos );
            auto s_type = add_action->storage_types_.at( pos );
            DVLOG( 40 ) << "Added partition:" << payload->identifier_
                        << ", to site:" << master_site;

            // there should not be a value there, as someone must have come
            // under our lock
            auto location_information =
                payload->get_location_information( partition_lock_mode::lock );
            DCHECK( !location_information );
            location_information =
                std::make_shared<partition_location_information>();
            location_information->master_location_ = master_site;
            location_information->partition_types_[master_site] = p_type;
            location_information->storage_types_[master_site] = s_type;
            location_information->version_ = 1;
            location_information->update_destination_slot_ =
                add_action->update_destination_slot_;
            payload->set_location_information( location_information );

            storage_stats_->add_partition_to_tier( master_site, s_type,
                                                   payload->identifier_ );
            tier_tracking_->add_partition( payload, master_site, s_type );
        }
    }

    plan->unlock_payloads_if_able( action->id_, partitions,
                                   partition_lock_mode::lock );

    stop_timer( SS_EXECUTE_ADD_PARTITIONS_ACTION_TIMER_ID );

    return outcome;
}

std::tuple<action_outcome, std::vector<context_timer>>
    pre_action_executor::add_partitions_to_data_site(
        const std::vector<partition_column_identifier>& pids,
        const std::vector<partition_type::type>&        partition_types,
        const std::vector<storage_tier_type::type>&     storage_types,
        const ::clientid id, const int32_t tid, const ::clientid translated_id,
        int master_site, const snapshot_vector& svv,
        uint32_t update_dest_slot ) {

    if( master_site != K_DATA_AT_ALL_SITES ) {
        propagation_configuration prop_config =
            periodic_site_selector_operations_
                ->get_site_propagation_configuration_by_position(
                    master_site, update_dest_slot );

        return internal_add_partitions_to_data_site(
            pids, partition_types, storage_types, id, tid, translated_id,
            master_site, svv, prop_config );
    }

    int num_sites = periodic_site_selector_operations_->get_num_sites();
    for( int site = 0; site < num_sites; site++ ) {
        propagation_configuration prop_config =
            periodic_site_selector_operations_
                ->get_site_propagation_configuration_by_position(
                    site, update_dest_slot );

        auto ret = internal_add_partitions_to_data_site(
            pids, partition_types, storage_types, id, tid, translated_id, site,
            svv, prop_config );
        if( std::get<0>( ret ) != action_outcome::OK ) {
            return ret;
        }
    }
    return std::make_tuple<>( action_outcome::OK,
                              std::vector<context_timer>() );
}

std::tuple<action_outcome, std::vector<context_timer>>
    pre_action_executor::internal_add_partitions_to_data_site(
        const std::vector<partition_column_identifier>& pids,
        const std::vector<partition_type::type>&        partition_types,
        const std::vector<storage_tier_type::type>&     storage_types,
        const ::clientid id, const int32_t tid, const ::clientid translated_id,
        int master_site, const snapshot_vector& svv,
        const propagation_configuration& prop_config ) {

    DVLOG( 40 ) << "Add partition:" << pids << "to site:" << master_site;
    commit_result result;
    make_rpc_and_get_service_timer(
        rpc_add_partitions,
        conn_pool_->getClient( master_site, id, tid, translated_id ), result,
        translated_id, svv, pids, master_site, partition_types, storage_types,
        prop_config );

    action_outcome outcome = action_outcome::OK;
    if( result.status != exec_status_type::COMMAND_OK ) {
        outcome = action_outcome::FAILED;
        DLOG( WARNING ) << "Unable to add partitions:" << pids
                        << ", to site:" << master_site << ", for client:" << id;
    }

    return std::make_tuple<>( outcome, result.timers );
}

std::tuple<action_outcome, std::vector<context_timer>>
    pre_action_executor::execute_add_replica_partitions_action(
        std::shared_ptr<pre_transaction_plan> plan,
        std::shared_ptr<pre_action> action, const ::clientid id,
        const int32_t tid, const ::clientid translated_id ) {
    DCHECK( action );
    DCHECK_EQ( pre_action_type::ADD_REPLICA_PARTITIONS, action->type_ );
    add_replica_partition_pre_action* add_action =
        (add_replica_partition_pre_action*) action->args_;
    std::vector<std::shared_ptr<partition_payload>> partitions =
        plan->get_partitions( add_action->partitions_,
                              add_action->partition_ids_ );

    auto ret = internal_execute_add_replica_partitions_action(
        action, id, tid, translated_id, partitions );
    plan->unlock_payloads_if_able( action->id_, partitions,
                                   partition_lock_mode::lock );
    return ret;
}
std::tuple<action_outcome, std::vector<context_timer>>
    pre_action_executor::internal_execute_add_replica_partitions_action(
        std::shared_ptr<pre_action> action, const ::clientid id,
        const int32_t tid, const ::clientid translated_id,
        std::vector<std::shared_ptr<partition_payload>>& partitions ) {
    start_timer( SS_EXECUTE_ADD_REPLICA_PARTITIONS_ACTION_TIMER_ID );

    DVLOG( 20 ) << "Execute add replica partitions:" << id
                << ", translated_id:" << translated_id;
    DCHECK( action );
    DCHECK_EQ( pre_action_type::ADD_REPLICA_PARTITIONS, action->type_ );
    add_replica_partition_pre_action* add_action =
        (add_replica_partition_pre_action*) action->args_;


    int destination = action->site_;

    int master_site = add_action->master_site_;

    if( master_site == K_DATA_AT_ALL_SITES ) {
        DVLOG( 10 )
            << "Add replica partitions, master site is K_DATA_AT_ALL_SITES";
        return std::make_tuple<>( action_outcome::FAILED,
                                  std::vector<context_timer>() );
    }

    DVLOG( k_ss_action_executor_log_level )
        << "SS EXECUTE: Add replica partitions:" << id << ", at:" << destination
        << ", master site:" << master_site
        << ", partitions:" << add_action->partition_ids_;

    DCHECK_NE( master_site, destination );


    DVLOG( 40 ) << "Add replica partition:" << add_action->partition_ids_;

    auto outcome = add_replica_partitions_to_data_sites(
        add_action->partition_ids_, add_action->partition_types_,
        add_action->storage_types_, id, tid, translated_id, master_site,
        destination );
    if( std::get<0>( outcome ) == action_outcome::OK ) {

        for( uint32_t pos = 0; pos < partitions.size(); pos++ ) {
            auto payload = partitions.at( pos );
            auto p_type = add_action->partition_types_.at( pos );
            auto s_type = add_action->storage_types_.at( pos );
            DVLOG( 40 )
                << "Added replica for partition:" << payload->identifier_
                << " to site:" << destination << ", master is:" << master_site;
            auto cur_location_information =
                payload->get_location_information( partition_lock_mode::lock );
            // this should exist
            DCHECK( cur_location_information );
            auto location_information =
                std::make_shared<partition_location_information>();
            location_information
                ->set_from_existing_location_information_and_increment_version(
                    cur_location_information );
            location_information->replica_locations_.emplace( destination );
            location_information->partition_types_.emplace( destination,
                                                            p_type );
            location_information->storage_types_.emplace( destination, s_type );
            payload->set_location_information( location_information );

            storage_stats_->add_partition_to_tier( destination, s_type,
                                                   payload->identifier_ );
            tier_tracking_->add_partition( payload, destination, s_type );
        }
    }

    stop_timer( SS_EXECUTE_ADD_REPLICA_PARTITIONS_ACTION_TIMER_ID );

    return outcome;
}

std::tuple<action_outcome, std::vector<context_timer>>
    pre_action_executor::add_replica_partitions_to_data_sites(
        const std::vector<partition_column_identifier>& pids,
        const std::vector<partition_type::type>&        partition_types,
        const std::vector<storage_tier_type::type>&     storage_types,
        const ::clientid id, const int32_t tid, const ::clientid translated_id,
        int master_site, int destination ) {
    DVLOG( 20 ) << "Adding replicas:" << pids << " from:" << master_site
                << ", to:" << destination;

    DVLOG( 20 ) << "Snapshot replica partition:" << id
                << ", at:" << master_site;

    action_outcome outcome = action_outcome::OK;

    // snapshot
    snapshot_partition_columns_results snap_results;
    make_rpc_and_get_service_timer(
        rpc_snapshot_partitions,
        conn_pool_->getClient( master_site, id, tid, translated_id ),
        snap_results, translated_id, pids );
    if( snap_results.status != exec_status_type::COMMAND_OK ) {
        DLOG( WARNING ) << "Unable to snapshot partitions:" << pids
                        << ", at site:" << master_site << ", for client:" << id
                        << ", translated_id:" << translated_id;
        return std::make_tuple<>( action_outcome::FAILED, snap_results.timers );
    }
    DVLOG( 20 ) << "Add replica partition:" << id << ", at:" << destination;

    // add the replica
    commit_result result;
    make_rpc_and_get_service_timer(
        rpc_add_replica_partitions,
        conn_pool_->getClient( destination, id, tid, translated_id ), result,
        translated_id, snap_results.snapshots, partition_types, storage_types );

    if( result.status != exec_status_type::COMMAND_OK ) {
        DLOG( WARNING ) << "Unable to add replica partitions:" << pids
                        << ", to site:" << destination << ", for client:" << id
                        << ", translated_id:" << translated_id;

        outcome = action_outcome::FAILED;
    }

    return std::make_tuple<>( outcome, snap_results.timers );
}

std::tuple<action_outcome, std::vector<context_timer>>
    pre_action_executor::internal_execute_remove_partitions_action(
        std::shared_ptr<pre_action> action, const ::clientid id,
        const int32_t tid, const ::clientid translated_id,
        const snapshot_vector&                           svv,
        std::vector<std::shared_ptr<partition_payload>>& partitions ) {
    start_timer( SS_EXECUTE_REMOVE_PARTITIONS_ACTION_TIMER_ID );

    DVLOG( 20 ) << "Execute remove partitions:" << id
                << ", translated_id:" << translated_id;
    DCHECK( action );
    DCHECK_EQ( pre_action_type::REMOVE_PARTITIONS, action->type_ );
    remove_partition_action* remove_action =
        (remove_partition_action*) action->args_;

    int destination = action->site_;
    if( destination == K_DATA_AT_ALL_SITES ) {
        DVLOG( 10 ) << "Remove replica partitions, destination site is "
                       "K_DATA_AT_ALL_SITES";
        return std::make_tuple<>( action_outcome::FAILED,
                                  std::vector<context_timer>() );
    }

    DVLOG( k_ss_action_executor_log_level )
        << "SS EXECUTE: Remove replica partitions:" << id
        << ", at:" << destination
        << ", partitions:" << remove_action->partition_ids_;

    DVLOG( 40 ) << "Remove partitions:" << remove_action->partition_ids_;

    auto outcome = remove_partitions_from_data_sites(
        remove_action->partition_ids_, id, tid, translated_id, destination,
        svv );
    if( std::get<0>( outcome ) == action_outcome::OK ) {
        for( auto payload : partitions ) {
            DVLOG( 40 ) << "Remove replica for partition:"
                        << payload->identifier_ << " to site:" << destination;
            auto cur_location_information =
                payload->get_location_information( partition_lock_mode::lock );
            // this should exist
            DCHECK( cur_location_information );
            if( cur_location_information->replica_locations_.count(
                    destination ) == 1 ) {
                auto location_information =
                    std::make_shared<partition_location_information>();
                location_information
                    ->set_from_existing_location_information_and_increment_version(
                        cur_location_information );
                location_information->replica_locations_.erase( destination );
                location_information->partition_types_.erase( destination );
                auto old_tier = std::get<1>(
                    location_information->get_storage_type( destination ) );
                location_information->storage_types_.erase( destination );
                payload->set_location_information( location_information );

                data_loc_tab_->modify_site_read_access_count(
                    destination,
                    payload->read_accesses_ /
                        cur_location_information->replica_locations_.size() );

                data_loc_tab_->decrease_sample_based_site_write_access_count(
                    destination,
                    payload->read_accesses_ /
                        cur_location_information->replica_locations_.size() );

                storage_stats_->remove_partition_from_tier(
                    destination, old_tier, payload->identifier_ );
                tier_tracking_->remove_partition( payload, destination,
                                                  old_tier );
            }
        }
    }

    stop_timer( SS_EXECUTE_REMOVE_PARTITIONS_ACTION_TIMER_ID );

    return outcome;
}

std::tuple<action_outcome, std::vector<context_timer>>
    pre_action_executor::remove_partitions_from_data_sites(
        const std::vector<partition_column_identifier>& pids,
        const ::clientid id, const int32_t tid, const ::clientid translated_id,
        int site, const snapshot_vector& svv ) {
    DVLOG( 20 ) << "Removing replicas:" << pids << " from:" << site;

    action_outcome outcome = action_outcome::OK;

    // remove the replica
    commit_result result;
    make_rpc_and_get_service_timer(
        rpc_remove_partitions,
        conn_pool_->getClient( site, id, tid, translated_id ), result,
        translated_id, svv, pids );

    if( result.status != exec_status_type::COMMAND_OK ) {
        DLOG( WARNING ) << "Unable to remove replica partitions:" << pids
                        << ", from site:" << site << ", for client:" << id;

        outcome = action_outcome::FAILED;
    }

    return std::make_tuple<>( outcome, result.timers );
}

std::tuple<action_outcome, std::vector<context_timer>>
    pre_action_executor::remaster_partitions_at_data_sites(
        const std::vector<partition_column_identifier>       pids,
        const std::vector<propagation_configuration>& new_prop_configs,
        const ::clientid id, const int32_t tid, const ::clientid translated_id,
        int source, int destination, const snapshot_vector& svv,
        std::vector<std::shared_ptr<partition_payload>>& partitions ) {

    DVLOG( 7 ) << "Releasing mastership at:" << source;
    // Release at remote site
    DVLOG( 20 ) << "Remastering:" << pids << " from:" << source
                << ", to:" << destination;
    release_result rr;
    make_rpc_and_get_service_timer(
        rpc_release_mastership,
        conn_pool_->getClient( source, id, tid, translated_id ), rr,
        translated_id, pids, destination, new_prop_configs, svv );
    if (rr.status != exec_status_type::MASTER_CHANGE_OK) {
        DLOG( WARNING ) << "Unable to release master of partitions:" << pids
                        << ", from site:" << source << ", for client:" << id
                        << ", translated_id:" << translated_id;
        return std::make_tuple<>( action_outcome::FAILED, rr.timers );
    }
    // make sure it succeeds
    DCHECK_EQ( rr.status, exec_status_type::MASTER_CHANGE_OK );

    for( auto payload : partitions ) {
        DVLOG( 40 ) << "Updating for partition:" << payload->identifier_;
        auto cur_location_information =
            payload->get_location_information( partition_lock_mode::lock );
        // it must exist at this point
        DCHECK( cur_location_information );

        // this should be true because otherwise the remastering would fail
        DCHECK_EQ( source, cur_location_information->master_location_ );
        DCHECK_EQ( 1, cur_location_information->replica_locations_.count(
                          destination ) );

        auto location_information =
            std::make_shared<partition_location_information>();
        location_information
            ->set_from_existing_location_information_and_increment_version(
                cur_location_information );
        location_information->replica_locations_.emplace( source );
        location_information->replica_locations_.erase( destination );
        location_information->master_location_ = destination;
        location_information->in_flight_ = true;
        // bump the version by one so that people wait for us
        location_information->session_req_ =
            1 + get_snapshot_version( rr.session_version_vector,
                                      payload->identifier_ );

        payload->set_location_information( location_information );
    }

    // Grant at destination site
    DVLOG( 7 ) << "Granting mastership at:" << destination;
    grant_result gr;
    make_rpc_and_get_service_timer(
        rpc_grant_mastership,
        conn_pool_->getClient( destination, id, tid, translated_id ), gr,
        translated_id, pids, new_prop_configs, rr.session_version_vector );
    // make sure it succeeds
    action_outcome outcome = action_outcome::OK;
    if( gr.status != exec_status_type::MASTER_CHANGE_OK ) {
        DLOG( WARNING ) << "Unable to grant master of partitions:" << pids
                        << ", to site:" << destination << ", for client:" << id
                        << ", translated_id:" << translated_id;
        outcome = action_outcome::FAILED;
    }

    for( const auto& pid : pids ) {
        DCHECK_EQ( get_snapshot_version( rr.session_version_vector, pid ) + 1,
                   get_snapshot_version( gr.session_version_vector, pid ) );
    }

    return std::make_tuple<>( outcome,
                              merge_context_timers( gr.timers, rr.timers ) );
}

std::tuple<action_outcome, std::vector<context_timer>>
    pre_action_executor::execute_remaster_partitions_action(
        std::shared_ptr<pre_transaction_plan> plan,
        std::shared_ptr<pre_action> action, const ::clientid id,
        const int32_t tid, const ::clientid translated_id ) {

    DCHECK( action );
    DCHECK_EQ( pre_action_type::REMASTER, action->type_ );

    remaster_partition_pre_action* remaster_action =
        (remaster_partition_pre_action*) action->args_;

    std::vector<std::shared_ptr<partition_payload>> partitions =
        plan->get_partitions( remaster_action->partitions_,
                              remaster_action->partition_ids_ );

    auto ret = internal_execute_remaster_partitions_action(
        action, id, tid, translated_id, plan->svv_, partitions );

    plan->unlock_payloads_if_able( action->id_, partitions,
                                   partition_lock_mode::lock );

    return ret;
}

std::tuple<action_outcome, std::vector<context_timer>>
    pre_action_executor::internal_execute_remaster_partitions_action(
        std::shared_ptr<pre_action> action, const ::clientid id,
        const int32_t tid, const ::clientid translated_id,
        const snapshot_vector&                           svv,
        std::vector<std::shared_ptr<partition_payload>>& partitions ) {

    start_timer( SS_EXECUTE_REMASTER_PARTITIONS_ACTION_TIMER_ID );

    DVLOG( 20 ) << "Execute remaster partitions:" << id;
    DCHECK( action );
    DCHECK_EQ( pre_action_type::REMASTER, action->type_ );

    int destination = action->site_;
    (void) destination;

    remaster_partition_pre_action* remaster_action =
        (remaster_partition_pre_action*) action->args_;
    int new_master = remaster_action->new_master_;
    int old_master = remaster_action->old_master_;

    if( ( new_master == K_DATA_AT_ALL_SITES ) or
        ( old_master == K_DATA_AT_ALL_SITES ) ) {
        DVLOG( 10 ) << "Remaster partitions, new_master or old_master site is "
                       "K_DATA_AT_ALL_SITES";
        return std::make_tuple<>( action_outcome::FAILED,
                                  std::vector<context_timer>() );
    }

    DVLOG( k_ss_action_executor_log_level )
        << "SS EXECUTE: Remaster partitions:" << id << ", to:" << new_master
        << ", from:" << old_master
        << ", partitions:" << remaster_action->partition_ids_;

    DVLOG( 40 ) << "Remaster partitions:" << remaster_action->partition_ids_;

    std::vector<propagation_configuration> new_prop_configs =
        periodic_site_selector_operations_->get_site_propagation_configurations(
            destination, remaster_action->update_destination_slots_ );

    auto outcome = remaster_partitions_at_data_sites(
        remaster_action->partition_ids_, new_prop_configs, id, tid,
        translated_id, old_master, new_master, svv, partitions );
    DVLOG( 40 ) << "Remaster partitions:" << remaster_action->partition_ids_
                << " done";

    if( std::get<0>( outcome ) == action_outcome::OK ) {
        for( auto payload : partitions ) {
            DVLOG( 40 ) << "Remastering for partition:" << payload->identifier_;
            auto cur_location_information =
                payload->get_location_information( partition_lock_mode::lock );
            // it must exist at this point
            DCHECK( cur_location_information );

            // this should be true because otherwise the remastering would fail
            DCHECK_EQ( new_master, cur_location_information->master_location_ );
            DCHECK_EQ( 1, cur_location_information->replica_locations_.count(
                              old_master ) );

            auto location_information =
                std::make_shared<partition_location_information>();
            location_information
                ->set_from_existing_location_information_and_increment_version(
                    cur_location_information );
            location_information->in_flight_ = false;

            payload->set_location_information( location_information );

            // Reads can still be served by the old master location site, so
            // decrease only the write count.

            data_loc_tab_->modify_site_write_access_count(
                old_master, -payload->write_accesses_ );
            data_loc_tab_->modify_site_write_access_count(
                new_master, payload->write_accesses_ );

            data_loc_tab_->decrease_sample_based_site_write_access_count(
                old_master, payload->sample_based_write_accesses_ );
            data_loc_tab_->increase_sample_based_site_write_access_count(
                new_master, payload->sample_based_write_accesses_ );
        }
    }

    stop_timer( SS_EXECUTE_REMASTER_PARTITIONS_ACTION_TIMER_ID );

    return outcome;
}

std::tuple<action_outcome, std::vector<context_timer>>
    pre_action_executor::execute_split_partition_action(
        std::shared_ptr<pre_transaction_plan> plan,
        std::shared_ptr<pre_action> action, const ::clientid id,
        const int32_t tid, const ::clientid translated_id ) {

    DCHECK( action );
    DCHECK_EQ( pre_action_type::SPLIT_PARTITION, action->type_ );
    split_partition_pre_action* split_action =
        (split_partition_pre_action*) action->args_;

    std::shared_ptr<partition_payload> partition = plan->get_partition(
        split_action->partition_, split_action->partition_id_ );

    std::vector<std::shared_ptr<partition_payload>> split_partitions;
    split_partitions.push_back( partition );

    auto ret = internal_execute_split_partition_action(
        action, id, tid, translated_id, plan->svv_, split_partitions );
    if( std::get<0>( ret ) == action_outcome::OK ) {
        DCHECK_EQ( 3, split_partitions.size() );
        plan->add_to_partition_mappings( split_partitions );
    }

    plan->unlock_payloads_if_able( action->id_, split_partitions,
                                   partition_lock_mode::lock );

    return ret;
}

std::tuple<action_outcome, std::vector<context_timer>>
    pre_action_executor::internal_execute_split_partition_action(
        std::shared_ptr<pre_action> action, const ::clientid id,
        const int32_t tid, const ::clientid translated_id,
        const snapshot_vector&                           svv,
        std::vector<std::shared_ptr<partition_payload>>& split_payloads ) {

    start_timer( SS_EXECUTE_SPLIT_PARTITIONS_ACTION_TIMER_ID );

    DVLOG( 20 ) << "Execute split partition:" << id
                << ", translated_id:" << translated_id;
    DCHECK( action );
    DCHECK_EQ( pre_action_type::SPLIT_PARTITION, action->type_ );

    int destination = action->site_;
    if( destination == K_DATA_AT_ALL_SITES ) {
        DVLOG( 10 ) << "Split partition, destination is "
                       "K_DATA_AT_ALL_SITES";
        return std::make_tuple<>( action_outcome::FAILED,
                                  std::vector<context_timer>() );
    }

    split_partition_pre_action* split_action =
        (split_partition_pre_action*) action->args_;
    cell_key split_point = split_action->split_point_;
    DVLOG( 20 ) << "Looking up split info:" << split_action->partition_id_
                << ", split_point:" << split_point;

    DVLOG( k_ss_action_executor_log_level )
        << "SS EXECUTE: Split partition:" << id << ", at:" << destination
        << ", partition:" << split_action->partition_id_
        << ", split_point:" << split_point;

    DCHECK_EQ( split_payloads.size(), 1 );
    std::shared_ptr<partition_payload> partition = split_payloads.at( 0 );

    partition_column_identifier pid = partition->identifier_;
    DVLOG( 20 ) << "Split partition:" << id << ", at:" << destination
                << ", pid:" << pid << ", point:" << split_point.row_id;

    add_split_to_partition_payload( partition, split_point.row_id,
                                    split_point.col_id );

    propagation_configuration low_prop_config =
        periodic_site_selector_operations_
            ->get_site_propagation_configuration_by_position(
                destination, split_action->low_update_destination_slot_ );
    propagation_configuration high_prop_config =
        periodic_site_selector_operations_
            ->get_site_propagation_configuration_by_position(
                destination, split_action->high_update_destination_slot_ );

    auto outcome = split_partition_at_data_site(
        id, tid, translated_id, pid, split_point, split_action->low_type_,
        split_action->high_type_, split_action->low_storage_type_,
        split_action->high_storage_type_, destination, svv,
        {low_prop_config, high_prop_config} );
    if( std::get<0>( outcome ) == action_outcome::OK ) {
        std::vector<std::shared_ptr<partition_payload>> split_partitions =
            data_loc_tab_->split_partition(
                partition, (uint64_t) split_point.row_id,
                (uint32_t) split_point.col_id, split_action->low_type_,
                split_action->high_type_, split_action->low_storage_type_,
                split_action->high_storage_type_,
                split_action->low_update_destination_slot_,
                split_action->high_update_destination_slot_,
                partition_lock_mode::lock );
        DCHECK_EQ( split_partitions.size(), 2 );
        auto low_part = split_partitions.at( 0 );
        auto high_part = split_partitions.at( 1 );
        split_payloads.push_back( low_part );
        split_payloads.push_back( high_part );

        data_loc_tab_->blend_max_write_accesses_if_greater(
            partition->write_accesses_, configs_.max_write_blend_rate_ );

        auto ori_tier =
            std::get<1>( partition->get_storage_type( destination ) );
        if ( ori_tier != split_action->low_storage_type_) {
            storage_stats_->change_partition_tier(
                destination, ori_tier, split_action->low_storage_type_,
                low_part->identifier_ );
        }
        if( ori_tier != split_action->high_storage_type_ ) {
            storage_stats_->change_partition_tier(
                destination, ori_tier, split_action->high_storage_type_,
                high_part->identifier_ );
        }
        tier_tracking_->add_partition( low_part, destination,
                                       split_action->low_storage_type_ );
        tier_tracking_->add_partition( high_part, destination,
                                       split_action->high_storage_type_ );
        tier_tracking_->remove_partition( partition, destination, ori_tier );

    } else {
        remove_repartition_op_from_partition_payload( partition );
    }

    stop_timer( SS_EXECUTE_SPLIT_PARTITIONS_ACTION_TIMER_ID );

    return outcome;
}

std::tuple<action_outcome, std::vector<context_timer>>
    pre_action_executor::split_partition_at_data_site(
        const ::clientid id, const int32_t tid, const ::clientid translated_id,
        const partition_column_identifier& pid, const cell_key& split_point,
        const partition_type::type&    low_type,
        const partition_type::type&    high_type,
        const storage_tier_type::type& low_storage_type,
        const storage_tier_type::type& high_storage_type, int destination,
        const snapshot_vector&                        svv,
        const std::vector<propagation_configuration>& new_prop_configs ) {
    DVLOG( 40 ) << "Calling split partition:" << id << ", at:" << destination
                << ", pid:" << pid << ", point:" << split_point.row_id;

    commit_result result;
    make_rpc_and_get_service_timer(
        rpc_split_partition,
        conn_pool_->getClient( destination, id, tid, translated_id ), result,
        translated_id, svv, pid, split_point, low_type, high_type,
        low_storage_type, high_storage_type, new_prop_configs );

    action_outcome outcome = action_outcome::OK;
    if( result.status != exec_status_type::COMMAND_OK ) {
        DLOG( WARNING ) << "Unable to split partition:" << pid
                        << ", at:" << destination << ", for client:" << id
                        << ", translated_id:" << translated_id;
        outcome = action_outcome::FAILED;
    }
    return std::make_tuple<>( outcome, result.timers );
}

std::tuple<action_outcome, std::vector<context_timer>>
    pre_action_executor::execute_merge_partition_action(
        std::shared_ptr<pre_transaction_plan> plan,
        std::shared_ptr<pre_action> action, const ::clientid id,
        const int32_t tid, const ::clientid translated_id ) {
    DCHECK( action );
    DCHECK_EQ( pre_action_type::MERGE_PARTITION, action->type_ );
    merge_partition_pre_action* merge_action =
        (merge_partition_pre_action*) action->args_;

    DVLOG( 20 ) << "Looking up merge info:" << merge_action->left_pid_ << ", "
                << merge_action->right_pid_;
    std::shared_ptr<partition_payload> left_partition =
        plan->get_partition( nullptr, merge_action->left_pid_ );
    std::shared_ptr<partition_payload> right_partition =
        plan->get_partition( nullptr, merge_action->right_pid_ );

    std::vector<std::shared_ptr<partition_payload>> merge_partitions;
    merge_partitions.push_back( left_partition );
    merge_partitions.push_back( right_partition );

    auto ret = internal_execute_merge_partition_action(
        action, id, tid, translated_id, plan->svv_, merge_partitions );

    if( std::get<0>( ret ) == action_outcome::OK ) {
        DCHECK_EQ( 3, merge_partitions.size() );
        plan->add_to_partition_mappings( merge_partitions );
    }

    plan->unlock_payloads_if_able( action->id_, merge_partitions,
                                   partition_lock_mode::lock );

    return ret;
}
std::tuple<action_outcome, std::vector<context_timer>>
    pre_action_executor::internal_execute_merge_partition_action(
        std::shared_ptr<pre_action> action, const ::clientid id,
        const int32_t tid, const ::clientid translated_id,
        const snapshot_vector&                           svv,
        std::vector<std::shared_ptr<partition_payload>>& merge_payloads ) {
    start_timer( SS_EXECUTE_MERGE_PARTITIONS_ACTION_TIMER_ID );

    DVLOG( 20 ) << "Execute merge partition:" << id
                << ", translated_id:" << translated_id;
    DCHECK( action );
    DCHECK_EQ( pre_action_type::MERGE_PARTITION, action->type_ );

    int destination = action->site_;
    if( destination == K_DATA_AT_ALL_SITES ) {
        DVLOG( 10 ) << "Merge partition, destination is "
                       "K_DATA_AT_ALL_SITES";
        return std::make_tuple<>( action_outcome::FAILED,
                                  std::vector<context_timer>() );
    }

    merge_partition_pre_action* merge_action =
        (merge_partition_pre_action*) action->args_;
    auto left_pid = merge_action->left_pid_;
    auto right_pid = merge_action->right_pid_;

    DVLOG( k_ss_action_executor_log_level )
        << "SS EXECUTE: Merge partition:" << id << ", at:" << destination
        << ", left_partition:" << left_pid << ", right_partition:" << right_pid;

    DCHECK_EQ( left_pid.table_id, right_pid.table_id );
    DCHECK_EQ( left_pid.partition_end + 1, right_pid.partition_start );

    DCHECK_EQ( 2, merge_payloads.size() );
    auto left_partition = merge_payloads.at( 0 );
    auto right_partition = merge_payloads.at( 1 );

    DCHECK_EQ( left_pid, left_partition->identifier_ );
    DCHECK_EQ( right_pid, right_partition->identifier_ );

    add_merge_to_partition_payloads( left_partition, right_partition );

    propagation_configuration prop_config =
        periodic_site_selector_operations_
            ->get_site_propagation_configuration_by_position(
                destination, merge_action->update_destination_slot_ );

    auto outcome = merge_partitions_at_data_site(
        id, tid, translated_id, left_pid, right_pid, merge_action->merge_type_,
        merge_action->merge_storage_type_, destination, svv, prop_config );

    if( std::get<0>( outcome ) == action_outcome::OK ) {
        auto merged_partition = data_loc_tab_->merge_partition(
            left_partition, right_partition, merge_action->merge_type_,
            merge_action->merge_storage_type_,
            merge_action->update_destination_slot_, partition_lock_mode::lock );

        auto location_information = merged_partition->get_location_information(
            partition_lock_mode::lock );
        DCHECK( location_information );
        // we should have sent this to the correct place
        DCHECK_EQ( destination, location_information->master_location_ );
        DVLOG( 40 ) << "Called merge partition:" << id << ", at:" << destination
                    << ", pids:" << left_pid << ", " << right_pid
                    << " to produce: " << merged_partition->identifier_;

        merge_payloads.push_back( merged_partition );

        auto low_tier =
            std::get<1>( left_partition->get_storage_type( destination ) );
        auto high_tier =
            std::get<1>( right_partition->get_storage_type( destination ) );
        if( merge_action->merge_storage_type_ != low_tier ) {
            storage_stats_->change_partition_tier(
                destination, low_tier, merge_action->merge_storage_type_,
                left_pid );
        }
        if( merge_action->merge_storage_type_ != high_tier ) {
            storage_stats_->change_partition_tier(
                destination, high_tier, merge_action->merge_storage_type_,
                right_pid );
        }
        tier_tracking_->add_partition( merged_partition, destination,
                                       merge_action->merge_storage_type_ );
        tier_tracking_->remove_partition( left_partition, destination,
                                          low_tier );
        tier_tracking_->remove_partition( right_partition, destination,
                                          high_tier );

    } else {
        remove_repartition_op_from_partition_payload( left_partition );
        remove_repartition_op_from_partition_payload( right_partition );
    }

    stop_timer( SS_EXECUTE_MERGE_PARTITIONS_ACTION_TIMER_ID );

    return outcome;
}

std::tuple<action_outcome, std::vector<context_timer>>
    pre_action_executor::merge_partitions_at_data_site(
        const ::clientid id, const int32_t tid, const ::clientid translated_id,
        const partition_column_identifier& left_pid,
        const partition_column_identifier& right_pid,
        const partition_type::type&        merge_type,
        const storage_tier_type::type& merge_storage_type, int destination,
        const snapshot_vector&           svv,
        const propagation_configuration& prop_config ) {
    DVLOG( 20 ) << "Merge partition:" << id
                << ", translated_id:" << translated_id << ", at:" << destination
                << ", left_pid:" << left_pid << ", right_pid:" << right_pid;

    commit_result result;
    make_rpc_and_get_service_timer(
        rpc_merge_partition,
        conn_pool_->getClient( destination, id, tid, translated_id ), result,
        translated_id, svv, left_pid, right_pid, merge_type, merge_storage_type,
        prop_config );

    action_outcome outcome = action_outcome::OK;
    if( result.status != exec_status_type::COMMAND_OK ) {
        DLOG( WARNING ) << "Could not merge partition:" << id
                        << ", translated_id:" << translated_id
                        << ", at:" << destination << ", left_pid:" << left_pid
                        << ", right_pid:" << right_pid;

        outcome = action_outcome::FAILED;
    }

    return std::make_tuple<>( outcome, result.timers );
}

std::tuple<action_outcome, std::vector<context_timer>>
    pre_action_executor::change_partition_output_destination_at_data_site(
        const ::clientid id, const int32_t tid, const ::clientid translated_id,
        const std::vector<partition_column_identifier>& pids, int destination,
        const propagation_configuration& prop_config,
        const snapshot_vector&           svv ) {
    DVLOG( 40 ) << "Calling change partition output destination:" << id
                << ", translated_id:" << translated_id << ", at:" << destination
                << ", pids:" << pids << ", prop_config:" << prop_config;

    commit_result result;
    make_rpc_and_get_service_timer(
        rpc_change_partition_output_destination,
        conn_pool_->getClient( destination, id, tid, translated_id ), result,
        translated_id, svv, pids, prop_config );

    action_outcome outcome = action_outcome::OK;
    if( result.status != exec_status_type::COMMAND_OK ) {
        DLOG( WARNING ) << "Unable to change partition output destination:"
                        << pids << ", at:" << destination
                        << ", for client:" << id
                        << ", translated_id:" << translated_id;
        outcome = action_outcome::FAILED;
    }
    return std::make_tuple<>( outcome, result.timers );
}

std::tuple<action_outcome, std::vector<context_timer>>
    pre_action_executor::execute_change_partition_type(
        std::shared_ptr<pre_transaction_plan> plan,
        std::shared_ptr<pre_action> action, const ::clientid id,
        const int32_t tid, const ::clientid translated_id ) {
    DCHECK( action );
    DCHECK_EQ( pre_action_type::CHANGE_PARTITION_TYPE, action->type_ );

    change_partition_type_action* change_action =
        (change_partition_type_action*) action->args_;

    std::vector<std::shared_ptr<partition_payload>> partitions =
        plan->get_partitions( change_action->partitions_,
                              change_action->partition_ids_ );

    auto ret = internal_execute_change_partition_type_action(
        action, id, tid, translated_id, plan->svv_, partitions );

    plan->unlock_payloads_if_able( action->id_, partitions,
                                   partition_lock_mode::lock );

    return ret;
}

std::tuple<action_outcome, std::vector<context_timer>>
    pre_action_executor::execute_change_partition_output_destination(
        std::shared_ptr<pre_transaction_plan> plan,
        std::shared_ptr<pre_action> action, const ::clientid id,
        const int32_t tid, const ::clientid translated_id ) {
    DCHECK( action );
    DCHECK_EQ( pre_action_type::CHANGE_OUTPUT_DESTINATION, action->type_ );

    change_output_destination_action* change_action =
        (change_output_destination_action*) action->args_;

    std::vector<std::shared_ptr<partition_payload>> partitions =
        plan->get_partitions( change_action->partitions_,
                              change_action->partition_ids_ );

    auto ret = internal_execute_change_partition_output_destination(
        action, id, tid, translated_id, plan->svv_, partitions );

    plan->unlock_payloads_if_able( action->id_, partitions,
                                   partition_lock_mode::lock );

    return ret;
}

std::tuple<action_outcome, std::vector<context_timer>>
    pre_action_executor::internal_execute_change_partition_output_destination(
        std::shared_ptr<pre_action> action, const ::clientid id,
        const int32_t tid, const ::clientid translated_id,
        const snapshot_vector&                           svv,
        std::vector<std::shared_ptr<partition_payload>>& partitions ) {
    start_timer(
        SS_EXECUTE_CHANGE_PARTITIONS_OUTPUT_DESTINATION_ACTION_TIMER_ID );

    DVLOG( 20 ) << "Execute change partitions output destination:" << id
                << ", translated_id:" << translated_id;
    DCHECK( action );
    DCHECK_EQ( pre_action_type::CHANGE_OUTPUT_DESTINATION, action->type_ );

    int destination = action->site_;

    if( destination == K_DATA_AT_ALL_SITES ) {
        DVLOG( 10 )
            << "Change partition output destination, destination site is "
               "K_DATA_AT_ALL_SITES";
        return std::make_tuple<>( action_outcome::FAILED,
                                  std::vector<context_timer>() );
    }

    change_output_destination_action* change_action =
        (change_output_destination_action*) action->args_;

    DVLOG( k_ss_action_executor_log_level )
        << "SS EXECUTE: Change partition output destination:" << id
        << ", at:" << destination
        << ", partitions:" << change_action->partition_ids_
        << ", generic slot:" << change_action->update_destination_slot_;

    DVLOG( 40 ) << "Change partitions output destination:"
                << change_action->partition_ids_;

    propagation_configuration prop_config =
        periodic_site_selector_operations_
            ->get_site_propagation_configuration_by_position(
                destination, change_action->update_destination_slot_ );

    auto outcome = change_partition_output_destination_at_data_site(
        id, tid, translated_id, change_action->partition_ids_, destination,
        prop_config, svv );

    DVLOG( 40 ) << "Change partitions output destination:"
                << change_action->partition_ids_ << " done";

    if( std::get<0>( outcome ) == action_outcome::OK ) {
        for( auto payload : partitions ) {
            DVLOG( 40 ) << "Change output destination for partition:"
                        << payload->identifier_;
            auto cur_location_information =
                payload->get_location_information( partition_lock_mode::lock );
            // it must exist at this point
            DCHECK( cur_location_information );

            // this should be true because otherwise the remastering would fail
            DCHECK_EQ( destination, cur_location_information->master_location_ );

            auto location_information =
                std::make_shared<partition_location_information>();
            location_information
                ->set_from_existing_location_information_and_increment_version(
                    cur_location_information );
            location_information->update_destination_slot_ =
                change_action->update_destination_slot_;

            payload->set_location_information( location_information );
        }
    }

    stop_timer(
        SS_EXECUTE_CHANGE_PARTITIONS_OUTPUT_DESTINATION_ACTION_TIMER_ID );

    return outcome;
}

std::tuple<action_outcome, std::vector<context_timer>>
    pre_action_executor::execute_transfer_remaster_partition(
        std::shared_ptr<pre_transaction_plan> plan,
        std::shared_ptr<pre_action> action, const ::clientid id,
        const int32_t tid, const ::clientid translated_id ) {

    DCHECK( action );
    DCHECK_EQ( pre_action_type::TRANSFER_REMASTER, action->type_ );

    transfer_remaster_action* transfer_action =
        (transfer_remaster_action*) action->args_;

    std::vector<std::shared_ptr<partition_payload>> partitions =
        plan->get_partitions( transfer_action->partitions_,
                              transfer_action->partition_ids_ );

    auto ret = internal_execute_change_partition_output_destination(
        action, id, tid, translated_id, plan->svv_, partitions );

    plan->unlock_payloads_if_able( action->id_, partitions,
                                   partition_lock_mode::lock );

    std::vector<context_timer> timers;
    return std::make_tuple<>( action_outcome::OK, timers );
}

std::tuple<action_outcome, std::vector<context_timer>>
    pre_action_executor::internal_execute_change_partition_type_action(
        std::shared_ptr<pre_action> action, const ::clientid id,
        const int32_t tid, const ::clientid translated_id,
        const snapshot_vector&                           svv,
        std::vector<std::shared_ptr<partition_payload>>& partitions ) {

    start_timer( SS_EXECUTE_CHANGE_PARTITIONS_TYPE_ACTION_TIMER_ID );

    DVLOG( 20 ) << "Execute change partition types:" << id;
    DCHECK( action );
    DCHECK_EQ( pre_action_type::CHANGE_PARTITION_TYPE, action->type_ );

    int destination = action->site_;

    change_partition_type_action* change_action =
        (change_partition_type_action*) action->args_;

    DVLOG( k_ss_action_executor_log_level )
        << "SS EXECUTE: Change partition types:" << id << ", at:" << destination
        << ", partitions:" << change_action->partition_ids_
        << ", partition types:" << change_action->partition_types_
        << ", storage types:" << change_action->storage_types_;

    DVLOG( 40 ) << "Change partition types:"
                << change_action->partition_ids_;

    auto outcome = change_partition_types_at_data_sites(
        change_action->partition_ids_, change_action->partition_types_,
        change_action->storage_types_, id, tid, translated_id, destination, svv,
        partitions );
    DVLOG( 40 ) << "Change partition types:" << change_action->partition_ids_
                << " done";

    if( std::get<0>( outcome ) == action_outcome::OK ) {
        DCHECK_EQ( partitions.size(), change_action->partition_types_.size() );
        for( uint32_t pos = 0; pos < partitions.size(); pos++ ) {
            partition_type::type p_type =
                change_action->partition_types_.at( pos );
            storage_tier_type::type s_type =
                change_action->storage_types_.at( pos );
            auto payload = partitions.at( pos );
            DVLOG( 40 ) << "Change partition type for partition:"
                        << payload->identifier_;
            auto cur_location_information =
                payload->get_location_information( partition_lock_mode::lock );
            // it must exist at this point
            DCHECK( cur_location_information );

            // this should be true because otherwise the remastering would fail
            auto location_information =
                std::make_shared<partition_location_information>();
            location_information
                ->set_from_existing_location_information_and_increment_version(
                    cur_location_information );
            location_information->partition_types_[destination] = p_type;
            auto old_tier = std::get<1>(
                location_information->get_storage_type( destination ) );
            location_information->storage_types_[destination] = s_type;

            payload->set_location_information( location_information );

            storage_stats_->change_partition_tier(
                destination, old_tier, s_type, payload->identifier_ );
            tier_tracking_->change_storage_tier( payload, destination, old_tier,
                                                 s_type );
        }
    }

    stop_timer( SS_EXECUTE_CHANGE_PARTITIONS_TYPE_ACTION_TIMER_ID );

    return outcome;
}

std::tuple<action_outcome, std::vector<context_timer>>
    pre_action_executor::change_partition_types_at_data_sites(
        const std::vector<partition_column_identifier> pids,
        const std::vector<partition_type::type>&       partition_types,
        const std::vector<storage_tier_type::type>&    storage_types,
        const ::clientid id, const int32_t tid, const ::clientid translated_id,
        int destination, const snapshot_vector& svv,
        std::vector<std::shared_ptr<partition_payload>>& partitions ) {

    DVLOG( 7 ) << "Changing partition type at:" << destination;
    // change at type
    DVLOG( 20 ) << "Changing partition type:" << pids << " at:" << destination
                << ", to partition:" << partition_types
                << ", storage types:" << storage_types;
    commit_result result;
    make_rpc_and_get_service_timer(
        rpc_change_partition_type,
        conn_pool_->getClient( destination, id, tid, translated_id ), result,
        translated_id, pids, partition_types, storage_types );
    if (result.status != exec_status_type::COMMAND_OK) {
        DLOG( WARNING ) << "Unable to change partition types:" << pids
                        << ", from site:" << destination << ", for client:" << id
                        << ", translated_id:" << translated_id;
        return std::make_tuple<>( action_outcome::FAILED, result.timers );
    }
    // make sure it succeeds
    //
    action_outcome outcome = action_outcome::OK;
    if( result.status != exec_status_type::COMMAND_OK ) {
        outcome = action_outcome::FAILED;
        DLOG( WARNING ) << "Unable to change partition types partitions:"
                        << pids << ", to site:" << destination
                        << ", for client:" << id;
    }

    return std::make_tuple<>( outcome, result.timers );
}

std::tuple<action_outcome, std::vector<context_timer>>
    pre_action_executor::internal_execute_transfer_remaster_partitions_action(
        std::shared_ptr<pre_action> action, const ::clientid id,
        const int32_t tid, const ::clientid translated_id,
        const snapshot_vector&                           svv,
        std::vector<std::shared_ptr<partition_payload>>& partitions ) {

    start_timer( SS_EXECUTE_TRANSFER_REMASTER_PARTITIONS_ACTION_TIMER_ID );

    DVLOG( 20 ) << "Execute transfer remaster partitions:" << id;
    DCHECK( action );
    DCHECK_EQ( pre_action_type::TRANSFER_REMASTER, action->type_ );

    int destination = action->site_;
    (void) destination;

    transfer_remaster_action* transfer_action =
        (transfer_remaster_action*) action->args_;
    int new_master = transfer_action->new_master_;
    int old_master = transfer_action->old_master_;

    if( ( new_master == K_DATA_AT_ALL_SITES ) or
        ( old_master == K_DATA_AT_ALL_SITES ) ) {
        DVLOG( 10 ) << "Remaster partitions, new_master or old_master site is "
                       "K_DATA_AT_ALL_SITES";
        return std::make_tuple<>( action_outcome::FAILED,
                                  std::vector<context_timer>() );
    }

    DVLOG( k_ss_action_executor_log_level )
        << "SS EXECUTE: Transfer remaster partitions:" << id
        << ", to:" << new_master << ", from:" << old_master
        << ", partitions:" << transfer_action->partition_ids_;

    DVLOG( 40 ) << "Transfer remaster partitions:"
                << transfer_action->partition_ids_;

    auto outcome = transfer_remaster_partitions_at_data_sites(
        transfer_action->partition_ids_, transfer_action->partition_types_,
        transfer_action->storage_types_, id, tid, translated_id, old_master,
        new_master, svv, partitions );
    DVLOG( 40 ) << "Transfer remaster partitions:"
                << transfer_action->partition_ids_ << " done";

    if( std::get<0>( outcome ) == action_outcome::OK ) {
        for( uint32_t pos = 0; pos < partitions.size(); pos++ ) {
            auto payload = partitions.at( pos );
            DVLOG( 40 ) << "Transfer remastering for partition:"
                        << payload->identifier_;
            auto cur_location_information =
                payload->get_location_information( partition_lock_mode::lock );
            // it must exist at this point
            DCHECK( cur_location_information );

            // this should be true because otherwise the remastering would fail
            DCHECK_EQ( old_master, cur_location_information->master_location_ );

            auto location_information =
                std::make_shared<partition_location_information>();
            location_information
                ->set_from_existing_location_information_and_increment_version(
                    cur_location_information );
            location_information->in_flight_ = false;

            auto old_tier = std::get<1>( location_information->get_storage_type(
                location_information->master_location_ ) );

            location_information->storage_types_.erase(
                location_information->master_location_ );
            location_information->partition_types_.erase(
                location_information->master_location_ );

            location_information->master_location_ = new_master;

            location_information->partition_types_[new_master] =
                transfer_action->partition_types_.at( pos );
            location_information->storage_types_[new_master] =
                transfer_action->storage_types_.at( pos );

            payload->set_location_information( location_information );

            data_loc_tab_->modify_site_write_access_count(
                old_master, -payload->write_accesses_ );
            data_loc_tab_->modify_site_write_access_count(
                new_master, payload->write_accesses_ );

            data_loc_tab_->modify_site_read_access_count(
                old_master, -payload->read_accesses_ );
            data_loc_tab_->modify_site_read_access_count(
                new_master, payload->read_accesses_ );

            data_loc_tab_->decrease_sample_based_site_write_access_count(
                old_master, payload->sample_based_write_accesses_ );
            data_loc_tab_->increase_sample_based_site_write_access_count(
                new_master, payload->sample_based_write_accesses_ );

            data_loc_tab_->decrease_sample_based_site_read_access_count(
                old_master, payload->sample_based_read_accesses_ );
            data_loc_tab_->increase_sample_based_site_read_access_count(
                new_master, payload->sample_based_read_accesses_ );

            storage_stats_->remove_partition_from_tier( old_master, old_tier,
                                                        payload->identifier_ );
            storage_stats_->add_partition_to_tier(
                new_master, transfer_action->storage_types_.at( pos ),
                payload->identifier_ );

            tier_tracking_->add_partition(
                payload, new_master,
                transfer_action->storage_types_.at( pos ) );
            tier_tracking_->remove_partition( payload, old_master, old_tier );
        }
    }

    stop_timer( SS_EXECUTE_TRANSFER_REMASTER_PARTITIONS_ACTION_TIMER_ID );

    return outcome;
}

std::tuple<action_outcome, std::vector<context_timer>>
    pre_action_executor::transfer_remaster_partitions_at_data_sites(
        const std::vector<partition_column_identifier> pids,
        const std::vector<partition_type::type>&       partition_types,
        const std::vector<storage_tier_type::type>&    storage_types,
        const ::clientid id, const int32_t tid, const ::clientid translated_id,
        int source, int destination, const snapshot_vector& svv,
        std::vector<std::shared_ptr<partition_payload>>& partitions ) {

    DVLOG( 7 ) << "Releasing transfer mastership at:" << source;
    // Release at remote site
    DVLOG( 20 ) << "Transfer remastering:" << pids << " from:" << source
                << ", to:" << destination;
    snapshot_partition_columns_results rr;
    make_rpc_and_get_service_timer(
        rpc_release_transfer_mastership,
        conn_pool_->getClient( source, id, tid, translated_id ), rr,
        translated_id, pids, destination, svv );
    if (rr.status != exec_status_type::MASTER_CHANGE_OK) {
        DLOG( WARNING ) << "Unable to release transfer master of partitions:"
                        << pids << ", from site:" << source
                        << ", for client:" << id
                        << ", translated_id:" << translated_id;
        return std::make_tuple<>( action_outcome::FAILED, rr.timers );
    }
    // make sure it succeeds
    DCHECK_EQ( rr.status, exec_status_type::MASTER_CHANGE_OK );

    // Grant at destination site
    DVLOG( 7 ) << "Granting transfer mastership at:" << destination;
    grant_result gr;
    make_rpc_and_get_service_timer(
        rpc_grant_transfer_mastership,
        conn_pool_->getClient( destination, id, tid, translated_id ), gr,
        translated_id, rr.snapshots, partition_types, storage_types );
    // make sure it succeeds
    action_outcome outcome = action_outcome::OK;
    if( gr.status != exec_status_type::MASTER_CHANGE_OK ) {
        DLOG( WARNING ) << "Unable to grant transfer master of partitions:"
                        << pids << ", to site:" << destination
                        << ", for client:" << id
                        << ", translated_id:" << translated_id;
        outcome = action_outcome::FAILED;
    }

    return std::make_tuple<>( outcome,
                              merge_context_timers( gr.timers, rr.timers ) );
}

action_outcome pre_action_executor::execute_pre_transaction_plan_action(
    std::shared_ptr<pre_transaction_plan> plan,
    std::shared_ptr<pre_action> action, const ::clientid id, const int32_t tid,
    const ::clientid translated_id ) {
    DCHECK( action );
    action_outcome outcome = action->wait_to_satisfy_dependencies();

    DVLOG( 20 ) << "Beginning executing action:" << id << ", tid:" << tid
                << ", translated_id:" << translated_id
                << ", action id:" << action->id_ << ", type:" << action->type_
                << ", outcome:" << outcome;
    if( outcome == action_outcome::FAILED ) {
        return outcome;
    }

    std::tuple<action_outcome, std::vector<context_timer>>
        action_outcome_and_timer;

    switch( action->type_ ) {
        case NONE: {
            break;
        }
        case REMASTER: {
            action_outcome_and_timer = execute_remaster_partitions_action(
                plan, action, id, tid, translated_id );
            break;
        }
        case ADD_PARTITIONS: {
            action_outcome_and_timer = execute_add_partitions_action(
                plan, action, id, tid, translated_id );
            break;
        }
        case ADD_REPLICA_PARTITIONS: {
            action_outcome_and_timer = execute_add_replica_partitions_action(
                plan, action, id, tid, translated_id );
            break;
        }
        case REMOVE_PARTITIONS: {
            // HDB-TODO
            break;
        }
        case SPLIT_PARTITION: {
            action_outcome_and_timer = execute_split_partition_action(
                plan, action, id, tid, translated_id );
            break;
        }
        case MERGE_PARTITION: {
            action_outcome_and_timer = execute_merge_partition_action(
                plan, action, id, tid, translated_id );
            break;
        }
        case CHANGE_OUTPUT_DESTINATION: {
            action_outcome_and_timer =
                execute_change_partition_output_destination(
                    plan, action, id, tid, translated_id );
            break;
        }
        case CHANGE_PARTITION_TYPE: {
            action_outcome_and_timer = execute_change_partition_type(
                plan, action, id, tid, translated_id );
            break;
        }
        case TRANSFER_REMASTER: {
            action_outcome_and_timer = execute_transfer_remaster_partition(
                plan, action, id, tid, translated_id );
            break;
        }
    }

    outcome = std::get<0>( action_outcome_and_timer );
    std::vector<context_timer> timers = std::get<1>( action_outcome_and_timer );

    DVLOG( 20 ) << "Done executing action:" << id << ", tid:" << tid
                << ", translated_id:" << translated_id
                << ", type:" << action->type_ << ", outcome:" << outcome;

    action->mark_complete( outcome );

    add_result_timers( action, timers );

    if( outcome == action_outcome::FAILED ) {
        plan->mark_plan_as_failed();
    }

    return outcome;
}

void pre_action_executor::add_result_timers(
    std::shared_ptr<pre_action>&      action,
    const std::vector<context_timer>& timers ) {
    action->model_prediction_.add_real_timers( timers );
    cost_model2_->add_results( action->model_prediction_ );
}

void pre_action_executor::try_to_get_payloads(
    std::vector<partition_column_identifier>&               pids,
    std::vector<std::shared_ptr<partition_payload>>& found_payloads,
    const partition_lock_mode&                       lock_mode ) {
    std::sort( pids.begin(), pids.end(), pid_sorter_ );

    for( const auto& pid : pids ) {
        auto part = data_loc_tab_->get_partition( pid, lock_mode );
        if (part) {
            found_payloads.push_back( part );
        }
    }
}
