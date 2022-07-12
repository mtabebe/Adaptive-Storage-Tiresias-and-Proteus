#include "update_enqueuer.h"

#include <glog/logging.h>
#include <glog/stl_logging.h>

#include "../../common/perf_tracking.h"
#include "../../common/thread_utils.h"
#include "../db/partition.h"
#include "../db/tables.h"

// The update_enqueuer holds a update_consumption_source, and pulls updates from
// the update_consumption_source, and applies them as stashed updates, by
// pushing them onto the partition queue. If the queue gets too large, it
// applies them itself

update_enqueuer::update_enqueuer()
    : tables_( nullptr ),
      configs_(),
      network_worker_thread_( nullptr ),
      update_consumption_source_( nullptr ),
      pid_bounds_( nullptr ),
      partition_subscription_info_( nullptr ),
      network_cv_(),
      network_mutex_(),
      applier_cv_(),
      applier_mutex_(),
      applier_worker_thread_( nullptr ),
      partitions_to_apply_in_current_nw_batch_(),
      partitions_for_applier_to_apply_(),
      split_threads_(),
      done_( false ) {}

update_enqueuer::~update_enqueuer() {
    // do not free tables_ we don't own it
    stop_enqueuing();
}

void update_enqueuer::set_update_consumption_source(
    std::shared_ptr<update_source_interface> update_consumption_source,
    const update_enqueuer_configs&           configs ) {
    configs_ = configs;
    update_consumption_source_ = update_consumption_source;

    if( partition_subscription_info_ ) {
        update_consumption_source_->set_subscription_info(
            partition_subscription_info_ );
    }

    update_consumption_source_->start();
}

void update_enqueuer::set_state(
    void* tables, partition_subscription_bounds* pid_bounds,
    update_enqueuer_subscription_information* partition_subscription_info ) {
    tables_ = tables;
    pid_bounds_ = pid_bounds;
    partition_subscription_info_ = partition_subscription_info;
    if( update_consumption_source_ ) {
        update_consumption_source_->set_subscription_info(
            partition_subscription_info_ );
    }
}

// start and stop enqueuing
void update_enqueuer::start_enqueuing() {
    if( network_worker_thread_ ) {
        return;
    }
    applier_worker_thread_ = std::unique_ptr<std::thread>(
        new std::thread( &update_enqueuer::run_applier_thread, this ) );
    network_worker_thread_ = std::unique_ptr<std::thread>( new std::thread(
        &update_enqueuer::run_network_enqueuer_thread, this ) );
}

void update_enqueuer::stop_enqueuing() {
    {
        std::unique_lock<std::mutex> lock( network_mutex_ );
        done_ = true;
    }
    network_cv_.notify_all();
    // we must clean up the GC first, otherwise the GC thread may try and
    // access chains that are no longer present
    if( network_worker_thread_ ) {
        join_thread( *network_worker_thread_ );
        network_worker_thread_ = nullptr;
    }
    {
        std::unique_lock<std::mutex> lock( applier_mutex_ );
        done_ = true;
    }
    applier_cv_.notify_all();
    if( applier_worker_thread_ ) {
        join_thread( *applier_worker_thread_ );
        applier_worker_thread_ = nullptr;
    }
    if( update_consumption_source_ ) {
        update_consumption_source_->stop();
    }

    split_threads_.wait_for_all_to_complete();

    if( update_consumption_source_ ) {
        update_consumption_source_ = nullptr;
    }
}

// add a new source
bool update_enqueuer::add_source( const propagation_configuration& config,
                                  const partition_column_identifier& pid, bool do_seek,
                                  const std::string& cause ) {
    DVLOG( 20 ) << "Adding active pid:" << pid;
    bool new_source =
        update_consumption_source_->add_source( config, pid, do_seek, cause );
    DVLOG( 20 ) << "Adding active pid:" << pid
                << ", created new source:" << new_source;
    return new_source;
}
bool update_enqueuer::add_sources(
    const propagation_configuration&         config,
    const std::vector<partition_column_identifier>& pids, bool do_seek,
    const std::string& cause ) {
    DVLOG( 20 ) << "Adding active pids:" << pids;
    bool new_source =
        update_consumption_source_->add_sources( config, pids, do_seek, cause );
    DVLOG( 20 ) << "Adding active pids:" << pids
                << ", created new source:" << new_source;
    return new_source;
}

// remove a source
std::tuple<bool, int64_t> update_enqueuer::remove_source(
    const propagation_configuration& config, const partition_column_identifier& pid,
    const std::string& cause ) {
    DVLOG( 20 ) << "Removing pid:" << pid;
    auto removed_source =
        update_consumption_source_->remove_source( config, pid, cause );
    DVLOG( 20 ) << "Removing pid:" << pid
                << ", removed source:" << std::get<0>( removed_source )
                << ", offset:" << std::get<1>( removed_source );
    return removed_source;
}

// pull updates
void update_enqueuer::enqueue_updates() {
    // don't take a lock nobody but this thread can modify this map
    update_consumption_source_->enqueue_updates( (void*) this );
}

std::string get_cause_string(
    const std::vector<partition_column_operation_identifier>&
        partition_buffer ) {
    for( const partition_column_operation_identifier& op : partition_buffer ) {
        if( op.op_code_ == K_REMASTER_OP ) {
            return k_remaster_at_replica_cause_string;
        } else if( op.op_code_ == K_SPLIT_OP ) {
            return k_split_cause_string;
        } else if( op.op_code_ == K_MERGE_OP ) {
            return k_merge_cause_string;
        } else if( op.op_code_ == K_CHANGE_DESTINATION_OP ) {
            return k_destination_change_cause_string;
        }
    }

    std::string cause_str;
    return cause_str;
}

// given a stashed update, decide whether to drop it, or emplace it onto a queue
void update_enqueuer::enqueue_stashed_update( stashed_update&& update ) {
    start_timer( ENQUEUE_STASHED_UPDATE_TIMER_ID );

    // look up the partition
    partition_column_identifier pid = update.deserialized_->pcid_;
    uint32_t is_new_partition = update.deserialized_->is_new_partition_;
    DVLOG( 10 ) << "Enqueue stashed:" << pid
                << ", version:" << update.commit_version_;

    // check to see if it is active
    uint64_t upper_bound = 0;
    bool     upper_found = pid_bounds_->look_up_upper_bound( pid, &upper_bound );
    if( !upper_found or ( update.commit_version_ > upper_bound ) ) {
        // we don't care
        DVLOG( 10 ) << "Destroying update:" << pid
                    << ", version:" << update.commit_version_
                    << ", as we are not interested in the partition, or its "
                    << "upper bound:" << upper_bound
                    << ", found:" << upper_found;

        destroy_stashed_update( update );


        stop_timer( ENQUEUE_STASHED_UPDATE_TIMER_ID );
        return;
    }

    uint64_t lower_bound = 0;
    bool lower_found = pid_bounds_->look_up_lower_bound( pid, &lower_bound );
    if( update.commit_version_ <= lower_bound ) {
        // we don't care
        DVLOG( 10 ) << "Destroying update:" << pid
                    << ", version:" << update.commit_version_
                    << ", as we are not interested in the partition, or its "
                    << "lower bound:" << lower_bound
                    << ", found:" << lower_found;

        destroy_stashed_update( update );

        stop_timer( ENQUEUE_STASHED_UPDATE_TIMER_ID );
        return;
    }

    // enqueue it
    tables*                    tables_ptr = (tables*) tables_;
    std::shared_ptr<partition> part = nullptr;
    table*                     table = tables_ptr->get_table( pid.table_id );
    update.deserialized_->table_ = (void*) table;
    if( !is_new_partition ) {
        part = tables_ptr->get_partition( pid );
    } else {
        // add partition
        DVLOG( 40 ) << "Enqueue forces adding partition:" << pid;
        part = tables_ptr->get_or_create_partition( pid );
        part->set_commit_version_and_update_queue_if_able(
            update.commit_version_ );
    }

    if( part == nullptr ) {
        if( update.commit_version_ < upper_bound ) {
            DLOG( FATAL ) << "No partition for pid:" << pid
                          << ", to enqueue update:" << update.commit_version_
                          << " upper_bound:" << upper_bound
                          << ", lower_bound:" << lower_bound;
        }
        stop_timer( ENQUEUE_STASHED_UPDATE_TIMER_ID );
        return;
    }

    std::string cause_str =
        get_cause_string( update.deserialized_->partition_updates_ );

    // start subscripbing as soon as possible
    tables_ptr->start_subscribing_and_create_partition_if_necessary(
        update.deserialized_->start_subscribing_,
        update.deserialized_->commit_vv_, cause_str );
    // and mark the ends as soon as possible
    tables_ptr->mark_subscription_end( update.deserialized_->stop_subscribing_,
                                       update.deserialized_->commit_vv_ );
    DCHECK_EQ( 0, update.deserialized_->switch_subscription_.size() % 2 );
    for( uint32_t pos = 0;
         pos < update.deserialized_->switch_subscription_.size() / 2; pos++ ) {
        auto info =
            update.deserialized_->switch_subscription_.at( ( pos * 2 ) + 1 );
        tables_ptr->start_subscribing_and_create_partition_if_necessary(
            {info}, update.deserialized_->commit_vv_, cause_str );
    }

    // if it is a split operation then we need to create the sub partitions
    bool is_split = create_and_pin_relevant_partitions( update.deserialized_ );

    uint32_t num_stashed = part->add_stashed_update( std::move( update ) );
    if( num_stashed >= configs_.num_updates_before_apply_self_ ) {
        add_partition_to_apply( pid, num_stashed );
    }
    if (is_split) {
        // start a thread with partition pid, to wait on the split

        start_split_thread( pid, update.commit_version_ );
    }

    stop_timer( ENQUEUE_STASHED_UPDATE_TIMER_ID );
}

void update_enqueuer::build_subscription_offsets(
    std::unordered_map<propagation_configuration, int64_t /*offset*/,
                       propagation_configuration_ignore_offset_hasher,
                       propagation_configuration_ignore_offset_equal_functor>&
        offsets ) {
    update_consumption_source_->build_subscription_offsets( offsets );
}

int update_enqueuer::gc_split_threads() {
    return split_threads_.gc_inactive_threads();
}


void update_enqueuer::start_split_thread( partition_column_identifier pid,
                                          uint64_t             version ) {
    thread_holder* t_holder = new thread_holder();
    DVLOG( 7 ) << "start_split_thread:" << pid;
    t_holder->set_thread( std::move( std::unique_ptr<std::thread>(
        new std::thread( &update_enqueuer::run_split_thread_holder, this,
                         t_holder, pid, version ) ) ) ) ;
    split_threads_.add_holder( t_holder );
}

void update_enqueuer::begin_enqueue_batch() {
    // initialize map
    DCHECK( partitions_to_apply_in_current_nw_batch_.empty() );

}
void update_enqueuer::end_enqueue_batch() {
    // copy map over
    if( partitions_to_apply_in_current_nw_batch_.empty() ) {
        return;
    }
    {
        std::unique_lock<std::mutex> lock( applier_mutex_ );
        for( const auto& entry : partitions_to_apply_in_current_nw_batch_ ) {
            partitions_for_applier_to_apply_[entry.first] = entry.second;
        }
    }
    applier_cv_.notify_one();

    partitions_to_apply_in_current_nw_batch_.clear();
}
void update_enqueuer::add_partition_to_apply( const partition_column_identifier& pid,
                                              uint32_t num_updates_to_stash ) {
    partitions_to_apply_in_current_nw_batch_[pid] = num_updates_to_stash;
}

void update_enqueuer::apply_updates() {
    partition_column_identifier_map_t<uint32_t> to_apply;
    {
        std::unique_lock<std::mutex> lock( applier_mutex_ );
        // constant time swap the underlying structure
        to_apply.swap( partitions_for_applier_to_apply_ );
    }
    for( const auto& entry : to_apply ) {
        apply_update_to_partition( entry.first, entry.second );
    }
}
void update_enqueuer::apply_update_to_partition(
    const partition_column_identifier& pid, uint32_t num_updates_to_apply ) {
    tables* tables_ptr = (tables*) tables_;
    auto    part = tables_ptr->get_partition( pid );
    if( part ) {
        DVLOG( 40 ) << "Apply updates to part:" << pid;
        part->apply_k_updates_or_empty( configs_.num_updates_to_apply_self_ );
    }
}

void update_enqueuer::run_split_thread_holder( thread_holder*       t_holder,
                                               partition_column_identifier pid,
                                               uint64_t             version ) {
    DCHECK( t_holder );
    run_split_thread( pid, version );
    t_holder->mark_as_done();
}

void update_enqueuer::run_split_thread( partition_column_identifier pid,
                                        uint64_t             version ) {
    DVLOG( k_significant_update_propagation_log_level ) << "run_split_thread:"
                                                        << pid;
    tables*                    tables_ptr = (tables*) tables_;
    std::shared_ptr<partition> part = tables_ptr->get_partition( pid );
    if( part == nullptr ) {
        // already done, so don't do anythign
        return;
    }

    DVLOG( k_significant_update_propagation_log_level )
        << "Split thread, pid:" << pid << ", wait until:" << version;
    part->wait_until_version_or_apply_updates( version );
    DVLOG( k_significant_update_propagation_log_level )
        << "Split thread, pid:" << pid << ", wait until:" << version
        << ", done!";
}

bool update_enqueuer::create_and_pin_relevant_partitions(
    deserialized_update* deserialized_ptr ) {
    bool is_split = false;
    table* t = (table*) deserialized_ptr->table_;
    for( const auto& poi : deserialized_ptr->partition_updates_ ) {
        if( poi.op_code_ == K_SPLIT_OP ) {

            std::tuple<partition_column_identifier, partition_column_identifier>
                new_pids = construct_split_partition_column_identifiers(
                    poi.identifier_, poi.data_64_, poi.data_32_ );
            DVLOG( 10 ) << "Enqueue split op:" << poi.identifier_
                        << ", col split:" << poi.data_32_
                        << ", row split:" << poi.data_64_;

            const auto& low_pid = std::get<0>( new_pids );
            const auto& high_pid = std::get<1>( new_pids );
            DVLOG( 40 ) << "Splitting adds:" << low_pid << ", and:" << high_pid;
            auto     low_part = t->get_or_create_partition_and_pin( low_pid );
            auto     high_part = t->get_or_create_partition_and_pin( high_pid );

            is_split = true;
        } else if( poi.op_code_ == K_MERGE_OP ) {
            partition_column_identifier cover_p = poi.identifier_;
            partition_column_identifier ori_p = deserialized_ptr->pcid_;

            std::tuple<partition_column_identifier, partition_column_identifier>
                new_pids = construct_split_partition_column_identifiers(
                    poi.identifier_, poi.data_64_, poi.data_32_ );

            partition_column_identifier low_pid = std::get<0>( new_pids );
            partition_column_identifier high_pid = std::get<0>( new_pids );
            partition_column_identifier other_p = low_pid;
            if ( ori_p == low_pid) {
                other_p = high_pid;
            }

            auto cover_part = t->get_or_create_partition_and_pin( cover_p );
            auto ori_part = t->get_or_create_partition_and_pin( ori_p );
            // auto other_part = t->get_or_create_partition_and_pin( other_p );
        }
    }
    (void) t;
    return is_split;
}

void update_enqueuer::run_applier_thread() {
    DVLOG( k_significant_update_propagation_log_level )
        << "Running update applier thread";
    for( ;; ) {
        std::chrono::high_resolution_clock::time_point start_time =
            std::chrono::high_resolution_clock::now();

        {
            std::unique_lock<std::mutex> lock( applier_mutex_ );
            if( done_ ) {
                break;
            }
        }

        apply_updates();

        std::chrono::high_resolution_clock::time_point end_time =
            std::chrono::high_resolution_clock::now();

        sleep_or_yield( start_time, end_time, applier_mutex_, applier_cv_ );
    }
    DVLOG( 10 ) << "Done running update applier thread";
}
void update_enqueuer::run_network_enqueuer_thread() {
    DVLOG( k_significant_update_propagation_log_level )
        << "Running network enqueuer thread";
    for( ;; ) {
        std::chrono::high_resolution_clock::time_point start_time =
            std::chrono::high_resolution_clock::now();

        {
            std::unique_lock<std::mutex> lock( network_mutex_ );
            if( done_ ) {
                break;
            }
        }

        enqueue_updates();

        std::chrono::high_resolution_clock::time_point end_time =
            std::chrono::high_resolution_clock::now();

        sleep_or_yield( start_time, end_time, network_mutex_, network_cv_ );
    }
    DVLOG( 10 ) << "Done running network enqueuer thread";
}

void update_enqueuer::sleep_or_yield(
    const std::chrono::high_resolution_clock::time_point& start,
    const std::chrono::high_resolution_clock::time_point& end, std::mutex& mut,
    std::condition_variable& cv ) {
    thread_sleep_or_yield(
        start, end, mut, cv,
        configs_.sleep_time_between_update_enqueue_iterations_ );
}

grouped_propagation_config::grouped_propagation_config()
    : offset_( 0 ), do_seek_( true ), pids_() {}

