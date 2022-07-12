#include "db.h"

#include <glog/logging.h>
#ifndef GLOG_STL_LOGGING_FOR_UNORDERED
#define GLOG_STL_LOGGING_FOR_UNORDERED
#endif
#include <glog/stl_logging.h>

#include "../../common/perf_tracking.h"
#include "../../common/thread_utils.h"

db::db()
    : tables_(),
      update_gen_( nullptr ),
      enqueuers_( nullptr ),
      tables_metadata_(),
      gc_worker_thread_( nullptr ),
      worker_mutex_(),
      worker_cv_(),
      done_( false ) {}
db::~db() {
    if( enqueuers_ ) {
        enqueuers_->stop_enqueuers();
    }
    stop_gc_worker();
}

void db::init( std::shared_ptr<update_destination_generator> update_gen,
               std::shared_ptr<update_enqueuers>             enqueuers,
               const tables_metadata&                        t_metadata ) {
    enqueuers_ = enqueuers;
    update_gen_ = update_gen;
    tables_metadata_ = t_metadata;

    DVLOG( 10 ) << "Initializing database as site:"
                << tables_metadata_.site_location_ << ", with "
                << tables_metadata_.expected_num_tables_
                << " tables, config:" << tables_metadata_;

    update_gen_->init();

    tables_.init( t_metadata, update_gen_, enqueuers_ );

    enqueuers_->set_tables( t_metadata, (void*) get_tables() );
    enqueuers_->set_do_not_subscribe_topics(
        update_gen_->get_propagation_configurations() );
    enqueuers_->start_enqueuers();

    start_gc_worker();
}

transaction_partition_holder* db::get_partition_holder(
    uint32_t client_id, const partition_column_identifier_set& write_pids,
    const partition_column_identifier_set&   read_pids,
    const partition_column_identifier_set&   do_not_apply_last_pids,
    const partition_lookup_operation& lookup_operation ) {
    transaction_partition_holder* holder =
        tables_.get_partition_holder( write_pids, read_pids, lookup_operation );
    if( holder ) {
        holder->add_do_not_apply_last( do_not_apply_last_pids );
    }
    // Add GC
    return holder;
}

transaction_partition_holder* db::get_partitions_with_begin(
    uint32_t client_id, const snapshot_vector& begin_timestamp,
    const partition_column_identifier_set&   write_pids,
    const partition_column_identifier_set&   read_pids,
    const partition_column_identifier_set&   do_not_apply_last_pids,
    const partition_lookup_operation& lookup_operation ) {
    transaction_partition_holder* holder =
        get_partition_holder( client_id, write_pids, read_pids,
                              do_not_apply_last_pids, lookup_operation );
    if ( holder ) {
        holder->begin_transaction( begin_timestamp );
    }
    return holder;
}

void db::add_partition( uint32_t                           client_id,
                        const partition_column_identifier& pid,
                        uint32_t                           master_location,
                        const partition_type::type&        p_type,
                        const storage_tier_type::type&     s_type,
                        const propagation_configuration&   prop_config ) {
    auto update_destination =
        update_gen_->get_update_destination_by_propagation_configuration(
            prop_config );

    add_partition( client_id, pid, master_location, p_type, s_type,
                   update_destination );
}
void db::add_partition( uint32_t                           client_id,
                        const partition_column_identifier& pid,
                        uint32_t                           master_location,
                        const partition_type::type&        p_type,
                        const storage_tier_type::type&     s_type ) {
    auto update_destination =
        update_gen_->get_update_destination_by_hashing_partition_id( pid );

    add_partition( client_id, pid, master_location, p_type, s_type,
                   update_destination );
}

void db::add_partition(
    uint32_t client_id, const partition_column_identifier& pid,
    uint32_t master_location, const partition_type::type& p_type,
    const storage_tier_type::type& s_type,
    std::shared_ptr<update_destination_interface> update_destination ) {
    std::shared_ptr<partition> p = std::move(
        tables_.add_partition( pid, p_type, s_type, false /* do pin*/, 0,
                               true /* do lock*/, update_destination ) );
    p->set_master_location( master_location );
    // unlock once it's been added
    p->unlock_partition();

    if( master_location == tables_metadata_.site_location_ ) {
        enqueuers_->add_interest_in_partition(
            pid, 0, p->get_partition_column_version_holder() );
    }
}

void db::add_replica_partitions(
    uint32_t cid, const std::vector<snapshot_partition_column_state>& snapshots,
    const std::vector<partition_type::type>&    p_types,
    const std::vector<storage_tier_type::type>& s_types ) {

    DCHECK_EQ( p_types.size(), snapshots.size() );

    std::vector<partition_column_identifier>      pids_to_add_sources;
    std::vector<propagation_configuration> sources_to_add;

    std::vector<std::shared_ptr<partition>> partitions;
    std::vector<std::shared_ptr<partition_column_version_holder>>
                      version_holders;
    std::vector<bool> do_seeks( snapshots.size(), true /*seek*/ );

    snapshot_vector snapshot;

    for( uint32_t pos = 0; pos < p_types.size(); pos++) {
        const auto& snapshot_state = snapshots.at( pos );
        const auto pid = snapshot_state.pcid;
        uint32_t   master_location = snapshot_state.master_location;
        partition_type::type p_type = p_types.at( pos );
        storage_tier_type::type s_type = s_types.at( pos );

        DVLOG( 10 ) << "add partition client:" << cid << ", partition:" << pid
                    << ", master_location:" << master_location
                    << ", p_type:" << p_type << ", s_type:" << s_type;
        auto add_ret = add_replica_partition(
            cid, snapshot_state.session_version_vector, pid, master_location,
            snapshot_state.columns, p_type, s_type );
        bool did_insert = std::get<0>( add_ret );
        if( did_insert ) {
            pids_to_add_sources.push_back( pid );
            sources_to_add.push_back( snapshot_state.prop_config );
            auto part =  std::get<1>( add_ret );
            partitions.push_back( part );
            set_snapshot_version(
                snapshot, pid,
                get_snapshot_version( snapshot_state.session_version_vector,
                                      pid ) );
            version_holders.push_back( part->get_partition_column_version_holder() );
        }
    }

    enqueuers_->add_sources_together_with_grouping(
        sources_to_add, pids_to_add_sources, snapshot, do_seeks /*seek*/,
        k_add_replica_cause_string );
    enqueuers_->add_interest_in_partitions( pids_to_add_sources, snapshot,
                                            version_holders );
    for (auto part : partitions) {
        part->unlock_partition();
    }
}

std::tuple<bool, std::shared_ptr<partition>> db::add_replica_partition(
    uint32_t client_id, const snapshot_vector& snapshot,
    const partition_column_identifier& pid, uint32_t master_location,
    const std::vector<snapshot_column_state>& columns,
    const partition_type::type&               p_type,
    const storage_tier_type::type&            s_type ) {
    DVLOG( 40 ) << "Adding replica partition:" << pid;
    DVLOG( 40 ) << "Adding replica partition:" << pid
                << ", snapshot:" << snapshot << ", p_type:" << p_type
                << ", s_type:" << s_type;

    uint64_t version = get_snapshot_version( snapshot, pid );
    if( version > 0 ) {
      version = version - 1;
    }

    std::shared_ptr<partition> part(
        tables_.create_partition_without_adding_to_table(
            pid, p_type, storage_tier_type::type::MEMORY, false, version ) );
    part->init_partition_from_snapshot( snapshot, columns, master_location );

    if( s_type == storage_tier_type::type::DISK ) {
        part->change_storage_tier( s_type );
    }

    bool did_insert = tables_.insert_partition( pid, part );
    if( !did_insert ) {
        DLOG( WARNING ) << "Adding replica already exists:" << pid;
        snapshot_vector wait_snapshot;
        set_snapshot_version( wait_snapshot, pid, version );
        // we need to make sure we wait
        auto holder =
            get_partitions_with_begin( client_id, wait_snapshot, {}, {pid}, {},
                                       partition_lookup_operation::GET );
        // we don't actually need this
        delete holder;
        part->unlock_partition();
    }

    return std::make_tuple( did_insert, part );
}

void db::remove_partition( uint32_t                    client_id,
                           const partition_column_identifier& pid ) {
    tables_.remove_partition( pid );
}

std::tuple<bool, snapshot_vector> db::split_partition(
    uint32_t client_id, const snapshot_vector& snapshot,
    const partition_column_identifier& old_pid, uint64_t row_split_point,
    uint32_t col_split_point, const partition_type::type& low_type,
    const partition_type::type&                   high_type,
    const storage_tier_type::type&                low_storage_type,
    const storage_tier_type::type&                high_storage_type,
    const std::vector<propagation_configuration>& prop_configs ) {
    DCHECK_EQ( 2, prop_configs.size() );

    split_partition_result split_result = tables_.split_partition(
        snapshot, old_pid, row_split_point, col_split_point, low_type,
        high_type, low_storage_type, high_storage_type,
        update_gen_->get_update_destination_by_propagation_configuration(
            prop_configs.at( 0 ) ),
        update_gen_->get_update_destination_by_propagation_configuration(
            prop_configs.at( 1 ) ) );
    // do op on split_result
    if( split_result.okay_ ) {
        enqueuers_->add_interest_in_partitions(
            {split_result.partition_low_->get_metadata().partition_id_,
             split_result.partition_high_->get_metadata().partition_id_},
            split_result.snapshot_,
            {split_result.partition_low_->get_partition_column_version_holder(),
             split_result.partition_high_->get_partition_column_version_holder()} );
    }
    return std::make_tuple( split_result.okay_, split_result.snapshot_ );
}

std::tuple<bool, snapshot_vector> db::merge_partition(
    uint32_t client_id, const snapshot_vector& snapshot,
    const partition_column_identifier& low_pid,
    const partition_column_identifier& high_pid,
    const partition_type::type&        merge_type,
    const storage_tier_type::type&     merge_storage_type,
    const propagation_configuration&   prop_config ) {
    split_partition_result merge_result = tables_.merge_partition(
        snapshot, low_pid, high_pid, merge_type, merge_storage_type,
        update_gen_->get_update_destination_by_propagation_configuration(
            prop_config ) );
    // do op on merge_result
    if( merge_result.okay_ ) {
        enqueuers_->add_interest_in_partitions(
            {merge_result.partition_cover_->get_metadata().partition_id_},
            merge_result.snapshot_,
            {merge_result.partition_cover_
                 ->get_partition_column_version_holder()} );
    }
    return std::make_tuple( merge_result.okay_, merge_result.snapshot_ );
}

std::tuple<bool, snapshot_vector> db::remaster_partitions(
    uint32_t client_id, const snapshot_vector& snapshot,
    const std::vector<partition_column_identifier>&      pids,
    const std::vector<propagation_configuration>& new_prop_configs,
    uint32_t new_master, bool is_release,
    const partition_lookup_operation& lookup_operation ) {
    snapshot_vector commit_vv;

    partition_column_identifier_set pid_set =
        convert_to_partition_column_identifier_set( pids );
    DCHECK_EQ( pid_set.size(), pids.size() );

    std::vector<bool> do_seeks( pids.size(), false /*no seek*/ );

    start_timer( REMASTER_BEGIN_TIMER_ID );
    transaction_partition_holder* holder = get_partitions_with_begin(
        client_id, snapshot, pid_set, {}, {}, lookup_operation );
    stop_timer( REMASTER_BEGIN_TIMER_ID );

    if( holder == nullptr ) {
        LOG( WARNING ) << "Unable to find partitions for remaster";
        return std::make_tuple( false, commit_vv );
    }

    if( is_release ) {
        bool is_master_ok =
            holder->does_site_master_write_set( get_site_location() );
        if( !is_master_ok ) {
            LOG( WARNING ) << "Unable to begin write transaction, data is not "
                              "mastered at site";
            holder->abort_transaction();
            return std::make_tuple( false, commit_vv );
        }
    }

    start_timer( PERFORM_REMASTER_TIMER_ID );
    auto remaster_ret = holder->remaster_partitions(
        pids, is_release, new_master, new_prop_configs );
    auto subscribe_snap = std::get<0>( remaster_ret );
    if(  !is_release ) {
        if( !new_prop_configs.empty() ) {
            holder->set_partition_update_destinations( pids, new_prop_configs,
                                                       update_gen_ );
        }
    }
    stop_timer( PERFORM_REMASTER_TIMER_ID );

    if( new_prop_configs.size() > 0 ) {
        enqueuers_->add_sources_together_with_grouping(
            new_prop_configs, pids, subscribe_snap, do_seeks /* no seek*/,
            k_remaster_at_master_cause_string );
    }

    start_timer( REMASTER_FINALIZE_TIMER_ID );
    commit_vv = holder->commit_transaction();
    stop_timer( REMASTER_FINALIZE_TIMER_ID );

    auto remaster_parts = std::get<1>(remaster_ret);
    DCHECK_EQ( remaster_parts.size(), pids.size() );
    std::vector<std::shared_ptr<partition_column_version_holder>>
        version_holders;
    for( uint32_t pos = 0; pos < pids.size(); pos++ ) {
        auto version_holder =
            remaster_parts.at( pos )->get_partition_column_version_holder();
        DCHECK_EQ( pids.at( pos ), version_holder->get_partition_column() );
        version_holders.push_back( version_holder );
    }

    if( !is_release ) {
        if( new_master == tables_metadata_.site_location_ ) {
            enqueuers_->add_interest_in_partitions( pids, commit_vv,
                                                    version_holders );
        }
    }

    delete holder;
    return std::make_tuple( true, commit_vv );
}

std::tuple<bool, snapshot_partition_column_state> db::snapshot_partition(
    uint32_t client_id, const partition_column_identifier& pid ) {

    DVLOG( 10 ) << "Snapshot partition:" << pid;

    snapshot_partition_column_state snap_state;
    transaction_partition_holder*   holder = get_partitions_with_begin(
        client_id, {}, {pid}, {pid}, {},
        // if we are trying to snapshot something that doesn't exist, that
        // doesn't make much sense
        partition_lookup_operation::GET );

    DVLOG( 10 ) << "Snapshot partition:" << pid << ", do snapshot now";
    if( holder == nullptr ) {
        return std::make_tuple( false, snap_state );
    }
    snap_state = holder->snapshot_partition( pid );
    delete holder;
    return std::make_tuple( true, snap_state );
}

std::tuple<bool, snapshot_vector> db::change_partition_output_destination(
    uint32_t client_id, const ::snapshot_vector& snapshot,
    const std::vector<::partition_column_identifier>& pids,
    const ::propagation_configuration&         new_prop_config ) {

    auto destination =
        update_gen_->get_update_destination_by_propagation_configuration(
            new_prop_config );

    snapshot_vector commit_vv;

    partition_column_identifier_set pid_set =
        convert_to_partition_column_identifier_set( pids );
    DCHECK_EQ( pid_set.size(), pids.size() );

    transaction_partition_holder* holder =
        get_partitions_with_begin( client_id, snapshot, pid_set, {}, {}, GET );

    if( holder == nullptr ) {
        LOG( WARNING ) << "Unable to find partitions for change partition "
                          "output destination";
        return std::make_tuple( false, commit_vv );
    }

    bool is_master_ok =
        holder->does_site_master_write_set( get_site_location() );
    if( !is_master_ok ) {
        LOG( WARNING ) << "Unable to begin write transaction, data is not "
                          "mastered at site";
        holder->abort_transaction();
        return std::make_tuple( false, commit_vv );
    }

    holder->change_partition_output_destination( destination );

    commit_vv = holder->commit_transaction();

    delete holder;
    return std::make_tuple( true, commit_vv );
}

std::tuple<bool, snapshot_vector> db::change_partition_types(
    uint32_t client_id, const std::vector<::partition_column_identifier>& pids,
    const std::vector<partition_type::type>&    part_types,
    const std::vector<storage_tier_type::type>& storage_types ) {
    DCHECK_EQ( pids.size(), part_types.size() );
    DCHECK_EQ( pids.size(), storage_types.size() );

    DVLOG( 10 ) << "Change partition types:" << client_id;

    snapshot_vector commit_vv;


    for( uint32_t pos = 0; pos < pids.size(); pos++ ) {
        const auto pid = pids.at( pos );
        const auto p_type = part_types.at( pos );
        const auto s_type = storage_types.at( pos );

        DVLOG( 10 ) << "Change partition type client:" << client_id
                    << ", partition:" << pid << ", p_type:" << p_type
                    << ", s_type:" << s_type;
        auto change_ret =
            change_partition_type( client_id, pid, p_type, s_type );
        bool did_change = std::get<0>( change_ret );

        DVLOG( 10 ) << "Change partition type client:" << client_id
                    << ", partition:" << pid << ", p_type:" << p_type
                    << ", s_type:" << s_type << ", did change:" << did_change;

        if( did_change ) {
            set_snapshot_version( commit_vv, pid, std::get<2>( change_ret ) );
        }
    }

    DVLOG( 10 ) << "Change partition types:" << client_id << ", okay!";
    return std::make_tuple<>( true, commit_vv );
}

std::tuple<bool, std::shared_ptr<partition>, uint64_t>
    db::change_partition_type( uint32_t                           client_id,
                               const partition_column_identifier& pid,
                               const partition_type::type&        part_type,
                               const storage_tier_type::type& storage_type ) {

    auto part = tables_.get_partition( pid );
    if( !part ) {
        return std::make_tuple<>( false, nullptr, 0 );
    }
    part->lock_partition();
    uint64_t version = part->get_most_recent_version();
    bool     did_change =
        part->change_partition_and_storage_type( part_type, storage_type );

    if( !did_change ) {
        DLOG( WARNING ) << "Unable to change partition type:" << pid;
    }

    part->unlock_partition();

    return std::make_tuple<>( did_change, part, version );
}

std::vector<propagation_configuration> db::get_propagation_configurations() {
    return update_gen_->get_propagation_configurations();
}

std::vector<int64_t> db::get_propagation_counts() {
    return update_gen_->get_propagation_counts();
}

tables*  db::get_tables() { return &tables_; }
uint32_t db::get_site_location() const {
    return tables_metadata_.site_location_;
}
std::shared_ptr<update_destination_generator>
    db::get_update_destination_generator() {
    return update_gen_;
}

void db::get_update_partition_states(
    std::vector<polled_partition_column_version_information>& polled_versions ) {
    tables_.get_update_partition_states( polled_versions );
}

void db::start_gc_worker() {
    if( gc_worker_thread_ ) {
        return;
    }
    gc_worker_thread_ = std::unique_ptr<std::thread>(
        new std::thread( &db::run_gc_worker_thread, this ) );
}

void db::do_gc_work() {
    DVLOG( 20 ) << "Doing GC work";
    tables_.gc_inactive_partitions();
    enqueuers_->gc_split_threads();
}

void db::run_gc_worker_thread() {
    DVLOG( 10 ) << "Running gc worker thread, gc_sleep_time:"
                << tables_metadata_.gc_sleep_time_;
    for( ;; ) {
        std::chrono::high_resolution_clock::time_point start_time =
            std::chrono::high_resolution_clock::now();

        {
            std::unique_lock<std::mutex> lock( worker_mutex_ );
            if( done_ ) {
                break;
            }
        }

        do_gc_work();

        std::chrono::high_resolution_clock::time_point end_time =
            std::chrono::high_resolution_clock::now();

        thread_sleep_or_yield( start_time, end_time, worker_mutex_, worker_cv_,
                               tables_metadata_.gc_sleep_time_ );
    }
    DVLOG( 10 ) << "Done running gc worker thread";
}

void db::stop_gc_worker() {
    {
        std::unique_lock<std::mutex> lock( worker_mutex_ );
        done_ = true;
    }
    worker_cv_.notify_all();
    if( gc_worker_thread_ == nullptr ) {
        return;
    }
    join_thread( *gc_worker_thread_ );
    gc_worker_thread_ = nullptr;
}

bool db::snapshot_and_remove_partitions(
    uint32_t client_id, const partition_column_identifier_set& pids,
    uint32_t                                      new_master_location,
    std::vector<snapshot_partition_column_state>& snapshot_state ) {
    DVLOG( 10 ) << "Snapshot and remove partitions:" << client_id;

    transaction_partition_holder* holder = get_partitions_with_begin(
        client_id, {}, pids, {}, {},
        // if we are trying to snapshot something that doesn't exist, that
        // doesn't make much sense
        partition_lookup_operation::GET );

    if( holder == nullptr ) {
        DVLOG( 10 ) << "Snapshot and remove partitions:" << client_id
                    << ", could not get partitions";
        return false;
    }
    for( const auto& pid : pids ) {
        DVLOG( 10 ) << "Snapshot partition:" << client_id << ", pid:" << pid;
        auto snap_state =
            holder->snapshot_partition_with_optional_commit( pid, false );
        snapshot_state.push_back( snap_state );
    }
    holder->commit_transaction();
    delete holder;

    for( const auto& pid : pids ) {
        DVLOG( 10 ) << "Remove partition:" << client_id << ", pid:" << pid;
        remove_partition( client_id, pid );
    }
    DVLOG( 10 ) << "Snapshot and remove partitions:" << client_id << ", okay!";
    return true;
}

std::tuple<bool, snapshot_vector> db::add_partitions_from_snapshots(
    uint32_t cid, const std::vector<snapshot_partition_column_state>& snapshots,
    const std::vector<partition_type::type>&    p_types,
    const std::vector<storage_tier_type::type>& s_types,
    uint32_t                                    master_location ) {
    DCHECK_EQ( snapshots.size(), p_types.size() );
    DCHECK_EQ( snapshots.size(), s_types.size() );

    DVLOG( 10 ) << "Add partitions from snapshots:" << cid
                << ", master:" << master_location;

    snapshot_vector commit_vv;

    std::vector<std::shared_ptr<partition>> partitions;
    std::vector<std::shared_ptr<partition_column_version_holder>> version_holders;
    std::vector<partition_column_identifier>              pids;

    for( uint32_t pos = 0; pos < snapshots.size(); pos++) {
        const auto& snapshot_state = snapshots.at( pos );
        const auto pid = snapshot_state.pcid;
        partition_type::type p_type = p_types.at( pos );
        storage_tier_type::type s_type = s_types.at( pos );
        DVLOG( 10 ) << "add partition client:" << cid << ", partition:" << pid
                    << ", master_location:" << master_location
                    << ", p_type:" << p_type << ", s_type:" << s_type;
        auto add_ret = add_replica_partition(
            cid, snapshot_state.session_version_vector, pid, master_location,
            snapshot_state.columns, p_type, s_type );
        bool did_insert = std::get<0>( add_ret );

        DVLOG( 10 ) << "add partition client:" << cid << ", partition:" << pid
                    << ", master_location:" << master_location
                    << ", p_type:" << p_type << ", s_type:" << s_type
                    << ", did insert:" << did_insert;

        if( did_insert ) {
            auto part =  std::get<1>( add_ret );
            pids.push_back( pid );
            partitions.push_back( part );
            set_snapshot_version(
                commit_vv, pid,
                get_snapshot_version( snapshot_state.session_version_vector,
                                      pid ) );
            version_holders.push_back(
                part->get_partition_column_version_holder() );
        }
    }

    enqueuers_->add_interest_in_partitions( pids, commit_vv, version_holders );
    for (auto part : partitions) {
        part->unlock_partition();
    }

    DVLOG( 10 ) << "Add partitions from snapshots:" << cid
                << ", master:" << master_location << ", okay!";
    return std::make_tuple<>( true, commit_vv );
}

cell_data_type db::get_cell_data_type( const cell_identifier& cid ) const {
    return tables_.get_cell_data_type( cid );
}

std::vector<std::vector<cell_widths_stats>> db::get_column_widths() const {
    return tables_.get_column_widths();
}
std::vector<std::vector<selectivity_stats>> db::get_selectivities() const {
    return tables_.get_selectivities();
}

partition_column_identifier_map_t<storage_tier_type::type>
    db::get_storage_tier_changes() const {
    return tables_.get_storage_tier_changes();
}

std::vector<::storage_tier_change> db::get_changes_to_storage_tiers() const {
    partition_column_identifier_map_t<storage_tier_type::type> changes =
        get_storage_tier_changes();

    std::vector<::storage_tier_change> storage_changes;
    storage_changes.reserve( changes.size() );
    int32_t site_loc = get_site_location();

    ::storage_tier_change c;
    for( const auto& entry : changes ) {
        c.pid = entry.first;
        c.site = site_loc;
        c.change = entry.second;
        storage_changes.emplace_back( c );
    }

    return storage_changes;
}

