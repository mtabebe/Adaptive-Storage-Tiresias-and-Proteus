#include "update_enqueuers.h"

#include <glog/logging.h>
#include <glog/stl_logging.h>

#include "../../common/perf_tracking.h"

// update_enqueuers, hold a collection of individual update_enqueuer's.
// The update_enqueuers also hold a collection of active partitions, which
// update_enqueuer's use to determine whether to drop updates.

update_enqueuers::update_enqueuers()
    : t_metadata_(),
      enqueuers_(),
      pid_bounds_(),
      pid_to_configs_(),
      partition_subscription_info_(),
      do_not_subscribe_to_topics_() {}

update_enqueuers::~update_enqueuers() {
    enqueuers_.clear();
    for( uint32_t table_id = 0; table_id < pid_to_configs_.size();
         table_id++ ) {
        delete pid_to_configs_.at( table_id );
        pid_to_configs_.at( table_id ) = nullptr;
    }
    pid_to_configs_.clear();
}

void update_enqueuers::add_enqueuer(
    std::shared_ptr<update_enqueuer> enqueuer ) {
    enqueuers_.push_back( enqueuer );
}

void update_enqueuers::set_tables( const tables_metadata& t_metadata,
                                   void*                  tables ) {
    t_metadata_ = t_metadata;
    pid_bounds_.set_expected_number_of_tables(
        t_metadata_.expected_num_tables_ );
    partition_subscription_info_.set_expected_number_of_tables(
        t_metadata_.expected_num_tables_ );
    pid_to_configs_.reserve( t_metadata_.expected_num_tables_ );

    for( auto& enqueuer : enqueuers_ ) {
        enqueuer->set_state( tables, &pid_bounds_,
                             &partition_subscription_info_ );
    }
}

void update_enqueuers::create_table( const table_metadata& metadata ) {
    DVLOG( 40 ) << "Update enqueuers create table:" << metadata;
    if( metadata.table_id_ < pid_to_configs_.size() ) {
        return;
    }

    DCHECK_EQ( metadata.table_id_, pid_to_configs_.size() );

    partition_column_identifier_folly_concurrent_hash_map_t<propagation_configuration>*
        table_to_config = new partition_column_identifier_folly_concurrent_hash_map_t<
            propagation_configuration>();
    pid_to_configs_.emplace_back( table_to_config );
    DCHECK_EQ( metadata.table_id_, pid_to_configs_.size() - 1 );

    pid_bounds_.create_table( metadata );
    partition_subscription_info_.create_table( metadata );
}

void update_enqueuers::set_do_not_subscribe_topics(
    const std::vector<propagation_configuration>& configs ) {
    for( const auto& config : configs ) {
        propagation_configuration config_version = config;
        config_version.offset = 0;
        DVLOG( 40 ) << "Adding:" << config_version << ", to do not subscribe";
        do_not_subscribe_to_topics_.emplace( config_version );
    }
}

void update_enqueuers::add_interest_in_partition(
    const partition_column_identifier& pid, uint64_t version,
    std::shared_ptr<partition_column_version_holder> version_holder ) {
    snapshot_vector snapshot;
    set_snapshot_version( snapshot, pid, version );
    add_interest_in_partitions( {pid}, snapshot, {version_holder} );
}
void update_enqueuers::add_interest_in_partitions(
    const std::vector<partition_column_identifier>& pids,
    const snapshot_vector&                          snapshot,
    const std::vector<std::shared_ptr<partition_column_version_holder>>&
        version_holders ) {
    DCHECK_EQ( pids.size(), version_holders.size() );
    for( uint32_t pos = 0; pos < pids.size(); pos++ ) {
        const auto& pid = pids.at( pos );
        auto        version_holder = version_holders.at( pos );
        DVLOG( k_update_propagation_log_level )
            << "Add active pids upper bound:" << pid << ", " << K_COMMITTED;
        uint64_t version = get_snapshot_version( snapshot, pid );
        subscription_bound bound( K_COMMITTED, version );
        pid_bounds_.set_upper_bound( pid, bound );
        mark_lower_bound_of_source( version_holder, pid );
    }

    std::unordered_map<propagation_configuration, int64_t /*offset*/,
                       propagation_configuration_ignore_offset_hasher,
                       propagation_configuration_ignore_offset_equal_functor>
        offsets;
    build_subscription_offsets( offsets );

    for( const auto& pid : pids ) {
        partition_subscription_info_.add_partition_subscription_information(
            pid, offsets );
    }
}

// start up all the enqueuers
void update_enqueuers::start_enqueuers() {
    DVLOG( 10 ) << "Starting update enqueuers";
    for( auto& enqueuer : enqueuers_ ) {
        enqueuer->start_enqueuing();
    }
}
void update_enqueuers::stop_enqueuers() {
    DVLOG( 10 ) << "Stopping update enqueuers";
    for( auto& enqueuer : enqueuers_ ) {
        enqueuer->stop_enqueuing();
    }
}

// Change the source of a partition, for remastering
// Note the new source will not drop updates due to the active_pids_
// But this will officially add the new source, and remove the old source
void update_enqueuers::change_sources(
    const partition_column_identifier&      pid,
    const propagation_configuration& old_source,
    const propagation_configuration& new_source, const std::string& cause ) {

    start_timer( UPDATE_ENQUEUERS_CHANGE_SOURCES_TIMER_ID );

    DVLOG( k_update_propagation_log_level )
        << "Change sources pid:" << pid << ", old_source:" << old_source
        << ", new_source:" << new_source << ", cause:" << cause;

    if( propagation_configurations_match_without_offset( old_source,
                                                         new_source ) ) {
        DVLOG( k_update_propagation_log_level )
            << "Not changing sources pid:" << pid
            << ", old_source:" << old_source << ", new_source:" << new_source
            << ", same source";

        stop_timer( UPDATE_ENQUEUERS_CHANGE_SOURCES_TIMER_ID );
        return;
    }

    DCHECK_LT( pid.table_id, pid_to_configs_.size() );
    pid_to_configs_.at( pid.table_id )->assign( pid, new_source );

    if( !should_add_source( new_source ) ) {
        DVLOG( 20 ) << "Not adding source:" << new_source << ", blacklisted";
    } else {
        uint32_t new_id = get_enqueuer_id( new_source ) % enqueuers_.size();
        bool     added_new_source = enqueuers_.at( new_id )->add_source(
            new_source, pid, false /* no need to seek, already added source*/,
            cause );
        if ( added_new_source) {
            partition_subscription_info_.add_source( new_source,
                                                     new_source.offset );
        }
        // no need to add this to the partition subscription info, because the
        // partitions should already be there
    }

    uint32_t old_id = get_enqueuer_id( old_source ) % enqueuers_.size();
    auto     removed_source_ret =
        enqueuers_.at( old_id )->remove_source( old_source, pid, cause );

    if( std::get<0>( removed_source_ret ) ) {
        partition_subscription_info_.remove_source(
            old_source, std::get<1>( removed_source_ret ) );
    }

    stop_timer( UPDATE_ENQUEUERS_CHANGE_SOURCES_TIMER_ID );
}

// Start subscribing to a source
void update_enqueuers::add_source( const propagation_configuration& config,
                                   const partition_column_identifier&      pid,
                                   const snapshot_vector&           snapshot,
                                   bool do_seek, const std::string& cause ) {
    add_sources( config, {pid}, snapshot, do_seek, cause );
}
void update_enqueuers::add_sources(
    const propagation_configuration&         config,
    const std::vector<partition_column_identifier>& pids,
    const snapshot_vector& snapshot, bool do_seek, const std::string& cause ) {

    start_timer( UPDATE_ENQUEUERS_ADD_SOURCE_TIMER_ID );

    DVLOG( 10 ) << "Add source:" << config << ", pids:" << pids
                << ", cause:" << cause;

    std::vector<partition_column_identifier> pids_to_add;

    for( const auto& pid : pids ) {
        DVLOG( k_update_propagation_log_level )
            << "Add active pids upper bound:" << pid << ", " << K_COMMITTED;

        subscription_bound bound( K_COMMITTED,
                                  get_snapshot_version( snapshot, pid ) );
        bool did_insertion = pid_bounds_.set_upper_bound( pid, bound );
        if ( !did_insertion) {
            DVLOG( k_update_propagation_log_level )
                << "Not adding active pids, as, uppper bound is lower "
                   "than snapshot";
            continue;
        }
        pids_to_add.push_back( pid );
        pid_bounds_.insert_lower_bound( pid, 0 );

        DCHECK_LT( pid.table_id, pid_to_configs_.size() );
        pid_to_configs_.at( pid.table_id )->insert_or_assign( pid, config );
    }

    if( enqueuers_.size() == 0 ) {
        stop_timer( UPDATE_ENQUEUERS_ADD_SOURCE_TIMER_ID );
        return;
    }

    if( !should_add_source( config ) ) {
        DVLOG( 20 ) << "Not adding source:" << config << ", blacklisted";

        stop_timer( UPDATE_ENQUEUERS_ADD_SOURCE_TIMER_ID );

        return;
    }

    uint32_t enqueuer_id = get_enqueuer_id( config ) % enqueuers_.size();
    bool     new_source = enqueuers_.at( enqueuer_id )
                          ->add_sources( config, pids_to_add, do_seek, cause );

    if ( new_source ) {
        partition_subscription_info_.add_source( config, config.offset );
    }

    if( !pids_to_add.empty() ) {
        std::unordered_map<
            propagation_configuration, int64_t /*offset*/,
            propagation_configuration_ignore_offset_hasher,
            propagation_configuration_ignore_offset_equal_functor>
            offsets;
        build_subscription_offsets( offsets );

        for( const auto& pid : pids_to_add ) {
            partition_subscription_info_.add_partition_subscription_information(
                pid, offsets );
        }
    }

    stop_timer( UPDATE_ENQUEUERS_ADD_SOURCE_TIMER_ID );
}

void update_enqueuers::build_subscription_offsets(
    std::unordered_map<propagation_configuration, int64_t /*offset*/,
                       propagation_configuration_ignore_offset_hasher,
                       propagation_configuration_ignore_offset_equal_functor>&
        offsets ) {
    for( auto enqueuer : enqueuers_ ) {
        enqueuer->build_subscription_offsets( offsets );
    }
}

void update_enqueuers::add_subscriptions_together_with_grouping(
    const std::vector<update_propagation_information>& subscriptions,
    const snapshot_vector& snapshot, const std::string& cause ) {
    std::unordered_map<propagation_configuration, grouped_propagation_config,
                       propagation_configuration_ignore_offset_hasher,
                       propagation_configuration_ignore_offset_equal_functor>
        min_offset_and_pids;

    for( const auto& subscription : subscriptions) {
        add_to_min_offset_and_pids(
            min_offset_and_pids, subscription.propagation_config_,
            subscription.identifier_, subscription.do_seek_ );
    }

    add_to_sources( min_offset_and_pids, snapshot, cause );
}

void update_enqueuers::add_sources_together_with_grouping(
    const std::vector<propagation_configuration>& configs,
    const std::vector<partition_column_identifier>&      pids,
    const snapshot_vector& snapshot, const std::vector<bool>& do_seeks,
    const std::string& cause ) {
    std::unordered_map<propagation_configuration, grouped_propagation_config,
                       propagation_configuration_ignore_offset_hasher,
                       propagation_configuration_ignore_offset_equal_functor>
        min_offset_and_pids;

    DCHECK_EQ( configs.size(), pids.size() );
    DCHECK_EQ( configs.size(), do_seeks.size() );
    for( uint32_t pos = 0; pos < configs.size(); pos++ ) {
        add_to_min_offset_and_pids( min_offset_and_pids, configs.at( pos ),
                                    pids.at( pos ), do_seeks.at( pos ) );
    }
    add_to_sources( min_offset_and_pids, snapshot, cause );
}

void update_enqueuers::add_to_sources(
    const std::unordered_map<
        propagation_configuration, grouped_propagation_config,
        propagation_configuration_ignore_offset_hasher,
        propagation_configuration_ignore_offset_equal_functor>&
                           min_offset_and_pids,
    const snapshot_vector& snapshot, const std::string& cause ) {

    for( const auto& config_offset_and_pids : min_offset_and_pids ) {
        propagation_configuration config = config_offset_and_pids.first;
        uint64_t    offset = config_offset_and_pids.second.offset_;
        bool        do_seek = config_offset_and_pids.second.do_seek_;
        const auto& pids = config_offset_and_pids.second.pids_;

        config.offset = offset;

        (void) do_seek;
        add_sources( config, pids, snapshot, do_seek, cause );
    }
}

void update_enqueuers::add_to_min_offset_and_pids(
    std::unordered_map<propagation_configuration, grouped_propagation_config,
                       propagation_configuration_ignore_offset_hasher,
                       propagation_configuration_ignore_offset_equal_functor>&
                                     min_offset_and_pids,
    const propagation_configuration& config, const partition_column_identifier& pid,
    bool do_seek ) {

    uint64_t ori_offset = config.offset;
    auto     found = min_offset_and_pids.find( config );
    if( found == min_offset_and_pids.end() ) {
      grouped_propagation_config entry;

      entry.offset_ = ori_offset;
      entry.do_seek_ = do_seek;
      entry.pids_.push_back( pid );

      min_offset_and_pids[config] = entry;
    } else {
        auto& entry = found->second;
        entry.pids_.push_back( pid );
        if( ori_offset < entry.offset_) {
            entry.offset_ = ori_offset;
        }
        entry.do_seek_ = entry.do_seek_ or do_seek;
    }
}

void update_enqueuers::remove_source( const propagation_configuration& config,
                                      const partition_column_identifier&      pid,
                                      const snapshot_vector&           snapshot,
                                      const std::string&               cause ) {
    start_timer( UPDATE_ENQUEUERS_REMOVE_SOURCE_TIMER_ID );

    DVLOG( k_update_propagation_log_level ) << "Remove source:" << config
                                            << ", pid:" << pid;

    uint64_t pid_version = get_snapshot_version( snapshot, pid );
    bool do_erase = pid_bounds_.remove_upper_bound( pid, pid_version );
    if( !do_erase ) {
        DVLOG( k_update_propagation_log_level )
            << "Told to remove source:" << config << ", pid:" << pid
            << ", but version is stale, so not removing";
        stop_timer( UPDATE_ENQUEUERS_REMOVE_SOURCE_TIMER_ID );
        return;
    }
    DCHECK_LT( pid.table_id, pid_to_configs_.size() );
    pid_to_configs_.at( pid.table_id )->erase( pid );

    if( enqueuers_.size() == 0 ) {
        stop_timer( UPDATE_ENQUEUERS_REMOVE_SOURCE_TIMER_ID );
        return;
    }
    uint32_t enqueuer_id = get_enqueuer_id( config ) % enqueuers_.size();
    auto removed_source_ret =
        enqueuers_.at( enqueuer_id )->remove_source( config, pid, cause );

    if( std::get<0>( removed_source_ret ) ) {
        partition_subscription_info_.remove_source(
            config, std::get<1>( removed_source_ret ) );
    }

    stop_timer( UPDATE_ENQUEUERS_REMOVE_SOURCE_TIMER_ID );
}

// Drop updates to a partition beyond a certain threshold
void update_enqueuers::mark_upper_bound_of_source(
    const propagation_configuration& config, const partition_column_identifier& pid ) {
    DVLOG( 5 ) << "Mark Upper Bound source:" << config << ", pid:" << pid;

    subscription_bound bound( config.offset, config.offset );
    pid_bounds_.set_upper_bound( pid, bound );
}

void update_enqueuers::mark_lower_bound_of_source(
    std::shared_ptr<partition_column_version_holder> version_holder,
    const partition_column_identifier&               pid ) {
    pid_bounds_.set_lower_bound_of_source( pid, version_holder );
}


// remove subscription by partition id
void update_enqueuers::remove_subscription(
    const partition_column_identifier& pid, const snapshot_vector& snapshot,
    const std::string& cause ) {
    DCHECK_LT( pid.table_id, pid_to_configs_.size() );
    auto table = pid_to_configs_.at( pid.table_id );

    auto found = table->find( pid );
    if( found != table->cend() ) {
        propagation_configuration config = found->second;
        remove_source( config, pid, snapshot, cause );
    }
    partition_subscription_info_.remove_partition_subscription_information(
        pid );
}

bool update_enqueuers::should_add_source(
    const propagation_configuration& config ) {
    bool should_add = ( do_not_subscribe_to_topics_.count( config ) == 0 );
    DVLOG( 40 ) << "Should add source:" << config
                << ", should_add:" << should_add;

    return should_add;
}

std::shared_ptr<update_enqueuers> make_update_enqueuers() {
    return std::make_shared<update_enqueuers>();
}

void update_enqueuers::gc_split_threads() {
    for( auto enqueuer : enqueuers_ ) {
        enqueuer->gc_split_threads();
    }
}
