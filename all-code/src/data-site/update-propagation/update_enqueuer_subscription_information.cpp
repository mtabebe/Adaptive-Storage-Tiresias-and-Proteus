#include "update_enqueuer_subscription_information.h"

#include <glog/logging.h>
#ifndef GLOG_STL_LOGGING_FOR_UNORDERED
#define GLOG_STL_LOGGING_FOR_UNORDERED
#endif
#include <glog/stl_logging.h>

#include "../db/version_types.h"

update_enqueuer_partition_subscription_information::
    update_enqueuer_partition_subscription_information(
        const partition_column_identifier& pid,
        const std::unordered_map<
            propagation_configuration, int64_t /*offset*/,
            propagation_configuration_ignore_offset_hasher,
            propagation_configuration_ignore_offset_equal_functor>&
            begin_subscription_configs )
    : pid_( pid ), begin_subscription_configs_() {
    for( const auto& entry : begin_subscription_configs ) {
        begin_subscription_configs_.insert( entry.first, entry.second );
    }
    DVLOG( 40 ) << "Created partition subscription information, partition:"
                << pid_ << ", configs:" << begin_subscription_configs;
}

void update_enqueuer_partition_subscription_information::
    add_new_subscription_offset_information(
        const propagation_configuration& config, int64_t offset,
        bool replace_with_new_if_prev_smaller ) {
    DVLOG( 40 ) << "Add new subscription offset ( pid:" << pid_
                << " ), config:" << config << ", offset:" << offset
                << ", replace_with_new_if_prev_smaller:"
                << replace_with_new_if_prev_smaller;

    for( ;; ) {
        bool do_break = true;
        auto insertion = begin_subscription_configs_.insert( config, offset );
        if( !insertion.second ) {
            int64_t prev_offset = insertion.first->second;
            bool    replace = false;
            if( replace_with_new_if_prev_smaller ) {
                replace = prev_offset < offset;
            } else {
                replace = prev_offset > offset;
            }
            if( replace ) {
                auto assigned = begin_subscription_configs_.assign_if_equal(
                    config, prev_offset, offset );
                if( !assigned ) {
                    do_break = false;
                }
                DVLOG( 40 ) << "Added offset:" << offset
                            << ", was assigned:" << do_break;
            } else {
                DVLOG( 40 )
                    << "Offset:" << offset
                    << ", previous offset:" << prev_offset
                    << ", not inserting, replace_with_new_if_prev_smaller:"
                    << replace_with_new_if_prev_smaller;
            }
        } else {
            DVLOG( 40 ) << "Inserted offset:" << offset;
        }
        if( do_break ) {
            break;
        }
    }
}

void update_enqueuer_partition_subscription_information::
    remove_subscription_offset_information(
        const propagation_configuration& config, int64_t upper_bound_offset ) {
    DVLOG( 40 ) << "Remove subscription offset ( pid:" << pid_
                << " ), config:" << config << ", offset:" << upper_bound_offset;

    for( ;; ) {
        bool do_break = true;

        auto found = begin_subscription_configs_.find( config );
        if( found != begin_subscription_configs_.end() ) {
            int64_t found_version = found->second;
            if( found_version <= upper_bound_offset ) {
                auto erased = begin_subscription_configs_.erase_if_equal(
                    config, found_version );
                DVLOG( 40 ) << "Removed offset:" << found_version
                            << ", erased:" << erased;
                if( erased != 1 ) {
                    do_break = false;
                }
            } else {
                DVLOG( 40 ) << "Offset:" << upper_bound_offset
                            << " less than previous offset:" << found_version
                            << ", not removing";
            }

        } else {
            DVLOG( 40 ) << "Config not found, considered erased";
        }
        if( do_break ) {
            break;
        }
    }
}

int64_t
    update_enqueuer_partition_subscription_information::get_subscription_offset(
        const propagation_configuration& config ) const {
    DVLOG( 40 ) << "Get subscription offset ( pid:" << pid_
                << " ), config:" << config;

    auto    found = begin_subscription_configs_.find( config );
    int64_t offset = K_NOT_COMMITTED;
    if( found != begin_subscription_configs_.end() ) {
        offset = found->second;
    }
    DVLOG( 40 ) << "Get subscription offset ( pid:" << pid_
                << " ), config:" << config << ", offset:" << offset;

    return offset;
}

void update_enqueuer_partition_subscription_information::
    add_in_subscription_offset_information(
        const propagation_configuration& config, int64_t offset ) {
    add_new_subscription_offset_information( config, offset,
                                             false /*prev smaller */ );
}
void update_enqueuer_partition_subscription_information::
    add_new_source_information( const propagation_configuration& config,
                                int64_t                          offset ) {
    add_new_subscription_offset_information( config, offset,
                                             true /*prev larger*/ );
}

std::unordered_map<propagation_configuration, int64_t,
                   propagation_configuration_ignore_offset_hasher,
                   propagation_configuration_ignore_offset_equal_functor>
    update_enqueuer_partition_subscription_information::
        get_subscription_offsets() const {
    std::unordered_map<propagation_configuration, int64_t,
                       propagation_configuration_ignore_offset_hasher,
                       propagation_configuration_ignore_offset_equal_functor>
        offsets;

    for( auto it = begin_subscription_configs_.cbegin();
         it != begin_subscription_configs_.cend(); ++it ) {
        const auto& prop_config = it->first;
        int64_t     offset = it->second;

        offsets[prop_config] = offset;
    }

    return offsets;
}

update_enqueuer_subscription_information::
    update_enqueuer_subscription_information()
    : pid_to_configs_(), expected_num_tables_( 0 ) {}
update_enqueuer_subscription_information::
    ~update_enqueuer_subscription_information() {

    for( uint32_t table_id = 0; table_id < pid_to_configs_.size();
         table_id++ ) {
        delete pid_to_configs_.at( table_id );
        pid_to_configs_.at( table_id ) = nullptr;
    }
    pid_to_configs_.clear();
}

void update_enqueuer_subscription_information::create_table(
    const table_metadata& metadata ) {
    DCHECK_EQ( metadata.table_id_, pid_to_configs_.size() );

    partition_column_identifier_folly_concurrent_hash_map_t<std::shared_ptr<
        update_enqueuer_partition_subscription_information>>* table_to_config =
        new partition_column_identifier_folly_concurrent_hash_map_t<
            std::shared_ptr<
                update_enqueuer_partition_subscription_information>>();
    pid_to_configs_.emplace_back( table_to_config );
    DCHECK_EQ( metadata.table_id_, pid_to_configs_.size() - 1 );
}
void update_enqueuer_subscription_information::set_expected_number_of_tables(
    uint32_t expected_num_tables ) {
    expected_num_tables_ = expected_num_tables;
    pid_to_configs_.reserve( expected_num_tables_ );
}

void update_enqueuer_subscription_information::
    add_partition_subscription_information(
        const partition_column_identifier& pid,
        const std::unordered_map<
            propagation_configuration, int64_t /*offset*/,
            propagation_configuration_ignore_offset_hasher,
            propagation_configuration_ignore_offset_equal_functor>&
            begin_subscription_configs ) {
    DVLOG( k_update_propagation_log_level )
        << "Add partition subscription information, partition:" << pid
        << ", configs:" << begin_subscription_configs;
    auto new_sub_info =
        std::make_shared<update_enqueuer_partition_subscription_information>(
            pid, begin_subscription_configs );

    DCHECK_LT( pid.table_id, pid_to_configs_.size() );

    auto insertion =
        pid_to_configs_.at( pid.table_id )->insert( pid, new_sub_info );
    if( !insertion.second ) {
        DVLOG( 40 ) << "Add partition subscription information, partition:"
                    << pid << ", configs:" << begin_subscription_configs
                    << ", already exists, maxing in offsets";

        auto old_sub_info = insertion.first->second;
        for( const auto& entry : begin_subscription_configs ) {
            old_sub_info->add_in_subscription_offset_information(
                entry.first, entry.second );
        }
    }
}

void update_enqueuer_subscription_information::
    remove_partition_subscription_information(
        const partition_column_identifier& pid ) {
    DVLOG( k_update_propagation_log_level )
        << "Remove partition subscription information, partition:" << pid;
    DCHECK_LT( pid.table_id, pid_to_configs_.size() );
    pid_to_configs_.at( pid.table_id )->erase( pid );
}

void update_enqueuer_subscription_information::add_source(
    const propagation_configuration& prop, int64_t offset ) {
    DVLOG( k_update_propagation_log_level )
        << "Add source to subscription information, prop:" << prop
        << ", offset:" << offset;

    for( auto pid_to_config_table : pid_to_configs_ ) {
        for( auto it = pid_to_config_table->cbegin();
             it != pid_to_config_table->cend(); ++it ) {
            auto pid_subscription = it->second;
            pid_subscription->add_new_source_information( prop, offset );
        }
    }
}

void update_enqueuer_subscription_information::remove_source(
    const propagation_configuration& prop, int64_t upper_bound_offset ) {
    DVLOG( k_update_propagation_log_level )
        << "Remove source to subscription information, prop:" << prop
        << ", offset:" << upper_bound_offset;
    for( auto pid_to_config_table : pid_to_configs_ ) {
        for( auto it = pid_to_config_table->cbegin();
             it != pid_to_config_table->cend(); ++it ) {
            auto pid_subscription = it->second;
            pid_subscription->remove_subscription_offset_information(
                prop, upper_bound_offset );
        }
    }
}

int64_t update_enqueuer_subscription_information::
    get_offset_of_partition_for_config(
        const partition_column_identifier& pid,
        const propagation_configuration&   prop ) const {
    DVLOG( 20 ) << "Get max offset of partition for config:" << pid
                << ", prop:" << prop;
    DCHECK_LT( pid.table_id, pid_to_configs_.size() );

    auto table = pid_to_configs_.at( pid.table_id );

    int64_t offset = K_NOT_COMMITTED;
    auto    found = table->find( pid );
    if( found != table->end() ) {
        auto part_sub = found->second;
        offset = part_sub->get_subscription_offset( prop );
    }
    DVLOG( k_update_propagation_log_level )
        << "Get max offset of partition for config:" << pid << ", prop:" << prop
        << ", is:" << offset;
    return offset;
}

int64_t update_enqueuer_subscription_information::
    get_max_offset_of_partitions_for_config(
        const std::vector<partition_column_identifier>& pids,
        const propagation_configuration&                prop ) const {
    DVLOG( 20 ) << "Get max offset of partitions for config:" << pids
                << ", prop:" << prop;
    int64_t max_offset = -1;
    for( const auto& pid : pids ) {
        int64_t offset = get_offset_of_partition_for_config( pid, prop );
        if( offset > max_offset ) {
            max_offset = offset;
        }
        if( max_offset == K_NOT_COMMITTED ) {
            break;
        }
    }
    if( max_offset == -1 ) {
        max_offset = K_NOT_COMMITTED;
    }
    DVLOG( k_update_propagation_log_level )
        << "Get max offset of partitions for config:" << pids
        << ", prop:" << prop << ", max_offset:" << max_offset;

    return max_offset;
}

