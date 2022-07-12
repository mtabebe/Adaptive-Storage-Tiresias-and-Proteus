#include "partition_subscription_bounds.h"

#include <glog/logging.h>

subscription_bound::subscription_bound( uint64_t bound, uint64_t version )
    : bound_( bound ), version_( version ) {}

partition_subscription_bounds::partition_subscription_bounds()
    : active_pids_lower_bound_(),
      active_pids_upper_bound_(),
      expected_num_tables_( 0 ) {}
partition_subscription_bounds::~partition_subscription_bounds() {
    DCHECK_EQ( active_pids_upper_bound_.size(),
               active_pids_lower_bound_.size() );

    for( uint32_t table_id = 0; table_id < active_pids_lower_bound_.size();
         table_id++ ) {
        delete active_pids_lower_bound_.at( table_id );
        delete active_pids_upper_bound_.at( table_id );

        active_pids_lower_bound_.at( table_id ) = nullptr;
        active_pids_upper_bound_.at( table_id ) = nullptr;
    }
    active_pids_lower_bound_.clear();
    active_pids_upper_bound_.clear();
}

void partition_subscription_bounds::create_table(
    const table_metadata& metadata ) {
    DCHECK_EQ( metadata.table_id_, active_pids_lower_bound_.size() );
    DCHECK_EQ( metadata.table_id_, active_pids_upper_bound_.size() );

    concurrent_partition_id_to_version_map* lower_table =
        new concurrent_partition_id_to_version_map();
    concurrent_partition_id_to_subscription_bound_map* upper_table =
        new concurrent_partition_id_to_subscription_bound_map();

    active_pids_lower_bound_.emplace_back( lower_table );
    active_pids_upper_bound_.emplace_back( upper_table );

    DCHECK_EQ( metadata.table_id_, active_pids_lower_bound_.size() - 1 );
    DCHECK_EQ( metadata.table_id_, active_pids_upper_bound_.size() - 1 );
}
void partition_subscription_bounds::set_expected_number_of_tables(
    uint32_t expected_num_tables ) {
    expected_num_tables_ = expected_num_tables;
    active_pids_lower_bound_.reserve( expected_num_tables_ );
    active_pids_upper_bound_.reserve( expected_num_tables_ );
}

void partition_subscription_bounds::insert_lower_bound(
    const partition_column_identifier& pid, uint64_t version ) {
    DVLOG( 40 ) << "Add active pids lower bound:" << pid << ", " << version;
    auto v_holder = std::make_shared<partition_column_version_holder>(
        pid, version, 0 /*epoch*/ );
    set_lower_bound_of_source(pid, v_holder);
}
void partition_subscription_bounds::set_lower_bound_of_source(
    const partition_column_identifier&               pid,
    std::shared_ptr<partition_column_version_holder> version_holder ) {
    DCHECK_LT( pid.table_id, active_pids_lower_bound_.size() );
    DCHECK_EQ( pid, version_holder->get_partition_column() );
    active_pids_lower_bound_.at( pid.table_id )
        ->insert_or_assign( pid, version_holder );
}

bool partition_subscription_bounds::remove_upper_bound(
    const partition_column_identifier& pid, uint64_t version ) {
    DVLOG( 40 ) << "Erase active pids upper bound:" << pid;

    DCHECK_LT( pid.table_id, active_pids_upper_bound_.size() );
    auto table_ptr = active_pids_upper_bound_.at( pid.table_id );

    bool do_erase = false;
    for( ;; ) {
        auto found = table_ptr->find( pid );
        if( found == table_ptr->end() ) {
            break;
        }
        auto bound = found->second;
        if( bound.version_ > version ) {
            break;
        }
        auto erased = table_ptr->erase_if_equal( pid, bound );
        if ( erased == 1 ) {
            do_erase = true;
            DVLOG( 10 ) << "active_pids_upper_bound_ erased:" << pid;
            break;
        }
    }
    return do_erase;
}

bool partition_subscription_bounds::set_upper_bound(
    const partition_column_identifier& pid, const subscription_bound& bound ) {

    DVLOG( 40 ) << "Update active pids upper bound:" << pid << ", "
                << bound.bound_;

    DCHECK_LT( pid.table_id, active_pids_upper_bound_.size() );
    auto table_ptr = active_pids_upper_bound_.at( pid.table_id );

    bool did_insertion = false;
    for( ;; ) {
        auto insertion = table_ptr->insert( pid, bound );
        if( insertion.second ) {
            did_insertion = true;
            DVLOG( 10 ) << "active_pids_upper_bound_ insert:" << pid
                        << ", bound_:" << bound.bound_
                        << ", version_:" << bound.version_;
            break;
        }
        auto prev_bound = insertion.first->second;
        if( prev_bound.version_ > bound.version_ ) {
            break;
        }
        auto assigned = table_ptr->assign_if_equal( pid, prev_bound, bound );
        if( assigned ) {
            did_insertion = true;
            DVLOG( 10 ) << "active_pids_upper_bound_ assign:" << pid
                        << ", bound_:" << bound.bound_
                        << ", version_:" << bound.version_;

            break;
        }
    }
    return did_insertion;
}

bool partition_subscription_bounds::look_up_upper_bound(
    const partition_column_identifier& pid, uint64_t* upper_bound ) const {
    bool     found = false;

    DCHECK_LT( pid.table_id, active_pids_upper_bound_.size() );
    auto table_ptr = active_pids_upper_bound_.at( pid.table_id );

    auto     upper_found = table_ptr->find( pid );
    if( upper_found != table_ptr->cend() ) {
        DCHECK( upper_bound );
        *upper_bound = upper_found->second.bound_;
        found = true;
        DVLOG( 40 ) << "Found upper bound:" << *upper_bound << ", pid:" << pid;
    }

    return found;
}

bool partition_subscription_bounds::look_up_lower_bound(
    const partition_column_identifier& pid, uint64_t* lower_bound ) const {
    bool     found = false;

    DCHECK_LT( pid.table_id, active_pids_lower_bound_.size() );
    auto table_ptr = active_pids_lower_bound_.at( pid.table_id );

    auto     lower_found = table_ptr->find( pid );
    if( lower_found != table_ptr->cend() ) {
        DCHECK( lower_bound );
        *lower_bound = lower_found->second->get_version();
        found = true;
        DVLOG( 40 ) << "Found lower bound:" << *lower_bound << ", pid:" << pid;
    }

    return found;
}
