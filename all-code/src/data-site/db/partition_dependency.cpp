#include "partition_dependency.h"

partition_dependency::partition_dependency()
    : version_counter_(),
      dependencies_( nullptr ),
      partition_id_( k_unassigned_partition ) {}

partition_dependency::~partition_dependency() {
    partition_id_ = k_unassigned_partition;
    if( dependencies_ != nullptr ) {
        delete dependencies_;
        dependencies_ = nullptr;
    }
}

void partition_dependency::init( uint64_t partition_id, uint64_t version,
                                 const snapshot_vector& base_snapshot,
                                 int32_t                num_records_in_chain ) {
    partition_id_ = partition_id;

    dependency_map* dep = new dependency_map();
    dep->init( partition_id_, nullptr, base_snapshot, num_records_in_chain );
    dependencies_ = dep;

    version_counter_.update_counter( version );
}

void partition_dependency::store_dependency( const snapshot_vector& snapshot,
                                             int32_t num_records_in_chain,
                                             const snapshot_vector& gc_lwm,
                                             int32_t always_gc ) {
    uint64_t version = get_snapshot_version( snapshot, partition_id_ );
    dependency_map* new_map = dependencies_->store_dependency(
        partition_id_, version, snapshot, num_records_in_chain, gc_lwm,
        always_gc );
    if( ( new_map != nullptr ) and ( new_map != dependencies_ ) ) {
        dependencies_ = new_map;
    }
}


void partition_dependency::set_version( uint64_t version ) {
    version_counter_.update_counter( version );
}

uint64_t partition_dependency::get_version() {
    return version_counter_.get_counter();
}

uint64_t partition_dependency::update_version_if_less_than(
    uint64_t new_version ) {
    return version_counter_.update_counter_if_less_than( new_version );
}

// low water mark, because the counter is always increasing
uint64_t partition_dependency::get_version_no_lock() {
    return version_counter_.get_counter_no_lock();
}
void partition_dependency::make_version_visible( uint64_t expected_version ) {
    uint64_t commit_version = version_counter_.incr_counter();
    DCHECK_EQ( commit_version, expected_version );
}
uint64_t partition_dependency::wait_until( uint64_t version ) {
    return version_counter_.wait_until( version );
}

void partition_dependency::get_dependency( snapshot_vector& snapshot ) {
    uint64_t        version = get_snapshot_version( snapshot, partition_id_ );
    dependency_map* dep_map = dependencies_;
    dep_map->get_dependency( partition_id_, version, snapshot );
}

