#include "write_buffer.h"

#include "versioned_row_record.h"

write_buffer::write_buffer()
    : commit_vv_(),
      partition_id_( k_unassigned_partition ),
      pcid_(),
      is_new_partition_( 0 ),
      do_not_propagate_( 1 ),
      record_buffer_(),
      cell_buffer_(),
      partition_buffer_(),
      start_subscribing_(),
      stop_subscribing_(),
      switch_subscription_()
#if defined( RECORD_COMMIT_TS )
      ,
      txn_ts_( 0 )
#endif
{
}

write_buffer::~write_buffer() {
    reset_state();
}
void write_buffer::reset_state() {
    commit_vv_.clear();
    partition_id_ = k_unassigned_partition;

    do_not_propagate_ = 1;
    is_new_partition_ = 0;

    record_buffer_.clear();
    cell_buffer_.clear();
    partition_buffer_.clear();

    start_subscribing_.clear();
    stop_subscribing_.clear();
    switch_subscription_.clear();
}

void write_buffer::set_version( uint64_t version ) const {
    for( auto &vri : record_buffer_ ) {
        row_record *r = vri.get_data();
        r->set_version( version );
    }
}

void write_buffer::store_propagation_information(
    const snapshot_vector &commit_vv, const partition_column_identifier &pcid,
    uint64_t partition_id, uint64_t version ) {
    commit_vv_ = commit_vv;
    partition_id_ = partition_id;
    pcid_ = pcid;
#if defined( RECORD_COMMIT_TS )
    txn_ts_ = std::chrono::duration_cast<std::chrono::nanoseconds>(
                  std::chrono::system_clock::now().time_since_epoch() )
                  .count();
#endif
    for( unsigned int pos = 0; pos < stop_subscribing_.size(); pos++ ) {
        // store the stop subscription state
        auto     pid = stop_subscribing_.at( pos ).identifier_;
        uint64_t version = get_snapshot_version( commit_vv, pid );
        stop_subscribing_.at( pos ).propagation_config_.offset = version;
    }
}

