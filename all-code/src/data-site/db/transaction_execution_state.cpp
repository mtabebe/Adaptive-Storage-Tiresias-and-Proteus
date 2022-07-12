#include "transaction_execution_state.h"

#include "mvcc_chain.h"

transaction_execution_state::transaction_execution_state()
    : master_site_( k_unassigned_master ),
      txn_state_(),
      lwm_(),
      repartitioned_records_(),
      buffer_(),
      no_prop_buffer_() {}

transaction_execution_state::~transaction_execution_state() {}

void transaction_execution_state::reset_state( uint32_t master_site ) {
    master_site_ = master_site;
    txn_state_.set_not_committed();
    lwm_.clear();
    repartitioned_records_.clear();
    buffer_.reset_state();
    no_prop_buffer_.reset_state();
    new_update_destination_ = nullptr;
}

void transaction_execution_state::abort_write() {
    txn_state_.set_aborted();

    buffer_.set_version( K_ABORTED );
    no_prop_buffer_.set_version( K_ABORTED );

    for( row_record* r : repartitioned_records_ ) {
        r->set_version( K_ABORTED );
    }

    repartitioned_records_.clear();
    buffer_.reset_state();
    no_prop_buffer_.reset_state();
}

void transaction_execution_state::set_low_watermark(
    const snapshot_vector& gc_lwm ) {
    lwm_ = gc_lwm;
}

transaction_state*    transaction_execution_state::get_transaction_state() {
    return &txn_state_;
}
const snapshot_vector& transaction_execution_state::get_low_watermark() const {
    return lwm_;
}

std::vector<row_record*>&
    transaction_execution_state::get_repartitioned_records() {
    return repartitioned_records_;
}

write_buffer* transaction_execution_state::get_write_buffer() {
    return &buffer_;
}
write_buffer* transaction_execution_state::get_write_no_prop_buffer() {
    return &no_prop_buffer_;
}

uint32_t transaction_execution_state::get_master_site() const {
    return master_site_;
}

void transaction_execution_state::set_committed(
    const snapshot_vector& commit_vv, const partition_column_identifier& pcid,
    uint64_t partition_id, uint64_t version ) {

    buffer_.set_version( version );
    buffer_.store_propagation_information( commit_vv, pcid, partition_id,
                                           version );

    no_prop_buffer_.set_version( version );
    no_prop_buffer_.store_propagation_information( commit_vv, pcid, partition_id,
                                                   version );

    for( row_record* r : repartitioned_records_ ) {
        r->set_version( version );
    }
}

void transaction_execution_state::add_to_write_buffer(
    const cell_identifier& cid, row_record* r, uint32_t op ) {
    versioned_row_record_identifier vri;
    vri.add_db_op( cid, r, op );
    buffer_.add_to_record_buffer( std::move( vri ) );
}
void transaction_execution_state::add_to_write_buffer(
    const cell_identifier& cid, packed_cell_data* p, uint32_t op ) {
    versioned_cell_data_identifier vci;
    vci.add_db_op( cid, p, op );
    buffer_.add_to_cell_buffer( std::move( vci ) );
}

void transaction_execution_state::add_to_no_prop_write_buffer(
    const cell_identifier& cid, row_record* r, uint32_t op ) {
    versioned_row_record_identifier vri;
    vri.add_db_op( cid, r, op );
    no_prop_buffer_.add_to_record_buffer( std::move( vri ) );
}

void transaction_execution_state::add_to_write_buffer(
    const partition_column_identifier& id, uint64_t data_64, uint32_t data_32,
    uint32_t op ) {
    partition_column_operation_identifier poi;
    poi.add_partition_op( id, data_64, data_32, op );
    buffer_.add_to_partition_buffer( std::move( poi ) );
}

void transaction_execution_state::add_to_write_buffer(
    const update_propagation_information& update_prop_info,
    bool                                  do_start_subscribing ) {
    buffer_.add_to_subscription_buffer( update_prop_info,
                                        do_start_subscribing );
}

void transaction_execution_state::add_to_write_buffer(
    const update_propagation_information& old_prop_info,
    const update_propagation_information& new_prop_info ) {
    buffer_.add_to_subscription_switch_buffer( old_prop_info, new_prop_info );
}
void transaction_execution_state::add_new_update_destination(
    std::shared_ptr<update_destination_interface> new_update_destination ) {
    new_update_destination_ = new_update_destination;
}
std::shared_ptr<update_destination_interface>
    transaction_execution_state::get_new_update_destination() {
    return new_update_destination_;
}

