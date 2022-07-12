#pragma once

#include <vector>

#include "../../common/snapshot_vector_funcs.h"
#include "../update-propagation/update_destination_interface.h"
#include "row_record.h"
#include "transaction_state.h"
#include "write_buffer.h"

class transaction_execution_state {
   public:
    transaction_execution_state();
    ~transaction_execution_state();

    void reset_state( uint32_t master_site );

    void set_low_watermark( const snapshot_vector& gc_lwm );

    transaction_state*        get_transaction_state();
    const snapshot_vector&    get_low_watermark() const;
    std::vector<row_record*>& get_repartitioned_records();
    write_buffer*             get_write_buffer();
    write_buffer*             get_write_no_prop_buffer();

    uint32_t get_master_site() const;

    void abort_write();

    void set_committed( const snapshot_vector&             commit_vv,
                        const partition_column_identifier& pcid,
                        uint64_t partition_col_id, uint64_t version );

    void add_to_write_buffer( const cell_identifier& cid, row_record* r,
                              uint32_t op );
    void add_to_write_buffer( const cell_identifier& cid, packed_cell_data* p,
                              uint32_t op );

    void add_to_no_prop_write_buffer( const cell_identifier& cid, row_record* r,
                                      uint32_t op );
    void add_to_write_buffer( const partition_column_identifier& id,
                              uint64_t data_64, uint32_t data_32, uint32_t op );
    void add_to_write_buffer(
        const update_propagation_information& update_prop_info,
        bool                                  do_start_subscribing );
    void add_to_write_buffer(
        const update_propagation_information& old_prop_info,
        const update_propagation_information& new_prop_info );

    void add_new_update_destination(
        std::shared_ptr<update_destination_interface> new_update_destination );
    std::shared_ptr<update_destination_interface> get_new_update_destination();

   private:
    uint32_t                 master_site_;
    transaction_state        txn_state_;
    snapshot_vector          lwm_;
    std::vector<row_record*> repartitioned_records_;
    write_buffer             buffer_;
    write_buffer             no_prop_buffer_;

    std::shared_ptr<update_destination_interface> new_update_destination_;
};
