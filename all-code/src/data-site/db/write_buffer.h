#pragma once
#include "../../common/hw.h"
#include "../update-propagation/update_propagation_types.h"
#include "partition_column_operation_identifier.h"
#include "versioned_data_identifier.h"
#include <chrono>
#include <cstdlib>
#include <memory>

class write_buffer {
   public:
    write_buffer();
    ~write_buffer();

    void               reset_state();

    ALWAYS_INLINE void add_to_record_buffer(
        versioned_row_record_identifier&& modified_record );
    ALWAYS_INLINE void add_to_cell_buffer(
        versioned_cell_data_identifier&& modified_cell );

    ALWAYS_INLINE void add_to_partition_buffer(
        partition_column_operation_identifier&& poi );
    ALWAYS_INLINE void add_to_subscription_buffer(
        const update_propagation_information& update_prop_info,
        bool                                  do_start_subscribing );
    ALWAYS_INLINE void add_to_subscription_switch_buffer(
        const update_propagation_information& old_sub,
        const update_propagation_information& new_sub );

    // ALWAYS_INLINE const versioned_row_record_identifier *get_buffer() const;
    ALWAYS_INLINE bool empty() const;

    void store_propagation_information( const snapshot_vector& commit_vv,
                                        const partition_column_identifier& pcid,
                                        uint64_t partition_id,
                                        uint64_t version );
    void set_version( uint64_t version ) const;

    snapshot_vector             commit_vv_;
    uint64_t                    partition_id_;
    partition_column_identifier pcid_;

    uint32_t is_new_partition_;
    uint32_t do_not_propagate_;

    std::vector<versioned_row_record_identifier>       record_buffer_;
    std::vector<versioned_cell_data_identifier>        cell_buffer_;

    std::vector<partition_column_operation_identifier> partition_buffer_;
    std::vector<update_propagation_information>        start_subscribing_;
    std::vector<update_propagation_information>        stop_subscribing_;
    std::vector<update_propagation_information>        switch_subscription_;
#if defined( RECORD_COMMIT_TS )
    uint64_t txn_ts_;
#endif
};


#include "write_buffer-inl.h"
