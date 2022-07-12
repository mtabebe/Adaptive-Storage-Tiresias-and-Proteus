#pragma once

#include <cstdint>

#include "../../common/hw.h"
#include "../db/partition_column_operation_identifier.h"
#include "../db/write_buffer.h"
#include "deserialized_cell_op.h"

struct serialized_update {
    int64_t length_;
    char*   buffer_;
};

struct deserialized_update {
    partition_column_identifier pcid_;
    uint32_t                    is_new_partition_;
    uint64_t                    txn_ts_;
    snapshot_vector             commit_vv_;

    std::vector<deserialized_cell_op>                  cell_updates_;
    std::vector<partition_column_operation_identifier> partition_updates_;

    std::vector<update_propagation_information> start_subscribing_;
    std::vector<update_propagation_information> stop_subscribing_;
    std::vector<update_propagation_information> switch_subscription_;

    void* table_;
};

// Serialization layout (len >= 20)
// total length (i64)
// message id (i64)
// magic_version (i32)
#if defined( RECORD_COMMIT_TS )
// timestamp (i64)
#endif
// ACTUAL SERIALIZATION DETAILS
// footer (i64) (length * magic_version * message id)

// SERIALIZATION DETAILS V2
// total length (i64)
// message id (i64)
// magic_version (i32)
#if defined( RECORD_COMMIT_TS )
// timestamp (i64)
#endif
// partition identifier ( i32 + (2 * i64) )
//   - table_id (i32)
//   - partition_start_ (i64)
//   - partition_end_ (i32)
// is_new_partition_ (i32)
// num pcids (i32)
// version vector size (i32) (2 * pcid * i64)
//   --pcid (i64)
//   --version (i64)
// num partition ops (i32)
// partition column operation struct
//    - partition_column_identifier src_identifier_;
//      -- table_id (i32)
//      -- partition_start_ (i64)
//      -- partition_end_ (i32)
//      -- column_start_ (i32)
//      -- column_end_ (i32)
//    - data_64 (i64)
//    - data_32 (i32)
//    - op_code_ (i32);
// num partition ops (i32)
// num cells (i32)
// cell struct
// --cid.table_id (i32)
// --cid.col_id (i32)
// --cid.key_ (i64)
// --cid.op_code (i64)
// -- based on op code
// [INSERT/WRITE]
// ----length (i32)
// ----buffer (length size)
// [DELETE]
// ---- nothing
// [start subscribing size]
// num subscriptions
// subscription size
// -- partition id size
// -- propagation_configuration size
// --- type (i32)
// --- partition i32
// --- offset  i64
// ---  topic string
// ---- len (i32)
// ---- buffer (length size)
// --- do seek ( i8 )
// [ stop subscribing size]
// num subscriptions
// subscription size
// [ switch subscription size]
// num subscriptions
// subscription size
// footer (i64) (lenght * version)

int64_t compute_serialization_length_2( write_buffer* write_buf );
int64_t compute_serialization_length_of_propagation_config(
    const propagation_configuration& config );

const packed_cell_data& get_cell_data_from_vri(
    const partition_column_identifier&     pcid,
    const versioned_row_record_identifier& vri );

void write_into_serialized_update_2( serialized_update& update,
                                     write_buffer*      write_buf );
uint64_t serialize_record_2( const partition_column_identifier&     pcid,
                             const versioned_row_record_identifier& vri,
                             serialized_update& update, uint64_t pos );
uint64_t serialize_cell_2( const partition_column_identifier&    pcid,
                           const versioned_cell_data_identifier& vci,
                           serialized_update& update, uint64_t pos );

uint64_t serialize_partition_2( const partition_column_operation_identifier& poi,
                                serialized_update& update, uint64_t pos );
uint64_t serialize_partition_id_2( const partition_column_identifier& pcid,
                                 serialized_update& update, uint64_t pos );

serialized_update serialize_write_buffer( write_buffer* write_buf );



deserialized_update* deserialize_update( const serialized_update& update );
deserialized_update* deserialize_update_2( const serialized_update& update );
uint64_t deserialize_cell_2( const serialized_update& update,
                             deserialized_update* deserialized, uint64_t pos );

uint64_t serialize_update_propagation_information_2(
    const update_propagation_information& info, serialized_update& update,
    uint64_t pos );
uint64_t deserialize_partition_2( const serialized_update& update,
                                  deserialized_update*     deserialized,
                                  uint64_t                 pos );
uint64_t deserialize_partition_id_2( const serialized_update&     update,
                                     partition_column_identifier* pcid,
                                     uint64_t                     pos );
uint64_t deserialize_update_propagation_information_2(
    const serialized_update&                     update,
    std::vector<update_propagation_information>* deserialized, uint64_t pos );

void clear_serialized_updates( std::vector<serialized_update>& updates );

// helpers based on consistent positions
ALWAYS_INLINE uint64_t
    get_serialized_update_len( const serialized_update& update );
ALWAYS_INLINE uint64_t
    get_serialized_update_message_id( const serialized_update& update );
ALWAYS_INLINE uint32_t
    get_serialized_update_version( const serialized_update& update );
ALWAYS_INLINE uint64_t
    get_serialized_update_txn_timestamp( const serialized_update& update );
ALWAYS_INLINE uint64_t
    get_serialized_update_txn_timestamp_offset( const serialized_update& update );
ALWAYS_INLINE uint64_t
    get_serialized_update_footer( const serialized_update& update );
ALWAYS_INLINE uint64_t get_serialized_update_min_length();

ALWAYS_INLINE bool is_serialized_update_footer_ok(
    const serialized_update& update );

#include "write_buffer_serialization-inl.h"
