#pragma once

#include "../../common/hw.h"
#include "../../common/snapshot_vector_funcs.h"
#if 0  // HDB-OUT
#include "../../persistence/table_persister.h"
#endif
#include "mvcc_types.h"
#include "row_record.h"

static const uint64_t k_unassigned_partition = UINT64_MAX;
static const uint32_t k_unassigned_master = UINT32_MAX;

class mvcc_chain {
   public:
    mvcc_chain();
    ~mvcc_chain();

    void init( int32_t num_row_records, uint64_t partition_id,
               mvcc_chain* next_chain );
    ALWAYS_INLINE bool is_chain_full() const;
    ALWAYS_INLINE bool is_chain_empty() const;
    ALWAYS_INLINE uint64_t get_partition_id() const;
    // better make sure that this is sane (i.e.) there
    // are no row_records
    ALWAYS_INLINE void set_partition_id( uint64_t partition_id );

    // will use a row_record, return null if not space
    row_record* create_new_row_record( transaction_state* txn_state );
    row_record* create_new_row_record( uint64_t version );
    row_record* create_new_row_record( uint64_t         version,
                                           snapshot_vector* vector_p );

    // will return the row_record that is within the window of this session
    // state
    // will return null if there isn't one that satisfies the state
    // to satisfy a version a row_record must be the largest row_record <=
    // submitted
    // state
    ALWAYS_INLINE row_record* find_row_record(
        const snapshot_vector& snapshot ) const;

    ALWAYS_INLINE row_record* get_latest_row_record(
        bool recurse = true ) const;
    row_record* internal_get_latest_row_record( bool      recurse,
                                                uint64_t& partition_id ) const;

    ALWAYS_INLINE mvcc_chain* get_next_link_in_chain() const;

    mvcc_chain* garbage_collect_chain( const snapshot_vector& min_version );

#if 0  // HDB-OUT
    void persist_chain( table_persister& persister ) const;
#endif

   private:
    class found_version_information {
       public:
        mvcc_chain* found_chain_;
        uint64_t    partition_id_;
        uint64_t    largest_satisfying_version_;
        uint64_t    change_partition_id_;
    };
#if 0
    static bool safe_to_delete( const snapshot_vector& min_version,
                                const snapshot_vector& chain_version );
#endif
    std::tuple<uint64_t, uint64_t> get_max_version_on_chain() const;

    template <bool store_version_information>
    row_record*    internal_find_row_record(
        const snapshot_vector&     snapshot,
        found_version_information* found_info ) const;

    template <row_record_creation_type rec_type>
    row_record* create_new_row_record( void* v );

    // header = 16 bytes
    // chains can be at most 2^32 size
    int32_t row_record_len_;

    std::atomic<int32_t> next_row_record_pos_;
    // can have at most 2^64 partitions
    std::atomic<uint64_t> partition_id_;

    // data = 8 bytes
    // fill from back to front
    // aborted row_records are left in as NOT_COMMITTED state
    row_record* row_records_;

    // next = 8 bytes
    mvcc_chain* next_chain_;
};

#include "mvcc_chain.tcc"
#include "mvcc_chain-inl.h"
