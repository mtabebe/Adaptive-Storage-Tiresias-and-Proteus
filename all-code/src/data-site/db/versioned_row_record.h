#pragma once

#include <tuple>

#include "../../common/constants.h"
#include "../../common/hw.h"

#if 0 // HDB-OUT
#include "../../persistence/table_persister.h"
#endif

#include "cell_identifier.h"
#include "mvcc_chain.h"

class versioned_row_record {
   public:
    versioned_row_record();
    ~versioned_row_record();

    void init( int64_t  key = k_unassigned_key,
               uint64_t partition_id = k_unassigned_partition,
               int32_t  num_records_in_chain = k_num_records_in_chain );

    bool repartition( std::vector<row_record*>& repartitioned_records,
                      int64_t key, uint64_t partition_id,
                      transaction_state*     txn_state,
                      int32_t                num_records_in_chain,
                      const snapshot_vector& gc_lwm );

    bool          has_record_been_inserted() const;
    ALWAYS_INLINE int64_t get_key() const;

    uint64_t get_partition_id( int64_t key ) const;

    row_record* read_record( int64_t                key,
                             const snapshot_vector& cli_version ) const;
    row_record* read_latest_record( int64_t key ) const;
    row_record* write_record( int64_t key, transaction_state* txn_state,
                          int32_t                num_records_in_chain,
                          const snapshot_vector& gc_lwm,
                          int32_t always_gc = k_always_gc_on_writes );
    row_record* insert_record( int64_t key, transaction_state* txn_state,
                               uint64_t partition_id,
                               int32_t  num_records_in_chain );

    std::tuple<row_record*, uint64_t>
        read_latest_record_and_partition_column_information(
            int64_t key ) const;

    void gc_record( const snapshot_vector& min_version );

#if 0 // HDB-OUT
    void persist_versioned_row_record( table_persister& persister ) const;
#endif

   private:
    int64_t record_key_;
    // Protected by the counter_lock with acquire/release semantics.
    // If we rely on the state we just loaded from the counter lock, we'll see
    // the chain we need. We might see intermediate state from future
    // transactions
    // modifying records next to us, but our functions should handle this
    mvcc_chain* record_chain_;
};

#include "versioned_row_record-inl.h"
