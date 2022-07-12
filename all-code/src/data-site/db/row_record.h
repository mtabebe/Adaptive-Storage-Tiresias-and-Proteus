#pragma once
#include <vector>

#include "../../common/hw.h"
#include "../../common/packed_pointer.h"
#include "../../persistence/data_persister.h"
#include "../../persistence/data_reader.h"
#include "packed_cell_data.h"
#include "transaction_state.h"
#include "version_types.h"

class row_record {
   public:
    row_record();
    ~row_record();

    void init_num_columns( uint32_t num_columns, bool do_data_copy );

    bool     is_present() const;
    uint32_t get_num_columns() const;

    void deep_copy( const row_record& other );
    void set_as_deleted();

    void split_vertically( uint32_t col_split, row_record* left,
                           row_record* right ) const;
    void merge_vertically( const row_record* left, const row_record* right,
                           uint32_t num_columns, uint32_t merge_point );

    uint64_t get_version() const;
    uint64_t spin_for_committed_version() const;
    uint64_t spin_until_not_in_commit_version() const;

    bool update_if_tombstone( uint64_t& partition_id ) const;
    void set_tombstone( uint64_t partition_id );

    void set_snapshot_vector( snapshot_vector* snap );

    ALWAYS_INLINE void set_transaction_state( transaction_state* txn_state );
    ALWAYS_INLINE void set_version( uint64_t version );

    packed_cell_data* get_row_data() const;
    bool get_cell_data( uint32_t col_pos, packed_cell_data& cell_data ) const;

    void restore_from_disk( data_reader*                       reader,
                            const std::vector<cell_data_type>& col_types );
    void persist_to_disk( data_persister*                    persister,
                          const std::vector<cell_data_type>& col_types ) const;

    friend std::ostream& operator<<( std::ostream& os, const row_record& r );

   private:
    // 8 bytes
    // we use release consume semantics
    std::atomic<packed_pointer> mvcc_ptr_;

    // 12 bytes
    uint16_t is_tombstone_;
    uint16_t is_deleted_;

    uint32_t flags_;

    // 4 bytes
    uint32_t num_columns_;

    // 8 bytes
    packed_cell_data* data_;
};

void set_version( transaction_state*          txn_state,
                  const std::vector<row_record*>& row_records );

#include "row_record-inl.h"
