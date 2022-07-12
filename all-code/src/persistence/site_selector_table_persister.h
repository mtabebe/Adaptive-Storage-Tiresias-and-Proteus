#pragma once

#include <vector>

#include "data_persister.h"

#include "../common/partition_funcs.h"
#include "../data-site/db/cell_identifier.h"
#include "../data-site/db/partition_metadata.h"
#include "../site-selector/partition_payload.h"

class site_selector_table_persister {
   public:
    site_selector_table_persister( const std::string& output_file_name );

    void init();
    // close and flush
    void finish();
    // explicit flush
    void flush();

    void set_chunk_information( uint64_t num_partitions, const std::string& dir,
                                uint32_t table_id,
                                uint32_t desired_chunk_size );

    // file layout
    // identity: id, num_columns, col_types, default_partition_size, default_column_size, default_part_type, num_records_in_chain,
    //           num_records_in_snapshot_chain, table_name,
    // [partition]
    //      partition_start, partition_end,column_start, column_end,
    //      master_location, master_type
    //      replica_locations (num, then each location and type)
    //      update_destination_slot_
    // END (k_unassigned_key)

    void persist_table_info( const table_metadata& table_info );
    void persist_partition_payload(
        std::shared_ptr<partition_payload> payload );

   private:
    data_persister persister_;

    bool                         persist_chunked_;
    std::vector<data_persister*> persisters_;
    uint32_t                     cur_persister_pos_;
};
