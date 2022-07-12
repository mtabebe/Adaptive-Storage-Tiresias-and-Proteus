#pragma once

#include "data_persister.h"

#include "../common/partition_funcs.h"
#include "../data-site/db/partition.h"
#include "../data-site/db/cell_identifier.h"

class data_site_table_persister {
   public:
    data_site_table_persister(
        const std::string&                            output_file_name,
        const std::string&                            partition_directory,
        const std::vector<propagation_configuration>& site_propagation_configs,
        uint32_t                                      site_id );

    void init();
    // close and flush
    void finish();
    // explicit flush
    void flush();

    void set_chunk_information( uint64_t num_partitions, const std::string& dir,
                                uint32_t table_id,
                                uint32_t desired_chunk_size );

    // file layout
    // identity: id, num_columns, col_types, default_partition_size, default_partition_col_size, num_records_in_chain,
    //           num_records_in_snapshot_chain, site_location, table_name,
    // [partition]
    //      partition_start, partition_end, column_start, column_end,
    //      is_master, layout
    // END (k_unassigned_key)
    //
    // partition file layout
    // table_id, partition_start, partition_end, column_start, column_end
    // master, version
    // update destination slot, update destination offset
    // [records]
    //      record_key
    //      versioned_pid
    //      version
    //      current_pid
    //      // per col
    //          is_present
    //          len
    //          data

    void persist_table_info( const table_metadata& table_info );
    void persist_partition( std::shared_ptr<partition> part );
    void persist_partition( std::shared_ptr<partition>     part,
                            const partition_type::type&    part_type,
                            const storage_tier_type::type& storage_type );

   private:
    void persist_master_partition( std::shared_ptr<partition> part );
    // slot, offset
    std::tuple<uint32_t, uint64_t> translate_update_information_to_slot(
        const update_propagation_information& up_info ) const;

    data_persister persister_;
    std::string    partition_directory_;

    bool                         persist_chunked_;
    std::vector<data_persister*> persisters_;
    uint32_t                     cur_persister_pos_;


    std::unordered_map<uint32_t, uint32_t> update_prop_configs_to_slots_;

    uint32_t site_id_;
};
