#pragma once

#include <unordered_map>

#include "../common/bucket_funcs.h"
#include "../data-site/db/tables.h"
#include "../data-site/db/version_types.h"
#include "data_location_rewriter_interface.h"
#include "data_site_persistence_manager.h"
#include "site_selector_persistence_manager.h"

class persistence_editor {
   public:
    persistence_editor( data_location_rewriter_interface* rewriter,
                        const std::string&                ss_in_directory,
                        const std::string&                sm_in_directory,
                        const std::string&                part_in_directory,
                        const std::string&                ss_out_directory,
                        const std::string&                sm_out_directory,
                        const std::string&                part_out_directory );

    void rewrite();

   private:
    partition_locations_map get_in_partition_locations();
    void                    write_tables_with_new_partitions(
        const partition_locations_map& partition_locations,
        const partition_locations_map& old_partition_locations );
    void persist_site_selector_table(
        const table_metadata& table_meta,
        const partition_column_identifier_map_t<partition_location_information>&
            partition_locations );

    void rewrite_data_site_table(
        uint32_t table_id, const std::vector<table_metadata>& tables_meta,
        const partition_column_identifier_map_t<partition_location_information>&
            partition_locations,
        const partition_column_identifier_map_t<partition_location_information>&
                                    old_partition_locations,
        const std::vector<tables*>& site_data_tables );

    void write_partition( std::vector<data_site_table_persister*>& persisters,
                          std::shared_ptr<partition>               part,
                          const partition_location_information&    part_loc );

    void initialize_table( data_site_table_reader*    reader,
                           data_site_table_persister* persister,
                           uint32_t table_id, uint64_t num_partitions,
                           const std::string& out_dir );
    void finish_table( data_site_table_reader*    reader,
                       data_site_table_persister* persister );

    void load_site_selector_table_data(
        site_selector_table_reader* loader,
        partition_column_identifier_map_t<partition_location_information>&
            partition_locations );
    void load_site_selector_table_partitions(
        site_selector_table_reader* loader,
        partition_column_identifier_map_t<partition_location_information>&
            partition_locations );

    void rewrite_at_all_site_partition(
        const partition_column_identifier&              pid,
        const partition_location_information&    part_loc,
        std::vector<data_site_table_persister*>& persisters,
        std::vector<data_site_table_reader*>&    readers );

    data_location_rewriter_interface* rewriter_;

    std::string ss_in_directory_;
    std::string sm_in_directory_;
    std::string part_in_directory_;

    std::string ss_out_directory_;
    std::string sm_out_directory_;
    std::string part_out_directory_;
};

