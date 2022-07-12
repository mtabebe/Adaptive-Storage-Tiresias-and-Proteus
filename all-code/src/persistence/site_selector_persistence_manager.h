#pragma once

#include "../common/bucket_funcs.h"
#include "../gen-cpp/gen-cpp/proto_types.h"
#include "data_persister.h"
#include "data_reader.h"
#include "persistence_configs.h"
#include "site_selector_table_persister.h"
#include "site_selector_table_reader.h"

class site_selector_persistence_manager {
   public:
    site_selector_persistence_manager(
        const std::string&                           directory,
        multi_version_partition_data_location_table* data_location_table,
        const std::vector<std::vector<propagation_configuration>>& prop_configs,
        const persistence_configs&                                 configs );

    void load();
    int  load_num_tables();
    void load_tables( uint32_t num_tables );

    void persist();

    void persist_location_metadata();
    void persist_tables();
    void persist_table( uint32_t table_id );

   private:
    void load_table_data( site_selector_table_reader* loader );
    void load_table_chunks( int32_t                     num_table_chunks,
                            site_selector_table_reader* meta_loader );
    void load_chunk_of_table( site_selector_table_reader* loader );

    std::string                                  directory_;
    multi_version_partition_data_location_table* data_location_table_;
    std::vector<std::vector<propagation_configuration>> prop_configs_;

    persistence_configs configs_;
};
