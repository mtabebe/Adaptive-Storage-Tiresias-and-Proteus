#pragma once

#include "data_persister.h"
#include "data_reader.h"
#include "data_site_table_persister.h"
#include "data_site_table_reader.h"
#include "persistence_configs.h"

class data_site_persistence_manager {
   public:
    data_site_persistence_manager(
        const std::string& data_site_directory,
        const std::string& partition_directory, tables* data_tables,
        std::shared_ptr<update_destination_generator> update_generator,
        const std::vector<std::vector<propagation_configuration>>&
                                   site_propagation_configs,
        const persistence_configs& configs, uint32_t site_id );

    void            load();
    tables_metadata load_tables_metadata();
    void load_tables( uint32_t num_tables );

    void persist();

    void persist_data_site_metadata();
    void persist_tables();
    void persist_table( uint32_t table_id );

   private:
    void load_table_data( data_site_table_reader* loader );
    void load_table_chunks( int32_t                 num_table_chunks,
                            data_site_table_reader* meta_loader );
    void load_chunk_of_table( data_site_table_reader* loader );

    std::string data_site_directory_;
    std::string partition_directory_;

    tables*                                       data_tables_;
    std::shared_ptr<update_destination_generator> update_generator_;
    std::vector<std::vector<propagation_configuration>>
        site_propagation_configs_;
    persistence_configs configs_;

    uint32_t site_id_;
};
