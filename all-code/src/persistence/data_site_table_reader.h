
#include "../common/hw.h"
#include "data_reader.h"

#include "../data-site/db/tables.h"
#include "../data-site/update-propagation/update_destination_generator.h"

class data_site_table_reader {
   public:
    data_site_table_reader(
        const std::string& input_file_name,
        const std::string& partition_directory, tables* data_tables,
        std::shared_ptr<update_destination_generator> update_generator,
        const std::vector<std::vector<propagation_configuration>>&
                 site_propagation_configs,
        uint32_t table_id, uint32_t site_id );

    // open
    void init();
    // close
    void close();

    table_metadata load_table_metadata();
    int32_t        load_num_table_chunks();
    void           load_persisted_table_data();

    std::tuple<std::shared_ptr<partition>, uint32_t /*slot*/,
               uint64_t /*offset*/, uint64_t /*version*/>
        read_entry();

    std::tuple<std::shared_ptr<partition>, uint32_t /*slot*/,
               uint64_t /*offset*/, uint64_t /*version*/>
        read_partition( const partition_column_identifier& pid,
                        uint32_t                           master_location,
                        const partition_type::type&        part_type,
                        const storage_tier_type::type&     storage_type );

    uint32_t get_table_id() const;
    std::string get_partition_directory() const;

   private:
    std::shared_ptr<update_destination_interface> get_update_destination(
        uint32_t update_destination_slot );

    void start_subscription( const partition_column_identifier& pid, uint32_t master,
                             uint32_t update_slot, uint64_t offset,
                             uint64_t version );

    data_reader data_reader_;
    std::string partition_directory_;

    tables*                                       data_tables_;
    std::shared_ptr<update_destination_generator> update_generator_;
    std::vector<std::vector<propagation_configuration>>
        site_propagation_configs_;

    uint32_t table_id_;
    uint32_t site_id_;
};
