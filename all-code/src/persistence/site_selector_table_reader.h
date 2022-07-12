#pragma once

#include "../common/hw.h"
#include "data_reader.h"

#include "../site-selector/multi_version_partition_table.h"

class site_selector_table_reader {
   public:
    site_selector_table_reader(
        const std::string&                           input_file_name,
        multi_version_partition_data_location_table* data_loc_tab,
        uint32_t                                     table_id );

    // open
    void init();
    // close
    void close();

    table_metadata load_table_metadata();
    void           load_persisted_table_data();
    int32_t        load_num_table_chunks();

    std::shared_ptr<partition_payload> read_entry();

    uint32_t get_table_id() const;
    multi_version_partition_data_location_table* get_data_location_table()
        const;

   private:
    data_reader               data_reader_;
    multi_version_partition_data_location_table* data_loc_tab_;
    uint32_t table_id_;
};
