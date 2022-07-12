#pragma once

#include "../common/constants.h"

class persistence_configs {
  public:
   bool load_chunked_tables_;
   bool persist_chunked_tables_;
   int32_t chunked_table_size_;
};

persistence_configs construct_persistence_configs(
    bool    load_chunked_tables = k_load_chunked_tables,
    bool    persist_chunked_tables = k_persist_chunked_tables,
    int32_t chunked_table_size = k_chunked_table_size );

std::ostream& operator<<( std::ostream&              os,
                          const persistence_configs& configs );

