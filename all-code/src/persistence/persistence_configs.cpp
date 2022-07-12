#include "persistence_configs.h"

#include <glog/logging.h>

persistence_configs construct_persistence_configs(
    bool load_chunked_tables, bool persist_chunked_tables,
    int32_t chunked_table_size ) {
    persistence_configs configs;

    configs.load_chunked_tables_ = load_chunked_tables;
    configs.persist_chunked_tables_ = persist_chunked_tables;
    configs.chunked_table_size_ = chunked_table_size;

    return configs;
}

std::ostream& operator<<( std::ostream&              os,
                          const persistence_configs& configs ) {

    os << "Persistence Configs: ["
       << " load_chunked_tables_:" << configs.load_chunked_tables_
       << " persist_chunked_tables_:" << configs.persist_chunked_tables_
       << " chunked_table_size_:" << configs.chunked_table_size_ << " ]";

    return os;
}

