#include "cell_data_type.h"

#include <glog/logging.h>

std::string cell_data_type_to_string( const cell_data_type& type ) {
    switch( type ) {
        case cell_data_type::UINT64:
            return "UINT64";
        case cell_data_type::INT64:
            return "INT64";
        case cell_data_type::DOUBLE:
            return "DOUBLE";
        case cell_data_type::STRING:
            return "STRING";
        case cell_data_type::MULTI_COLUMN:
            return "MULTI_COLUMN";
    }
    return "UNKNOWN";
}

data_type::type cell_data_type_to_data_type( const cell_data_type& type ) {
    switch( type ) {
        case cell_data_type::UINT64:
            return data_type::type::UINT64;
        case cell_data_type::INT64:
            return data_type::type::INT64;
        case cell_data_type::DOUBLE:
            return data_type::type::DOUBLE;
        case cell_data_type::STRING:
            return data_type::type::STRING ;
        case cell_data_type::MULTI_COLUMN:
            DLOG( WARNING ) << "Cannot convert" << cell_data_type::MULTI_COLUMN
                            << ", to data_type";
    }
    return data_type::type::UINT64;
}
cell_data_type data_type_to_cell_data_type( const data_type::type& type ) {
    switch( type ) {
        case data_type::type::UINT64:
            return cell_data_type::UINT64;
        case data_type::type::INT64:
            return cell_data_type::INT64;
        case data_type::type::DOUBLE:
            return cell_data_type::DOUBLE;
        case data_type::type::STRING:
            return cell_data_type::STRING;
    }
    return cell_data_type::UINT64;
}

