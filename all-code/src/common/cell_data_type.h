#pragma once

#include <string>

#include "../gen-cpp/gen-cpp/proto_types.h"
#include "snapshot_vector_funcs.h"

const static int16_t K_INT_RECORD_TYPE = 1;
const static int16_t K_STRING_RECORD_TYPE = 2;
const static int16_t K_BUFFER_RECORD_TYPE = 3;
const static int16_t K_SNAPSHOT_VECTOR_RECORD_TYPE = 4;

enum class cell_data_type {
    UINT64,
    INT64,
    DOUBLE,
    STRING,
    MULTI_COLUMN,
};

std::string cell_data_type_to_string( const cell_data_type& type );

data_type::type cell_data_type_to_data_type( const cell_data_type& type );
cell_data_type data_type_to_cell_data_type( const data_type::type& type );

inline std::ostream& operator<<( std::ostream& os, const cell_data_type& c ) {
    os << cell_data_type_to_string( c );
    return os;
}
