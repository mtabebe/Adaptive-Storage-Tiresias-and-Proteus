#pragma once

#include <string>

#include "../../common/hw.h"
#include "../../gen-cpp/gen-cpp/proto_types.h"

enum update_destination_type {
    UNKNOWN_UPDATE_DESTINATION,
    VECTOR_DESTINATION,
    KAFKA_DESTINATION,
    NO_OP_DESTINATION,
};

enum update_source_type {
    UNKNOWN_UPDATE_SOURCE,
    VECTOR_SOURCE,
    KAFKA_SOURCE,
    NO_OP_SOURCE,
};

class update_propagation_information {
   public:
    propagation_configuration propagation_config_;
    partition_column_identifier identifier_;
    bool                      do_seek_;
};

ALWAYS_INLINE std::string update_destination_type_string(
    const update_destination_type& up_type );
ALWAYS_INLINE update_destination_type
    string_to_update_destination_type( const std::string& s );

ALWAYS_INLINE std::string update_source_type_string(
    const update_source_type& up_type );
ALWAYS_INLINE update_source_type
    string_to_update_source_type( const std::string& s );

#include "update_propagation_types-inl.h"
