#pragma once

#include <glog/logging.h>

inline bool packed_cell_data::is_present() const {
    bool present = ( is_deleted_ == 0 );
    return present;
}

inline int16_t  packed_cell_data::get_type() const { return record_type_; }
inline uint32_t packed_cell_data::get_length() const { return data_size_; }

inline char* packed_cell_data::get_buffer_data() const {
    DVLOG( 20 ) << "Getting record as buffer";

    char* data_as_str = nullptr;
    if( is_pointer_ == 0 ) {
        data_as_str = (char*) &data_;
    } else {
        data_as_str = (char*) data_;
    }
    DVLOG( 20 ) << "Getting record as buffer:"
                << std::string( data_as_str, data_size_ );
    return data_as_str;
}
