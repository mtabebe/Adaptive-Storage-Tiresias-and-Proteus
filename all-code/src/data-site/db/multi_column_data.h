#pragma once

#include <cstdint>
#include <string>
#include <tuple>
#include <vector>

#include "../../common/cell_data_type.h"
#include "../../persistence/data_persister.h"
#include "../../persistence/data_reader.h"

class multi_column_data_type {
  public:
   multi_column_data_type( const cell_data_type& c, uint32_t data_size );

   friend std::ostream& operator<<( std::ostream&                 os,
                                    const multi_column_data_type& data );

   friend bool operator<( const multi_column_data_type& lhs,
                          const multi_column_data_type& rhs );

   friend bool operator==( const multi_column_data_type& lhs,
                           const multi_column_data_type& rhs );


   cell_data_type type_;
   uint32_t       size_;
};

inline bool operator!=( const multi_column_data_type& lhs,
                        const multi_column_data_type& rhs ) {
    return !( lhs == rhs );
}

inline bool operator>( const multi_column_data_type& lhs,
                       const multi_column_data_type& rhs ) {
    return rhs < lhs;
}
inline bool operator<=( const multi_column_data_type& lhs,
                        const multi_column_data_type& rhs ) {
    return !( lhs > rhs );
}
inline bool operator>=( const multi_column_data_type& lhs,
                        const multi_column_data_type& rhs ) {
    return !( lhs < rhs );
}

std::vector<multi_column_data_type> construct_multi_column_data_types(
    const std::vector<cell_data_type>& col_types );

class multi_column_data {
   public:
    multi_column_data();
    multi_column_data( const std::vector<multi_column_data_type>& types );

    void deep_copy( const multi_column_data& other );

    void copy_in_data( uint32_t start_col, const multi_column_data& other,
                       uint32_t other_start_col, uint32_t num_cols );

    bool remove_data( uint32_t col );

    bool set_uint64_data( uint32_t col, uint64_t data );
    bool set_int64_data( uint32_t col, int64_t data );
    bool set_double_data( uint32_t col, double data );
    bool set_string_data( uint32_t col, const std::string& data );

    std::tuple<bool, uint64_t> get_uint64_data( uint32_t col ) const;
    std::tuple<bool, int64_t> get_int64_data( uint32_t col ) const;
    std::tuple<bool, double> get_double_data( uint32_t col ) const;
    std::tuple<bool, std::string> get_string_data( uint32_t col ) const;

    uint32_t get_num_columns() const;
    multi_column_data_type get_column_type( uint32_t column ) const;

    uint32_t compute_persistence_size() const;
    void persist_to_disk( data_persister* persister ) const;
    void restore_from_disk( data_reader* reader );

    bool empty() const;

    void add_value_to_data( const multi_column_data& to_add,
                            int32_t                  multiplier );
    void divide_by_constant( double divisor );

    friend std::ostream& operator<<( std::ostream&            os,
                                     const multi_column_data& data );
    friend bool operator<( const multi_column_data& lhs,
                           const multi_column_data& rhs );

    friend bool operator==( const multi_column_data& lhs,
                            const multi_column_data& rhs );

   private:
    bool set_data( uint32_t col, void* buf_ptr, const cell_data_type& type,
                   uint32_t size );
    std::tuple<bool, const char*> get_data_pointer( uint32_t col,
                                                    const cell_data_type& type,
                                                    uint32_t size ) const;

    void adjust_column_size( uint32_t col, uint32_t size );

    std::vector<multi_column_data_type> types_;
    std::vector<uint32_t>               offsets_;
    std::vector<bool>                   present_;

    std::vector<char> data_;
};

inline bool operator!=( const multi_column_data& lhs,
                        const multi_column_data& rhs ) {
    return !( lhs == rhs );
}

inline bool operator>( const multi_column_data& lhs,
                       const multi_column_data& rhs ) {
    return rhs < lhs;
}
inline bool operator<=( const multi_column_data& lhs,
                        const multi_column_data& rhs ) {
    return !( lhs > rhs );
}
inline bool operator>=( const multi_column_data& lhs,
                        const multi_column_data& rhs ) {
    return !( lhs < rhs );
}
