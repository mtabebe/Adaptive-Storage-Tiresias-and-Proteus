#pragma once

#include <cstdint>
#include <folly/Hash.h>
#include <iostream>
#include <unordered_map>

#include "../../gen-cpp/gen-cpp/dynamic_mastering_types.h"

static const int64_t k_unassigned_key = INT64_MAX;
static const int32_t k_unassigned_col = INT32_MAX;

class cell_identifier {
   public:
    cell_identifier();
    cell_identifier( uint32_t table_id, uint64_t key, uint32_t col_id );
    cell_identifier( const ::cell_key& ck );

    uint32_t table_id_;
    uint32_t col_id_;
    uint64_t key_;
};

struct cell_identifier_sort_functor {
    bool operator()( const ::cell_identifier& ci1,
                     const ::cell_identifier& ci2 ) const {
        if( ci1.table_id_ != ci2.table_id_ ) {
            return ( ci1.table_id_ < ci2.table_id_ );
        } else if( ci1.key_ != ci2.key_ ) {
            return ( ci1.key_ < ci2.key_ );
        }
        return ( ci1.col_id_ < ci2.col_id_ );
    }
};

struct cell_identifier_equal_functor {
    bool operator()( const ::cell_identifier& ci1,
                     const ::cell_identifier& ci2 ) const {
        return ( ( ci1.table_id_ == ci2.table_id_ ) &&
                 ( ci1.key_ == ci2.key_ ) && ( ci1.col_id_ == ci2.col_id_ ) );
    }
};

struct cell_identifier_key_hasher {
    std::size_t operator()( const ::cell_identifier& ci ) const {
        std::size_t seed = 0;
        seed = folly::hash::hash_combine( seed, ci.table_id_ );
        seed = folly::hash::hash_combine( seed, ci.key_ );
        seed = folly::hash::hash_combine( seed, ci.col_id_ );
        return seed;
    }
};

bool is_cid_within_pcid( const partition_column_identifier& pcid,
                         const cell_identifier&             cid );

cell_identifier create_cell_identifier( uint32_t table_id, uint32_t col_id,
                                        uint64_t row_id );
cell_key_ranges cell_key_ranges_from_cell_identifier(
    const cell_identifier& cid );

std::ostream& operator<<( std::ostream& os, const cell_identifier& cid );

template <class T>
using cell_identifier_map_t =
    std::unordered_map<cell_identifier, T, cell_identifier_key_hasher,
                       cell_identifier_equal_functor>;

