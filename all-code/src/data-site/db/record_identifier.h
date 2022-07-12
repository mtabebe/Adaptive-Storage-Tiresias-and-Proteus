#pragma once

#include <cstdint>
#include <folly/Hash.h>
#include <iostream>

#include "../../gen-cpp/gen-cpp/dynamic_mastering_types.h"

class record_identifier {
   public:
    record_identifier();
    record_identifier( uint32_t table_id, uint64_t key );
    record_identifier( const ::primary_key& pk );

    uint32_t table_id_;
    uint64_t key_;
};

struct record_identifier_sort_functor {
    bool operator()( const ::record_identifier& ri1,
                     const ::record_identifier& ri2 ) const {
        if( ri1.table_id_ != ri2.table_id_ ) {
            return ( ri1.table_id_ < ri2.table_id_ );
        }
        return ( ri1.key_ < ri2.key_ );
    }
};

struct record_identifier_equal_functor {
    bool operator()( const ::record_identifier& ri1,
                     const ::record_identifier& ri2 ) const {
        return ( ( ri1.table_id_ == ri2.table_id_ ) &&
                 ( ri1.key_ == ri2.key_ ) );
    }
};

struct record_identifier_key_hasher {
    std::size_t operator()( const ::record_identifier& ri ) const {
        std::size_t seed = 0;
        seed = folly::hash::hash_combine( seed, ri.table_id_ );
        seed = folly::hash::hash_combine( seed, ri.key_ );
        return seed;
    }
};

bool is_rid_within_pid( const partition_identifier& pid,
                        const record_identifier&    rid );

std::ostream& operator<<( std::ostream& os, const record_identifier& rid );
