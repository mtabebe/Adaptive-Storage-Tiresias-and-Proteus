#pragma once

#include "../../gen-cpp/gen-cpp/dynamic_mastering_types.h"
#include "../../gen-cpp/gen-cpp/proto_types.h"
#include "../db/transaction_partition_holder.h"
#include "serialize.h"
#include <folly/AtomicHashMap.h>
#include <folly/Hash.h>
#include <vector>

// Can't use a typedef with template use aliases
using function_skeleton = sproc_result ( * )(
    transaction_partition_holder *database, const clientid id,
    const std::vector<cell_key_ranges> &write_ckrs,
    const std::vector<cell_key_ranges> &read_ckrs, std::vector<arg_code> &codes,
    std::vector<void *> &values, void *opaque );
using scan_function_skeleton = void ( * )(
    transaction_partition_holder *database, const clientid id,
    const std::vector<scan_arguments> &scan_args, std::vector<arg_code> &codes,
    std::vector<void *> &values, void *opaque, scan_result &ret_val );

struct function_identifier {
    std::string           name;
    std::vector<arg_code> arg_codes;

    function_identifier( const std::string &         name,
                         const std::vector<arg_code> arg_codes )
        : name( name ) {
        this->arg_codes.reserve( arg_codes.size() );
        // Function identifiers do not use length of arrays
        for( auto iter = arg_codes.begin(); iter != arg_codes.end(); iter++ ) {
            arg_code loc_code = *iter;
            loc_code.array_length = 0;
            this->arg_codes.emplace_back( std::move( loc_code ) );
        }
    }
};

struct function_identifier_hasher {
    std::size_t operator()( const function_identifier &func_id ) const {
        DVLOG( 20 ) << "Hashing:" << func_id.name;
        std::size_t seed = folly::hash::hash_range( func_id.arg_codes.begin(),
                                                    func_id.arg_codes.end(), 0,
                                                    arg_code_hasher() );
        seed = folly::hash::hash_combine( seed, func_id.name );
        return seed;
    }
};

class sproc_lookup_table {
   public:
    sproc_lookup_table( size_t max_registerable_functions,
                        size_t max_registerable_scan_functions );

    void register_function( function_identifier func_id,
                            function_skeleton   skel );
    void register_scan_function( function_identifier func_id,
                                 scan_function_skeleton   skel );

    function_skeleton lookup_function( const function_identifier &func_id );
    scan_function_skeleton lookup_scan_function( const function_identifier &func_id );

   private:
    folly::AtomicHashMap<int64_t, function_skeleton> sproc_map_;
    size_t map_size_;

    folly::AtomicHashMap<int64_t, scan_function_skeleton> scan_map_;
    size_t scan_map_size_;
};

