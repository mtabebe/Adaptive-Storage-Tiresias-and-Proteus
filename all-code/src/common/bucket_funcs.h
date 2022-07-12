#pragma once

// N.B. This file must be included below "dynamic_mastering_types.h"
// Otherwise it won't know about bucket_key
// We don't want to include the import path in here because this is copied to
// the site_selector and the site_manager, which have different paths for the
// generated
// thrift code

#include <folly/Hash.h>
#include <glog/logging.h>
#include <memory>
#include <vector>

#include "../gen-cpp/gen-cpp/dynamic_mastering_types.h"
#include "cell_data_type.h"
#include "constants.h"
#include "hw.h"

typedef std::tuple<int32_t /*table id*/, uint64_t /* table size*/,
                   uint64_t /*partition size*/, uint32_t /* col size */,
                   uint32_t /* num columns */,
                   std::vector<cell_data_type> /*col types*/,
                   partition_type::type /* default type */,
                   storage_tier_type::type /* default type */>
    table_partition_information;

inline uint64_t get_hash_pos_with_size( uint64_t key, uint64_t size ) {
    return ( key % size );
}

// For building the binary tree in std::set
// Put buckets with the same map and range next to each other
// Ensures that multi_bucket_keys with same values hash to the same size_t
struct bucket_key_sort_functor {
    bool operator()( const ::bucket_key& rbk1,
                     const ::bucket_key& rbk2 ) const {
        if( rbk1.map_key != rbk2.map_key ) {
            return ( rbk1.map_key < rbk2.map_key );
        }
        return ( rbk1.range_key < rbk2.range_key );
    }
};

struct bucket_key_equal_functor {
    bool operator()( const ::bucket_key& rbk1,
                     const ::bucket_key& rbk2 ) const {
        return ( rbk1.map_key == rbk2.map_key &&
                 rbk1.range_key == rbk2.range_key );
    }
};

inline uint64_t pk_to_range_key(
    const ::primary_key&                            pk,
    const std::vector<table_partition_information>& table_partition_info ) {
    DVLOG( 30 ) << "PK:" << pk;
    uint64_t bucket_size =
        std::get<2>( table_partition_info.at( pk.table_id ) );
    uint64_t table_size = std::get<1>( table_partition_info.at( pk.table_id ) );

    uint64_t hash_pos = get_hash_pos_with_size( pk.row_id, table_size );
    uint64_t range_key = hash_pos / bucket_size;
    DVLOG( 30 ) << "PK:" << pk << " maps to range_key: " << range_key
                << ", bucket_size:" << bucket_size
                << ", table_size:" << table_size << ", hash_pos:" << hash_pos;

    return range_key;
}

inline uint64_t low_row_id_from_range_key(
    const ::bucket_key&                             bk,
    const std::vector<table_partition_information>& table_partition_info ) {
    uint64_t num_keys = std::get<1>( table_partition_info.at( bk.map_key ) );
    uint64_t bucket_size = std::get<2>( table_partition_info.at( bk.map_key ) );
    uint64_t low = std::min( bk.range_key * bucket_size, num_keys );
    return low;
}

inline uint64_t high_row_id_from_range_key_given_low(
    const ::bucket_key& bk, uint64_t low,
    const std::vector<table_partition_information>& table_partition_info ) {
    uint64_t bucket_size = std::get<2>( table_partition_info.at( bk.map_key ) );
    uint64_t num_keys = std::get<1>( table_partition_info.at( bk.map_key ) );
    uint64_t high = std::min( low + ( bucket_size - 1 ), num_keys - 1);
    return high;
}

inline uint64_t high_row_id_from_range_key(
    const ::bucket_key&                             bk,
    const std::vector<table_partition_information>& table_partition_info ) {
    uint64_t low = low_row_id_from_range_key( bk, table_partition_info );
    return high_row_id_from_range_key_given_low( bk, low,
                                                 table_partition_info );
}

inline ::bucket_key get_bucket_key(
    const ::primary_key&                            pk,
    const std::vector<table_partition_information>& table_partition_info ) {
    ::bucket_key bk;
    bk.map_key = pk.table_id;
    bk.range_key = pk_to_range_key( pk, table_partition_info );
    return bk;
}

inline std::shared_ptr<std::vector<::bucket_key>> get_bucket_keys(
    const std::vector<::primary_key>&               pks,
    const std::vector<table_partition_information>& table_partition_info ) {
    std::shared_ptr<std::vector<::bucket_key>> keys(
        new std::vector<::bucket_key>( pks.size() ) );
    // No loop carried dependencies, vectorize with SIMD on supported platforms
    __SIMD_LOOP__
    for( unsigned i = 0; i < pks.size(); i++ ) {
        keys->at( i ).map_key = pks.at( i ).table_id;
        keys->at( i ).range_key =
            pk_to_range_key( pks.at( i ), table_partition_info );
    }
    bucket_key_sort_functor  sort_functor;
    bucket_key_equal_functor equal_functor;

    std::sort( keys->begin(), keys->end(), sort_functor );
    auto new_end = std::unique( keys->begin(), keys->end(), equal_functor );
    keys->resize( std::distance( keys->begin(), new_end ) );

    return keys;
}

struct bucket_key_hasher {
    std::size_t operator()( const bucket_key& bk ) const {
        std::size_t seed = 0;
        seed = folly::hash::hash_combine( seed, bk.map_key );
        seed = folly::hash::hash_combine( seed, bk.range_key );
        return seed;
    }
};

static inline std::vector<::primary_key>
    convert_site_bucket_keys_to_primary_keys(
        const std::vector<::bucket_key>&                bucket_keys,
        const std::vector<table_partition_information>& table_partition_info ) {
    std::vector<::primary_key> pks;  // default create
    // then map sites to pks
    DVLOG( 30 ) << "Constructing pks list";
    for( const ::bucket_key& bk : bucket_keys ) {
        DVLOG( 30 ) << "Mapping pks from bucket key:" << bk;
        uint64_t low = low_row_id_from_range_key( bk, table_partition_info );
        uint64_t high = high_row_id_from_range_key_given_low(
            bk, low, table_partition_info );
        for( uint64_t key = low; key <= high; key++ ) {
            ::primary_key pk;
            pk.table_id = bk.map_key;
            pk.row_id = key;

            pks.push_back( pk );
        }
    }
    return pks;
}


