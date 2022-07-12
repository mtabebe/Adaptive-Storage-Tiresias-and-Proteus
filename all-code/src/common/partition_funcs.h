#pragma once

// N.B. This file must be included below "dynamic_mastering_types.h"
// Otherwise it won't know about partition_identifier
// We don't want to include the import path in here because this is copied to
// the site_selector and the site_manager, which have different paths for the
// generated
// thrift code

#include <folly/Hash.h>
#include <glog/logging.h>
#include <memory>
#include <set>
#include <unordered_set>
#include <vector>

#include <folly/concurrency/ConcurrentHashMap.h>

#include "../gen-cpp/gen-cpp/dynamic_mastering_types.h"
#include "../gen-cpp/gen-cpp/proto_types.h"
#include "hw.h"

struct partition_identifier_sort_functor {
    bool operator()( const ::partition_identifier& pi1,
                     const ::partition_identifier& pi2 ) const {
        if( pi1.table_id != pi2.table_id ) {
            return ( pi1.table_id < pi2.table_id );
        } else if( pi1.partition_start != pi2.partition_start ) {
            return ( pi1.partition_start < pi2.partition_start );
        }
        return ( pi1.partition_end < pi2.partition_end );
    }
};

struct partition_identifier_equal_functor {
    bool operator()( const ::partition_identifier& pi1,
                     const ::partition_identifier& pi2 ) const {
        return ( ( pi1.table_id == pi2.table_id ) &&
                 ( pi1.partition_start == pi2.partition_start ) &&
                 ( pi1.partition_end == pi2.partition_end ) );
    }
};

struct partition_identifier_key_hasher {
    std::size_t operator()( const ::partition_identifier& pi ) const {
        std::size_t seed = 0;
        seed = folly::hash::hash_combine( seed, pi.table_id );
        seed = folly::hash::hash_combine( seed, pi.partition_start );
        seed = folly::hash::hash_combine( seed, pi.partition_end );
        return seed;
    }
};

struct partition_column_identifier_sort_functor {
    bool operator()( const ::partition_column_identifier& pci1,
                     const ::partition_column_identifier& pci2 ) const {
        if( pci1.table_id != pci2.table_id ) {
            return ( pci1.table_id < pci2.table_id );
        } else if( pci1.partition_start != pci2.partition_start ) {
            return ( pci1.partition_start < pci2.partition_start );
        } else if( pci1.partition_end != pci2.partition_end ) {
            return ( pci1.partition_end < pci2.partition_end );
        } else if( pci1.column_start != pci2.column_start ) {
            return ( pci1.column_start < pci2.column_start );
        }

        return ( pci1.column_end < pci2.column_end );
    }
};

struct partition_column_identifier_equal_functor {
    bool operator()( const ::partition_column_identifier& pci1,
                     const ::partition_column_identifier& pci2 ) const {
        return ( ( pci1.table_id == pci2.table_id ) &&
                 ( pci1.partition_start == pci2.partition_start ) &&
                 ( pci1.partition_end == pci2.partition_end ) &&
                 ( pci1.column_start == pci2.column_start ) &&
                 ( pci1.column_end == pci2.column_end ) );
    }
};

struct partition_column_identifier_key_hasher {
    std::size_t operator()( const ::partition_column_identifier& pci ) const {
        std::size_t seed = 0;
        seed = folly::hash::hash_combine( seed, pci.table_id );
        seed = folly::hash::hash_combine( seed, pci.partition_start );
        seed = folly::hash::hash_combine( seed, pci.partition_end );
        seed = folly::hash::hash_combine( seed, pci.column_start );
        seed = folly::hash::hash_combine( seed, pci.column_end );

        return seed;
    }
};

typedef std::set<partition_identifier, partition_identifier_sort_functor>
    partition_identifier_set;
typedef std::unordered_set<partition_identifier,
                           partition_identifier_key_hasher,
                           partition_identifier_equal_functor>
    partition_identifier_unordered_set;
template <class T>
using partition_identifier_map_t =
    std::unordered_map<partition_identifier, T, partition_identifier_key_hasher,
                       partition_identifier_equal_functor>;

template <class T>
using partition_identifier_folly_concurrent_hash_map_t =
    folly::ConcurrentHashMap<partition_identifier, T,
                             partition_identifier_key_hasher,
                             partition_identifier_equal_functor>;

typedef partition_identifier_map_t<std::vector<::primary_key>>
    partition_identifier_to_pks;

typedef std::set<partition_column_identifier,
                 partition_column_identifier_sort_functor>
    partition_column_identifier_set;
typedef std::unordered_set<partition_column_identifier,
                           partition_column_identifier_key_hasher,
                           partition_column_identifier_equal_functor>
    partition_column_identifier_unordered_set;
template <class T>
using partition_column_identifier_map_t =
    std::unordered_map<partition_column_identifier, T,
                       partition_column_identifier_key_hasher,
                       partition_column_identifier_equal_functor>;

template <class T>
using partition_column_identifier_folly_concurrent_hash_map_t =
    folly::ConcurrentHashMap<partition_column_identifier, T,
                             partition_column_identifier_key_hasher,
                             partition_column_identifier_equal_functor>;

typedef partition_column_identifier_map_t<std::vector<::cell_key>>
    partition_column_identifier_to_cks;
typedef partition_column_identifier_map_t<std::vector<::cell_key_ranges>>
    partition_column_identifier_to_ckrs;

bool is_pid_subsumed_by_pid( const ::partition_identifier& needle,
                             const ::partition_identifier& haystack );
bool is_rid_greater_than_pid( const ::partition_identifier& pid,
                              const ::primary_key&          rid );
bool is_rid_within_pid( const ::primary_key&          rid,
                        const ::partition_identifier& pid );

bool is_pcid_subsumed_by_pcid( const ::partition_column_identifier& needle,
                               const ::partition_column_identifier& haystack );
bool is_cid_greater_than_pcid( const ::partition_column_identifier& pid,
                               const ::cell_key&                    cid );
bool is_cid_within_pcid( const ::cell_key&                    cid,
                         const ::partition_column_identifier& pcid );
bool is_ckr_fully_within_pcid( const ::cell_key_ranges&             ckr,
                               const ::partition_column_identifier& pcid );
bool is_ckr_partially_within_pcid( const ::cell_key_ranges&             ckr,
                                   const ::partition_column_identifier& pcid );

bool do_pcids_intersect( const ::partition_column_identifier& a,
                         const ::partition_column_identifier& b );

partition_identifier_set convert_to_partition_identifier_set(
    const std::vector<::partition_identifier>& pids );

partition_column_identifier_set convert_to_partition_column_identifier_set(
    const std::vector<::partition_column_identifier>& pcids );

uint32_t normalize_column_id( uint32_t                           raw_column_id,
                              const partition_column_identifier& pcid );

struct cell_key_ranges_sort_functor {
    bool operator()( const ::cell_key_ranges& ckr1,
                     const ::cell_key_ranges& ckr2 ) const {
        if( ckr1.table_id != ckr2.table_id ) {
            return ( ckr1.table_id < ckr2.table_id );
        } else if( ckr1.row_id_start != ckr2.row_id_start ) {
            return ( ckr1.row_id_start < ckr2.row_id_start );
        } else if( ckr1.row_id_end != ckr2.row_id_end ) {
            return ( ckr1.row_id_end < ckr2.row_id_end );
        } else if( ckr1.col_id_start != ckr2.col_id_start ) {
            return ( ckr1.col_id_start < ckr2.col_id_start );
        }

        return ( ckr1.col_id_end < ckr2.col_id_end );
    }
};

struct cell_key_ranges_equal_functor {
    bool operator()( const ::cell_key_ranges& ckr1,
                     const ::cell_key_ranges& ckr2 ) const {
        return ( ( ckr1.table_id == ckr2.table_id ) &&
                 ( ckr1.row_id_start == ckr2.row_id_start ) &&
                 ( ckr1.row_id_end == ckr2.row_id_end ) &&
                 ( ckr1.col_id_start == ckr2.col_id_start ) &&
                 ( ckr1.col_id_end == ckr2.col_id_end ) );
    }
};

struct cell_key_ranges_hasher {
    std::size_t operator()( const ::cell_key_ranges& ckr ) const {
        std::size_t seed = 0;
        seed = folly::hash::hash_combine( seed, ckr.table_id );
        seed = folly::hash::hash_combine( seed, ckr.row_id_start );
        seed = folly::hash::hash_combine( seed, ckr.row_id_end );
        seed = folly::hash::hash_combine( seed, ckr.col_id_start );
        seed = folly::hash::hash_combine( seed, ckr.col_id_end );

        return seed;
    }
};

partition_type::type string_to_partition_type( const std::string& str );
std::vector<partition_type::type> strings_to_partition_types(
    const std::string& str );
storage_tier_type::type string_to_storage_type( const std::string& str );
std::vector<storage_tier_type::type> strings_to_storage_types(
    const std::string& str );

cell_key create_cell_key( uint32_t table_id, uint32_t col_id, uint64_t row_id );
partition_column_identifier create_partition_column_identifier(
    uint32_t table_id, uint64_t row_start, uint64_t row_end, uint32_t col_start,
    uint32_t col_end );

primary_key create_primary_key( uint32_t table_id, uint64_t row );
cell_key_ranges create_cell_key_ranges( uint32_t table_id, uint64_t row_start,
                                        uint64_t row_end, uint32_t col_start,
                                        uint32_t col_end );

cell_key_ranges cell_key_ranges_from_ck( const cell_key& ck );
cell_key_ranges cell_key_ranges_from_pcid(
    const partition_column_identifier& pcid );

cell_key_ranges shape_ckr_to_pcid( const cell_key_ranges&             ckr,
                                   const partition_column_identifier& pcid );

uint32_t get_number_of_rows( const partition_column_identifier& pid );
uint32_t get_number_of_columns( const partition_column_identifier& pid );
uint32_t get_number_of_cells( const partition_column_identifier& pid );

uint32_t get_number_of_rows( const cell_key_ranges& ckr );
uint32_t get_number_of_columns( const cell_key_ranges& ckr );
uint32_t get_number_of_cells( const cell_key_ranges& ckr );

bool is_ckr_greater_than_pcid( const cell_key_ranges&             ckr,
                               const partition_column_identifier& pcid );
