#pragma once

#include <vector>

#include "../common/partition_funcs.h"
#include "../data-site/db/partition_metadata.h"

// 32 bits of partition start
// 16 bits of col start
// 8 bits of table
// 8 bits of arrival stats (is_read, is_scan, is_write, filler )
typedef uint64_t query_arrival_access_bytes;

bool is_query_arrival_access_read( const query_arrival_access_bytes& b );
bool is_query_arrival_access_write( const query_arrival_access_bytes& b );
bool is_query_arrival_access_scan( const query_arrival_access_bytes& b );
cell_key_ranges get_query_arrival_access_ckr(
    const query_arrival_access_bytes&  b,
    const std::vector<table_metadata>& metas );
cell_key_ranges get_query_arrival_access_ckr(
    const query_arrival_access_bytes& b, const table_metadata& meta );
cell_key_ranges get_query_arrival_access_ckr(
    const query_arrival_access_bytes& b, uint64_t part_size,
    uint32_t col_size );

std::vector<query_arrival_access_bytes> generate_query_arrival_access_bytes(
    const cell_key_ranges& ckr, bool is_read, bool is_scan, bool is_write,
    const std::vector<table_metadata>& metas );
void generate_query_arrival_access_bytes(
    std::vector<query_arrival_access_bytes>& bytes, const cell_key_ranges& ckr,
    bool is_read, bool is_scan, bool is_write,
    const std::vector<table_metadata>& metas );

std::vector<query_arrival_access_bytes> generate_query_arrival_access_bytes(
    const cell_key_ranges& ckr, bool is_read, bool is_scan, bool is_write,
    const table_metadata& meta );
void generate_query_arrival_access_bytes(
    std::vector<query_arrival_access_bytes>& bytes, const cell_key_ranges& ckr,
    bool is_read, bool is_scan, bool is_write, const table_metadata& meta );

std::vector<query_arrival_access_bytes> generate_query_arrival_access_bytes(
    const cell_key_ranges& ckr, bool is_read, bool is_scan, bool is_write,
    uint64_t part_size, uint32_t col_size );
void generate_query_arrival_access_bytes(
    std::vector<query_arrival_access_bytes>& bytes, const cell_key_ranges& ckr,
    bool is_read, bool is_scan, bool is_write, uint64_t part_size,
    uint32_t col_size );

