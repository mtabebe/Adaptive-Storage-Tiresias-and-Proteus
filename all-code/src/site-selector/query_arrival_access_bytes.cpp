#include "query_arrival_access_bytes.h"

#include <bitset>

#include <glog/logging.h>


static constexpr uint64_t k_access_is_scan = 1;
static constexpr uint64_t k_access_is_read = 2;
static constexpr uint64_t k_access_is_write = 4;

static constexpr uint64_t k_access_bits_size = 8;
static constexpr uint64_t k_access_bits_start = 64 - k_access_bits_size;  // 56

static constexpr uint64_t k_table_bits_size = 8;
static constexpr uint64_t k_table_bits_start =
    k_access_bits_start - k_table_bits_size;  // 48

static constexpr uint64_t k_column_bits_size = 16;
static constexpr uint64_t k_column_bits_start =
    k_table_bits_start - k_column_bits_size;  // 32

static constexpr uint64_t k_row_bits_size = 32;
static constexpr uint64_t k_row_bits_start =
    k_column_bits_start - k_row_bits_size;  // 0

static_assert( k_row_bits_start == 0, "k_row_bits != 0" );
static_assert( k_column_bits_start == 32, "k_row_bits != 0" );
static_assert( k_table_bits_start == 48, "k_row_bits != 0" );
static_assert( k_access_bits_start == 56, "k_row_bits != 0" );

static_assert( ( k_access_bits_size + k_table_bits_size + k_column_bits_size +
                 k_row_bits_size ) == 64,
               "The bit usage size should map to 64 bits" );

uint64_t extract_bits( uint64_t u, uint64_t start, uint64_t amount ) {
    DVLOG( 50 ) << "Extract:" << amount << " bits starting from:" << start
                << ", of:" << u << ", bits:" << std::bitset<64>( u );
    // https://stackoverflow.com/questions/8011700/how-do-i-extract-specific-n-bits-of-a-32-bit-unsigned-integer-in-c
    uint64_t mask = ( ( 1ULL << amount ) - 1 ) << start;
    uint64_t isolated = u & mask;
    // shift them down
    uint64_t extracted = isolated >> start;
    DVLOG( 50 ) << "Extract:" << amount << " bits starting from:" << start
                << ", of:" << u << ", returning:" << extracted
                << ", bits:" << std::bitset<64>( extracted );
    return extracted;
}

uint64_t set_bits( uint64_t u, uint64_t start, uint64_t amount ) {
    DVLOG( 50 ) << "Set:" << amount << " bits starting from:" << start
                << ", of:" << u << ", bits:" << std::bitset<64>( u );
    // get the lowest amount bits
    uint64_t extracted = extract_bits( u, 0, amount );
    // shift them in
    uint64_t ret = ( extracted << start );
    DVLOG( 50 ) << "Set:" << amount << " bits starting from:" << start
                << ", of:" << u << ", returning:" << ret
                << ", bits:" << std::bitset<64>( ret );

    return ret;
}
bool is_query_arrival_access_type( const query_arrival_access_bytes& b,
                                   uint64_t access_type ) {
    DVLOG( 50 ) << "is_query_arrival_access_type:" << b
                << ", type:" << access_type;
    uint64_t arg_byte =
        extract_bits( b, k_access_bits_start, k_access_bits_size );
    bool ret = ( arg_byte & access_type );
    DVLOG( 50 ) << "is_query_arrival_access_type:" << b
                << ", type:" << access_type << ", returning:" << ret;
    return ret;
}

bool is_query_arrival_access_read( const query_arrival_access_bytes& b ) {
    return is_query_arrival_access_type( b, k_access_is_read );
}
bool is_query_arrival_access_write( const query_arrival_access_bytes& b ) {
    return is_query_arrival_access_type( b, k_access_is_write );
}
bool is_query_arrival_access_scan( const query_arrival_access_bytes& b ) {
    return is_query_arrival_access_type( b, k_access_is_scan );
}
cell_key_ranges get_query_arrival_access_ckr(
    const query_arrival_access_bytes&  b,
    const std::vector<table_metadata>& metas ) {

    uint32_t table = extract_bits( b, k_table_bits_start, k_table_bits_size );
    DCHECK_LT( table, metas.size() );
    DCHECK_EQ( metas.at( table ).table_id_, table );
    return get_query_arrival_access_ckr(
        b, metas.at( table ).default_tracking_partition_size_,
        metas.at( table ).default_tracking_column_size_ );
}
cell_key_ranges get_query_arrival_access_ckr(
    const query_arrival_access_bytes& b, const table_metadata& meta ) {
    uint32_t table = extract_bits( b, k_table_bits_start, k_table_bits_size );
    DCHECK_EQ( meta.table_id_, table );
    return get_query_arrival_access_ckr( b,
                                         meta.default_tracking_partition_size_,
                                         meta.default_tracking_column_size_ );
}
cell_key_ranges get_query_arrival_access_ckr(
    const query_arrival_access_bytes& b, uint64_t part_size,
    uint32_t col_size ) {

    DVLOG( 50 ) << "get_query_arrival_access_ckr:" << b
                << ", part_size:" << part_size << ", col_size:" << col_size
                << ", bits:" << std::bitset<64>( b );

    // get the table, 48 or 8 bits
    uint32_t table = extract_bits( b, k_table_bits_start, k_table_bits_size );
    uint32_t column =
        extract_bits( b, k_column_bits_start, k_column_bits_size );
    uint64_t row = extract_bits( b, k_row_bits_start, k_row_bits_size );

    uint32_t col_start = column * col_size;
    uint32_t col_end = col_start + ( col_size - 1 );

    uint64_t row_start = row * part_size;
    uint64_t row_end = row_start + ( part_size - 1 );

    cell_key_ranges ckr =
        create_cell_key_ranges( table, row_start, row_end, col_start, col_end );

    DVLOG( 50 ) << "get_query_arrival_access_ckr:" << b
                << ", part_size:" << part_size << ", col_size:" << col_size
                << ", returning:" << ckr;

    return ckr;
}

query_arrival_access_bytes generate_query_arrival_access_byte(
    query_arrival_access_bytes b, uint32_t table_id, uint32_t col,
    uint64_t row ) {
    DVLOG( 50 ) << "generate_query_arrival_access_byte access bytes:" << b
                << ", table_id:" << table_id << ", col:" << col
                << ", row:" << row;
    query_arrival_access_bytes ret =
        set_bits( b, k_access_bits_start, k_access_bits_size ) |
        set_bits( table_id, k_table_bits_start, k_table_bits_size ) |
        set_bits( col, k_column_bits_start, k_column_bits_size ) |
        set_bits( row, k_row_bits_start, k_row_bits_size );
    DVLOG( 50 ) << "generate_query_arrival_access_byte access bytes:" << b
                << ", table_id:" << table_id << ", col:" << col
                << ", row:" << row << ", returning:" << ret
                << ", bits:" << std::bitset<64>( ret );

    return ret;
}

query_arrival_access_bytes generate_query_arrival_access_mode_byte(
    bool is_read, bool is_scan, bool is_write ) {
    query_arrival_access_bytes b = 0;
    if( is_read ) {
        b = b | k_access_is_read;
    }
    if( is_scan ) {
        b = b | k_access_is_scan;
    }
    if( is_write ) {
        b = b | k_access_is_write;
    }

    return b;
}

query_arrival_access_bytes generate_query_arrival_byte(
    bool is_read, bool is_scan, bool is_write, uint32_t table_id, uint32_t col,
    uint64_t row ) {
    uint64_t access_byte =
        generate_query_arrival_access_mode_byte( is_read, is_scan, is_write );
    return generate_query_arrival_access_byte( access_byte, table_id, col,
                                               row );
}

void generate_query_arrival_access_bytes(
    std::vector<query_arrival_access_bytes>& ret, const cell_key_ranges& ckr,
    bool is_read, bool is_scan, bool is_write,
    const std::vector<table_metadata>& metas ) {
    DCHECK_LT( ckr.table_id, metas.size() );
    generate_query_arrival_access_bytes( ret, ckr, is_read, is_scan, is_write,
                                         metas.at( ckr.table_id ) );
}
void generate_query_arrival_access_bytes(
    std::vector<query_arrival_access_bytes>& ret, const cell_key_ranges& ckr,
    bool is_read, bool is_scan, bool is_write, const table_metadata& meta ) {
    DCHECK_EQ( ckr.table_id, meta.table_id_ );
    generate_query_arrival_access_bytes( ret, ckr, is_read, is_scan, is_write,
                                         meta.default_tracking_partition_size_,
                                         meta.default_tracking_column_size_ );
}

void generate_query_arrival_access_bytes(
    std::vector<query_arrival_access_bytes>& ret, const cell_key_ranges& ckr,
    bool is_read, bool is_scan, bool is_write, uint64_t part_size,
    uint32_t col_size ) {
    DVLOG( 50 ) << "generate_query_arrival_access_bytes:" << ckr
                << "is_read:" << is_read << ", is_scan:" << is_scan
                << ", is_write:" << is_write << ", part_size:" << part_size
                << ", col_size:" << col_size;

    query_arrival_access_bytes access_byte =
        generate_query_arrival_access_mode_byte( is_read, is_scan, is_write );

    for( int64_t row = ckr.row_id_start; row <= ckr.row_id_end;
         row += part_size ) {
        uint64_t translated_row = ( row / part_size );
        for( int32_t col = ckr.col_id_start; col <= ckr.col_id_end;
             col += col_size ) {
            uint32_t translated_col = ( col / col_size );

            ret.push_back( generate_query_arrival_access_byte(
                access_byte, ckr.table_id, translated_col, translated_row ) );
        }
    }

    DVLOG( 50 ) << "generate_query_arrival_access_bytes:" << ckr
                << "is_read:" << is_read << ", is_scan:" << is_scan
                << ", is_write:" << is_write << ", part_size:" << part_size
                << ", col_size:" << col_size << ", returning:" << ret;
}

std::vector<query_arrival_access_bytes> generate_query_arrival_access_bytes(
    const cell_key_ranges& ckr, bool is_read, bool is_scan, bool is_write,
    const std::vector<table_metadata>& metas ) {
    std::vector<query_arrival_access_bytes> ret;
    generate_query_arrival_access_bytes( ret, ckr, is_read, is_scan, is_write,
                                         metas );
    return ret;
}
std::vector<query_arrival_access_bytes> generate_query_arrival_access_bytes(
    const cell_key_ranges& ckr, bool is_read, bool is_scan, bool is_write,
    const table_metadata& meta ) {
    std::vector<query_arrival_access_bytes> ret;
    generate_query_arrival_access_bytes( ret, ckr, is_read, is_scan, is_write,
                                         meta );
    return ret;
}
std::vector<query_arrival_access_bytes> generate_query_arrival_access_bytes(
    const cell_key_ranges& ckr, bool is_read, bool is_scan, bool is_write,
    uint64_t part_size, uint32_t col_size ) {
    std::vector<query_arrival_access_bytes> ret;
    generate_query_arrival_access_bytes( ret, ckr, is_read, is_scan, is_write,
                                         part_size, col_size );
    return ret;
}

