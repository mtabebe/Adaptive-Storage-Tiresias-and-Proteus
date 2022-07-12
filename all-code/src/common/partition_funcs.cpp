#include "partition_funcs.h"

#include "string_utils.h"

bool partition_identifier::operator<(
    const partition_identifier& other ) const {
    return partition_identifier_sort_functor{}( *this, other );
}

bool partition_column_identifier::operator<(
    const partition_column_identifier& other ) const {
    return partition_column_identifier_sort_functor{}( *this, other );
}

bool cell_key_ranges::operator<(
    const cell_key_ranges& other ) const {
    return cell_key_ranges_sort_functor{}( *this, other );
}


bool is_pid_subsumed_by_pid( const ::partition_identifier& needle,
                             const ::partition_identifier& haystack ) {
  if (needle.table_id != haystack.table_id) {
    return false;
  }
  return ( ( needle.partition_start >= haystack.partition_start ) and
           ( needle.partition_end <= haystack.partition_end ) );
}

bool is_rid_greater_than_pid( const ::partition_identifier& pid,
                                            const ::primary_key& rid ) {
    if( pid.table_id < rid.table_id ) {
        return true;
    }
    return ( rid.row_id > pid.partition_end );
    // Otherwise less than or within range, so it is not greater than
}

bool is_rid_within_pid( const ::primary_key&          rid,
                        const ::partition_identifier& pid ) {
    if (rid.table_id != pid.table_id) {
        return false;
    }
    return ( ( pid.partition_start <= rid.row_id ) and
             ( rid.row_id <= pid.partition_end ) );
}

bool is_pcid_subsumed_by_pcid( const ::partition_column_identifier& needle,
                               const ::partition_column_identifier& haystack ) {
    if( needle.table_id != haystack.table_id ) {
        return false;
    }
    if( ( needle.partition_start >= haystack.partition_start ) and
        ( needle.partition_end <= haystack.partition_end ) ) {
        return ( ( needle.column_start >= haystack.column_start ) and
                 ( needle.column_end <= haystack.column_end ) );
    }
    return false;
}

bool do_boxes_interset( uint64_t left_a, uint64_t right_a, uint64_t left_b,
                        uint64_t right_b ) {
    // if have one of [ left_a, right_a, left_b, right_b] or [ left_b, right_b,
    // left_a, right_a], then don't interset

    if( ( right_a < left_b ) or ( right_b < left_a ) ) {
        return false;
    }
    return true;
}


bool do_columns_intersect(
 const ::partition_column_identifier& a, const ::partition_column_identifier& b) {
    return do_boxes_interset( a.column_start, a.column_end, b.column_start,
                              b.column_end );
}

bool do_rows_intersect(
 const ::partition_column_identifier& a, const ::partition_column_identifier& b) {
    return do_boxes_interset( a.partition_start, a.partition_end,
                              b.partition_start, b.partition_end );
}

bool do_pcids_intersect( const ::partition_column_identifier& a,
                         const ::partition_column_identifier& b ) {
    if( a.table_id != b.table_id ) {
        // different tables
        return false;
    }
    bool cols_intersect = do_columns_intersect( a, b );
    if ( !cols_intersect) {
      return false;
    }
    bool rows_intersect = do_rows_intersect( a, b );

    return rows_intersect;
}

bool is_cid_greater_than_pcid( const ::partition_column_identifier& pcid,
                               const ::cell_key&                    cid ) {
    if( pcid.table_id < cid.table_id ) {
        return true;
    }
    // Otherwise less than or within range, so it is not greater than
    if( cid.row_id > pcid.partition_end ) {
        return ( cid.col_id > pcid.column_end );
    }
    return true;
}

bool is_cid_within_pcid( const ::cell_key&                    cid,
                         const ::partition_column_identifier& pcid ) {
    if( cid.table_id != pcid.table_id ) {
        return false;
    } else if( ( pcid.partition_start > cid.row_id ) or
               ( cid.row_id > pcid.partition_end ) ) {
        return false;
    } else if( ( pcid.column_start > cid.col_id ) or
               ( cid.col_id > pcid.column_end ) ) {
        return false;
    }

    return true;
}

bool is_ckr_fully_within_pcid( const ::cell_key_ranges&             ckr,
                               const ::partition_column_identifier& pcid ) {
    ::cell_key ck_low;
    ::cell_key ck_high;

    ck_low.table_id = ckr.table_id;
    ck_low.row_id = ckr.row_id_start;
    ck_low.col_id = ckr.col_id_start;

    ck_high.table_id = ckr.table_id;
    ck_high.row_id = ckr.row_id_end;
    ck_high.col_id = ckr.col_id_end;

    return ( is_cid_within_pcid( ck_low, pcid ) and
             is_cid_within_pcid( ck_high, pcid ) );
}
bool is_ckr_partially_within_pcid( const ::cell_key_ranges&             ckr,
                                   const ::partition_column_identifier& pcid ) {
    ::partition_column_identifier other_pcid;
    other_pcid.table_id = ckr.table_id;
    other_pcid.partition_start = ckr.row_id_start;
    other_pcid.partition_end = ckr.row_id_end;
    other_pcid.column_start = ckr.col_id_start;
    other_pcid.column_end = ckr.col_id_end;

    return do_pcids_intersect( other_pcid, pcid );
}

bool primary_key::operator<(
    const primary_key& other ) const {
    if( this->table_id != other.table_id ) {
        return ( this->table_id < other.table_id );
    }
    return ( this->row_id < other.row_id );
}

bool cell_key::operator<( const cell_key& other ) const {
    if( this->table_id != other.table_id ) {
        return ( this->table_id < other.table_id );
    } else if( this->row_id != other.row_id ) {
        return ( this->row_id < other.row_id );
    }
    return ( this->col_id < other.col_id );
}

partition_identifier_set convert_to_partition_identifier_set(
    const std::vector<::partition_identifier>& pids ) {
    partition_identifier_set pid_sets( pids.begin(), pids.end() );
    return pid_sets;
}
partition_column_identifier_set convert_to_partition_column_identifier_set(
    const std::vector<::partition_column_identifier>& pcids ) {
    partition_column_identifier_set pcid_sets( pcids.begin(), pcids.end() );
    return pcid_sets;
}

uint32_t normalize_column_id( uint32_t                           raw_col_id,
                              const partition_column_identifier& pcid ) {
    DCHECK_GE( raw_col_id, pcid.column_start );
    DCHECK_LE( raw_col_id, pcid.column_end );
    return ( raw_col_id - pcid.column_start );
}

partition_type::type string_to_partition_type( const std::string& str ) {
    std::string lower = str;
    std::transform( lower.begin(), lower.end(), lower.begin(), ::tolower );

    for( const auto& p_entry : _partition_type_VALUES_TO_NAMES ) {
        std::string comp_lower( p_entry.second );
        std::transform( comp_lower.begin(), comp_lower.end(),
                        comp_lower.begin(), ::tolower );
        if( lower.compare( comp_lower ) == 0 ) {
            return partition_type::type( p_entry.first );
        }
    }
    DLOG( FATAL ) << "Unable to convert:" << str << ", to partition type";

    return partition_type::type::ROW;
}

std::vector<partition_type::type> strings_to_partition_types(
    const std::string& str ) {
    auto                              strings = split_string( str, ',' );
    std::vector<partition_type::type> types;
    for( const auto& s : strings ) {
        types.emplace_back( string_to_partition_type( s ) );
    }
    return types;
}

partition_identifier create_partition_identifier( uint32_t table_id,
                                                  uint64_t start,
                                                  uint64_t end ) {
    partition_identifier id;
    id.table_id = table_id;
    id.partition_start = start;
    id.partition_end = end;
    return id;
}

storage_tier_type::type string_to_storage_type( const std::string& str ) {
    std::string lower = str;
    std::transform( lower.begin(), lower.end(), lower.begin(), ::tolower );

    for( const auto& p_entry : _storage_tier_type_VALUES_TO_NAMES ) {
        std::string comp_lower( p_entry.second );
        std::transform( comp_lower.begin(), comp_lower.end(),
                        comp_lower.begin(), ::tolower );
        if( lower.compare( comp_lower ) == 0 ) {
            return storage_tier_type::type( p_entry.first );
        }
    }
    DLOG( FATAL ) << "Unable to convert:" << str << ", to storage tier type";

    return storage_tier_type::type::MEMORY;
}
std::vector<storage_tier_type::type> strings_to_storage_types(
    const std::string& str ) {
    auto                              strings = split_string( str, ',' );
    std::vector<storage_tier_type::type> types;
    for( const auto& s : strings ) {
        types.emplace_back( string_to_storage_type( s ) );
    }
    return types;
}

partition_column_identifier create_partition_column_identifier(
    uint32_t table_id, uint64_t row_start, uint64_t row_end, uint32_t col_start,
    uint32_t col_end ) {
    partition_column_identifier pc_id;

    pc_id.table_id = table_id;
    pc_id.partition_start = row_start;
    pc_id.partition_end = row_end;
    pc_id.column_start = col_start;
    pc_id.column_end = col_end;

    return pc_id;
}

primary_key create_primary_key( uint32_t table_id, uint64_t row ) {
    primary_key pk;
    pk.table_id = table_id;
    pk.row_id = row;
    return pk;
}

cell_key_ranges create_cell_key_ranges( uint32_t table_id, uint64_t row_start,
                                        uint64_t row_end, uint32_t col_start,
                                        uint32_t col_end ) {
    cell_key_ranges ckr;

    ckr.table_id = table_id;
    ckr.row_id_start = row_start;
    ckr.row_id_end = row_end;
    ckr.col_id_start = col_start;
    ckr.col_id_end = col_end;

    return ckr;
}


cell_key create_cell_key( uint32_t table_id, uint32_t col_id,
                          uint64_t row_id ) {
    cell_key ck;
    ck.table_id = table_id;
    ck.col_id = col_id;
    ck.row_id = row_id;
    return ck;
}
cell_key_ranges cell_key_ranges_from_ck( const cell_key& ck ) {
    cell_key_ranges ckr;
    ckr.table_id = ck.table_id;
    ckr.col_id_start = ck.col_id;
    ckr.col_id_end = ck.col_id;
    ckr.row_id_start = ck.row_id;
    ckr.row_id_end = ck.row_id;

    return ckr;
}
cell_key_ranges cell_key_ranges_from_pcid(
    const partition_column_identifier& pcid ) {
    return create_cell_key_ranges( pcid.table_id, pcid.partition_start,
                                   pcid.partition_end, pcid.column_start,
                                   pcid.column_end );
}

cell_key_ranges shape_ckr_to_pcid( const cell_key_ranges&             ckr,
                                   const partition_column_identifier& pcid ) {
    DCHECK( is_ckr_partially_within_pcid( ckr, pcid ) );
    cell_key_ranges shaped_ckr = ckr;
    shaped_ckr.col_id_start =
        std::max( shaped_ckr.col_id_start, pcid.column_start );
    shaped_ckr.col_id_end = std::min( shaped_ckr.col_id_end, pcid.column_end );
    shaped_ckr.row_id_start =
        std::max( shaped_ckr.row_id_start, pcid.partition_start );
    shaped_ckr.row_id_end =
        std::min( shaped_ckr.row_id_end, pcid.partition_end );

    DCHECK_LE( shaped_ckr.col_id_start, shaped_ckr.col_id_end );
    DCHECK_LE( shaped_ckr.row_id_start, shaped_ckr.row_id_end );

    DCHECK( is_ckr_fully_within_pcid( shaped_ckr, pcid ) );

    return shaped_ckr;
}

uint32_t get_number_of_rows( const partition_column_identifier& pid ) {
    return ( 1 + pid.partition_end - pid.partition_start );
}
uint32_t get_number_of_columns( const partition_column_identifier& pid ) {
  return
( 1 + pid.column_end - pid.column_start );
}
uint32_t get_number_of_cells( const partition_column_identifier& pid ) {
    return get_number_of_columns( pid ) * get_number_of_rows( pid );
}

uint32_t get_number_of_rows( const cell_key_ranges& ckr ) {
    return ( 1 + ckr.row_id_end - ckr.row_id_start );
}
uint32_t get_number_of_columns( const cell_key_ranges& ckr) {
    return ( 1 + ckr.col_id_end - ckr.col_id_start );
}
uint32_t get_number_of_cells( const cell_key_ranges& ckr ) {
    return get_number_of_rows( ckr ) * get_number_of_columns( ckr );
}

bool is_ckr_greater_than_pcid( const cell_key_ranges&             ckr,
                               const partition_column_identifier& pcid ) {
    partition_column_identifier pcid_from_ckr =
        create_partition_column_identifier( ckr.table_id, ckr.row_id_start,
                                            ckr.row_id_end, ckr.col_id_start,
                                            ckr.col_id_end );
    return ( pcid < pcid_from_ckr );
}
