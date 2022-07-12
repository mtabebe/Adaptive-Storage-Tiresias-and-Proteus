#pragma once

inline std::vector<partition_column_identifier>
    generate_partition_column_identifiers(
        uint32_t table_id, uint64_t r_start, uint64_t r_end, uint32_t c_start,
        uint32_t                                        c_end,
        const std::vector<table_partition_information>& data_sizes ) {
    std::vector<partition_column_identifier> pids;

    uint64_t row_start = r_start;

    while( row_start <= r_end ) {
        uint64_t cur_start = row_start;
        uint32_t col_start = c_start;
        while( col_start <= c_end ) {
            DVLOG( 40 ) << "Row Start:" << row_start
                        << ", Col Start:" << col_start;
            auto pid = generate_partition_column_identifier(
                table_id, cur_start, col_start, data_sizes );

            row_start = pid.partition_end + 1;
            col_start = pid.column_end + 1;

            pids.push_back( pid );
        }
    }

    return pids;
}

inline std::vector<partition_column_identifier>
    generate_partition_column_identifiers_from_ckrs(
        const std::vector<cell_key_ranges>&             ckrs,
        const std::vector<table_partition_information>& data_sizes ) {
    std::vector<partition_column_identifier> pids;

    for( const auto& ckr : ckrs ) {
        auto loc_pids = generate_partition_column_identifiers(
            ckr.table_id, ckr.row_id_start, ckr.row_id_end, ckr.col_id_start,
            ckr.col_id_end, data_sizes );

        pids.insert( pids.end(), loc_pids.begin(), loc_pids.end() );
    }

    return pids;
}

inline partition_column_identifier generate_partition_column_identifier(
    uint32_t table_id, uint64_t row, uint32_t col,
    const std::vector<table_partition_information>& data_sizes ) {
    DVLOG( 40 ) << "Generate PCID: ( table:" << table_id << ", row:" << row
                << ", col:" << col << ")";
    const auto& part_info = data_sizes.at( table_id );
    uint64_t    p_size = std::get<2>( part_info );
    uint32_t    c_size = std::get<3>( part_info );
    uint32_t    num_cols = std::get<4>( part_info );

    uint64_t p_start = ( row / p_size ) * p_size;
    uint64_t p_end = p_start + ( p_size - 1 );

    uint32_t c_start = ( col / c_size ) * c_size;
    uint32_t c_end = std::min( num_cols, c_start + ( c_size - 1 ) );

    auto pid = create_partition_column_identifier( table_id, p_start, p_end,
                                                   c_start, c_end );

    DVLOG( 40 ) << "Generate PCID: ( table:" << table_id << ", row:" << row
                << ", col:" << col << "), PID:" << pid;

    return pid;
}

inline partition_column_identifier
    generate_partition_column_identifier_from_cid(
        const cell_identifier&                          cid,
        const std::vector<table_partition_information>& data_sizes ) {
    return generate_partition_column_identifier( cid.table_id_, cid.key_,
                                                 cid.col_id_, data_sizes );
}
