#include "site_selector_query_stats.h"

#include <glog/logging.h>

cached_ss_stat_holder::cached_ss_stat_holder()
    : cached_cell_sizes_(),
      cached_selectivity_(),
      cached_num_reads_(),
      cached_num_writes_(),
      cached_contention_() {}

table_width_stats::table_width_stats( const table_metadata& meta )
    : table_id_( meta.table_id_ ), widths_( meta.num_columns_ ) {
    for( uint32_t pos = 0; pos < meta.num_columns_; pos++ ) {
        widths_[pos] = 0;
    }
}

double table_width_stats::get_cell_width( uint32_t col_id ) const {
    DCHECK_GE( col_id, 0 );
    DCHECK_LT( col_id, widths_.size() );
    return widths_.at( col_id );
}
std::vector<double> table_width_stats::get_cell_widths(
    const partition_column_identifier& pid ) const {
    DCHECK_EQ( pid.table_id, (int32_t) table_id_ );
    return get_cell_widths( pid.column_start, pid.column_end );
}
std::vector<double> table_width_stats::get_cell_widths(
    uint32_t col_start, uint32_t col_end ) const {
    DCHECK_LE( col_start, col_end );
    DCHECK_GE( col_start, 0 );
    DCHECK_LT( col_end, widths_.size() );

    std::vector<double> found_widths( ( col_end - col_start ) + 1, 0 );
    uint32_t            pos = 0;
    for( uint32_t col_id = col_start; col_id <= col_end; col_id++ ) {
        found_widths.at( pos ) = get_cell_width( col_id );
        pos += 1;
    }
    return found_widths;
}

std::vector<double> table_width_stats::get_and_cache_cell_widths(
    const partition_column_identifier& pid,
    cached_cell_sizes_by_table&        col_sizes ) const {
    std::vector<double> widths;

    uint32_t table_id = pid.table_id;
    DCHECK_EQ( table_id, table_id_ );

    for( uint32_t col = (uint32_t) pid.column_start;
         col <= (uint32_t) pid.column_end; col++ ) {
        auto   found = col_sizes[table_id].find( col );
        double size = 0;
        if( found == col_sizes[table_id].end() ) {
            size = get_cell_width( col );
            col_sizes[table_id][col] = size;
        } else {
            size = found->second;
        }
        widths.emplace_back( size );
    }

    return widths;
}

void table_width_stats::set_cell_width( uint32_t table_id, uint32_t col_id,
                                        double width ) {
    DCHECK_EQ( table_id, table_id_ );
    DCHECK_LT( col_id, widths_.size() );
    if( widths_.at( col_id ) != width ) {
        widths_.at( col_id ) = width;
    }
}

table_selectivity_stats::table_selectivity_stats( const table_metadata& meta )
    : table_id_( meta.table_id_ ), selectivity_( meta.num_columns_ ) {
    for( uint32_t pos = 0; pos < meta.num_columns_; pos++ ) {
        selectivity_[pos] = 0;
    }
}

double table_selectivity_stats::get_cell_selectivity( uint32_t col_id ) const {
    DCHECK_LT( col_id, selectivity_.size() );
    return selectivity_.at( col_id );
}
double table_selectivity_stats::get_selectivity(
    const partition_column_identifier& pid ) const {
    DCHECK_EQ( pid.table_id, table_id_ );
    return get_selectivity( pid.column_start, pid.column_end );
}
double table_selectivity_stats::get_selectivity( uint32_t col_start,
                                                 uint32_t col_end ) const {
    double sel = 1.0;
    for( uint32_t col = col_start; col <= col_end; col++ ) {
        sel = sel * get_cell_selectivity( col );
    }
    return sel;
}
double table_selectivity_stats::get_and_cache_cell_selectivity(
    uint32_t col_id, cached_selectivity_by_table& cached_selectivity ) const {
    DCHECK_LT( col_id, selectivity_.size() );

    auto   found = cached_selectivity[table_id_].find( col_id );
    double sel = 0;
    if( found == cached_selectivity[table_id_].end() ) {
        sel = get_cell_selectivity( col_id );
        cached_selectivity[table_id_][col_id] = sel;
    } else {
        sel = found->second;
    }
    return sel;
}
double table_selectivity_stats::get_and_cache_selectivity(
    const partition_column_identifier& pid,
    cached_selectivity_by_table&       cached_selectivity ) const {
    DCHECK_EQ( pid.table_id, table_id_ );
    return get_and_cache_selectivity( pid.column_start, pid.column_end,
                                      cached_selectivity );
}
double table_selectivity_stats::get_and_cache_selectivity(
    uint32_t col_start, uint32_t col_end,
    cached_selectivity_by_table& cached_selectivity ) const {
    double sel = 1.0;
    for( uint32_t col = col_start; col < col_end; col++ ) {
        sel = sel * get_and_cache_cell_selectivity( col, cached_selectivity );
    }
    return sel;

}

void table_selectivity_stats::set_cell_selectivity( uint32_t table_id,
                                                    uint32_t col_id,
                                                    double   sel ) {
    DCHECK_EQ( table_id, table_id_ );
    DCHECK_LT( col_id, selectivity_.size() );
    if( selectivity_.at( col_id ) != sel ) {
        selectivity_.at( col_id ) = sel;
    }
}

table_access_frequency::table_access_frequency(
    const table_metadata& meta, std::atomic<uint64_t>* total_num_updates,
    std::atomic<uint64_t>* total_num_reads )
    : table_id_( meta.table_id_ ),
      total_num_updates_( total_num_updates ),
      total_num_reads_( total_num_reads ),
      running_num_updates_( meta.num_columns_ ),
      num_update_txns_( meta.num_columns_ ),
      running_num_reads_( meta.num_columns_ ),
      num_read_txns_( meta.num_columns_ ) {
    for( uint32_t col = 0; col < meta.num_columns_; col++ ) {

        running_num_updates_[col] = 0;
        num_update_txns_[col] = 0;

        running_num_reads_[col] = 0;
        num_read_txns_[col] = 0;
    }
}

double table_access_frequency::get_average_contention( uint32_t col) const {
    DCHECK_LT( col, running_num_updates_.size() );

    double updates_to_parts = (double) running_num_updates_.at( col );
    double total_num_updates = (double) total_num_updates_->load();

    if( total_num_updates == 0 ) {
        return 0;
    }

    return updates_to_parts / total_num_updates;
}
double table_access_frequency::get_average_contention(
    const partition_column_identifier& pid ) const {
    DCHECK_EQ( table_id_, pid.table_id );

    DCHECK_LT( pid.column_start, running_num_updates_.size() );
    DCHECK_LT( pid.column_end, running_num_updates_.size() );

    double updates_to_parts = 0;
    for( int32_t pos = pid.column_start; pos <= pid.column_end; pos++ ) {
        updates_to_parts += (double) running_num_updates_.at( pos );
    }

    double total_num_updates = (double) total_num_updates_->load();

    if( total_num_updates == 0 ) {
        return 0;
    }

    return updates_to_parts / total_num_updates;
}
double table_access_frequency::get_and_cache_average_contention(
    uint32_t col, cached_accesses_by_table& cached_contention ) const {
    auto   found = cached_contention[table_id_].find( col );
    double cont = 0;
    if( found == cached_contention[table_id_].end() ) {
        cont = get_average_contention( col );
        cached_contention[table_id_][col] = cont;
    } else {
        cont = found->second;
    }
    return cont;
}

double table_access_frequency::get_and_cache_average_contention(
    const partition_column_identifier& pid,
    cached_accesses_by_table&          cached_contention ) const {
    DCHECK_EQ( table_id_, pid.table_id );
    double cont = 0;
    for( int32_t col = pid.column_start; col <= pid.column_end; col++ ) {
        cont += get_and_cache_average_contention( col, cached_contention );
    }

    return cont;
}

double table_access_frequency::get_average_num_reads(
    const partition_column_identifier& pid ) const {
    return get_average_num_ops( pid, running_num_reads_, num_read_txns_ );
}
double table_access_frequency::get_average_num_updates(
    const partition_column_identifier& pid ) const {
    return get_average_num_ops( pid, running_num_updates_, num_update_txns_ );
}

double table_access_frequency::get_and_cache_average_num_reads(
    uint32_t col, cached_accesses_by_table& cached_reads ) const {
    auto   found = cached_reads[table_id_].find( col );
    double num = 0;
    if( found == cached_reads[table_id_].end() ) {
        num = get_average_num_reads( col );
        cached_reads[table_id_][col] = num;
    } else {
        num = found->second;
    }
    return num;
}
double table_access_frequency::get_and_cache_average_num_reads(
    const partition_column_identifier& pid,
    cached_accesses_by_table&          cached_reads ) const {
    DCHECK_EQ( table_id_, pid.table_id );
    double cont = 0;
    for( int32_t col = pid.column_start; col <= pid.column_end; col++ ) {
        cont += get_and_cache_average_num_reads( col, cached_reads );
    }
    return cont;
}

double table_access_frequency::get_and_cache_average_num_updates(
    uint32_t col, cached_accesses_by_table& cached_updates ) const {
    auto   found = cached_updates[table_id_].find( col );
    double num = 0;
    if( found == cached_updates[table_id_].end() ) {
        num = get_average_num_updates( col );
        cached_updates[table_id_][col] = num;
    } else {
        num = found->second;
    }
    return num;
}
double table_access_frequency::get_and_cache_average_num_updates(
    const partition_column_identifier& pid,
    cached_accesses_by_table&          cached_updates ) const {
    DCHECK_EQ( table_id_, pid.table_id );
    double cont = 0;
    for( int32_t col = pid.column_start; col <= pid.column_end; col++ ) {
        cont += get_and_cache_average_num_updates( col, cached_updates );
    }
    return cont;
}

double table_access_frequency::get_average_num_reads( uint32_t col ) const {
    return get_average_num_ops( col, running_num_reads_, num_read_txns_ );
}
double table_access_frequency::get_average_num_updates( uint32_t col ) const {
    return get_average_num_ops( col, running_num_updates_, num_update_txns_ );
}

double table_access_frequency::get_average_num_ops(
    uint32_t col, const std::vector<std::atomic<uint64_t>>& running_num_ops,
    const std::vector<std::atomic<uint64_t>>& num_txns ) const {
    double num_ops = 0;
    double loc_num_txns = (double) num_txns.at( col );
    double loc_running = (double) running_num_ops.at( col );
    if( loc_running ) {
        num_ops += ( loc_num_txns / loc_running );
    }

    return num_ops;
}

double table_access_frequency::get_average_num_ops(
    const partition_column_identifier&        pid,
    const std::vector<std::atomic<uint64_t>>& running_num_ops,
    const std::vector<std::atomic<uint64_t>>& num_txns ) const {
    DCHECK_EQ( table_id_, pid.table_id );

    DCHECK_LT( pid.column_start, running_num_ops.size() );
    DCHECK_LT( pid.column_end, running_num_ops.size() );
    DCHECK_LT( pid.column_start, num_txns.size() );
    DCHECK_LT( pid.column_end, num_txns.size() );

    double num_ops = 0;
    for( int32_t pos = pid.column_start; pos <= pid.column_end; pos++ ) {
        double loc_num_txns = (double) num_txns.at( pos );
        double loc_running = (double) running_num_ops.at( pos );
        if( loc_running ) {
            num_ops += ( loc_num_txns / loc_running );
        }
    }

    return num_ops;
}

void table_access_frequency::add_accesses_internal(
    std::atomic<uint64_t>*              total_num,
    std::vector<std::atomic<uint64_t>>& running_num_ops,
    std::vector<std::atomic<uint64_t>>& num_op_txns,
    const std::vector<uint64_t>&        counts ) {
    if( counts.empty() ) {
        return;
    }
    DCHECK_EQ( counts.size(), running_num_ops.size() + 1 );
    DCHECK_EQ( counts.size(), num_op_txns.size() + 1 );

    // last pos is the total
    total_num->fetch_add( counts.at( counts.size() - 1 ) );

    for( uint32_t pos = 0; pos < num_op_txns.size(); pos++ ) {
        uint64_t count = counts.at( pos );
        if( count == 0 ) {
            continue;
        }

        num_op_txns.at( pos ).fetch_add( 1 );
        running_num_ops.at( pos ).fetch_add( count );
    }
}

void table_access_frequency::add_accesses(
    const std::vector<uint64_t>& writes, const std::vector<uint64_t>& reads ) {
    add_accesses_internal( total_num_updates_, running_num_updates_,
                           num_update_txns_, writes );
    add_accesses_internal( total_num_reads_, running_num_reads_, num_read_txns_,
                           reads );
}

site_selector_query_stats::site_selector_query_stats()
    : num_tables_( 0 ),
      num_columns_(),
      total_num_updates_( 0 ),
      total_num_reads_( 0 ),
      table_widths_(),
      table_selectivity_(),
      table_accesses_() {}

void site_selector_query_stats::create_table( const table_metadata& meta ) {
    if( num_tables_ == meta.table_id_ ) {
        num_tables_ += 1;
        table_widths_.emplace_back( table_width_stats( meta ) );
        table_selectivity_.emplace_back( table_selectivity_stats( meta ) );
        table_accesses_.emplace_back( table_access_frequency(
            meta, &total_num_updates_, &total_num_reads_ ) );
        num_columns_.emplace_back( meta.num_columns_ );
    }
}

std::vector<double> site_selector_query_stats::get_cell_widths(
    const partition_column_identifier& pid ) const {
    DCHECK_LT( pid.table_id, table_widths_.size() );
    return table_widths_.at( pid.table_id ).get_cell_widths( pid );
}

std::vector<double> site_selector_query_stats::get_and_cache_cell_widths(
    const partition_column_identifier& pid,
    cached_cell_sizes_by_table&        col_sizes ) const {
    DCHECK_LT( pid.table_id, table_widths_.size() );
    return table_widths_.at( pid.table_id )
        .get_and_cache_cell_widths( pid, col_sizes );
}
std::vector<double> site_selector_query_stats::get_and_cache_cell_widths(
    const partition_column_identifier& pid,
    cached_ss_stat_holder&             cached ) const {
    return get_and_cache_cell_widths( pid, cached.cached_cell_sizes_ );
}

double site_selector_query_stats::get_average_scan_selectivity(
    const partition_column_identifier& pid ) const {
    DCHECK_LT( pid.table_id, table_selectivity_.size() );
    return table_selectivity_.at( pid.table_id ).get_selectivity( pid );
}
double site_selector_query_stats::get_and_cache_average_scan_selectivity(
    const partition_column_identifier& pid,
    cached_selectivity_by_table&       cached ) const {
    DCHECK_LT( pid.table_id, table_selectivity_.size() );
    return table_selectivity_.at( pid.table_id )
        .get_and_cache_selectivity( pid, cached );
}
double site_selector_query_stats::get_and_cache_average_scan_selectivity(
    const partition_column_identifier& pid,
    cached_ss_stat_holder&             cached ) const {
    return get_and_cache_average_scan_selectivity( pid,
                                                   cached.cached_selectivity_ );
}

double site_selector_query_stats::get_average_num_reads(
    const partition_column_identifier& pid ) const {
    DCHECK_LT( pid.table_id, table_accesses_.size() );
    return table_accesses_.at( pid.table_id ).get_average_num_reads( pid );
}
double site_selector_query_stats::get_and_cache_average_num_reads(
    const partition_column_identifier& pid,
    cached_accesses_by_table&          cached ) const {
    DCHECK_LT( pid.table_id, table_accesses_.size() );
    return table_accesses_.at( pid.table_id )
        .get_and_cache_average_num_reads( pid, cached );
}
double site_selector_query_stats::get_and_cache_average_num_reads(
    const partition_column_identifier& pid,
    cached_ss_stat_holder&             cached ) const {
    return get_and_cache_average_num_reads( pid, cached.cached_num_reads_ );
}

double site_selector_query_stats::get_average_num_updates(
    const partition_column_identifier& pid ) const {
    DCHECK_LT( pid.table_id, table_accesses_.size() );
    return table_accesses_.at( pid.table_id ).get_average_num_updates( pid );
}
double site_selector_query_stats::get_and_cache_average_num_updates(
    const partition_column_identifier& pid,
    cached_accesses_by_table&          cached ) const {
    DCHECK_LT( pid.table_id, table_accesses_.size() );
    return table_accesses_.at( pid.table_id )
        .get_and_cache_average_num_updates( pid, cached );
}
double site_selector_query_stats::get_and_cache_average_num_updates(
    const partition_column_identifier& pid,
    cached_ss_stat_holder&             cached ) const {
    return get_and_cache_average_num_updates( pid, cached.cached_num_writes_ );
}

double site_selector_query_stats::get_average_contention(
    const partition_column_identifier& pid ) const {
    DCHECK_LT( pid.table_id, table_accesses_.size() );
    return table_accesses_.at( pid.table_id ).get_average_contention( pid );
}
double site_selector_query_stats::get_and_cache_average_contention(
    const partition_column_identifier& pid,
    cached_accesses_by_table&          cached ) const {
    DCHECK_LT( pid.table_id, table_accesses_.size() );
    return table_accesses_.at( pid.table_id )
        .get_and_cache_average_contention( pid, cached );
}
double site_selector_query_stats::get_and_cache_average_contention(
    const partition_column_identifier& pid,
    cached_ss_stat_holder&             cached ) const {
    return get_and_cache_average_contention( pid, cached.cached_contention_ );
}

void site_selector_query_stats::set_cell_width( uint32_t table_id,
                                                uint32_t col_id,
                                                double   width ) {
    DCHECK_LT( table_id, table_widths_.size() );
    table_widths_.at( table_id ).set_cell_width( table_id, col_id, width );
}
void site_selector_query_stats::set_column_selectivity( uint32_t table_id,
                                                        uint32_t col_id,
                                                        double   selectivity ) {
    DCHECK_LT( table_id, table_selectivity_.size() );
    table_selectivity_.at( table_id )
        .set_cell_selectivity( table_id, col_id, selectivity );
}

void site_selector_query_stats::add_transaction_accesses(
    const std::vector<cell_key_ranges>& writes,
    const std::vector<cell_key_ranges>& reads ) {
    // per table sizes
    std::vector<std::vector<uint64_t>> col_writes = fill_observations( writes );
    std::vector<std::vector<uint64_t>> col_reads = fill_observations( reads );

    DCHECK_EQ( col_writes.size(), col_reads.size() );
    DCHECK_EQ( col_writes.size(), num_tables_ );

    for( uint32_t table_id = 0; table_id < num_tables_; table_id++ ) {
        table_accesses_.at( table_id )
            .add_accesses( col_writes.at( table_id ),
                           col_reads.at( table_id ) );
    }
}

std::vector<std::vector<uint64_t>> site_selector_query_stats::fill_observations(
    const std::vector<cell_key_ranges>& ckrs ) const {
    std::vector<std::vector<uint64_t>> counts( num_tables_ );

    for( const auto& ckr : ckrs ) {
        if( counts.at( ckr.table_id ).empty() ) {
            DCHECK_LT( ckr.table_id, num_columns_.size() );
            counts.at( ckr.table_id ) =
                std::vector<uint64_t>( num_columns_.at( ckr.table_id ) + 1, 0 );
        }
        DCHECK( !counts.at( ckr.table_id ).empty() );
        uint64_t num_ops = ( ckr.row_id_end - ckr.row_id_start ) + 1;
        uint32_t total_pos = num_columns_.at( ckr.table_id );
        for( int32_t col = ckr.col_id_start; col <= ckr.col_id_end; col++ ) {
            DCHECK_LT( col, counts.at( ckr.table_id ).size() );
            counts.at( ckr.table_id ).at( col ) =
                num_ops + counts.at( ckr.table_id ).at( col );
            counts.at( ckr.table_id ).at( total_pos ) =
                num_ops + counts.at( ckr.table_id ).at( total_pos );
        }
    }

    return counts;
}
