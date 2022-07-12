#include "table_stats.h"

#include <glog/logging.h>

#include "../../concurrency/atomic_double.h"

table_stats::table_stats( const table_metadata& metadata )
    : table_id_( metadata.table_id_ ),
      col_types_( metadata.col_types_ ),
      running_col_sizes_( metadata.col_types_.size() ),
      col_counts_( metadata.col_types_.size() ),
      running_selectivity_( metadata.col_types_.size() ),
      selectivity_count_( metadata.col_types_.size() ),
      storage_tier_lock_(),
      storage_tier_changes_() {
    DCHECK_EQ( col_types_.size(), col_counts_.size() );
    DCHECK_EQ( col_types_.size(), col_counts_.size() );

    for ( uint32_t pos = 0; pos < col_types_.size(); pos++) {
        running_col_sizes_[pos] = 0;
        col_counts_[pos] = 0;
        running_selectivity_[pos] = 0;
        selectivity_count_[pos] = 0;

        switch( col_types_.at( pos ) ) {
            case cell_data_type::UINT64: {
                running_col_sizes_.at( pos ) = sizeof( uint64_t );
                col_counts_.at( pos ) = 1;
                break;
            }
            case cell_data_type::INT64: {
                running_col_sizes_.at( pos ) = sizeof( int64_t );
                col_counts_.at( pos ) = 1;
                break;
            }
            case cell_data_type::DOUBLE: {
                running_col_sizes_.at( pos ) = sizeof( double );
                col_counts_.at( pos ) = 1;
                break;
            }
            case cell_data_type::STRING: {
                break;
            }
            case cell_data_type::MULTI_COLUMN: {
                break;
            }
        }
    }
}
table_stats::~table_stats() {}

std::vector<cell_widths_stats> table_stats::get_average_cell_widths() const {
    DCHECK_EQ( running_col_sizes_.size(), col_counts_.size() );
    std::vector<cell_widths_stats> csws;
    for( uint32_t pos = 0; pos < running_col_sizes_.size(); pos++ ) {
        csws.emplace_back( create_cell_widths_stats(
            col_counts_.at( pos ), running_col_sizes_.at( pos ) ) );
        DVLOG( 40 ) << "Get average cell width:" << pos << "="
                    << csws.at( pos );
    }
    return csws;
}

std::vector<selectivity_stats> table_stats::get_selectivity_stats() const  {
    DCHECK_EQ( running_selectivity_.size(), selectivity_count_.size() );
    std::vector<selectivity_stats> sel_stats;
    for( uint32_t pos = 0; pos < selectivity_count_.size(); pos++ ) {
        sel_stats.emplace_back( create_selectivity_stats(
            selectivity_count_.at( pos ), running_selectivity_.at( pos ) ) );
    }
    return sel_stats;
}

void table_stats::update_width( uint32_t col_id, int32_t old_width,
                                int32_t new_width ) {
    if( old_width < 0 ) {
        return add_width( col_id, new_width );
    }

    if( new_width == old_width ) {
        return;
    }

    DCHECK_LT( col_id, col_types_.size() );
    if( col_types_.at( col_id ) != cell_data_type::STRING ) {
        return;
    }
    DCHECK_LT( col_id, running_col_sizes_.size() );
    double diff = (double) new_width - (double) old_width;
    DVLOG( 40 ) << "Update width:" << col_id << "=" << diff
                << "(old:" << old_width << ", new: " << new_width << ")";
    atomic_fetch_add( &running_col_sizes_, col_id, diff );
}
void table_stats::add_width( uint32_t col_id, int32_t new_width ) {
    if( new_width < 0 ) {
        return;
    }

    DCHECK_LT( col_id, col_types_.size() );
    if( col_types_.at( col_id ) != cell_data_type::STRING ) {
        return;
    }
    DCHECK_LT( col_id, col_counts_.size() );
    col_counts_.at( col_id ).fetch_add( 1 );
    DCHECK_LT( col_id, running_col_sizes_.size() );
    double diff = (double)new_width;
    DVLOG( 40 ) << "Add width:" << col_id << "=" << diff;
    atomic_fetch_add( &running_col_sizes_, col_id, diff );
}
void table_stats::remove_width( uint32_t col_id, int32_t old_width ) {
    if( old_width < 0 ) {
        return;
    }
    DCHECK_LT( col_id, col_types_.size() );
    if( col_types_.at( col_id ) != cell_data_type::STRING ) {
        return;
    }
    DCHECK_LT( col_id, running_col_sizes_.size() );
    double diff = ( -(double) old_width );
    DVLOG( 40 ) << "Remove width:" << col_id << "=" << diff;
    atomic_fetch_add( &running_col_sizes_, col_id, diff );
    col_counts_.at( col_id ).fetch_sub( 1 );
}

void table_stats::add_selectivity( uint32_t col_id, double selectivity ) {
    DCHECK_LT( col_id, selectivity_count_.size() );
    selectivity_count_.at( col_id ).fetch_add( 1 );

    DCHECK_LT( col_id, running_selectivity_.size() );
    atomic_fetch_add( &running_selectivity_, col_id, selectivity );
}

void table_stats::record_storage_tier_change(
    const partition_column_identifier& pid,
    const storage_tier_type::type&     tier ) {
    DVLOG( 40 ) << "Recording storage tier change:" << pid
                << ", to tier:" << tier;
    {
        std::lock_guard<std::mutex> guard( storage_tier_lock_ );
        storage_tier_changes_.emplace( pid, tier );
    }
}
void table_stats::get_storage_tier_changes(
    partition_column_identifier_map_t<storage_tier_type::type>& changes ) {
    DVLOG( 40 ) << "Getting storage tier changes";
    partition_column_identifier_map_t<storage_tier_type::type>
        loc_storage_tier_changes;
    {
        std::lock_guard<std::mutex> guard( storage_tier_lock_ );
        loc_storage_tier_changes.swap( storage_tier_changes_ );
    }
    changes.insert( loc_storage_tier_changes.begin(),
                    loc_storage_tier_changes.end() );
}
