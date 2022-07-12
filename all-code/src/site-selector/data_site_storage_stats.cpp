#include "data_site_storage_stats.h"

#include <glog/logging.h>

#include "site_selector_metadata.h"

double estimate_partition_storage_size(
    const std::vector<double>&         widths,
    const partition_column_identifier& pid ) {
    double width = 0;
    for( int32_t col = pid.column_start; col <= pid.column_end; col++ ) {
        double num_rows =
            (double) ( ( pid.partition_end - pid.partition_start ) + 1 );
        DCHECK_LT( (uint32_t) col, widths.size() );
        width += ( num_rows * widths.at( col ) );
    }
    return width;
}

data_site_storage_stat::data_site_storage_stat(
    uint32_t site_id, const std::vector<storage_tier_type::type>& tiers )
    : site_id_( site_id ), per_tier_num_rows_per_column_per_table_() {
    for( const auto& tier : tiers ) {
        per_tier_num_rows_per_column_per_table_[tier] = {};
    }
}

data_site_storage_stat::~data_site_storage_stat() {
    for( auto& entry : per_tier_num_rows_per_column_per_table_ ) {
        const auto& tier = entry.first;
        for( uint32_t table_id = 0;
             table_id <
             per_tier_num_rows_per_column_per_table_.at( tier ).size();
             table_id++ ) {
            delete[] per_tier_num_rows_per_column_per_table_.at( tier ).at(
                table_id );
            per_tier_num_rows_per_column_per_table_.at( tier ).at( table_id ) =
                nullptr;
        }
        per_tier_num_rows_per_column_per_table_.at( tier ).clear();
    }
    per_tier_num_rows_per_column_per_table_.clear();
}

void data_site_storage_stat::create_table( uint32_t table_id,
                                           uint32_t num_columns ) {
    for( auto& entry : per_tier_num_rows_per_column_per_table_ ) {
        const auto& tier = entry.first;
        DCHECK_EQ( table_id,
                   per_tier_num_rows_per_column_per_table_.at( tier ).size() );
        auto entries = new std::atomic<int64_t>[num_columns];
        for ( uint32_t col = 0; col < num_columns; col++) {
            entries[col] = 0;
        }
        per_tier_num_rows_per_column_per_table_.at( tier ).emplace_back(
            entries );
    }

}

double data_site_storage_stat::get_storage_tier_size(
    const storage_tier_type::type&          tier,
    const std::vector<std::vector<double>>& widths ) const {
    DCHECK_LE( tier, k_num_storage_types );
    DCHECK_EQ( per_tier_num_rows_per_column_per_table_.count( tier ), 1 );

    const auto& tier_stats = per_tier_num_rows_per_column_per_table_.at( tier );
    DCHECK_LE( tier_stats.size(), widths.size() );

    double tier_size = 0;

    for( uint32_t table_id = 0; table_id < tier_stats.size(); table_id++ ) {
        const auto& table_stats = tier_stats.at( table_id );
        for( uint32_t col_id = 0; col_id < widths.at( table_id ).size();
             col_id++ ) {
            tier_size +=
                ( widths.at( table_id ).at( col_id ) * table_stats[col_id] );
            DVLOG( 40 ) << "Site:" << site_id_ << ", tier::" << tier
                        << ", col_id:" << col_id
                        << ", width:" << widths.at( table_id ).at( col_id )
                        << ", count:" << table_stats[col_id]
                        << ", tier size so far:" << tier_size;
        }
    }

    return tier_size;
}

void data_site_storage_stat::change_count_in_tier(
    uint32_t site, const storage_tier_type::type& tier,
    const partition_column_identifier& pid, int64_t multiplier ) {
    DCHECK_EQ( site, site_id_ );
    DCHECK_LE( tier, k_num_storage_types );
    DCHECK_EQ( per_tier_num_rows_per_column_per_table_.count( tier ), 1 );

    auto& tier_stats = per_tier_num_rows_per_column_per_table_.at( tier );
    DCHECK_LT( pid.table_id, tier_stats.size() );

    auto& table_stats = tier_stats.at( pid.table_id );

    int64_t num_entries =
        multiplier * ( ( pid.partition_end - pid.partition_start ) + 1 );

    for( int32_t col_id = pid.column_start; col_id <= pid.column_end;
         col_id++ ) {
        table_stats[col_id].fetch_add( num_entries );
        DVLOG( 40 ) << "Site:" << site_id_ << ", tier:" << tier
                    << ", col:" << col_id << ", count:" << table_stats[col_id]
                    << ", fetch added:" << num_entries;
    }
}

data_site_storage_stats::data_site_storage_stats(
    uint32_t num_sites,
    const std::unordered_map<storage_tier_type::type, double>& storage_limits )
    : num_sites_( num_sites ),
      num_tables_( 0 ),
      table_widths_(),
      per_site_stats_(),
      storage_limits_( storage_limits ) {
    per_site_stats_.reserve( num_sites_ );
    DCHECK_LE( storage_limits_.size(), k_num_storage_types + 1 );
    std::vector<storage_tier_type::type> tiers;
    for (const auto& entry: storage_limits_) {
        tiers.emplace_back( entry.first );
    }
    for( uint32_t site_id = 0; site_id < num_sites_; site_id++ ) {
        per_site_stats_.emplace_back(
            data_site_storage_stat( site_id, tiers ) );
    }
}

data_site_storage_stats::~data_site_storage_stats() { per_site_stats_.clear(); }

void data_site_storage_stats::create_table( const table_metadata& meta ) {
    DCHECK_EQ( num_tables_, table_widths_.size() );

    if( num_tables_ == meta.table_id_ ) {
        num_tables_ += 1;
        table_widths_.emplace_back(
            std::vector<double>( meta.num_columns_, 0 ) );
        for( uint32_t i = 0; i < num_sites_; i++ ) {
            per_site_stats_.at( i ).create_table( meta.table_id_,
                                                  meta.num_columns_ );
        }
    }
}

double data_site_storage_stats::get_storage_tier_size(
    uint32_t site, const storage_tier_type::type& tier ) const {
    DCHECK_LT( site, num_sites_ );
    DCHECK_LE( tier, k_num_storage_types );
    DCHECK_EQ( 1, storage_limits_.count( tier ) );
    DCHECK_LT( site, per_site_stats_.size() );

    return per_site_stats_.at( site ).get_storage_tier_size( tier,
                                                             table_widths_ );
}

std::unordered_map<storage_tier_type::type, std::vector<double>>
    data_site_storage_stats::get_storage_tier_sizes() const {
    std::unordered_map<storage_tier_type::type, std::vector<double>> ret;

    for( const auto& entry : storage_limits_ ) {
        std::vector<double> ratios( num_sites_, 0 );
        for( uint32_t site = 0; site < num_sites_; site++ ) {
            ratios.at( site ) = get_storage_tier_size( site, entry.first );
        }
        ret[entry.first] = ratios;
    }

    return ret;
}

double data_site_storage_stats::get_partition_size(
    const partition_column_identifier& pid ) const {
    DCHECK_LT( pid.table_id, table_widths_.size() );
    return estimate_partition_storage_size( table_widths_.at( pid.table_id ),
                                            pid );
}

std::tuple<uint32_t, uint32_t> data_site_storage_stats::get_site_iter_bounds(
    uint32_t site ) const {
    if( site == K_DATA_AT_ALL_SITES ) {
        DCHECK_GT( num_sites_, 0 );
        return std::make_tuple<>( site, num_sites_ - 1 );
    }

    return std::make_tuple<>( site, site );
}

void data_site_storage_stats::change_partition_tier(
    uint32_t site, const storage_tier_type::type& old_tier,
    const storage_tier_type::type&     new_tier,
    const partition_column_identifier& pid ) {
    if( new_tier == old_tier ) {
        return;
    }
    DCHECK_LT( pid.table_id, num_tables_ );
    DCHECK_LE( old_tier, k_num_storage_types );
    DCHECK_LE( new_tier, k_num_storage_types );
    DCHECK_EQ( 1, storage_limits_.count( old_tier ) );
    DCHECK_EQ( 1, storage_limits_.count( new_tier ) );

    auto s_bounds = get_site_iter_bounds( site );

    for( uint32_t s = std::get<0>( s_bounds ); s <= std::get<1>( s_bounds );
         s++ ) {
        DCHECK_LT( s, num_sites_ );
        per_site_stats_.at( s ).change_count_in_tier( s, new_tier, pid, 1 );
        per_site_stats_.at( s ).change_count_in_tier( s, old_tier, pid, -1 );
    }
}

void data_site_storage_stats::add_partition_to_tier(
    uint32_t site, const storage_tier_type::type& tier,
    const partition_column_identifier& pid ) {
    DCHECK_LT( pid.table_id, num_tables_ );
    DCHECK_LE( tier, k_num_storage_types );
    DCHECK_EQ( 1, storage_limits_.count( tier ) );

    auto s_bounds = get_site_iter_bounds( site );

    for( uint32_t s = std::get<0>( s_bounds ); s <= std::get<1>( s_bounds );
         s++ ) {
        DCHECK_LT( s, num_sites_ );
        per_site_stats_.at( s ).change_count_in_tier( s, tier, pid, 1 );
    }
}
void data_site_storage_stats::remove_partition_from_tier(
    uint32_t site, const storage_tier_type::type& tier,
    const partition_column_identifier& pid ) {
    DCHECK_LT( pid.table_id, num_tables_ );
    DCHECK_LE( tier, k_num_storage_types );
    DCHECK_EQ( 1, storage_limits_.count( tier ) );

    auto s_bounds = get_site_iter_bounds( site );

    for( uint32_t s = std::get<0>( s_bounds ); s <= std::get<1>( s_bounds );
         s++ ) {
        DCHECK_LT( s, num_sites_ );
        per_site_stats_.at( s ).change_count_in_tier( s, tier, pid, -1 );
    }
}
void data_site_storage_stats::update_table_widths(
    uint32_t table_id, const std::vector<double>& widths ) {
    DCHECK_LT( table_id, num_tables_ );
    DCHECK_LT( table_id, table_widths_.size() );

    auto& t_widths = table_widths_.at( table_id );

    DCHECK_LE( widths.size(), t_widths.size() );
    for( uint32_t pos = 0; pos < widths.size(); pos++ ) {
        DVLOG( 40 ) << "Set widths of table:" << table_id << ", col:" << pos
                    << ", to:" << widths.at( pos );
        t_widths.at( pos ) = widths.at( pos );
    }
}

void data_site_storage_stats::update_table_widths_from_stats(
    const site_selector_query_stats& stats ) {
    partition_column_identifier pid =
        create_partition_column_identifier( 0, 0, 0, 0, 0 );
    for( uint32_t table_id = 0; table_id < num_tables_; table_id++ ) {
        pid.column_end = table_widths_.at( table_id ).size();
        pid.table_id = table_id;
        if( pid.column_end > 0 ) {
            pid.column_end = pid.column_end - 1;
            update_table_widths( table_id, stats.get_cell_widths( pid ) );
        }
    }
}

double data_site_storage_stats::get_storage_limit(
    uint32_t site, const storage_tier_type::type& tier ) const {
    DCHECK_LT( tier, storage_limits_.size() );
    DCHECK_EQ( 1, storage_limits_.count( tier ) );
    return storage_limits_.at( tier );
}

double data_site_storage_stats::get_storage_ratio(
    uint32_t site, const storage_tier_type::type& tier ) const {
    return compute_storage_ratio( get_storage_tier_size( site, tier ),
                                  get_storage_limit( site, tier ) );
}

// site, tier
std::unordered_map<storage_tier_type::type, std::vector<double>>
    data_site_storage_stats::get_storage_ratios() const {
    std::unordered_map<storage_tier_type::type, std::vector<double>> ret;

    for( const auto& entry : storage_limits_ ) {
        std::vector<double> ratios( num_sites_, 0 );
        for( uint32_t site = 0; site < num_sites_; site++ ) {
            ratios.at( site ) = get_storage_ratio( site, entry.first );
        }
        ret[entry.first] = ratios;
    }

    return ret;
}
std::unordered_map<storage_tier_type::type, std::vector<double>>
    data_site_storage_stats::get_storage_ratios_from_sizes(
        const std::unordered_map<storage_tier_type::type, std::vector<double>>&
            sizes ) const {
    std::unordered_map<storage_tier_type::type, std::vector<double>> ret;

    for( const auto& entry : sizes ) {
        std::vector<double> ratios( entry.second.size(), 0 );
        for( uint32_t site = 0; site < entry.second.size(); site++ ) {
            ratios.at( site ) =
                compute_storage_ratio( entry.second.at( site ),
                                       get_storage_limit( site, entry.first ) );
        }
        ret[entry.first] = ratios;
    }

    return ret;
}
double data_site_storage_stats::compute_storage_ratio( double size,
                                                       double limit ) const {
    return ( size / limit );
}
