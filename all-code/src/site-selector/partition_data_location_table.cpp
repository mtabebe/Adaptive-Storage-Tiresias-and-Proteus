#include "partition_data_location_table.h"

// partition_data_location_table

partition_data_location_table::partition_data_location_table(
    const partition_data_location_table_configs& configs )
    : configs_( configs ),
      sample_based_site_read_access_counts_( configs_.num_sites_ ),
      sample_based_site_write_access_counts_( configs_.num_sites_ ),
      site_read_access_counts_( configs_.num_sites_ ),
      site_write_access_counts_( configs_.num_sites_ ),
      max_write_access_to_partition_( 1 ),
      query_stats_( std::make_unique<site_selector_query_stats>() ),
      storage_stats_( std::make_unique<data_site_storage_stats>(
          configs_.num_sites_, configs_.storage_tier_limits_ ) ),
      table_() {}

partition_data_location_table::~partition_data_location_table() {}

uint32_t partition_data_location_table::create_table(
    const table_metadata& metadata ) {
    uint32_t id = table_.create_table( metadata );
    query_stats_->create_table( metadata );
    storage_stats_->create_table( metadata );
    return id;
}

uint32_t partition_data_location_table::get_num_tables() const {
    return table_.get_num_tables();
}

std::vector<std::shared_ptr<partition_payload>>
    partition_data_location_table::get_partitions(
        const std::vector<cell_key_ranges>& cks,
        const partition_lock_mode&          lock_mode ) const {
    return table_.get_partitions( cks, lock_mode );
}

std::shared_ptr<partition_payload> partition_data_location_table::get_partition(
    const cell_key& ck, const partition_lock_mode& lock_mode ) const {
    return table_.get_partition( ck, lock_mode );
}
std::shared_ptr<partition_payload> partition_data_location_table::get_partition(
    const partition_column_identifier& pid,
    const partition_lock_mode&         lock_mode ) const {
    return table_.get_partition( pid, lock_mode );
}
std::vector<std::shared_ptr<partition_payload>>
    partition_data_location_table::get_partition(
        const cell_key_ranges& ckr, const partition_lock_mode& lock_mode ) const {
    return table_.get_partition( ckr, lock_mode );
}

std::vector<std::shared_ptr<partition_payload>>
    partition_data_location_table::get_or_create_partitions(
        const std::vector<cell_key_ranges>& cks,
        const partition_lock_mode&          lock_mode ) {
    return table_.get_or_create_partitions( cks, lock_mode );
}

grouped_partition_information
    partition_data_location_table::get_partitions_and_group(
        const std::vector<cell_key_ranges>& sorted_write_cks,
        const std::vector<cell_key_ranges>& sorted_read_cks,
        const partition_lock_mode& lock_mode, int num_sites,
        bool allow_missing ) const {
    return table_.get_partitions_and_group( sorted_write_cks, sorted_read_cks,
                                            lock_mode, num_sites,
                                            allow_missing );
}
grouped_partition_information
    partition_data_location_table::get_or_create_partitions_and_group(
        const std::vector<cell_key_ranges>& sorted_write_cks,
        const std::vector<cell_key_ranges>& sorted_read_cks,
        const partition_lock_mode& lock_mode, int num_sites ) {
    return table_.get_or_create_partitions_and_group(
        sorted_write_cks, sorted_read_cks, lock_mode, num_sites );
}

std::shared_ptr<partition_payload>
    partition_data_location_table::insert_partition(
        std::shared_ptr<partition_payload> payload,
        const partition_lock_mode&         lock_mode ) {
    return table_.insert_partition( payload, lock_mode );
}
std::shared_ptr<partition_payload>
    partition_data_location_table::remove_partition(
        std::shared_ptr<partition_payload> payload ) {
    return table_.remove_partition( payload );
}

std::vector<std::shared_ptr<partition_payload>>
    partition_data_location_table::split_partition(
        std::shared_ptr<partition_payload> partition, uint64_t row_split_point,
        uint32_t col_split_point, const partition_type::type& low_type,
        const partition_type::type&    high_type,
        const storage_tier_type::type& low_storage_type,
        const storage_tier_type::type& high_storage_type,
        uint32_t                       low_update_destination_slot,
        uint32_t                       high_update_destination_slot,
        const partition_lock_mode&     lock_mode ) {
    return table_.split_partition(
        partition, row_split_point, col_split_point, low_type, high_type,
        low_storage_type, high_storage_type, low_update_destination_slot,
        high_update_destination_slot, lock_mode );
}
std::shared_ptr<partition_payload>
    partition_data_location_table::merge_partition(
        std::shared_ptr<partition_payload> low_partition,
        std::shared_ptr<partition_payload> high_partition,
        const partition_type::type&        merge_type,
        const storage_tier_type::type&     merge_storage_type,
        uint32_t                           update_destination_slot,
        const partition_lock_mode&         lock_mode ) {
    return table_.merge_partition( low_partition, high_partition, merge_type,
                                   merge_storage_type, update_destination_slot,
                                   lock_mode );
}

double partition_data_location_table::compute_average_write_accesses() const {
    // Get the total number of partitions.
    int total_num_partitions = get_approx_total_number_of_partitions();
    DVLOG( 7 ) << "Total Number of partitions: " << total_num_partitions;
    uint64_t total_acceses = 0;
    for( size_t i = 0; i < site_write_access_counts_.size(); i++ ) {
        total_acceses += site_write_access_counts_.at( i );
    }
    DVLOG( 7 ) << "Total # of accesses: " << total_acceses;

    return ( ( (double) total_acceses ) / total_num_partitions );
}

double
    partition_data_location_table::compute_standard_deviation_write_accesses()
        const {
    int64_t max_num_accesses = max_write_access_to_partition_;
    // std deviation from data range (C is 6, because it is 3 deviations from
    // the mean)
    DVLOG( 7 ) << "Max # of accesses: " << max_num_accesses;
    return ( ( (double) max_num_accesses ) / 6 );
}

double partition_data_location_table::compute_average_read_accesses() const {
    // Get the total number of partitions.
    int      total_num_partitions = get_approx_total_number_of_partitions();
    uint64_t total_acceses = 0;
    for( size_t i = 0; i < site_read_access_counts_.size(); i++ ) {
        total_acceses += site_read_access_counts_.at( i );
    }

    return ( ( (double) total_acceses ) / total_num_partitions );
}

double partition_data_location_table::get_current_load_balance_score() const {
    std::vector<uint64_t> per_site_access_counts;
    uint64_t              total_access_counts = 0;
    double                load_balance_score = 0.0;

    int num_sites = (int) configs_.num_sites_;

    for( int i = 0; i < num_sites; i++ ) {
        int64_t site_total = site_read_access_counts_.at( i ) +
                             site_write_access_counts_.at( i );
        DCHECK( site_total >= 0 );
        per_site_access_counts.push_back( (uint64_t) site_total );
        total_access_counts += (uint64_t) site_total;
    }

    double expected_per_site_freq = 1.0 / num_sites;
    for( int i = 0; i < num_sites; i++ ) {
        double true_freq =
            ( (double) per_site_access_counts.at( i ) ) / total_access_counts;
        double diff = sqrt( pow( true_freq - expected_per_site_freq, 2 ) );
        load_balance_score -= diff;
    }
    return load_balance_score;
}

std::vector<double> partition_data_location_table::get_per_site_load_factors()
    const {
    std::vector<double> per_site_access_counts;
    uint64_t            total_access_counts = 0;

    int num_sites = (int) configs_.num_sites_;

    for( int i = 0; i < num_sites; i++ ) {
        uint64_t site_total = site_read_access_counts_.at( i ) +
                              site_write_access_counts_.at( i );
        DCHECK( site_total >= 0 );
        per_site_access_counts.push_back( (double) site_total );
        total_access_counts += (uint64_t) site_total;
    }

    for( int i = 0; i < num_sites; i++ ) {
        per_site_access_counts.at( i ) =
            per_site_access_counts.at( i ) / total_access_counts;
    }
    return per_site_access_counts;
}

void partition_data_location_table::increase_tracking_counts(
    int                                              destination,
    std::vector<std::shared_ptr<partition_payload>>& read_partitions,
    std::vector<std::shared_ptr<partition_payload>>& write_partitions ) {

    modify_site_read_access_count( destination, read_partitions.size() );
    for( const auto& partition : read_partitions ) {
        partition->read_accesses_++;
    }
    modify_site_write_access_count( destination, write_partitions.size() );
    for( const auto& partition : write_partitions ) {
        partition->write_accesses_++;
    }
}

uint64_t partition_data_location_table::get_approx_total_number_of_partitions()
    const {
    return table_.get_approx_total_number_of_partitions();
}

site_selector_query_stats* partition_data_location_table::get_stats() const {
    return query_stats_.get();
}
data_site_storage_stats* partition_data_location_table::get_storage_stats()
    const {
    return storage_stats_.get();
}


multi_version_partition_data_location_table*
    partition_data_location_table::get_partition_location_table() {
    return &table_;
}

uint64_t partition_data_location_table::get_default_partition_size_for_table(
    uint32_t table_id ) const {
    return table_.get_default_partition_size_for_table( table_id );
}

table_metadata partition_data_location_table::get_table_metadata(
    uint32_t table_id ) const {
    return table_.get_table_metadata( table_id );
}

partition_data_location_table_configs
    partition_data_location_table::get_configs() const {
    return configs_;
}
