#pragma once

inline void
    partition_data_location_table::increase_sample_based_site_read_access_count(
        int site, uint64_t count ) {
    DVLOG( 10 ) << "Increase sample based site read access count by:" << count
                << ", site:" << site;
    DCHECK_LT( site, sample_based_site_read_access_counts_.size() );
    sample_based_site_read_access_counts_.at( site ) += count;
}

inline void
    partition_data_location_table::decrease_sample_based_site_read_access_count(
        int site, uint64_t count ) {
    DVLOG( 10 ) << "Decrease sample based site read access count by:" << count
                << ", site:" << site;
    DCHECK_LT( site, sample_based_site_read_access_counts_.size() );
    sample_based_site_read_access_counts_.at( site ) -= count;
}

inline void partition_data_location_table::
    increase_sample_based_site_write_access_count( int site, uint64_t count ) {
    DVLOG( 10 ) << "Increase sample based site write access count by:" << count
                << ", site:" << site;
    DCHECK_LT( site, sample_based_site_write_access_counts_.size() );
    sample_based_site_write_access_counts_.at( site ) += count;
}

inline void partition_data_location_table::
    decrease_sample_based_site_write_access_count( int site, uint64_t count ) {
    DVLOG( 10 ) << "Decrease sample based site write access count by:" << count
                << ", site:" << site;
    DCHECK_LT( site, sample_based_site_write_access_counts_.size() );
    sample_based_site_write_access_counts_.at( site ) -= count;
}

inline void partition_data_location_table::modify_site_read_access_count(
    int site, int64_t count ) {
    DVLOG( 10 ) << "Modify site read access count by:" << count
                << ", site:" << site;
    site_read_access_counts_.at( site ) += count;
    DVLOG( 10 ) << "Site read access count:" << site
                << ", count:" << site_write_access_counts_.at( site );
}
inline void partition_data_location_table::modify_site_write_access_count(
    int site, int64_t count ) {
    DVLOG( 10 ) << "Modify site write access count by:" << count
                << ", site:" << site;
    site_write_access_counts_.at( site ) += count;
    DVLOG( 10 ) << "Site write access count:" << site
                << ", count:" << site_write_access_counts_.at( site );
}

inline std::vector<std::atomic<int64_t>>*
    partition_data_location_table::get_site_read_access_counts() {
    return &site_read_access_counts_;
}
inline std::vector<std::atomic<int64_t>>*
    partition_data_location_table::get_site_write_access_counts() {
    return &site_write_access_counts_;
}

inline void partition_data_location_table::set_max_write_accesses_if_greater(
    int64_t count ) {
    int64_t existing_max = max_write_access_to_partition_;
    if( count > existing_max ) {
        max_write_access_to_partition_.compare_exchange_strong( existing_max,
                                                                count );
    }
}

inline void partition_data_location_table::blend_max_write_accesses_if_greater(
    int64_t count, double blend_rate ) {
    int64_t existing_max = max_write_access_to_partition_;
    if( count >= existing_max ) {
      // blend the two such that it decreases the amount of max write
      double count_blended = ( count * blend_rate );
      int64_t blended =
          ( count_blended ) + ( ( existing_max - count_blended ) * blend_rate );
      max_write_access_to_partition_.compare_exchange_strong( existing_max,
                                                              blended );
    }
}
