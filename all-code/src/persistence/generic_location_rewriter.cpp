#include "generic_location_rewriter.h"

#include <glog/logging.h>

#include "../site-selector/partition_data_location_table.h"

generic_location_hash_based_editor::generic_location_hash_based_editor(
    const std::vector<table_metadata>& tables_metadata, int num_sites,
    const std::vector<std::vector<propagation_configuration>>& prop_configs,
    const persistence_configs&                                 per_configs,
    std::shared_ptr<update_destination_generator>              update_generator,
    std::shared_ptr<update_enqueuers>                          enqueuers,
    const std::unordered_map<int, std::unordered_set<int>>& replica_locs,
    const partition_type::type&    master_type,
    const storage_tier_type::type& master_storage_type,
    const std::unordered_map<int, partition_type::type>&    replica_types,
    const std::unordered_map<int, storage_tier_type::type>& storage_types )
    : data_location_rewriter_interface( tables_metadata, num_sites,
                                        prop_configs, per_configs,
                                        update_generator, enqueuers ),
      replica_locs_( replica_locs ),
      master_type_( master_type ),
      master_storage_type_( master_storage_type ),
      replica_types_( replica_types ),
      storage_types_( storage_types ) {}
generic_location_hash_based_editor::~generic_location_hash_based_editor() {}

void generic_location_hash_based_editor::rewrite_partition_locations(
    partition_locations_map& partition_locations ) {

    partition_column_identifier_key_hasher hasher;

    for( auto& table_entry : partition_locations ) {
        for( auto& part_entry : table_entry.second ) {
            auto  pid = part_entry.first;
            auto& part_loc = part_entry.second;

            std::size_t pid_hash = hasher( pid );

            int master = pid_hash % num_sites_;
            int replica_slot = pid_hash % prop_configs_.at( master ).size();

            part_loc.partition_types_.clear();
            part_loc.storage_types_.clear();

            if( part_loc.master_location_ == K_DATA_AT_ALL_SITES ) {
                master = K_DATA_AT_ALL_SITES;
            }

            part_loc.master_location_ = master;
            part_loc.partition_types_.emplace( master, master_type_ );
            part_loc.storage_types_.emplace( master, master_storage_type_ );

            part_loc.replica_locations_.clear();
            auto replica_loc_found = replica_locs_.find( master );
            if( replica_loc_found != replica_locs_.end() ) {
                for( int r : replica_loc_found->second ) {
                    part_loc.replica_locations_.emplace( r );
                    part_loc.partition_types_.emplace( r,
                                                       replica_types_.at( r ) );
                    part_loc.storage_types_.emplace( r,
                                                     storage_types_.at( r ) );
                }
            }
            part_loc.update_destination_slot_ = replica_slot;
        }
    }
}

generic_single_master_based_editor::generic_single_master_based_editor(
    const std::vector<table_metadata>& tables_metadata, int num_sites,
    const std::vector<std::vector<propagation_configuration>>& prop_configs,
    const persistence_configs&                                 per_configs,
    std::shared_ptr<update_destination_generator>              update_generator,
    std::shared_ptr<update_enqueuers> enqueuers, bool replicate_fully,
    const partition_type::type&    master_type,
    const storage_tier_type::type& master_storage_type,
    const std::unordered_map<int, partition_type::type>&    replica_types,
    const std::unordered_map<int, storage_tier_type::type>& storage_types )
    : data_location_rewriter_interface( tables_metadata, num_sites,
                                        prop_configs, per_configs,
                                        update_generator, enqueuers ),
      replicate_fully_( replicate_fully ),
      master_type_( master_type ),
      master_storage_type_( master_storage_type ),
      replica_types_( replica_types ),
      storage_types_( storage_types ) {}
generic_single_master_based_editor::~generic_single_master_based_editor() {}

void generic_single_master_based_editor::rewrite_partition_locations(
    partition_locations_map& partition_locations ) {

    partition_column_identifier_key_hasher hasher;

    for( auto& table_entry : partition_locations ) {
        for( auto& part_entry : table_entry.second ) {
            auto  pid = part_entry.first;
            auto& part_loc = part_entry.second;

            std::size_t pid_hash = hasher( pid );

            int master = 0;  // hash the pid or range?
            int replica_slot = pid_hash % prop_configs_.at( master ).size();

            part_loc.partition_types_.clear();
            part_loc.storage_types_.clear();

            if( part_loc.master_location_ == K_DATA_AT_ALL_SITES ) {
                master = K_DATA_AT_ALL_SITES;
            }

            part_loc.master_location_ = master;
            part_loc.partition_types_.emplace( master, master_type_ );
            part_loc.storage_types_.emplace( master, master_storage_type_ );

            part_loc.replica_locations_.clear();
            if( replicate_fully_ ) {
                for( int r = 0; r < num_sites_; r++ ) {
                    part_loc.replica_locations_.emplace(
                        r);
                    part_loc.partition_types_.emplace( r,
                                                       replica_types_.at( r ) );
                    part_loc.storage_types_.emplace( r,
                                                     storage_types_.at( r ) );
                }
            }
            part_loc.update_destination_slot_ = replica_slot;
        }
    }
}

generic_range_based_editor::generic_range_based_editor(
    const std::vector<table_metadata>& tables_metadata, int num_sites,
    const std::vector<std::vector<propagation_configuration>>& prop_configs,
    const persistence_configs&                                 per_configs,
    std::shared_ptr<update_destination_generator>              update_generator,
    std::shared_ptr<update_enqueuers>                          enqueuers,
    const std::unordered_map<int, std::unordered_set<int>>& replica_locs,
    const partition_type::type&    master_type,
    const storage_tier_type::type& master_storage_type,
    const std::unordered_map<int, partition_type::type>&    replica_types,
    const std::unordered_map<int, storage_tier_type::type>& storage_types,
    bool is_fixed_range, double probability_threshold )
    : data_location_rewriter_interface( tables_metadata, num_sites,
                                        prop_configs, per_configs,
                                        update_generator, enqueuers ),
      replica_locs_( replica_locs ),
      master_type_( master_type ),
      master_storage_type_( master_storage_type ),
      replica_types_( replica_types ),
      storage_types_( storage_types ),
      is_fixed_range_( is_fixed_range ),
      probability_threshold_( probability_threshold ),
      dist_( nullptr /* no zipf needed*/ ) {}
generic_range_based_editor::~generic_range_based_editor() {}

void generic_range_based_editor::rewrite_partition_locations(
    partition_locations_map& partition_locations ) {

    for( auto& table_entry : partition_locations ) {

        int64_t min_part_start = INT64_MAX;
        int64_t max_part_end = 0;

        std::vector<partition_column_identifier> pids;

        // find the range
        for( auto& part_entry : table_entry.second ) {
            auto pid = part_entry.first;

            if( pid.partition_start <= min_part_start ) {
                min_part_start = pid.partition_start;
            }
            if( pid.partition_end >= max_part_end ) {
                max_part_end = pid.partition_end;
            }
            pids.push_back( pid );
        }
        std::sort( pids.begin(), pids.end() );

        int num_prop_dests_per_site = 0;
        for( int site = 0; site < num_sites_; site++ ) {
            if( (int) prop_configs_.at( site ).size() >=
                num_prop_dests_per_site ) {
                num_prop_dests_per_site = prop_configs_.at( site ).size();
            }
        }

        int64_t site_range_count =
            ( ( max_part_end - min_part_start ) / num_sites_ ) + 1;
        int64_t next_site_range = min_part_start + site_range_count;
        DVLOG( 10 ) << "Site prop range count:" << site_range_count;
        int site = 0;

        int64_t prop_range_count =
            ( ( max_part_end - min_part_start ) / num_prop_dests_per_site ) + 1;
        DVLOG( 10 ) << "Range prop range count:" << prop_range_count;
        int64_t next_prop_range = min_part_start + prop_range_count;
        int      prop = 0;

        // assign to range
        for( auto pid : pids ) {
            auto& part_loc = table_entry.second[pid];

            if( pid.partition_start > next_site_range ) {
                site = site + 1;
                next_site_range = next_site_range + site_range_count;
            }

            if( pid.partition_start > next_prop_range ) {
                prop = prop + 1;
                next_prop_range = next_prop_range + prop_range_count;
            }

            int master = site;
            int replica_slot = prop;

            if( !is_fixed_range_ ) {
                double p = dist_.get_uniform_double();
                if( p >= probability_threshold_ ) {

                    master = dist_.get_uniform_int( 0, num_sites_ );
                    replica_slot = dist_.get_uniform_int(
                        0, prop_configs_.at( master ).size() );

                    DVLOG( 10 ) << "Rewrite: " << pid << ", p:" << p
                                << ", selected site_to_write:" << master;
                }
            }

            part_loc.partition_types_.clear();
            part_loc.storage_types_.clear();

            if( part_loc.master_location_ == K_DATA_AT_ALL_SITES ) {
                master = K_DATA_AT_ALL_SITES;
            }

            part_loc.master_location_ = master;
            part_loc.partition_types_.emplace( master, master_type_ );
            part_loc.storage_types_.emplace( master, master_storage_type_ );

            part_loc.replica_locations_.clear();
            auto replica_loc_found = replica_locs_.find( master );
            if( replica_loc_found != replica_locs_.end() ) {
                for( int r : replica_loc_found->second ) {
                    part_loc.replica_locations_.emplace( r );
                    part_loc.partition_types_.emplace( r,
                                                       replica_types_.at( r ) );
                    part_loc.storage_types_.emplace( r,
                                                     storage_types_.at( r ) );
                }
            }
            part_loc.update_destination_slot_ = replica_slot;

            DVLOG( 20 ) << "Pid:" << pid << ", part loc:" << part_loc;
        }
    }
}

generic_no_op_based_editor::generic_no_op_based_editor(
    const std::vector<table_metadata>& tables_metadata, int num_sites,
    const std::vector<std::vector<propagation_configuration>>& prop_configs,
    const persistence_configs&                                 per_configs,
    std::shared_ptr<update_destination_generator>              update_generator,
    std::shared_ptr<update_enqueuers>                          enqueuers )
    : data_location_rewriter_interface( tables_metadata, num_sites,
                                        prop_configs, per_configs,
                                        update_generator, enqueuers ) {}
generic_no_op_based_editor::~generic_no_op_based_editor() {}

void generic_no_op_based_editor::rewrite_partition_locations(
    partition_locations_map& partition_locations ) {
    // do nothing
    for( auto& table_entry : partition_locations ) {
        for( auto& part_entry : table_entry.second ) {
            auto  pid = part_entry.first;
            auto& part_loc = part_entry.second;

            LOG( INFO ) << "Partition:" << pid << ", part_loc:" << part_loc;
        }
    }
}

