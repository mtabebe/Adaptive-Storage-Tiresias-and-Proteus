#include "grouped_partition_information.h"

#include <glog/logging.h>
#include <glog/stl_logging.h>

#include "../common/perf_tracking.h"
#include "../data-site/db/mvcc_chain.h"
#include "partition_data_location_table.h"

grouped_partition_information::grouped_partition_information( int num_sites )
    : payloads_(),
      partitions_set_(),
      write_pids_to_shaped_ckrs_(),
      read_pids_to_shaped_ckrs_(),
      partition_location_informations_(),
      new_partitions_(),
      existing_write_partitions_(),
      existing_read_partitions_(),
      new_write_partitions_(),
      new_read_partitions_(),
      new_partitions_set_(),
      existing_write_partitions_set_(),
      existing_read_partitions_set_(),
      new_write_partitions_set_(),
      new_read_partitions_set_(),
      write_site_count_( num_sites, 0 ),
      read_site_count_( num_sites, 0 ),
      site_locations_( num_sites, false ),
      inflight_pids_(),
      inflight_snapshot_vector_() {}

void grouped_partition_information::add_partition_information(
    const std::vector<std::shared_ptr<partition_payload>>& payloads,
    const cell_key_ranges& ck, bool is_write,
    const partition_lock_mode& lock_mode ) {
    for( auto& payload : payloads ) {
        add_partition_information( payload, ck, is_write, lock_mode );
    }
}

bool add_if_new( const std::shared_ptr<partition_payload>&        payload,
                 const partition_column_identifier&               pid,
                 std::vector<std::shared_ptr<partition_payload>>& payloads,
                 partition_column_identifier_set& existing_pids ) {
    DCHECK_EQ( pid, payload->identifier_ );
    DCHECK_EQ( existing_pids.size(), payloads.size() );
    bool ret = false;

    if( existing_pids.count( pid ) == 0 ) {
        existing_pids.insert( pid );
        payloads.emplace_back( payload );
        ret = true;
    }

    return ret;
}

void grouped_partition_information::add_partition_information(
    const std::shared_ptr<partition_payload>& payload,
    const cell_key_ranges& ck, bool is_write,
    const partition_lock_mode& lock_mode ) {

    const auto& pid = payload->identifier_;
    DVLOG( 40 ) << "Adding partition information:" << pid
                << ", is_write:" << is_write;

    DCHECK( is_ckr_partially_within_pcid( ck, pid ) );

    auto shaped_ckr = shape_ckr_to_pcid( ck, pid );
    DCHECK( is_ckr_fully_within_pcid( shaped_ckr, pid ) );
    if( is_write ) {
        write_pids_to_shaped_ckrs_[pid].push_back( shaped_ckr );
    } else {
        read_pids_to_shaped_ckrs_[pid].push_back( shaped_ckr );
    }

    // add to general payloads
    bool newly_added_partition =
        add_if_new( payload, pid, payloads_, partitions_set_ );

    // add location info
    auto location_information = payload->get_location_information( lock_mode );

    partition_location_informations_[pid] = location_information;

    if( !location_information or
        ( location_information->master_location_ == k_unassigned_master ) ) {
        DVLOG( 40 ) << "Is new partition";
        if( newly_added_partition ) {
            new_partitions_.push_back( payload );
            new_partitions_set_.insert( pid );
        }

        if( is_write ) {
            add_if_new( payload, pid, new_write_partitions_,
                        new_write_partitions_set_ );
        } else {
            add_if_new( payload, pid, new_read_partitions_,
                        new_read_partitions_set_ );
        }
        return;
    }

    bool new_write = false;
    bool new_read = false;

    if( is_write ) {
        new_write = add_if_new( payload, pid, existing_write_partitions_,
                                existing_write_partitions_set_ );
        if( new_write and location_information->in_flight_ ) {
            DVLOG( 10 ) << "Partition:" << payload->identifier_
                        << ", is_write:" << is_write << ", and in flight:"
                        << location_information->session_req_;
            inflight_pids_.push_back( payload->identifier_ );
            set_snapshot_version( inflight_snapshot_vector_,
                                  payload->identifier_,
                                  location_information->session_req_ );
        }
    } else {
        new_read = add_if_new( payload, pid, existing_read_partitions_,
                               existing_read_partitions_set_ );
    }

    DVLOG( 40 ) << "Adding partition information:" << payload->identifier_
                << ", master:" << location_information->master_location_;
    if( location_information->master_location_ == K_DATA_AT_ALL_SITES ) {
        return;
    }
    if( new_write or new_read) {
        add_to_site_mappings( is_write,
                              (int) location_information->master_location_ );
    }

    if( !is_write and new_read ) {
        for( auto replica_site : location_information->replica_locations_ ) {
            DVLOG( 40 ) << "Adding partition information:"
                        << payload->identifier_
                        << ", replica:" << replica_site;
            add_to_site_mappings( is_write, (int) replica_site );
        }
    }
}

void grouped_partition_information::add_to_site_mappings( bool is_write,
                                                          int  site ) {

    site_locations_[site] = true;
    if( is_write ) {
        write_site_count_[site] = write_site_count_[site] + 1;
    } else {
        read_site_count_[site] = read_site_count_[site] + 1;
    }
}

std::vector<int> grouped_partition_information::get_no_change_destinations()
    const {
    start_timer( SS_GET_NO_CHANGE_DESTINATIONS_POSSIBILITIES_TIMER_ID );

    DVLOG( 40 ) << "Getting no change destinations";
    std::vector<int> eligible_destinations;
    int              num_sites = site_locations_.size();
    int              read_size = existing_read_partitions_.size();
    int              write_size = existing_write_partitions_.size();
    for( int site = 0; site < num_sites; site++ ) {
        DVLOG( 40 ) << "Considering site:" << site;
        if( !site_locations_.at( site ) ) {
            DVLOG( 40 ) << "No data at site:" << site;
            continue;
        }
        int  read_count = read_site_count_.at( site );
        int  write_count = write_site_count_.at( site );

        DVLOG( 40 ) << "Read site count:" << read_count
                    << ", read_size:" << read_size
                    << ", write site count:" << write_count
                    << ", write_size:" << write_size;

        if( ( write_count == write_size ) and ( read_count == read_size ) ) {
            DVLOG( 40 ) << "Adding eligibile no change site:" << site;
            eligible_destinations.push_back( site );
        }
    }

    stop_timer( SS_GET_NO_CHANGE_DESTINATIONS_POSSIBILITIES_TIMER_ID );
    return eligible_destinations;
}

per_site_grouped_partition_information::per_site_grouped_partition_information()
    : site_( -1 ),
      write_pids_(),
      read_pids_(),
      payloads_(),
      read_payloads_(),
      write_payloads_() {}
per_site_grouped_partition_information::per_site_grouped_partition_information(
    int site_id )
    : site_( site_id ),
      write_pids_(),
      read_pids_(),
      payloads_(),
      read_payloads_(),
      write_payloads_() {}
