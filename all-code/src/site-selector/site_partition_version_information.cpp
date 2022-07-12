#include "site_partition_version_information.h"

#include <glog/logging.h>

site_partition_version_information::site_partition_version_information()
    : partition_versions_() {}
site_partition_version_information::~site_partition_version_information() {
    for( uint32_t pos = 0; pos < partition_versions_.size(); pos++ ) {
        delete partition_versions_.at( pos );
        partition_versions_.at( pos ) = nullptr;
    }
    partition_versions_.clear();
}

void site_partition_version_information::add_table( uint32_t table_id ) {
    DCHECK_EQ( table_id, partition_versions_.size() );

    auto entry = new folly::ConcurrentHashMap<
        partition_column_identifier, int64_t,
        partition_column_identifier_key_hasher,
        partition_column_identifier_equal_functor>();

    partition_versions_.push_back( entry );
}

void site_partition_version_information::add_tables( uint32_t num_tables ) {
    for( uint32_t tid = 0; tid < num_tables; tid++ ) {
        add_table( tid );
    }
}

uint64_t site_partition_version_information::get_version_of_partition(
    const partition_column_identifier& pid ) const {
    DCHECK_LT( pid.table_id, partition_versions_.size() );

    // not found
    uint64_t version = K_NOT_COMMITTED;

    auto entry = partition_versions_.at( pid.table_id );
    auto found = entry->find( pid );
    if( found != entry->end() ) {
        version = (uint64_t) found->second;
    }

    DVLOG( 40 ) << "Get version of partition:" << pid
                << ", version:" << version;

    return version;
}
void site_partition_version_information::set_version_of_partition(
    const partition_column_identifier& pid, int64_t version ) {
    DCHECK_LT( pid.table_id, partition_versions_.size() );

    auto entry = partition_versions_.at( pid.table_id );

    DVLOG( 40 ) << "Set version of partition:" << pid
                << ", version:" << version;

    if( version == -1 ) {
        entry->erase( pid );
        return;
    }

    auto inserted = entry->insert( pid, version );
    if( !inserted.second ) {
        if( inserted.first->second < version ) {
            entry->assign( pid, version );
        }
    }
}

void site_partition_version_information::set_version_of_partitions(
    const std::vector<polled_partition_column_version_information>&
        version_infos ) {
    for( const auto& version_info : version_infos ) {
        set_version_of_partition( version_info.pcid, version_info.version );
    }
}

sites_partition_version_information::sites_partition_version_information(
    std::shared_ptr<cost_modeller2> cost_model2 )
    : cost_model2_( cost_model2 ),
      site_infos_(),
      site_topic_translation_(),
      num_updates_to_topics_(),
      num_topics_( 0 ) {}

void sites_partition_version_information::init_partition_states(
    int num_sites, int num_tables ) {
    site_infos_ = std::vector<site_partition_version_information>( num_sites );

    for( int site_id = 0; site_id < num_sites; site_id++ ) {
        site_infos_.at( site_id ).add_tables( num_tables );
        site_topic_translation_.emplace_back();
    }
}

void sites_partition_version_information::init_topics( int num_topics ) {
    num_updates_to_topics_ = std::vector<std::atomic<uint64_t>>( num_topics );

    for( int topic = 0; topic < num_topics; topic++ ) {
        num_updates_to_topics_.at( topic ) = 0;
    }
    num_topics_ = num_topics;
}

std::vector<uint64_t>
    sites_partition_version_information::get_version_of_partition(
        const partition_column_identifier& pid ) const {

    std::vector<uint64_t> versions( site_infos_.size(), 0 );
    for( uint32_t site = 0; site < site_infos_.size(); site++ ) {
        versions.at( site ) =
            site_infos_.at( site ).get_version_of_partition( pid );
    }

    return versions;
}
uint64_t sites_partition_version_information::get_version_of_partition(
    int site, const partition_column_identifier& pid ) const {
    DCHECK_LT( site, site_infos_.size() );
    return site_infos_.at( site ).get_version_of_partition( pid );
}

void sites_partition_version_information::set_version_of_partition(
    int site, const partition_column_identifier& pid, uint64_t version ) {
    DCHECK_LT( site, site_infos_.size() );
    site_infos_.at( site ).set_version_of_partition( pid, version );
}

void sites_partition_version_information::set_version_of_partitions(
    int site, const std::vector<polled_partition_column_version_information>&
                  version_infos ) {
    DCHECK_LT( site, site_infos_.size() );
    site_infos_.at( site ).set_version_of_partitions( version_infos );
}

void sites_partition_version_information::set_topic_update_count(
    uint32_t topic_offset, uint64_t count ) {
    DCHECK_LT( topic_offset, num_updates_to_topics_.size() );

    num_updates_to_topics_.at( topic_offset ).store( count );
}

void sites_partition_version_information::set_site_topic_translation(
    uint32_t site, uint32_t config_pos, uint32_t renamed_offset ) {
    DCHECK_LT( site, site_topic_translation_.size() );
    DCHECK_EQ( config_pos, site_topic_translation_.at( site ).size() );
    DCHECK_LT( renamed_offset, num_updates_to_topics_.size() );

    site_topic_translation_.at( site ).push_back( renamed_offset );
}

std::vector<uint64_t>
    sites_partition_version_information::get_num_updates_to_topics() const {
    std::vector<uint64_t> update_counts( num_updates_to_topics_.size(), 0 );
    for( uint32_t pos = 0; pos < num_updates_to_topics_.size(); pos++ ) {
        update_counts.at( pos ) = get_num_updates_to_topic( pos );
    }
    return update_counts;
}
std::vector<std::vector<uint64_t>>
    sites_partition_version_information::get_num_updates_to_topics_by_site()
        const {
    std::vector<std::vector<uint64_t>> update_counts;
    for( uint32_t site = 0; site < site_topic_translation_.size(); site++ ) {
        update_counts.emplace_back( site_topic_translation_.at( site ).size(),
                                    0 );
        for( uint32_t topic_pos = 0;
             topic_pos < site_topic_translation_.at( site ).size();
             topic_pos++ ) {
            update_counts.at( site ).at( topic_pos ) = get_num_updates_to_topic(
                site_topic_translation_.at( site ).at( topic_pos ) );
        }
    }
    return update_counts;
}
uint64_t sites_partition_version_information::get_num_updates_to_topic(
    uint32_t topic ) const {
    uint64_t num_updates = num_updates_to_topics_.at( topic );
    return num_updates;
}
uint64_t sites_partition_version_information::get_num_updates_to_topic_by_site(
    uint32_t site, uint32_t topic ) const {
    DCHECK_LT( site, site_topic_translation_.size() );
    DCHECK_LT( topic, site_topic_translation_.at( site ).size() );

    return get_num_updates_to_topic(
        site_topic_translation_.at( site ).at( topic ) );
}

double sites_partition_version_information::
    estimate_number_of_updates_need_to_wait_for(
        const partition_column_identifier& pid, uint32_t master,
        uint32_t destination, uint64_t required_session_version_state,
        uint64_t num_writes, uint64_t updates_to_topic ) const {

    DVLOG( 40 ) << "Estimated number of updates to wait for pid:" << pid
                << ", master:" << master << ", destination:" << destination
                << ", required version:" << required_session_version_state
                << ", num writes:" << updates_to_topic;

    if( ( master == destination ) or ( required_session_version_state == 0 ) or
        ( num_writes == 0 ) ) {
        DVLOG( 40 ) << "Estimated number of updates to wait for pid:" << pid
                    << ", master:" << master << ", destination:" << destination
                    << ", required version:" << required_session_version_state
                    << ", num writes:" << updates_to_topic
                    << ", num updates to wait for:" << 0;
        return 0;
    }

    double extrapolated_num_writes = cost_model2_->get_site_operation_count(
        num_writes /* do not normalize because the update rate is not normalized*/ );

    DVLOG( 40 ) << "Extrapolated num writes:" << extrapolated_num_writes
                << ", num_writes:" << num_writes;

    double update_to_topic_rate =
        (double) updates_to_topic / (double) extrapolated_num_writes;

    // if not found it is K_NOT_COMMITTED which will be greater
    uint64_t version_at_replica = get_version_of_partition( destination, pid );
    uint64_t num_part_updates_needed = 0;
    if( version_at_replica < required_session_version_state ) {
        num_part_updates_needed =
            required_session_version_state - version_at_replica;
    } else if( version_at_replica == K_NOT_COMMITTED ) {
        num_part_updates_needed = 1;
    }

    double estimate = (double) num_part_updates_needed * update_to_topic_rate;

    DVLOG( 40 ) << "Estimated number of updates to wait for pid:" << pid
                << ", master:" << master << ", destination:" << destination
                << ", required version:" << required_session_version_state
                << ", num writes:" << updates_to_topic
                << ", extrapolated_num_writes:" << extrapolated_num_writes
                << ", update to topic rate:" << update_to_topic_rate
                << ", num updates to wait for:" << num_part_updates_needed
                << ", estimate:" << estimate;

    return estimate;
}

double sites_partition_version_information::will_estimate_updates_be_zero(
    const std::shared_ptr<partition_location_information>& info, uint32_t site,
    uint64_t session_version ) const {
    return ( ( info == nullptr ) or
             ( info->master_location_ == k_unassigned_master ) or
             ( info->master_location_ == site ) or ( session_version == 0 ) );
}

double sites_partition_version_information::
    estimate_number_of_updates_need_to_wait_for(
        const std::shared_ptr<partition_payload>&              part,
        const std::shared_ptr<partition_location_information>& info,
        uint32_t site, uint64_t session_version ) const {
    if( will_estimate_updates_be_zero( info, site, session_version ) ) {
        return 0;
    }

    return estimate_number_of_updates_need_to_wait_for(
        part->identifier_, info->master_location_, site, session_version,
        part->get_contention(),
        get_num_updates_to_topic_by_site( info->master_location_,
                                          info->update_destination_slot_ ) );
}

double sites_partition_version_information::
    estimate_number_of_updates_need_to_wait_for(
        const std::vector<std::shared_ptr<partition_payload>>& parts,
        const std::vector<std::shared_ptr<partition_location_information>>&
                               infos,
        const snapshot_vector& session, uint32_t site ) const {
    DCHECK_EQ( infos.size(), parts.size() );

    double max_num_updates_seen_so_far = 0;

    std::vector<int64_t> lazy_num_updates_to_topics( num_topics_, -1 );
    for( uint32_t pos = 0; pos < infos.size(); pos++ ) {
        const auto& part = parts.at( pos );
        const auto& info = infos.at( pos );

        uint64_t session_version =
            get_snapshot_version( session, part->identifier_ );

        DVLOG( 40 ) << "Estimating number of updates needed to wait for, for "
                       "partition: "
                    << part->identifier_ << ", info:" << info
                    << ", session version:" << session_version;

        if( will_estimate_updates_be_zero( info, site, session_version ) ) {
            continue;
        }
        uint32_t lazy_pos = site_topic_translation_.at( info->master_location_ )
                                .at( info->update_destination_slot_ );
        if( lazy_num_updates_to_topics.at( lazy_pos ) == -1 ) {
            lazy_num_updates_to_topics.at( lazy_pos ) =
                get_num_updates_to_topic( lazy_pos );
        }

        double num_updates = estimate_number_of_updates_need_to_wait_for(
            part->identifier_, info->master_location_, site, session_version,
            part->get_contention(), lazy_num_updates_to_topics.at( lazy_pos ) );
        if( num_updates > max_num_updates_seen_so_far ) {
            max_num_updates_seen_so_far = num_updates;
        }
        DVLOG( 40 ) << "Estimating number of updates needed to wait for, for "
                       "partition: "
                    << part->identifier_ << ", info:" << info
                    << ", session version:" << session_version
                    << ", estimate:" << num_updates
                    << ", max estimate so far:" << max_num_updates_seen_so_far;
    }
    DVLOG( 40 ) << "Estimated number of updates to wait for:"
                << max_num_updates_seen_so_far;
    return max_num_updates_seen_so_far;
}

double sites_partition_version_information::
    estimate_number_of_updates_need_to_wait_for(
        const std::vector<std::shared_ptr<partition_payload>>& parts,
        const partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>>& partition_location_informations,
        uint32_t site, const snapshot_vector& session ) const {
    double max_num_updates_seen_so_far = 0;

    std::vector<int64_t> lazy_num_updates_to_topics( num_topics_, -1 );
    for( const auto& part : parts ) {
        const auto& pid = part->identifier_;
        auto        found = partition_location_informations.find( pid );
        if( found == partition_location_informations.end() ) {
            continue;
        }
        const auto& info = found->second;
        uint64_t    session_version = get_snapshot_version( session, pid );
        if( will_estimate_updates_be_zero( info, site, session_version ) ) {
            continue;
        }
        uint32_t lazy_pos = site_topic_translation_.at( info->master_location_ )
                                .at( info->update_destination_slot_ );
        if( lazy_num_updates_to_topics.at( lazy_pos ) == -1 ) {
            lazy_num_updates_to_topics.at( lazy_pos ) =
                get_num_updates_to_topic( lazy_pos );
        }

        double num_updates = estimate_number_of_updates_need_to_wait_for(
            part->identifier_, info->master_location_, site, session_version,
            part->get_contention(), lazy_num_updates_to_topics.at( lazy_pos ) );
        if( num_updates > max_num_updates_seen_so_far ) {
            max_num_updates_seen_so_far = num_updates;
        }
    }
    DVLOG( 40 ) << "Estimated number of updates to wait for:"
                << max_num_updates_seen_so_far;
    return max_num_updates_seen_so_far;
}

