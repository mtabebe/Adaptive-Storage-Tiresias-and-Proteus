#pragma once

#include <folly/concurrency/ConcurrentHashMap.h>
#include <vector>

#include "../common/partition_funcs.h"
#include "../common/snapshot_vector_funcs.h"
#include "../data-site/db/mvcc_chain.h"
#include "../gen-cpp/gen-cpp/proto_types.h"
#include "cost_modeller.h"
#include "partition_payload.h"

class site_partition_version_information {
   public:
    site_partition_version_information();
    ~site_partition_version_information();

    void add_table( uint32_t table_id );
    void add_tables( uint32_t num_tables );

    uint64_t get_version_of_partition(
        const partition_column_identifier& pid ) const;
    void set_version_of_partition( const partition_column_identifier& pid,
                                   int64_t                            version );

    void set_version_of_partitions(
        const std::vector<polled_partition_column_version_information>&
            version_infos );

   private:
    std::vector<
        folly::ConcurrentHashMap<partition_column_identifier, int64_t,
                                 partition_column_identifier_key_hasher,
                                 partition_column_identifier_equal_functor>*>
        partition_versions_;
};

class sites_partition_version_information {
   public:
    sites_partition_version_information(
        std::shared_ptr<cost_modeller2> cost_model2 );

    void init_partition_states( int num_sites, int num_tables );
    void init_topics( int num_topics );

    std::vector<uint64_t> get_version_of_partition(
        const partition_column_identifier& pid ) const;
    uint64_t get_version_of_partition(
        int site, const partition_column_identifier& pid ) const;

    void set_version_of_partition( int                                site,
                                   const partition_column_identifier& pid,
                                   uint64_t                           version );
    void set_version_of_partitions(
        int site,
        const std::vector<polled_partition_column_version_information>&
            version_infos );

    std::vector<uint64_t>              get_num_updates_to_topics() const;
    std::vector<std::vector<uint64_t>> get_num_updates_to_topics_by_site()
        const;
    uint64_t get_num_updates_to_topic( uint32_t topic ) const;
    uint64_t get_num_updates_to_topic_by_site( uint32_t site,
                                               uint32_t topic ) const;

    void set_site_topic_translation( uint32_t site, uint32_t config_pos,
                                     uint32_t renamed_offset );
    void set_topic_update_count( uint32_t topic_offset, uint64_t count );

    double estimate_number_of_updates_need_to_wait_for(
        const std::vector<std::shared_ptr<partition_payload>>& parts,
        const std::vector<std::shared_ptr<partition_location_information>>&
                               infos,
        const snapshot_vector& session, uint32_t site ) const;
    double estimate_number_of_updates_need_to_wait_for(
        const std::shared_ptr<partition_payload>&              part,
        const std::shared_ptr<partition_location_information>& info,
        uint32_t site, uint64_t session_version ) const;
    double estimate_number_of_updates_need_to_wait_for(
        const std::vector<std::shared_ptr<partition_payload>>& parts,
        const partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>>& partition_location_informations,
        uint32_t site, const snapshot_vector& session ) const;

   private:
    double estimate_number_of_updates_need_to_wait_for(
        const partition_column_identifier& pid, uint32_t master,
        uint32_t destination, uint64_t required_session_version_state,
        uint64_t num_writes, uint64_t updates_to_topic ) const;
    double will_estimate_updates_be_zero(
        const std::shared_ptr<partition_location_information>& info,
        uint32_t site, uint64_t session_version ) const;

    std::shared_ptr<cost_modeller2>                 cost_model2_;
    std::vector<site_partition_version_information> site_infos_;
    std::vector<std::vector<uint32_t>>              site_topic_translation_;
    std::vector<std::atomic<uint64_t>>              num_updates_to_topics_;
    uint32_t                                        num_topics_;
};

