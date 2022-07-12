#pragma once

#include <list>
#include <shared_mutex>
#include <unordered_map>

#include "../common/partition_funcs.h"
#include "cost_modeller.h"
#include "cost_modeller_types.h"
#include "partition_data_location_table.h"
#include "partition_payload.h"
#include "query_arrival_predictor.h"
#include "site_selector_metadata.h"
#include "site_selector_query_stats.h"

class transaction_prediction_tier {
   public:
    bool                         is_master_;
    uint32_t                     num_replicas_;
    bool                         use_arrival_time_;
    epoch_time                   arrival_time_;
    double                       arrival_count_;
    transaction_prediction_stats stats_;
};

struct transaction_prediction_tier_equal_functor {
    bool operator()( const transaction_prediction_tier& a1,
                     const transaction_prediction_tier& a2 ) const {
    transaction_prediction_stats_equal_functor equal_func;
    return ( ( a1.is_master_ == a2.is_master_ ) and
             ( a1.num_replicas_ == a2.num_replicas_ ) and
             ( a1.use_arrival_time_ == a2.use_arrival_time_ ) and
             ( a1.arrival_time_ == a2.arrival_time_ ) and
             ( a1.arrival_count_ == a2.arrival_count_ ) and
             ( equal_func( a1.stats_, a2.stats_ ) ) );
    }
};

struct transaction_prediction_tier_hasher {
    std::size_t operator()( const transaction_prediction_tier& a ) const {
        std::size_t seed = 0;
        seed = folly::hash::hash_combine( seed, a.is_master_ );
        seed = folly::hash::hash_combine( seed, a.num_replicas_ );
        seed = folly::hash::hash_combine( seed, a.use_arrival_time_ );
        seed = folly::hash::hash_combine( seed, a.arrival_time_ );
        seed = folly::hash::hash_combine( seed, a.arrival_count_ );
        transaction_prediction_stats_hasher hasher;
        seed = folly::hash::hash_combine( seed, hasher( a.stats_ ) );
        return seed;
    }
};

class partition_tier_tracker {
   public:
    partition_tier_tracker( uint32_t                       site,
                            const storage_tier_type::type& tier );

    void remove_partition( const partition_column_identifier& pid );
    void add_partition( const partition_column_identifier& pid,
                        const transaction_prediction_tier& stat_bucket,
                        const cost_modeller2*              cost_model,
                        const site_selector_query_stats*   ss_stats,
                        double                             arrival_time_scalar,
                        double default_arrival_score );

    void update_statistics( const transaction_prediction_tier&   bucketizer,
                            const partition_data_location_table* data_loc_tab,
                            const cost_modeller2*                cost_model,
                            const site_selector_query_stats*     ss_stats,
                            query_arrival_predictor*    query_predictor,
                            cached_ss_stat_holder&      cached_stats,
                            query_arrival_cached_stats& cached_arrival_stats,
                            double                      arrival_time_scalar,
                            double                      default_arrival_score );

    void get_candidates_in_order(
        uint32_t limit, std::vector<partition_column_identifier>& pids ) const;

   private:
    void internal_remove_partition( const partition_column_identifier& pid );
    void internal_add_partition(
        const partition_column_identifier& pid,
        const transaction_prediction_tier& stat_bucket,
        const cost_modeller2*              cost_model,
        const site_selector_query_stats* ss_stats, double arrival_time_scalar,
        double default_arrival_score, cached_ss_stat_holder& cached_stats,
        std::unordered_map<transaction_prediction_tier,
                           partition_column_identifier_unordered_set,
                           transaction_prediction_tier_hasher,
                           transaction_prediction_tier_equal_functor>&
            stat_buckets,
        partition_column_identifier_map_t<transaction_prediction_tier>&
            pid_to_stat_mapping,
        std::list<std::tuple<double, transaction_prediction_tier>>& scores );

    uint32_t                     site_;
    storage_tier_type::type      tier_;

    mutable std::shared_mutex lock_;

    std::unordered_map<transaction_prediction_tier,
                       partition_column_identifier_unordered_set,
                       transaction_prediction_tier_hasher,
                       transaction_prediction_tier_equal_functor>
        stat_buckets_;
    partition_column_identifier_map_t<transaction_prediction_tier>
        pid_to_stat_mapping_;
    std::list<std::tuple<double, transaction_prediction_tier>> scores_;
};

class partition_tier_tracking {
   public:
    partition_tier_tracking(
        std::shared_ptr<partition_data_location_table>& data_loc_tab,
        std::shared_ptr<cost_modeller2>&                cost_model,
        std::shared_ptr<query_arrival_predictor>&       query_predictor );

    std::vector<partition_column_identifier> get_removal_candidates_in_order(
        uint32_t site, const storage_tier_type::type& tier,
        uint32_t limit ) const;

    void remove_partition( const std::shared_ptr<partition_payload>& part,
                           uint32_t site, const storage_tier_type::type& tier );
    void remove_partition( const partition_column_identifier& pid,
                           uint32_t site, const storage_tier_type::type& tier );
    void add_partition( const std::shared_ptr<partition_payload>& part,
                        uint32_t site, const storage_tier_type::type& tier );
    void change_storage_tier( const std::shared_ptr<partition_payload>& part,
                              uint32_t                                  site,
                              const storage_tier_type::type& old_tier,
                              const storage_tier_type::type& new_tier );

    void update_statistics();
    void update_statistics( const TimePoint& offset_time );

   private:
    std::tuple<uint32_t, uint32_t> get_site_iter_bounds( uint32_t site ) const;

    std::shared_ptr<partition_data_location_table> data_loc_tab_;
    std::shared_ptr<cost_modeller2>                cost_model_;
    std::shared_ptr<query_arrival_predictor>       query_predictor_;
    site_selector_query_stats*                     ss_stats_;

    uint32_t num_sites_;
    std::unordered_map<storage_tier_type::type,
                       std::vector<std::shared_ptr<partition_tier_tracker>>>
                                tier_trackers_;
    transaction_prediction_tier bucket_;
    TimePoint                   offset_time_;

    double arrival_time_scalar_;
    double arrival_time_default_score_;
};
