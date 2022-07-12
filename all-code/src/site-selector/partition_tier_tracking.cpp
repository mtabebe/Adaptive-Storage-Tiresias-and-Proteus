#include "partition_tier_tracking.h"

#include <cfloat>
#include <climits>

#include <glog/logging.h>

std::tuple<epoch_time, double> get_query_arrival_bucket(
    const partition_column_identifier& pid, const epoch_time& bucket_time,
    double count_bucket, bool is_master,
    query_arrival_predictor*    query_predictor,
    query_arrival_cached_stats& cached_arrival_stats ) {
    auto prediction = query_predictor->get_next_access_from_cached_stats(
        pid, is_master, cached_arrival_stats );
    epoch_time ret = translate_to_epoch_time( TimePoint::max() );
    double     arrival_count = 0;
    if( prediction.found_ ) {
        epoch_time epoch_offset =
            translate_to_epoch_time( prediction.offset_time_ );
        ret = ( ( epoch_offset / bucket_time ) * bucket_time );
        arrival_count = bucketize_double( prediction.count_, count_bucket ) + 1;
    }
    return std::make_tuple<>( ret, arrival_count );
}

transaction_prediction_tier get_transaction_prediction_stats(
    uint32_t site, const std::shared_ptr<partition_payload>& part,
    const std::shared_ptr<partition_location_information>& loc_info,
    const transaction_prediction_tier& bucket, const cost_modeller2* cost_model,
    const site_selector_query_stats* ss_stats,
    query_arrival_predictor*         query_predictor,
    cached_ss_stat_holder&           cached_stats,
    query_arrival_cached_stats&      cached_arrival_stats ) {
    transaction_prediction_tier stat_tier;
    stat_tier.is_master_ = loc_info->master_location_ == site;
    stat_tier.num_replicas_ = loc_info->replica_locations_.size();
    transaction_prediction_stats stats(
        std::get<1>( part->get_partition_type( site ) ),
        std::get<1>( part->get_storage_type( site ) ),
        cost_model->normalize_contention_by_time( part->get_contention() ),
        0 /* num updates needed */,
        ss_stats->get_and_cache_cell_widths( part->identifier_, cached_stats ),
        part->get_num_rows(), false /* scan */,
        0 /* MTODO-SCAN scan selectivity */, true,
        ( cost_model->normalize_contention_by_time( part->get_read_load() ) /
          (double) ( stat_tier.num_replicas_ + 1 ) ) *
            ss_stats->get_and_cache_average_num_reads( part->identifier_,
                                                       cached_stats ),
        stat_tier.is_master_,
        cost_model->normalize_contention_by_time( part->get_contention() ) *
            ss_stats->get_and_cache_average_num_updates( part->identifier_,
                                                         cached_stats ) );
    stat_tier.stats_ = bucket.stats_.bucketize( stats );
    stat_tier.use_arrival_time_ = bucket.use_arrival_time_;
    stat_tier.arrival_time_ = 0;
    stat_tier.arrival_count_ = 1;
    if( stat_tier.use_arrival_time_ ) {
        auto arrival_bucket = get_query_arrival_bucket(
            part->identifier_, bucket.arrival_time_, bucket.arrival_count_,
            stat_tier.is_master_, query_predictor, cached_arrival_stats );
        stat_tier.arrival_time_ = std::get<0>( arrival_bucket );
        stat_tier.arrival_count_ = std::get<1>( arrival_bucket );
    }
    return stat_tier;
}

double get_stat_bucket_score( const transaction_prediction_tier& tier,
                              const cost_modeller2*              cost_model,
                              double arrival_time_scalar,
                              double default_score ) {
    auto                                      stats = tier.stats_;
    std::vector<transaction_prediction_stats> txn_stats = {stats};
    double                                    before_score = cost_model
                              ->predict_single_site_transaction_execution_time(
                                  0 /*site load*/, txn_stats )
                              .get_prediction();
    double upcoming_access_score = 1;
    if( tier.use_arrival_time_ ) {
        double denom = (double) tier.arrival_time_;
        if( tier.arrival_time_ < DBL_MAX - 1 ) {
            denom += 1;
            upcoming_access_score =
                // further access ==> smaller score ==> more likely to be
                // removed
                ( upcoming_access_score / denom ) * arrival_time_scalar *
                ( tier.arrival_count_ + 1 );
        } else {
            upcoming_access_score = default_score;
        }
    }

    // MTODO-Generalize
    txn_stats.at( 0 ).storage_type_ = storage_tier_type::type::DISK;
    double after_score = DBL_MAX;
    if( !tier.is_master_ and tier.num_replicas_ >= 1 ) {
        double ori_num_reads = stats.num_point_reads_;
        double num_sites = tier.num_replicas_ + 1;
        double new_num_reads =
            ( ori_num_reads * num_sites ) / ( num_sites - 1 );
        txn_stats.at( 0 ).num_point_reads_ = new_num_reads;
        after_score = cost_model
                          ->predict_single_site_transaction_execution_time(
                              0 /*site load*/, txn_stats )
                          .get_prediction();

        if( tier.stats_.storage_type_ == storage_tier_type::type::MEMORY ) {
            txn_stats.at( 0 ).num_point_reads_ = ori_num_reads;
            double change_score =
                cost_model
                    ->predict_single_site_transaction_execution_time(
                        0 /*site load*/, txn_stats )
                    .get_prediction();
            if( change_score < after_score ) {
                after_score = change_score;
            }
        }
    }
    double score = ( upcoming_access_score ) * ( after_score - before_score );
    return score;
}

partition_tier_tracker::partition_tier_tracker(
    uint32_t site, const storage_tier_type::type& tier )
    : site_( site ),
      tier_( tier ),
      lock_(),
      stat_buckets_(),
      pid_to_stat_mapping_(),
      scores_() {}

void partition_tier_tracker::remove_partition(
    const partition_column_identifier& pid ) {
  lock_.lock();
  internal_remove_partition( pid );
  lock_.unlock();
}
void partition_tier_tracker::add_partition(
    const partition_column_identifier& pid,
    const transaction_prediction_tier& stat, const cost_modeller2* cost_model,
    const site_selector_query_stats* ss_stats, double arrival_time_scalar,
    double arrival_time_default_score ) {
    cached_ss_stat_holder cached_stats;
    // lock
    lock_.lock();
    internal_remove_partition( pid );
    internal_add_partition( pid, stat, cost_model, ss_stats,
                            arrival_time_scalar, arrival_time_default_score,
                            cached_stats, stat_buckets_, pid_to_stat_mapping_,
                            scores_ );
    lock_.unlock();
}

void partition_tier_tracker::internal_remove_partition(
    const partition_column_identifier& pid ) {
    auto found_pid = pid_to_stat_mapping_.find( pid );
    if( found_pid == pid_to_stat_mapping_.end() ) {
        return;
    }
    auto pid_bucket = found_pid->second;
    auto found_bucket = stat_buckets_.find( pid_bucket );

    pid_to_stat_mapping_.erase( found_pid );

    if( found_bucket == stat_buckets_.end() ) {
        return;
    }

    found_bucket->second.erase( pid );
    if( found_bucket->second.size() > 0 ) {
        return;
    }

    DCHECK( found_bucket->second.empty() );
    stat_buckets_.erase( found_bucket );

    transaction_prediction_tier_equal_functor equal_func;

    for( auto it = scores_.begin(); it != scores_.end(); it++ ) {
        auto score_entry = *it;
        if( equal_func( std::get<1>( score_entry ), pid_bucket ) ) {
            scores_.erase( it );
            break;
        }
    }
}

void partition_tier_tracker::internal_add_partition(
    const partition_column_identifier& pid,
    const transaction_prediction_tier& stat_bucket,
    const cost_modeller2* cost_model, const site_selector_query_stats* ss_stats,
    double arrival_time_scalar, double arrival_time_default_score,
    cached_ss_stat_holder& cached_stats,
    std::unordered_map<transaction_prediction_tier,
                       partition_column_identifier_unordered_set,
                       transaction_prediction_tier_hasher,
                       transaction_prediction_tier_equal_functor>& stat_buckets,
    partition_column_identifier_map_t<transaction_prediction_tier>&
        pid_to_stat_mapping,
    std::list<std::tuple<double, transaction_prediction_tier>>& scores ) {
    pid_to_stat_mapping_[pid] = stat_bucket;
    auto found = stat_buckets.find( stat_bucket );
    if( found != stat_buckets.end() ) {
        found->second.insert( pid );
        return;
    }


    partition_column_identifier_unordered_set pids = {pid};
    stat_buckets.emplace( stat_bucket, pids );

    double score =
        get_stat_bucket_score( stat_bucket, cost_model, arrival_time_scalar,
                               arrival_time_default_score );

    bool inserted = false;
    for( auto it = scores.begin(); it != scores.end(); it++ ) {
        const auto& entry = *it;
        if( score < std::get<0>( entry ) ) {
            scores.insert( it, std::make_tuple<>( score, stat_bucket ) );
            inserted = true;
            break;
        }
    }
    if( !inserted ) {
        scores.push_back( std::make_tuple<>( score, stat_bucket ) );
    }
}

void partition_tier_tracker::update_statistics(
    const transaction_prediction_tier&   bucket,
    const partition_data_location_table* data_loc_tab,
    const cost_modeller2* cost_model, const site_selector_query_stats* ss_stats,
    query_arrival_predictor*    query_predictor,
    cached_ss_stat_holder&      cached_stats,
    query_arrival_cached_stats& cached_arrival_stats,
    double arrival_time_scalar, double arrival_time_default_score ) {
    std::unordered_map<transaction_prediction_tier,
                       partition_column_identifier_unordered_set,
                       transaction_prediction_tier_hasher,
                       transaction_prediction_tier_equal_functor>
        new_stat_buckets;
    partition_column_identifier_map_t<transaction_prediction_tier>
        new_pid_to_stat_mapping;
    std::list<std::tuple<double, transaction_prediction_tier>> new_scores;

    // we will loose stuff not in the old one, but that seems like a reasonable
    // trade-off
    lock_.lock_shared();
    auto pid_mapping = pid_to_stat_mapping_;
    lock_.unlock_shared();

    for( const auto& entry : pid_mapping ) {
        const auto&                        pid = entry.first;
        std::shared_ptr<partition_payload> part =
            data_loc_tab->get_partition( pid, partition_lock_mode::no_lock );
        if( !part ) {
            continue;
        }
        auto loc_info = part->atomic_get_location_information();
        if( !loc_info ) {
            continue;
        }
        if( ( site_ != loc_info->master_location_ ) and
            ( loc_info->replica_locations_.count( site_ ) == 0 ) and
            ( loc_info->master_location_ != K_DATA_AT_ALL_SITES ) ) {
            continue;
        }
        auto found_tier = loc_info->get_storage_type( site_ );
        if( ( !std::get<0>( found_tier ) ) and
            ( std::get<1>( found_tier ) != tier_ ) ) {
            continue;
        }

        transaction_prediction_tier stat = get_transaction_prediction_stats(
            site_, part, loc_info, bucket, cost_model, ss_stats,
            query_predictor, cached_stats, cached_arrival_stats );

        // ok now we should add
        //
        internal_add_partition( pid, stat, cost_model, ss_stats,
                                arrival_time_scalar, arrival_time_default_score,
                                cached_stats, new_stat_buckets,
                                new_pid_to_stat_mapping, new_scores );
    }

    lock_.lock();

    // now we update (lock)
    pid_to_stat_mapping_.swap( new_pid_to_stat_mapping );
    stat_buckets_.swap( new_stat_buckets );
    scores_.swap( new_scores );

    lock_.unlock();
}

void partition_tier_tracker::get_candidates_in_order(
    uint32_t limit, std::vector<partition_column_identifier>& pids ) const {

    uint32_t added = 0;

    lock_.lock_shared();

    for( auto it = scores_.begin(); it != scores_.end(); it++ ) {
        if( added >= limit ) {
            break;
        }
        const auto& score_entry = *it;
        const auto& bucket = std::get<1>( score_entry );
        auto        found_bucket = stat_buckets_.find( bucket );
        if( found_bucket != stat_buckets_.end() ) {
            pids.insert( pids.end(), found_bucket->second.begin(),
                         found_bucket->second.end() );
            added += ( found_bucket->second.size() );
        }
    }

    lock_.lock_shared();
}

partition_tier_tracking::partition_tier_tracking(
    std::shared_ptr<partition_data_location_table>& data_loc_tab,
    std::shared_ptr<cost_modeller2>&                cost_model,
    std::shared_ptr<query_arrival_predictor>&       query_predictor )
    : data_loc_tab_( data_loc_tab ),
      cost_model_( cost_model ),
      query_predictor_( query_predictor ),
      ss_stats_( data_loc_tab->get_stats() ),
      num_sites_( data_loc_tab->get_configs().num_sites_ ),
      tier_trackers_(),
      bucket_(),
      offset_time_(),
      arrival_time_scalar_(
          query_predictor->get_configs().arrival_time_score_scalar_ ),
      arrival_time_default_score_(
          query_predictor->get_configs()
              .arrival_time_no_prediction_default_score_ ) {
    bucket_.stats_ = data_loc_tab->get_configs().partition_tracking_bucket_;
    bucket_.use_arrival_time_ =
        query_predictor->get_configs().use_query_arrival_predictor_;
    bucket_.arrival_time_ =
        translate_to_epoch_time( query_predictor->get_configs().time_bucket_ );
    bucket_.arrival_count_ = query_predictor->get_configs().count_bucket_;

    for( const auto& entry :
         data_loc_tab->get_configs().storage_tier_limits_ ) {
        const auto&                                          tier = entry.first;
        std::vector<std::shared_ptr<partition_tier_tracker>> trackers(
            num_sites_, nullptr );
        for( uint32_t site = 0; site < num_sites_; site++ ) {
            trackers.at( site ) =
                std::make_shared<partition_tier_tracker>( site, tier );
        }

        tier_trackers_.emplace( tier, trackers );
    }
}

std::tuple<uint32_t, uint32_t> partition_tier_tracking::get_site_iter_bounds(
    uint32_t site ) const {
    if( site == K_DATA_AT_ALL_SITES ) {
        DCHECK_GT( num_sites_, 0 );
        return std::make_tuple<>( site, num_sites_ - 1 );
    }

    return std::make_tuple<>( site, site );
}

std::vector<partition_column_identifier>
    partition_tier_tracking::get_removal_candidates_in_order(
        uint32_t site, const storage_tier_type::type& tier,
        uint32_t limit ) const {

    DCHECK_EQ( 1, tier_trackers_.count( tier ) );

    const auto& tier_tracker = tier_trackers_.at( tier );
    auto        sites_iter = get_site_iter_bounds( site );
    std::vector<partition_column_identifier> pids;

    for( uint32_t site = std::get<0>( sites_iter );
         site <= std::get<1>( sites_iter ); site++ ) {
        DCHECK_LT( site, num_sites_ );
        DCHECK_LT( site, tier_tracker.size() );

        tier_tracker.at( site )->get_candidates_in_order( limit, pids );
    }
    return pids;
}

void partition_tier_tracking::remove_partition(
    const std::shared_ptr<partition_payload>& part, uint32_t site,
    const storage_tier_type::type& tier ) {
    remove_partition( part->identifier_, site, tier );
}

void partition_tier_tracking::remove_partition(
    const partition_column_identifier& pid, uint32_t site,
    const storage_tier_type::type& tier ) {
    DCHECK_EQ( 1, tier_trackers_.count( tier ) );

    auto& tier_tracker = tier_trackers_.at( tier );
    auto  sites_iter = get_site_iter_bounds( site );

    for( uint32_t site = std::get<0>( sites_iter );
         site <= std::get<1>( sites_iter ); site++ ) {
        DCHECK_LT( site, num_sites_ );
        DCHECK_LT( site, tier_tracker.size() );

        tier_tracker.at( site )->remove_partition( pid );
    }
}
void partition_tier_tracking::add_partition(
    const std::shared_ptr<partition_payload>& part, uint32_t site,
    const storage_tier_type::type& tier ) {
    DCHECK_EQ( 1, tier_trackers_.count( tier ) );

    auto& tier_tracker = tier_trackers_.at( tier );
    auto  sites_iter = get_site_iter_bounds( site );

    cached_ss_stat_holder cached_stats;

    auto loc_info = part->atomic_get_location_information();
    if( !loc_info ) {
        return;
    }

    query_arrival_cached_stats cached_arrival_stats( offset_time_ );

    transaction_prediction_tier stat_bucket = get_transaction_prediction_stats(
        site, part, loc_info, bucket_, cost_model_.get(), ss_stats_,
        query_predictor_.get(), cached_stats, cached_arrival_stats );

    for( uint32_t site = std::get<0>( sites_iter );
         site <= std::get<1>( sites_iter ); site++ ) {
        DCHECK_LT( site, num_sites_ );
        DCHECK_LT( site, tier_tracker.size() );

        tier_tracker.at( site )->add_partition(
            part->identifier_, stat_bucket, cost_model_.get(), ss_stats_,
            arrival_time_scalar_, arrival_time_default_score_ );
    }
}
void partition_tier_tracking::change_storage_tier(
    const std::shared_ptr<partition_payload>& part, uint32_t site,
    const storage_tier_type::type& old_tier,
    const storage_tier_type::type& new_tier ) {
    if( old_tier == new_tier ) {
        return;
    }
    DCHECK_EQ( 1, tier_trackers_.count( old_tier ) );
    DCHECK_EQ( 1, tier_trackers_.count( new_tier ) );

    auto& old_tier_tracker = tier_trackers_.at( old_tier );
    auto& new_tier_tracker = tier_trackers_.at( new_tier );
    auto  sites_iter = get_site_iter_bounds( site );

    cached_ss_stat_holder cached_stats;
    auto                  loc_info = part->atomic_get_location_information();
    if( !loc_info ) {
        return;
    }

    query_arrival_cached_stats cached_arrival_stats( offset_time_ );

    transaction_prediction_tier stat_bucket = get_transaction_prediction_stats(
        site, part, loc_info, bucket_, cost_model_.get(), ss_stats_,
        query_predictor_.get(), cached_stats, cached_arrival_stats );

    for( uint32_t site = std::get<0>( sites_iter );
         site <= std::get<1>( sites_iter ); site++ ) {
        DCHECK_LT( site, num_sites_ );
        DCHECK_LT( site, old_tier_tracker.size() );
        DCHECK_LT( site, new_tier_tracker.size() );

        new_tier_tracker.at( site )->add_partition(
            part->identifier_, stat_bucket, cost_model_.get(), ss_stats_,
            arrival_time_scalar_, arrival_time_default_score_ );
        old_tier_tracker.at( site )->remove_partition( part->identifier_ );
    }
}

void partition_tier_tracking::update_statistics() {
    return update_statistics( Clock::now() );
}
void partition_tier_tracking::update_statistics(
    const TimePoint& offset_time ) {
    offset_time_ = offset_time;
    cached_ss_stat_holder      cached_stats;
    query_arrival_cached_stats cached_arrival_stats( offset_time );

    for( auto& entry : tier_trackers_ ) {
        for( auto& tracker : entry.second ) {
            tracker->update_statistics(
                bucket_, data_loc_tab_.get(), cost_model_.get(), ss_stats_,
                query_predictor_.get(), cached_stats, cached_arrival_stats,
                arrival_time_scalar_, arrival_time_default_score_ );
        }
    }
}
