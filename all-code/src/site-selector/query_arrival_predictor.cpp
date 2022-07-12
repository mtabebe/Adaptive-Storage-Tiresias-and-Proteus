#include "query_arrival_predictor.h"

#include "../common/predictor/simple_early_query_predictor.h"

std::shared_ptr<early_query_predictor> construct_early_query_predictor(
    const query_arrival_predictor_configs& configs ) {
    switch( configs.predictor_type_ ) {
        case query_arrival_predictor_type::SIMPLE_QUERY_PREDICTOR: {
            return std::make_shared<simple_early_query_predictor>();
        }
    }

    return nullptr;
}

query_arrival_predictor::query_arrival_predictor(
    const query_arrival_predictor_configs& configs )
    : configs_( configs ), metas_(), eqp_predictors_() {
}
query_arrival_predictor_configs query_arrival_predictor::get_configs() const {
    return configs_;
}

void query_arrival_predictor::add_table_metadata( const table_metadata& meta ) {
    if( meta.table_id_ == metas_.size() ) {
        metas_.emplace_back( meta );
        eqp_predictors_.emplace_back(
            construct_early_query_predictor( configs_ ) );
    }
}

void query_arrival_predictor::add_query_observation_from_current_time(
    const std::vector<cell_key_ranges>& write_set,
    const std::vector<cell_key_ranges>& read_set ) {
    if( !configs_.use_query_arrival_predictor_ ) {
        return;
    }

    TimePoint cur_time = Clock::now();
    return add_query_observation( write_set, read_set, cur_time );
}

void query_arrival_predictor::add_query_observation(
    const std::vector<cell_key_ranges>& write_set,
    const std::vector<cell_key_ranges>& read_set,
    const TimePoint&                    time_point ) {
    if( !configs_.use_query_arrival_predictor_ ) {
        return;
    }
    std::unordered_map<uint32_t, std::vector<query_arrival_access_bytes>>
        table_accesses;
    for( const auto& ckr : write_set ) {
        generate_query_arrival_access_bytes(
            table_accesses[ckr.table_id], ckr, false /*is read */,
            false /* is scan */, true /* is write */, metas_ );
    }
    for( const auto& ckr : read_set ) {
        generate_query_arrival_access_bytes(
            table_accesses[ckr.table_id], ckr, true /*is read */,
            false /* is scan */, false /* is write */, metas_ );
    }
    epoch_time epoch = translate_to_epoch_time( time_point );
    for( const auto& entry : table_accesses ) {
        DCHECK_LT( entry.first, eqp_predictors_.size() );
        eqp_predictors_.at( entry.first )
            ->add_observations( entry.second, epoch );
    }
}

query_arrival_prediction
    query_arrival_predictor::get_next_access_from_current_time(
        const partition_column_identifier& pid, bool allow_reads,
        bool allow_scans, bool allow_writes ) {
    query_arrival_prediction ret;
    if( !configs_.use_query_arrival_predictor_ ) {
      return ret;
    }
    TimePoint cur_time = Clock::now();
    return get_next_access( pid, allow_reads, allow_scans, allow_writes,
                            cur_time, cur_time );
}

query_arrival_prediction query_arrival_predictor::get_next_access(
    const partition_column_identifier& pid, bool allow_reads, bool allow_scans,
    bool allow_writes, const TimePoint& min_time,
    const TimePoint& offset_time ) {
    return get_next_access( pid, allow_reads, allow_scans, allow_writes,
                            min_time, offset_time, nullptr );
}
query_arrival_prediction query_arrival_predictor::get_next_access(
    const partition_column_identifier& pid, bool allow_reads, bool allow_scans,
    bool allow_writes, const TimePoint& min_time, const TimePoint& offset_time,
    std::unordered_map<query_arrival_access_bytes, epoch_time>*
        prev_predictions ) {

    query_arrival_prediction ret;

    if( !configs_.use_query_arrival_predictor_ ) {
        return ret;
    }

    std::unordered_map<uint32_t, std::vector<query_arrival_access_bytes>>
         table_accesses;
    auto ckr = create_cell_key_ranges( pid.table_id, pid.partition_start,
                                       pid.partition_end, pid.column_start,
                                       pid.column_end );
    if( allow_reads ) {
        generate_query_arrival_access_bytes(
            table_accesses[ckr.table_id], ckr, true /*is read */,
            false /* is scan */, false /* is write */, metas_ );
    }
    if( allow_scans ) {
        generate_query_arrival_access_bytes(
            table_accesses[ckr.table_id], ckr, false /*is read */,
            true /* is scan */, false /* is write */, metas_ );
    }
    if( allow_writes ) {
        generate_query_arrival_access_bytes(
            table_accesses[ckr.table_id], ckr, false /*is read */,
            false /* is scan */, true /* is write */, metas_ );
    }

    auto min_time_epoch = translate_to_epoch_time( min_time );
    auto offset_time_epoch = translate_to_epoch_time( offset_time );

    if( table_accesses.empty() ) {
        return ret;
    }
    epoch_time most_recent_time = std::numeric_limits<epoch_time>::max();
    epoch_time found_offset_time = std::numeric_limits<epoch_time>::max();

    for( const auto& access : table_accesses ) {
        DCHECK_LT( access.first, eqp_predictors_.size() );
        for( const auto& query : access.second ) {
            epoch_time pred_time = std::numeric_limits<epoch_time>::max();
            bool do_prediction = true;
            if( prev_predictions != nullptr ) {
                auto prev_found = prev_predictions->find( query );
                if( prev_found != prev_predictions->end() ) {
                    pred_time = prev_found->second;
                    do_prediction = false;
                }
            }
            if( do_prediction ) {
                pred_time = eqp_predictors_.at( access.first )
                                ->get_predicted_arrival(
                                    query, configs_.access_threshold_ );
                if( prev_predictions != nullptr ) {
                    prev_predictions->emplace( query, pred_time );
                }
            }
            if( pred_time != std::numeric_limits<epoch_time>::max() ) {
                ret.count_ += 1;
                if( pred_time >= min_time_epoch ) {
                    if( pred_time < most_recent_time ) {
                        most_recent_time = pred_time;
                        found_offset_time = 0;
                        if( most_recent_time > offset_time_epoch ) {
                            found_offset_time =
                                most_recent_time - offset_time_epoch;
                        }
                    }
                    ret.found_ = true;
                } // >= min_time
            } // != max
        } // query
    } // table

    ret.predicted_time_ = translate_to_time_point( most_recent_time );
    ret.offset_time_ = translate_to_time_point( found_offset_time );
    return ret;
}

void query_arrival_predictor::add_query_schedule(
    const std::vector<cell_key_ranges>& write_set,
    const std::vector<cell_key_ranges>& read_set,
    const TimePoint&                    time_point ) {

    if( !configs_.use_query_arrival_predictor_ ) {
        return;
    }

    std::unordered_map<uint32_t, std::vector<query_arrival_access_bytes>>
        accesses;
    for( const auto& ckr : write_set ) {
        generate_query_arrival_access_bytes(
            accesses[ckr.table_id], ckr, false /*is read */,
            false /* is scan */, true /* is write */, metas_ );
    }
    for( const auto& ckr : read_set ) {
        generate_query_arrival_access_bytes(
            accesses[ckr.table_id], ckr, true /*is read */, false /* is scan */,
            false /* is write */, metas_ );
    }
    epoch_time e_time = translate_to_epoch_time( time_point );
    for( const auto& entry : accesses ) {
        DCHECK_LT( entry.first, eqp_predictors_.size() );
        eqp_predictors_.at( entry.first )
            ->add_query_schedule( entry.second, e_time );
    }
}

void query_arrival_predictor::train_model() {
    // Do nothing
}

query_arrival_prediction
    query_arrival_predictor::get_next_access_from_cached_stats(
        const partition_column_identifier& pid, bool is_write,
        query_arrival_cached_stats& cached ) {
    if( is_write ) {
        auto found = cached.write_pids_.find( pid );
        if( found != cached.write_pids_.end() ) {
            return found->second;
        }
    } else {
        auto found = cached.read_pids_.find( pid );
        if( found != cached.read_pids_.end() ) {
            return found->second;
        }
    }

    auto got_time =
        get_next_access( pid, true /* allow reads */, true /* allow scans */,
                         is_write /* allow writes */, cached.time_,
                         cached.time_, &cached.internal_predictions_ );

    if( is_write ) {
        cached.write_pids_[pid] = got_time;
    } else {
        cached.read_pids_[pid] = got_time;
    }

    return got_time;
}

epoch_time translate_to_epoch_time( const TimePoint& time_point ) {
    uint64_t in_seconds = std::chrono::duration_cast<std::chrono::seconds>(
                              time_point.time_since_epoch() )
                              .count();
    return in_seconds;
}
TimePoint translate_to_time_point( const epoch_time& epoch ) {
    TimePoint found_time = TimePoint::max();
    if( epoch != std::numeric_limits<epoch_time>::max() ) {
        const Clock::duration dur = std::chrono::seconds( epoch );
        found_time = TimePoint( dur );
    }
    return found_time;
}

query_arrival_predictor_configs construct_query_arrival_predictor_configs(
    bool                                use_query_arrival_predictor,
    const query_arrival_predictor_type& pred_type, double access_threshold,
    uint64_t time_bucket, double count_bucket, double arrival_time_score_scalar,
    double arrival_time_no_prediction_default_score ) {
    query_arrival_predictor_configs configs;

    configs.use_query_arrival_predictor_ = use_query_arrival_predictor;
    configs.predictor_type_ = pred_type;
    configs.access_threshold_ = access_threshold;
    configs.time_bucket_ = TimePoint( std::chrono::seconds( time_bucket ) );
    configs.count_bucket_ = count_bucket;
    configs.arrival_time_score_scalar_ = arrival_time_score_scalar;
    configs.arrival_time_no_prediction_default_score_ =
        arrival_time_no_prediction_default_score;

    return configs;
}

std::ostream& operator<<( std::ostream&                          os,
                          const query_arrival_predictor_configs& configs ) {
    os << "Query Arrival Predictor Configs: [ use_query_arrival_predictor_:"
       << configs.use_query_arrival_predictor_ << ", predictor_type_:"
       << query_arrival_predictor_type_string( configs.predictor_type_ )
       << ", access_threshold_:" << configs.access_threshold_
       << ", time_bucket_ (seconds):"
       << std::chrono::duration_cast<std::chrono::seconds>(
              configs.time_bucket_.time_since_epoch() )
              .count()
       << ", count_bucket_:" << configs.count_bucket_
       << ", arrival_time_score_scalar_:" << configs.arrival_time_score_scalar_
       << ", arrival_time_no_prediction_default_score_:"
       << configs.arrival_time_no_prediction_default_score_ << " ]";
    return os;
}

query_arrival_cached_stats::query_arrival_cached_stats()
    : write_pids_(), read_pids_(), internal_predictions_(), time_() {
    time_ = Clock::now();
}

query_arrival_cached_stats::query_arrival_cached_stats(
    const TimePoint& offset_time )
    : write_pids_(),
      read_pids_(),
      internal_predictions_(),
      time_( offset_time ) {}

query_arrival_prediction::query_arrival_prediction()
    : found_( false ),
      predicted_time_( TimePoint::max() ),
      offset_time_( TimePoint::max() ),
      count_( 0 ) {}
