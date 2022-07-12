#pragma once

#include <chrono>

#include "../common/constants.h"
#include "../common/predictor/early_query_predictor.h"
#include "query_arrival_access_bytes.h"
#include "query_arrival_predictor_types.h"

class query_arrival_predictor_configs {
   public:
    bool                         use_query_arrival_predictor_;
    query_arrival_predictor_type predictor_type_;
    double                       access_threshold_;

    TimePoint                    time_bucket_;
    double                       count_bucket_;

    double                       arrival_time_score_scalar_;
    double                       arrival_time_no_prediction_default_score_;
};

class query_arrival_prediction {
   public:
    query_arrival_prediction();

    bool      found_;
    TimePoint predicted_time_;
    TimePoint offset_time_;
    double    count_;
};

class query_arrival_cached_stats {
   public:
    query_arrival_cached_stats();
    query_arrival_cached_stats( const TimePoint& offset_time );

    partition_column_identifier_map_t<query_arrival_prediction> write_pids_;
    partition_column_identifier_map_t<query_arrival_prediction> read_pids_;

    std::unordered_map<query_arrival_access_bytes, epoch_time>
        internal_predictions_;

    TimePoint time_;
};

query_arrival_predictor_configs construct_query_arrival_predictor_configs(
    bool use_query_arrival_predictor = k_use_query_arrival_predictor,
    const query_arrival_predictor_type& pred_type =
        k_query_arrival_predictor_type,
    double   access_threshold = k_query_arrival_access_threshold,
    uint64_t time_bucket = k_query_arrival_time_bucket,
    double   count_bucket = k_query_arrival_count_bucket,
    double   arrival_time_score_scalar = k_query_arrival_time_score_scalar,
    double   arrival_time_no_prediction_default_score =
        k_query_arrival_default_score );

std::ostream& operator<<( std::ostream&                          os,
                          const query_arrival_predictor_configs& configs );

class query_arrival_predictor {
   public:
    query_arrival_predictor( const query_arrival_predictor_configs& configs );

    query_arrival_predictor_configs get_configs() const;

    void add_table_metadata( const table_metadata& meta );

    void train_model();

    void add_query_observation( const std::vector<cell_key_ranges>& write_set,
                                const std::vector<cell_key_ranges>& read_set,
                                const TimePoint& time_point );
    void add_query_observation_from_current_time(
        const std::vector<cell_key_ranges>& write_set,
        const std::vector<cell_key_ranges>& read_set );

    query_arrival_prediction get_next_access_from_cached_stats(
        const partition_column_identifier& pid, bool is_write,
        query_arrival_cached_stats& cached );

    query_arrival_prediction get_next_access_from_current_time(
        const partition_column_identifier& pid, bool allow_reads,
        bool allow_scans, bool allow_writes );

    query_arrival_prediction get_next_access(
        const partition_column_identifier& pid, bool allow_reads,
        bool allow_scans, bool allow_writes, const TimePoint& min_time,
        const TimePoint& offset_time );

    void add_query_schedule( const std::vector<cell_key_ranges>& write_set,
                             const std::vector<cell_key_ranges>& read_set,
                             const TimePoint&                    time_point );

   private:
    query_arrival_prediction get_next_access(
        const partition_column_identifier& pid, bool allow_reads,
        bool allow_scans, bool allow_writes, const TimePoint& min_time,
        const TimePoint& offset_time,
        std::unordered_map<query_arrival_access_bytes, epoch_time>*
            prev_predictions );

    query_arrival_predictor_configs                    configs_;
    std::vector<table_metadata>                        metas_;
    std::vector<std::shared_ptr<early_query_predictor>> eqp_predictors_;
};

epoch_time translate_to_epoch_time( const TimePoint& time_point );
TimePoint translate_to_time_point( const epoch_time& epoch );

