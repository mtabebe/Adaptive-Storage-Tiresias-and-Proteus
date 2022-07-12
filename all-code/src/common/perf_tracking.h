#pragma once
#include <atomic>
#include <chrono>
#include <cinttypes>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include <folly/Optional.h>

#include "../gen-cpp/gen-cpp/proto_types.h"
#include "constants.h"
#include "perf_tracking_defs.h"

// to update the file run the generate_perf_tracking_defs.sh script

#define NUMBER_OF_MODEL_TIMERS 91

static const int64_t
    model_timer_positions_to_timer_ids[NUMBER_OF_MODEL_TIMERS] = {
        CHANGE_TYPE_FROM_SORTED_COLUMN_TIMER_ID,
        COL_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID,
        COL_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID,
        COL_PARTITION_BEGIN_WRITE_ACQUIRE_LOCKS_TIMER_ID,
        COL_READ_LATEST_RECORD_TIMER_ID, COL_READ_RECORD_TIMER_ID,
        COL_REPARTITION_INSERT_TIMER_ID, COL_SCAN_RECORDS_TIMER_ID,
        COLUMN_INSERT_RECORD_TIMER_ID, COLUMN_WRITE_RECORD_TIMER_ID,
        ENQUEUE_STASHED_UPDATE_TIMER_ID,
        MERGE_COL_RECORDS_VERTICALLY_SORTED_LEFT_RIGHT_UNSORTED_TIMER_ID,
        MERGE_COL_RECORDS_VERTICALLY_SORTED_TIMER_ID,
        MERGE_COL_RECORDS_VERTICALLY_UNSORTED_TIMER_ID,
        MERGE_COLUMN_RECORDS_HORIZONTALLY_TIMER_ID,
        MERGE_ROW_RECORDS_HORIZONTALLY_TIMER_ID,
        MERGE_ROW_RECORDS_VERTICALLY_TIMER_ID,
        MERGE_SORTED_COLUMN_RECORDS_HORIZONTALLY_TIMER_ID,
        PARTITION_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID,
        PARTITION_BEGIN_WRITE_ACQUIRE_LOCKS_TIMER_ID,
        PARTITION_BUILD_COMMIT_SNAPSHOT_TIMER_ID, REMOVE_PARTITION_TIMER_ID,
        ROW_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID,
        ROW_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID, ROW_INSERT_RECORD_TIMER_ID,
        ROW_PARTITION_BEGIN_WRITE_ACQUIRE_LOCKS_TIMER_ID,
        ROW_READ_LATEST_RECORD_TIMER_ID, ROW_READ_RECORD_TIMER_ID,
        ROW_SCAN_RECORDS_TIMER_ID, ROW_WRITE_RECORD_TIMER_ID,
        SERIALIZE_WRITE_BUFFER_TIMER_ID,
        SORTED_COL_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID,
        SORTED_COL_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID,
        SORTED_COL_PARTITION_BEGIN_WRITE_ACQUIRE_LOCKS_TIMER_ID,
        SORTED_COL_READ_LATEST_RECORD_TIMER_ID, SORTED_COL_READ_RECORD_TIMER_ID,
        SORTED_COL_REPARTITION_INSERT_TIMER_ID,
        SORTED_COL_SCAN_RECORDS_TIMER_ID, SORTED_COLUMN_INSERT_RECORD_TIMER_ID,
        SORTED_COLUMN_WRITE_RECORD_TIMER_ID,
        SPLIT_COL_RECORDS_HORIZONTALLY_TIMER_ID,
        SPLIT_COL_RECORDS_VERTICALLY_SORTED_TIMER_ID,
        SPLIT_COL_RECORDS_VERTICALLY_TIMER_ID,
        SPLIT_ROW_RECORDS_HORIZONTALLY_TIMER_ID,
        SPLIT_ROW_RECORDS_VERTICALLY_TIMER_ID,
        SPLIT_SORTED_COL_RECORDS_HORIZONTALLY_TIMER_ID,
        CHANGE_TYPE_FROM_SORTED_MULTI_COLUMN_TIMER_ID,
        MULTI_COL_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID,
        MULTI_COL_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID,
        MULTI_COL_PARTITION_BEGIN_WRITE_ACQUIRE_LOCKS_TIMER_ID,
        MULTI_COL_READ_LATEST_RECORD_TIMER_ID, MULTI_COL_READ_RECORD_TIMER_ID,
        MULTI_COL_REPARTITION_INSERT_TIMER_ID, MULTI_COL_SCAN_RECORDS_TIMER_ID,
        MULTI_COLUMN_INSERT_RECORD_TIMER_ID, MULTI_COLUMN_WRITE_RECORD_TIMER_ID,
        SORTED_MULTI_COL_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID,
        SORTED_MULTI_COL_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID,
        SORTED_MULTI_COL_PARTITION_BEGIN_WRITE_ACQUIRE_LOCKS_TIMER_ID,
        SORTED_MULTI_COL_READ_LATEST_RECORD_TIMER_ID,
        SORTED_MULTI_COL_READ_RECORD_TIMER_ID,
        SORTED_MULTI_COL_REPARTITION_INSERT_TIMER_ID,
        SORTED_MULTI_COL_SCAN_RECORDS_TIMER_ID,
        SORTED_MULTI_COLUMN_INSERT_RECORD_TIMER_ID,
        SORTED_MULTI_COLUMN_WRITE_RECORD_TIMER_ID,
        SPLIT_MULTI_COL_RECORDS_VERTICALLY_TIMER_ID,
        MERGE_MULTI_COL_RECORDS_VERTICALLY_TIMER_ID,
        SPLIT_MULTI_SORTED_COL_RECORDS_VERTICALLY_TIMER_ID,
        MERGE_MULTI_SORTED_COL_RECORDS_VERTICALLY_TIMER_ID,
        ROW_RESTORE_FROM_DISK_TIMER_ID,
        COL_RESTORE_FROM_DISK_TIMER_ID,
        SORTED_COL_RESTORE_FROM_DISK_TIMER_ID,
        MULTI_COL_RESTORE_FROM_DISK_TIMER_ID,
        SORTED_MULTI_COL_RESTORE_FROM_DISK_TIMER_ID,
        ROW_PERSIST_TO_DISK_TIMER_ID,
        COL_PERSIST_TO_DISK_TIMER_ID,
        SORTED_COL_PERSIST_TO_DISK_TIMER_ID,
        MULTI_COL_PERSIST_TO_DISK_TIMER_ID,
        SORTED_MULTI_COL_PERSIST_TO_DISK_TIMER_ID,
        READ_ROW_FROM_DISK_TIMER_ID,
        READ_COL_FROM_DISK_TIMER_ID,
        READ_SORTED_COL_FROM_DISK_TIMER_ID,
        READ_MULTI_COL_FROM_DISK_TIMER_ID,
        READ_SORTED_MULTI_COL_FROM_DISK_TIMER_ID,
        ROW_SCAN_DISK_TIMER_ID,
        COL_SCAN_DISK_TIMER_ID,
        SORTED_COL_SCAN_DISK_TIMER_ID,
        MULTI_COL_SCAN_DISK_TIMER_ID,
        SORTED_MULTI_COL_SCAN_DISK_TIMER_ID,
        SS_EXECUTE_ONE_SHOT_SCAN_AT_SITES_TIMER_ID,

        // always last
        RPC_AT_DATA_SITE_SERVICE_TIME_TIMER_ID,
};

static const std::unordered_set<uint64_t> k_relevant_model_timer_ids(
    model_timer_positions_to_timer_ids,
    model_timer_positions_to_timer_ids + NUMBER_OF_MODEL_TIMERS );

class performance_counter_accumulator {
   public:
    performance_counter_accumulator( uint64_t counter_id );
    void add_new_data_point( uint64_t counter_id, double data_point );

    double get_average_in_us() const;
    void reset_counter();

    uint64_t        counter_id_;
    uint64_t        counter_seen_count_;
    double          counter_average_;
    double          counter_min_;
    double          counter_max_;
};

class performance_counter {
   public:
    performance_counter( uint64_t num_counters );

    bool add_new_data_point( int32_t thread_id, uint64_t counter_id,
                             double data_point, bool overwrite );

    void reset_performance_counters( int32_t thread_id );

    performance_counter_accumulator get_accumulated_counter(
        uint64_t counter_id ) const;
    void aggregate_counter(
        uint64_t counter_id,
        std::unordered_map<uint64_t, performance_counter_accumulator>&
            aggregate_results ) const;

    bool matches_thread_id( int32_t thread_id ) const;

    std::atomic<int32_t>                         thread_id_;
    std::vector<performance_counter_accumulator> accumulators_;
};

class performance_counters {
   public:
    performance_counters( size_t num_counters, uint64_t num_threads );
    ~performance_counters();

    void insert_new_data_point( int32_t thread_id, uint64_t counter_id,
                                double value );

    void reset_performance_counters_for_thread( int32_t thread_id );

    performance_counter* get_performance_counter( int32_t thread_id ) const;

    std::unordered_map<uint64_t, performance_counter_accumulator>
        aggregate_counters(
            bool                                do_filter,
            const std::unordered_set<uint64_t>& ids_to_aggregate ) const;

    std::vector<performance_counter*> counters_;
    uint64_t                         num_counters_;
    uint64_t                         num_threads_;
};

void init_global_perf_counters( uint64_t num_global_counters,
                                uint64_t num_global_clients,
                                uint64_t num_model_counters,
                                uint64_t num_model_clients,
                                uint64_t num_client_multipliers );
void init_global_state();
std::unordered_map<uint64_t, performance_counter_accumulator>
                                             aggregate_thread_counters();
extern std::unique_ptr<performance_counters> global_performance_counters;
extern std::unique_ptr<performance_counters> per_context_performance_counters;
extern std::mutex                            dump_counters_lock;
extern bool                                  dumped_counters;
extern thread_local int32_t                  tlocal_thread_id;

void dump_counters( int signum );
uint32_t get_thread_id();

#if !defined( DISABLE_TIMERS )

#define start_timer( counter_id )                                     \
    std::chrono::high_resolution_clock::time_point counter_id##_var = \
        std::chrono::high_resolution_clock::now()

#define stop_and_store_timer( counter_id, _lat )                          \
    std::chrono::high_resolution_clock::time_point counter_id##_end_var =     \
        std::chrono::high_resolution_clock::now();                            \
    std::chrono::duration<double, std::nano> counter_id##_elapsed =           \
        counter_id##_end_var - counter_id##_var;                              \
    double counter_id##_elapsed_count = counter_id##_elapsed.count();         \
    _lat = counter_id##_elapsed_count;                                        \
    DVLOG( k_timer_log_level ) << "TIMER:" << timer_names[counter_id] << ": " \
                               << counter_id##_elapsed_count;                 \
    if( counter_id##_elapsed_count >= k_slow_timer_log_time_threshold ) {     \
        VLOG( k_slow_timer_error_log_level )                                  \
            << "SLOW TIMER:" << timer_names[counter_id] << ": "               \
            << counter_id##_elapsed_count;                                    \
    }                                                                         \
    global_performance_counters->insert_new_data_point(                       \
        get_thread_id(), counter_id, counter_id##_elapsed_count );            \
    if( model_timer_positions[counter_id] >= 0 ) {                            \
        per_context_performance_counters->insert_new_data_point(              \
            get_thread_id(), model_timer_positions[counter_id],               \
            counter_id##_elapsed_count );                                     \
    }

// I can't figure out how to re-use the code in this macro with the
// stringification, so I just duplicated it
#define stop_timer( counter_id ) \
    std::chrono::high_resolution_clock::time_point counter_id##_end_var =     \
        std::chrono::high_resolution_clock::now();                            \
    std::chrono::duration<double, std::nano> counter_id##_elapsed =           \
        counter_id##_end_var - counter_id##_var;                              \
    double counter_id##_elapsed_count = counter_id##_elapsed.count();         \
    DVLOG( k_timer_log_level ) << "TIMER:" << timer_names[counter_id] << ": " \
                               << counter_id##_elapsed_count;                 \
    if( counter_id##_elapsed_count >= k_slow_timer_log_time_threshold ) {     \
        VLOG( k_slow_timer_error_log_level )                                  \
            << "SLOW TIMER:" << timer_names[counter_id] << ": "               \
            << counter_id##_elapsed_count;                                    \
    }                                                                         \
    global_performance_counters->insert_new_data_point(                       \
        get_thread_id(), counter_id, counter_id##_elapsed_count );            \
    if( model_timer_positions[counter_id] >= 0 ) {                            \
        per_context_performance_counters->insert_new_data_point(              \
            get_thread_id(), model_timer_positions[counter_id],               \
            counter_id##_elapsed_count );                                     \
    }


#define reset_context_timers()                                               \
    per_context_performance_counters->reset_performance_counters_for_thread( \
        get_thread_id() )

#else

#define start_timer( counter_id )
#define stop_timer( counter_id )
#define stop_and_store_timer( counter_id, _lat )
#define reset_context_timers()

#endif

#if defined( ENABLE_EXTRA_TIMERS )

#define start_extra_timer( counter_id ) start_timer( counter_id )
#define stop_extra_timer( counter_id ) stop_timer( counter_id )

#else

#define start_extra_timer( counter_id )
#define stop_extra_timer( counter_id )

#endif

std::vector<context_timer> get_context_timers();
std::vector<context_timer> get_aggregated_global_timers(
    const std::unordered_set<uint64_t>& counter_ids );

