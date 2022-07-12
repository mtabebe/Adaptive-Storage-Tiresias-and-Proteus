#include "perf_tracking.h"

#include <unordered_map>
#include <signal.h>
#include <unistd.h>
#include <limits>

#include <folly/Hash.h>
#include <glog/logging.h>
#include <sys/syscall.h>

#include "constants.h"

std::unique_ptr<performance_counters> global_performance_counters = nullptr;
std::unique_ptr<performance_counters> per_context_performance_counters =
    nullptr;
std::mutex dump_counters_lock;
bool dumped_counters = false;
thread_local int32_t tlocal_thread_id = -1;

uint32_t get_thread_id() {
    if( tlocal_thread_id == -1 ) {
        pid_t tid = syscall( SYS_gettid );
        DCHECK_NE( tid, -1 );
        tlocal_thread_id = (int32_t) tid;
    }
    return tlocal_thread_id;
}

void performance_counter_accumulator::reset_counter() {
    counter_seen_count_ = 0;
    counter_average_ = 0;
    counter_min_ = std::numeric_limits<double>::max();
    counter_max_ = -std::numeric_limits<double>::max();
}

performance_counter_accumulator::performance_counter_accumulator(
    uint64_t counter_id )
    : counter_id_( counter_id ),
      counter_seen_count_( 0 ),
      counter_average_( 0 ),
      counter_min_( std::numeric_limits<double>::max() ),
      counter_max_( -std::numeric_limits<double>::max() ) {}

void performance_counter_accumulator::add_new_data_point( uint64_t counter_id,
                                                          double data_point ) {
  /*
    DVLOG( 40 ) << "Add new data point, counter_id:" << counter_id
                << ", value:" << data_point;
                */
    DCHECK_EQ( counter_id, counter_id_ );
    counter_average_ = (counter_average_ * counter_seen_count_ + data_point)/(counter_seen_count_ + 1);
    counter_seen_count_++;
    if( data_point < counter_min_ ) {
        counter_min_ = data_point;
    }
    if( data_point > counter_max_ ) {
        counter_max_ = data_point;
    }
}

double performance_counter_accumulator::get_average_in_us() const {
    return counter_average_ / 1000;
}

performance_counter::performance_counter( uint64_t num_counters )
    : thread_id_( -1 ), accumulators_() {
    accumulators_.reserve( num_counters );
    for( uint64_t counter_id = 0; counter_id < num_counters; counter_id++) {
        accumulators_.emplace_back( counter_id );
    }
}

bool performance_counter::add_new_data_point( int32_t  thread_id,
                                              uint64_t counter_id,
                                              double   data_point,
                                              bool     overwrite ) {
    // TODO handle thread conflict
    if( !overwrite ) {
        int32_t read_thread_id = thread_id_;
        if( read_thread_id == -1 ) {
            thread_id_.compare_exchange_strong( read_thread_id, thread_id );
        }
        if( !matches_thread_id( thread_id ) ) {
            return false;
        }
    }
    accumulators_.at( counter_id ).add_new_data_point( counter_id, data_point );
    return true;
}

void performance_counter::reset_performance_counters( int32_t thread_id ) {
    if( !matches_thread_id( thread_id ) ) {
        return;
    }
    for( auto &accum : accumulators_ ) {
        accum.reset_counter();
    }
}

performance_counter_accumulator performance_counter::get_accumulated_counter(
    uint64_t counter_id ) const {
    return accumulators_.at( counter_id );
}

void performance_counter::aggregate_counter(
    uint64_t counter_id,
    std::unordered_map<uint64_t, performance_counter_accumulator>
        &aggregate_results ) const {
    const auto &cnt = accumulators_.at( counter_id );
    if( cnt.counter_seen_count_ == 0 ) {
        return;
    }
    auto        search = aggregate_results.find( cnt.counter_id_ );
    if( search == aggregate_results.end() ) {
        aggregate_results.insert( {cnt.counter_id_, cnt} );
    } else {
        performance_counter_accumulator &prev_cnt = search->second;
        prev_cnt.counter_average_ =
            ( prev_cnt.counter_average_ * prev_cnt.counter_seen_count_ +
              cnt.counter_average_ * cnt.counter_seen_count_ ) /
            ( prev_cnt.counter_seen_count_ + cnt.counter_seen_count_ );
        prev_cnt.counter_seen_count_ += cnt.counter_seen_count_;
        prev_cnt.counter_min_ =
            std::min( prev_cnt.counter_min_, cnt.counter_min_ );
        prev_cnt.counter_max_ =
            std::max( prev_cnt.counter_max_, cnt.counter_max_ );
    }
}

bool performance_counter::matches_thread_id( int32_t thr_id ) const {
    // Sanity check
    bool matches = true;
#if !defined( NDEBUG )
    matches = ( thr_id == thread_id_ );
#endif
    return matches;
}

performance_counters::performance_counters( size_t num_counters,
                                            size_t num_threads )
    : counters_(), num_counters_( num_counters ), num_threads_( num_threads ) {
    counters_.reserve( num_threads + 1);
    for ( size_t t = 0; t < num_threads + 1; t++) {
        counters_.push_back( new performance_counter( num_counters_ ) );
    }
}
performance_counters::~performance_counters() {
    for( uint32_t pos = 0; pos < counters_.size(); pos++ ) {
        delete counters_.at( pos );
        counters_.at( pos ) = nullptr;
    }
    counters_.clear();
}

void performance_counters::insert_new_data_point( int32_t  thread_id,
                                                  uint64_t counter_id,
                                                  double   value ) {

    // Make a counter key
    uint32_t thread_hash_pos = thread_id % num_threads_;
    DCHECK_LT( counter_id, num_counters_ );

    /*
    DVLOG( 40 ) << "Insert new data point, thread_id:" << thread_id
                << ", thread_hash_pos:" << thread_hash_pos
                << ", counter_id:" << counter_id << ", value:" << value;
                */

    bool added = counters_.at( thread_hash_pos )
                     ->add_new_data_point( thread_id, counter_id, value,
                                           false /*overwrite*/ );
    if( !added ) {
        counters_.at( num_threads_ )
            ->add_new_data_point( thread_id, counter_id, value,
                                  true /*overwrite*/ );
    }
}

std::unordered_map<uint64_t, performance_counter_accumulator>
    performance_counters::aggregate_counters(
        bool                                do_filter,
        const std::unordered_set<uint64_t> &ids_to_aggregate ) const {

    std::unordered_map<uint64_t, performance_counter_accumulator>
        aggregate_results;

    for( uint64_t t_id = 0; t_id < num_threads_ + 1; t_id++ ) {
        const performance_counter *perf_counter = counters_.at( t_id );
        if( do_filter ) {
            for( uint64_t id : ids_to_aggregate ) {
                perf_counter->aggregate_counter( id, aggregate_results );
            }
        } else {
            for( uint64_t id = 0; id < num_counters_; id++ ) {
                perf_counter->aggregate_counter( id, aggregate_results );
            }
        }
    }

    return aggregate_results;
}

void performance_counters::reset_performance_counters_for_thread(
    int32_t thread_id) {
    uint32_t thread_hash_pos = thread_id % num_threads_;
    counters_.at( thread_hash_pos )->reset_performance_counters( thread_id );
}

performance_counter *performance_counters::get_performance_counter(
    int32_t thr_id ) const {
    uint32_t thread_hash_pos = thr_id % num_threads_;
    if( counters_.at( thread_hash_pos )->matches_thread_id( thr_id ) ) {
        return counters_.at( thread_hash_pos );
    }
    return nullptr;
}

void init_global_perf_counters( uint64_t num_global_counters,
                                uint64_t num_global_clients,
                                uint64_t num_model_counters,
                                uint64_t num_model_clients,
                                uint64_t num_client_multipliers ) {
    global_performance_counters = std::make_unique<performance_counters>(
        num_global_counters, num_global_clients * num_client_multipliers );
    per_context_performance_counters = std::make_unique<performance_counters>(
        num_model_counters, num_model_clients * num_client_multipliers );
}

void init_global_state() {
    init_constants();
    init_global_perf_counters(
        END_TIMER_COUNTERS + 1, k_bench_num_clients, NUMBER_OF_MODEL_TIMERS + 1,
        k_bench_num_clients,
        k_perf_counter_client_multiplier *
            std::max( (int32_t) k_num_site_selector_worker_threads_per_client,
                      (int32_t) k_ss_num_poller_threads ) );
}

std::unordered_map<uint64_t, performance_counter_accumulator>
    aggregate_thread_counters() {
    return global_performance_counters->aggregate_counters( false, {} );
}

void dump_counters( int signum ) {

    dump_counters_lock.lock();
    if( dumped_counters ) {
        dump_counters_lock.unlock();
        return;
    }
    LOG( INFO ) << "Caught signal: " << signum;
    LOG(INFO) << "Name,Average,Min,Max,Count";
#if !defined( DISABLE_TIMERS )
    std::unordered_map<uint64_t, performance_counter_accumulator>
        aggregate_results = std::move( aggregate_thread_counters() );
    for( const auto &kv : aggregate_results ) {
        LOG( INFO ) << timer_names[ kv.second.counter_id_ ] << ": " << kv.second.counter_average_ << ", " << kv.second.counter_min_ << ", " << kv.second.counter_max_ << ", " << kv.second.counter_seen_count_;
    }
    dumped_counters = true;

    /*
    //Get core if we SIGSEGV
    if( signum == SIGSEGV ) {
        LOG(INFO) << "sending self SIGSEGV";
        signal( SIGSEGV, SIG_DFL );
        kill( getpid(), SIGSEGV );
    }
    */
    LOG(INFO) << "Terminating process...";
#endif


    dump_counters_lock.unlock();

    exit( 0 );
}

std::vector<context_timer> get_context_timers() {
    std::vector<context_timer> timers;

    if( per_context_performance_counters == nullptr ) {
        return timers;
    }

    int32_t thr_id = get_thread_id();

#if !defined( DISABLE_TIMERS )

    auto perf_counter =
        per_context_performance_counters->get_performance_counter( thr_id );
    if( perf_counter != nullptr ) {
        for( uint64_t counter_id = 0;
             counter_id < per_context_performance_counters->num_counters_;
             counter_id++ ) {
            performance_counter_accumulator p_cnt =
                perf_counter->get_accumulated_counter( counter_id );
            uint64_t counter_seen_count = p_cnt.counter_seen_count_;
            if( counter_seen_count > 0 ) {
                context_timer timer;
                timer.counter_id =
                    model_timer_positions_to_timer_ids[counter_id];
                timer.counter_seen_count = counter_seen_count;
                timer.timer_us_average = p_cnt.get_average_in_us();

                timers.push_back( timer );
            }
        }
    }

#endif

    return timers;
}

std::vector<context_timer> get_aggregated_global_timers(
    const std::unordered_set<uint64_t> &counter_ids ) {
    std::vector<context_timer> timers;

    if ( global_performance_counters == nullptr) {
        return timers;
    }

    auto aggregated_results =
        global_performance_counters->aggregate_counters( true, counter_ids );

    for( auto agg_res : aggregated_results ) {
        context_timer       timer;

        timer.counter_id = agg_res.first;
        performance_counter_accumulator perf_counter = agg_res.second;

        timer.counter_seen_count = perf_counter.counter_seen_count_;
        timer.timer_us_average = perf_counter.get_average_in_us();
        timers.push_back( timer );
    }

    return timers;
}

