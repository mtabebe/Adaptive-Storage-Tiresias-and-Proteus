#include "benchmark_statistics.h"

#include <glog/logging.h>

static const double k_nano_to_sec = 1000000000;

benchmark_statistics::benchmark_statistics()
    : shift_( 0 ),
      outcome_counters_(),
      outcome_lats_(),
      running_time_ns_( 0 ) {}
benchmark_statistics::~benchmark_statistics() {}

void benchmark_statistics::init(
    const std::vector<workload_operation_enum>& operations ) {
    // prefill the vector so it is always look up
    uint32_t min = UINT32_MAX;
    uint32_t max = 0;
    for( const workload_operation_enum& op : operations ) {
        if( op <= min ) {
            min = op;
        }
        if( op >= max ) {
            max = op;
        }
    }
    uint32_t range = ( max - min ) + 1;
    shift_ = min;
    outcome_counters_.reserve( range );
    outcome_lats_.reserve( range );
    for( uint32_t i = 0; i < range; i++ ) {
        // num outcomes is 2
        DVLOG( 15 ) << "Initializing outcome_counters";
        outcome_counters_.emplace_back(
            k_workload_operation_outcomes_to_strings.size(), 0 );
        outcome_lats_.emplace_back(
            k_workload_operation_outcomes_to_strings.size(), 0 );
    }
}

void benchmark_statistics::log_statistics() const {
    VLOG( 0 ) << "Running time:" << running_time_ns_ << " ns";
    double time_sec = running_time_ns_ / k_nano_to_sec;
    for( uint32_t i = 0; i < outcome_counters_.size(); i++ ) {
        uint64_t success_count =
            outcome_counters_.at( i ).at( WORKLOAD_OP_SUCCESS );
        uint64_t fail_count =
            outcome_counters_.at( i ).at( WORKLOAD_OP_FAILURE );
        uint64_t expected_fail_count =
            outcome_counters_.at( i ).at( WORKLOAD_OP_EXPECTED_ABORT );

        double success_lat = outcome_lats_.at( i ).at( WORKLOAD_OP_SUCCESS );

        workload_operation_enum op = i + shift_;

        VLOG( 0 ) << workload_operation_string( op )
                  << " - SUCCESS:" << success_count;
        VLOG( 0 ) << workload_operation_string( op )
                  << " - SUCCESS TPS:" << success_count / time_sec;
        VLOG( 0 ) << workload_operation_string( op )
                  << " - FAIL:" << fail_count;
        VLOG( 0 ) << workload_operation_string( op )
                  << " - EXPECTED_FAIL:" << expected_fail_count;
        double fail_pct = 0;
        if( fail_count > 0 ) {
            fail_pct = ( 100 * (double) fail_count ) /
                       (double) ( success_count + fail_count );
        }
        VLOG( 0 ) << workload_operation_string( op )
                  << " - FAIL PCT:" << fail_pct;

        double lat = 0;
        if( success_count > 0 ) {
            lat = success_lat / (double) success_count;
        }
        VLOG( 0 ) << workload_operation_string( op )
                  << " - SUCCESS LAT:" << lat;
    }
}

// they must be mergeable
bool benchmark_statistics::merge( const benchmark_statistics& other ) {
    for( uint32_t i = 0; i < other.outcome_counters_.size(); i++ ) {
        workload_operation_enum other_op = i + other.shift_;
        uint32_t                my_pos = other_op - shift_;
        if( my_pos >= outcome_counters_.size() ) {
            // ignores underflow but w/e
            LOG( WARNING ) << "Unable to merge:"
                           << workload_operation_string( other_op );
            return false;
        }

        for( uint32_t j = 0; j < other.outcome_counters_.at( i ).size(); j++ ) {
            uint64_t count = other.outcome_counters_.at( i ).at( j );
            double   lats = other.outcome_lats_.at( i ).at( j );

            outcome_counters_.at( my_pos ).at( j ) += count;
            outcome_lats_.at( my_pos ).at( j ) += lats;
        }
    }
    running_time_ns_ = std::max( running_time_ns_, other.running_time_ns_ );
    return true;
}
