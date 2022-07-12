#include "workload_operation_selector.h"

#include <numeric>

#include <glog/logging.h>
#include <glog/stl_logging.h>

workload_operation_selector::workload_operation_selector()
    : workload_probabilities_( 100, UNDEFINED_OP ) {}
workload_operation_selector::~workload_operation_selector() {}

void workload_operation_selector::init(
    const std::vector<workload_operation_enum>& workload_ops,
    const std::vector<uint32_t>&                likelihoods ) {
    DCHECK_EQ( workload_ops.size(), likelihoods.size() );
    // sums everything in the array
    DCHECK_EQ( 100,
               std::accumulate( likelihoods.begin(), likelihoods.end(), 0 ) );
    // Fills in the positions, likelihoods must sum to 100;
    // the current_operation position
    uint32_t op_iter = 0;
    // the limit on the probability for the current item
    uint32_t sum = likelihoods.at( op_iter );
    uint32_t pos = 0;
    while( pos < 100 ) {
        if( pos >= sum ) {
            // if we are now past the current operations likelihood then
            // we must go to the next operation
            op_iter++;
            sum += likelihoods.at( op_iter );
        } else {
            // fill in the current position with the current op
            workload_probabilities_.at( pos ) = workload_ops.at( op_iter );
            pos++;
        }
    }
    DVLOG( 5 ) << "Workload Probabilities:" << workload_probabilities_;
}
