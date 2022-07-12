#pragma once

#include <vector>
#include <memory>
#include <utility>

#include "partition_access.h"

//TODO: fill out the reference information so we can unfold access counts
//For now, this corresponds to partition IDs
//Should really contain information about where in the partition we are

struct tracking_summary {

    tracking_summary() {
    }

    tracking_summary(
        transaction_partition_accesses& txn_partition_accesses,
        across_transaction_access_correlations& across_txn_accesses
    ) :
        txn_partition_accesses_( txn_partition_accesses ),
        across_txn_accesses_( across_txn_accesses ) {}

    bool is_placeholder_sample() const {
        return txn_partition_accesses_.partition_accesses_.empty();
    }

    transaction_partition_accesses txn_partition_accesses_;
    across_transaction_access_correlations across_txn_accesses_;
    
};
