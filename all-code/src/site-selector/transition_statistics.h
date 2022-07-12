#pragma once

#include "../common/partition_funcs.h"

#include <unordered_map>
#include <memory>
#include <mutex>


struct transition_statistics {

    void adjust_transaction_accesses( const transaction_partition_accesses &part_accesses, int64_t adjustment ) {
        std::lock_guard<std::mutex> lk( mutex_ );

        DVLOG( 20 ) << "Adjusting txn accesses...";

        for( const partition_access &part_access : part_accesses.partition_accesses_ ) {
            auto search = transitions_.find( part_access.partition_id_ );
            if( search == transitions_.end() ) {
                transitions_.emplace( std::make_pair( part_access.partition_id_, 1 ) );
            } else {
                int64_t &count = search->second;
                count += adjustment;
            }
        }
    }

    void add_transaction_accesses( const transaction_partition_accesses &part_accesses ) {
        return adjust_transaction_accesses( part_accesses, 1 );
    }

    void remove_transaction_accesses( const transaction_partition_accesses &part_accesses ) {
        return adjust_transaction_accesses( part_accesses, -1 );
    }

    // if you are going to modify or access this, you must grab the mutex
    partition_column_identifier_map_t<int64_t> transitions_;
    std::mutex mutex_;
};
