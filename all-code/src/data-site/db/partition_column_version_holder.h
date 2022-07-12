#pragma once

#include <atomic>

#include "partition_metadata.h"

class partition_column_version_holder {
   public:
    partition_column_version_holder( const partition_column_identifier& pcid,
                                     uint64_t version, uint64_t poll_epoch );

    void set_version_and_epoch( uint64_t new_version, uint64_t new_epoch );
    void set_version( uint64_t new_version );
    void set_epoch( uint64_t new_epoch );

    partition_column_identifier get_partition_column() const;
    uint64_t                    get_version() const;
    uint64_t                    get_epoch() const;

    void get_version_and_epoch(
        const partition_column_identifier& pcid_to_check,
        uint64_t* version_to_set, uint64_t* poll_epoch_to_set ) const;

   private:
    partition_column_identifier pcid_;
    std::atomic<uint64_t>       version_;
    std::atomic<uint64_t>       poll_epoch_;
};
