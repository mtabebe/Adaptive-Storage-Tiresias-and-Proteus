#pragma once

#include <memory>
#include <vector>

#include "../../common/hw.h"
#include "partition_metadata.h"
#include "versioned_row_record.h"

class versioned_row_records {
   public:
    versioned_row_records( const partition_metadata& metadata );

    void init_records();
    ALWAYS_INLINE uint32_t get_record_pos( uint64_t key ) const;

    ALWAYS_INLINE void set_record(
        uint64_t key, std::shared_ptr<versioned_row_record> record );
    ALWAYS_INLINE std::shared_ptr<versioned_row_record> get_record(
        uint64_t key ) const;

   private:
    partition_metadata                            metadata_;
    std::vector<std::shared_ptr<versioned_row_record>> records_;
};

#include "versioned_row_records-inl.h"
