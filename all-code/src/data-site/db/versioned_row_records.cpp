#include "versioned_row_records.h"

versioned_row_records::versioned_row_records( const partition_metadata& metadata )
    : metadata_( metadata ), records_() {
    uint32_t num_records = ( metadata_.partition_id_.partition_end -
                             metadata_.partition_id_.partition_start ) +
                           1;
    records_.assign( num_records, nullptr );
}

void versioned_row_records::init_records() {
    for( uint32_t pos = 0; pos < records_.size(); pos++ ) {
        int64_t key = metadata_.partition_id_.partition_start + pos;
        auto    record_ptr = std::make_shared<versioned_row_record>();
        record_ptr->init( key, metadata_.partition_id_hash_,
                          metadata_.num_records_in_chain_ );
        records_.at( pos ) = std::move( record_ptr );
        DVLOG( 40 ) << "Init record (key):" << key << ", pos:" << pos
                    << ", record:" << records_.at( pos )
                    << ", use count:" << records_.at( pos ).use_count();
    }
}

