#include "site_selector_table_persister.h"

#include <glog/logging.h>

#include "../common/gdcheck.h"

site_selector_table_persister::site_selector_table_persister(
    const std::string& output_file_name )
    : persister_( output_file_name ),
      persist_chunked_( false ),
      persisters_(),
      cur_persister_pos_( 0 ) {}

void site_selector_table_persister::set_chunk_information(
    uint64_t num_partitions, const std::string& dir, uint32_t table_id,
    uint32_t desired_chunk_size ) {

    persist_chunked_ = true;
    uint32_t num_persisters = num_partitions / desired_chunk_size;
    if( num_persisters == 0 ) {
        num_persisters = 1;
    }

    for( uint32_t chunk_id = 0; chunk_id < num_persisters; chunk_id++ ) {
        std::string output_file_name = dir + "/" + std::to_string( table_id ) +
                                       "-" + std::to_string( chunk_id ) +
                                       ".loc";
        persisters_.emplace_back( new data_persister( output_file_name ) );
    }
}

void site_selector_table_persister::init() {
    persister_.open();
    for( auto p : persisters_ ) {
        p->open();
    }
}
// explicit flush
void site_selector_table_persister::flush() {
    persister_.flush();
    for( auto p : persisters_ ) {
        p->flush();
    }
}

// close and flush
void site_selector_table_persister::finish() {
    // mark this as the end
    persister_.write_i64( k_unassigned_key );
    persister_.close();

    for( uint32_t pos = 0; pos < persisters_.size(); pos++ ) {
        data_persister* p = persisters_.at( pos );

        p->write_i64( k_unassigned_key );
        p->close();

        delete p;
        persisters_.at( pos ) = nullptr;
    }

    persisters_.clear();
}

void site_selector_table_persister::persist_table_info(
    const table_metadata& table_info ) {
    int32_t  table_id = table_info.table_id_;
    int32_t  num_columns = table_info.num_columns_;
    uint64_t partition_size = table_info.default_partition_size_;
    uint32_t partition_col_size = table_info.default_column_size_;
    uint32_t part_type = table_info.default_partition_type_;
    uint32_t num_records_in_snapshot_chain =
        table_info.num_records_in_snapshot_chain_;
    uint32_t num_records_in_chain = table_info.num_records_in_chain_;
    std::string table_name = table_info.table_name_;

    DVLOG( 40 ) << "Persisting table info [table_id:" << table_id
                << ", num_columns:" << num_columns
                << ", partition_size:" << partition_size
                << ", partition_column_size:" << partition_col_size
                << ", default_partition_type:" << part_type
                << ", num_records_in_chain:" << num_records_in_chain
                << ", num_records_in_snapshot_chain:"
                << num_records_in_snapshot_chain
                << ", table_name:" << table_name << "]";

    data_persister* p = &persister_;

    p->write_i32( table_id );
    p->write_i32( num_columns );
    for( const auto& col_type : table_info.col_types_ ) {
        p->write_i32( (uint32_t) col_type );
    }
    p->write_i64( partition_size );
    p->write_i32( partition_col_size );
    p->write_i32( (uint32_t) part_type );
    p->write_i32( num_records_in_chain );
    p->write_i32( num_records_in_snapshot_chain );
    p->write_str( table_name );

    if( persist_chunked_ ) {
        p->write_i32( persisters_.size() );
    }
}

void site_selector_table_persister::persist_partition_payload(
    std::shared_ptr<partition_payload> payload ) {
    GDCHECK( payload );

    data_persister* p = &persister_;
    if( persist_chunked_ ) {
        cur_persister_pos_ += 1;
        cur_persister_pos_ = cur_persister_pos_ % persisters_.size();
        p = persisters_.at( cur_persister_pos_ );
    }

    bool locked= payload->try_lock();
    if( !locked ) {
        LOG( ERROR ) << "Persistence: unable to lock payload:"
                     << payload->identifier_;
    }

    auto location_info = payload->get_location_information();
    GDCHECK( location_info );

    DVLOG( 40 ) << "Persisting partition_payload:" << payload->identifier_
                << ", at location info:" << *location_info;

    p->write_i64( payload->identifier_.partition_start );
    p->write_i64( payload->identifier_.partition_end );
    p->write_i32( payload->identifier_.column_start );
    p->write_i32( payload->identifier_.column_end );
    p->write_i32( location_info->master_location_ );
    p->write_i32( location_info->replica_locations_.size() );
    for( const auto& entry: location_info->replica_locations_ ) {
        uint32_t replica_loc = entry;
        p->write_i32( replica_loc );
    }
    DCHECK_EQ( 1 + location_info->replica_locations_.size(),
               location_info->partition_types_.size() );
    p->write_i32( location_info->partition_types_.size() );
    for( const auto& entry : location_info->partition_types_ ) {
        p->write_i32( entry.first );
        p->write_i32( (uint32_t) entry.second );
    }
    DCHECK_EQ( 1 + location_info->replica_locations_.size(),
               location_info->storage_types_.size() );
    p->write_i32( location_info->storage_types_.size() );
    for( const auto& entry : location_info->storage_types_ ) {
        p->write_i32( entry.first );
        p->write_i32( (uint32_t) entry.second );
    }


    p->write_i32( location_info->update_destination_slot_ );

    if ( locked) {
        payload->unlock();
    }
}
