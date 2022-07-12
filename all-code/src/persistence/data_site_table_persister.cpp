#include "data_site_table_persister.h"

#include <glog/logging.h>

#include "../common/gdcheck.h"

data_site_table_persister::data_site_table_persister(
    const std::string& output_file_name, const std::string& partition_directory,
    const std::vector<propagation_configuration>& site_propagation_configs,
    uint32_t                                      site_id )
    : persister_( output_file_name ),
      partition_directory_( partition_directory ),
      persist_chunked_( false ),
      persisters_(),
      cur_persister_pos_( 0 ),
      update_prop_configs_to_slots_(),
      site_id_( site_id ) {
    for( uint32_t slot_number = 0;
         slot_number < site_propagation_configs.size(); slot_number++ ) {
        update_prop_configs_to_slots_[site_propagation_configs.at( slot_number )
                                          .partition] = slot_number;
        update_prop_configs_to_slots_[slot_number] = slot_number;
    }
}

void data_site_table_persister::init() {
    persister_.open();
    for( auto p : persisters_ ) {
        p->open();
    }

}
// explicit flush
void data_site_table_persister::flush() {
    persister_.flush();
    for( auto p : persisters_ ) {
        p->flush();
    }
}

// close and flush
void data_site_table_persister::finish() {
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

void data_site_table_persister::persist_table_info(
    const table_metadata& table_info ) {

    int32_t  table_id = table_info.table_id_;
    int32_t  num_columns = table_info.num_columns_;
    uint64_t partition_size = table_info.default_partition_size_;
    uint32_t             partition_col_size = table_info.default_column_size_;
    partition_type::type part_type = table_info.default_partition_type_;
    uint32_t num_records_in_snapshot_chain =
        table_info.num_records_in_snapshot_chain_;
    uint32_t num_records_in_chain = table_info.num_records_in_chain_;
    uint32_t site_loc = table_info.site_location_;
    GDCHECK_EQ( site_loc, site_id_ );

    std::string table_name = table_info.table_name_;

    DVLOG( 40 ) << "Persisting table info [table_id:" << table_id
                << ", num_columns:" << num_columns
                << ", partition_size:" << partition_size
                << ", partition_col_size:" << partition_col_size
                << ", default_partition_type:" << part_type
                << ", num_records_in_chain:" << num_records_in_chain
                << ", num_records_in_snapshot_chain:"
                << num_records_in_snapshot_chain
                << ", site_location:" << site_loc
                << ", table_name:" << table_name << "]";

    data_persister* p = &persister_;

    p->write_i32( table_id );
    p->write_i32( num_columns );
    DCHECK_EQ( num_columns, table_info.col_types_.size() );
    for( const auto& col : table_info.col_types_ ) {
        p->write_i32( (uint32_t) col );
    }

    p->write_i64( partition_size );
    p->write_i32( partition_col_size );
    p->write_i32( (uint32_t) part_type );
    p->write_i32( num_records_in_chain );
    p->write_i32( num_records_in_snapshot_chain );
    p->write_i32( site_loc );
    p->write_str( table_name );

    if( persist_chunked_ ) {
        p->write_i32( persisters_.size() );
    }
}

void data_site_table_persister::persist_partition(
    std::shared_ptr<partition> part ) {
    GDCHECK( part );
    persist_partition( part, part->get_partition_type(),
                       part->get_storage_tier_type() );
}
void data_site_table_persister::persist_partition(
    std::shared_ptr<partition> part, const partition_type::type& part_type,
    const storage_tier_type::type& storage_type ) {
    GDCHECK( part );

    data_persister* p = &persister_;
    if( persist_chunked_ ) {
        cur_persister_pos_ += 1;
        cur_persister_pos_ = cur_persister_pos_ % persisters_.size();
        p = persisters_.at( cur_persister_pos_ );
    }

    auto     meta = part->get_metadata();
    uint32_t master_location = part->get_master_location();

    DVLOG( 40 ) << "Persisting partition:" << meta.partition_id_;

    if( !part->is_active() ) {
        DLOG( INFO ) << "Partition:" << meta.partition_id_
                     << ", is not active, not persisting";
        return;
    }

    p->write_i64( meta.partition_id_.partition_start );
    p->write_i64( meta.partition_id_.partition_end );
    p->write_i32( meta.partition_id_.column_start );
    p->write_i32( meta.partition_id_.column_end );

    uint8_t is_master = ( uint8_t )( master_location == site_id_ );

    p->write_i32( master_location );
    p->write_i8( is_master );

    p->write_i32( (uint32_t) part_type );
    p->write_i32( (uint32_t) storage_type );

    if( is_master ) {
        // create the partition in the partition_dir
        persist_master_partition( part );
    }
}

void data_site_table_persister::persist_master_partition(
    std::shared_ptr<partition> part ) {
    GDCHECK( part );
    auto pid = part->get_metadata().partition_id_;

    std::string outfile = partition_directory_ + "/" +
                          std::to_string( pid.partition_start ) + "-" +
                          std::to_string( pid.partition_end ) + "-" +
                          std::to_string( pid.column_start ) + "-" +
                          std::to_string( pid.column_end ) + "-" +
                          std::to_string( part->get_master_location() ) + ".p";

    DVLOG( 40 ) << "Persisting master partition:" << pid << ", to:" << outfile;

    data_persister part_persister( outfile );

    part->persist_data( &part_persister,
                        translate_update_information_to_slot(
                            part->get_update_propagation_information() ) );
}

std::tuple<uint32_t, uint64_t>
    data_site_table_persister::translate_update_information_to_slot(
        const update_propagation_information& up_info ) const {
    // HDB-PR offset is always 0
    return std::make_tuple<>( update_prop_configs_to_slots_.at(
                                  up_info.propagation_config_.partition ),
                              0 );
}

void data_site_table_persister::set_chunk_information(
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

