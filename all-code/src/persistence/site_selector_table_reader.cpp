#include "site_selector_table_reader.h"

#include <glog/logging.h>

#include "../common/gdcheck.h"
#include "../data-site/db/cell_identifier.h"

site_selector_table_reader::site_selector_table_reader(
    const std::string&                           input_file_name,
    multi_version_partition_data_location_table* data_loc_tab,
    uint32_t                                     table_id )
    : data_reader_( input_file_name ),
      data_loc_tab_( data_loc_tab ),
      table_id_( table_id ) {}

// open
void site_selector_table_reader::init() { data_reader_.open(); }
// close
void site_selector_table_reader::close() { data_reader_.close(); }

table_metadata site_selector_table_reader::load_table_metadata() {
    DVLOG( 20 ) << "Loading table location metadata for table_id:" << table_id_;

    uint32_t                    read_table_id = 0;
    uint32_t                    read_num_columns = 0;
    std::vector<cell_data_type> read_col_types;
    uint64_t                    read_partition_size = 0;
    uint64_t                    read_partition_col_size = 0;
    partition_type::type        read_part_type;
    uint32_t                    read_num_records_in_snapshot_chain = 0;
    uint32_t                    read_num_records_in_chain = 0;
    std::string                 read_table_name;

    data_reader_.read_i32( (uint32_t*) &read_table_id );
    data_reader_.read_i32( (uint32_t*) &read_num_columns );
    uint32_t read_col_type;
    for( uint32_t i = 0; i < read_num_columns; i++ ) {
        data_reader_.read_i32( (uint32_t*) &read_col_type );
        read_col_types.emplace_back( (cell_data_type) read_col_type );
    }
    data_reader_.read_i64( (uint64_t*) &read_partition_size );
    data_reader_.read_i32( (uint32_t*) &read_partition_col_size );
    uint32_t read_part_type_int = 0;
    data_reader_.read_i32( (uint32_t*) &read_part_type_int );
    read_part_type = (partition_type::type) read_part_type_int;
    data_reader_.read_i32( (uint32_t*) &read_num_records_in_chain );
    data_reader_.read_i32( (uint32_t*) &read_num_records_in_snapshot_chain );
    data_reader_.read_str( &read_table_name );

    DVLOG( 40 ) << "Read table_id:" << read_table_id;
    DVLOG( 40 ) << "Read num_columns:" << read_num_columns;
    DVLOG( 40 ) << "Read partition_size:" << read_partition_size;
    DVLOG( 40 ) << "Read partition_column_size:" << read_partition_col_size;
    DVLOG( 40 ) << "Read default_partition_type:" << read_part_type;
    DVLOG( 40 ) << "Read num_records_in_chain:" << read_num_records_in_chain;
    DVLOG( 40 ) << "Read num_records_in_snapshot_chain:"
                << read_num_records_in_snapshot_chain;
    DVLOG( 40 ) << "Read table_name:" << read_table_name;

    GDCHECK_EQ( table_id_, read_table_id );

    table_metadata table_meta = data_loc_tab_->get_table_metadata( table_id_ );

    GDCHECK_EQ( read_table_id, table_meta.table_id_ );
    GDCHECK_EQ( read_num_columns, table_meta.num_columns_ );
    GDCHECK_EQ( read_col_types, table_meta.col_types_ );
    GDCHECK_EQ( read_partition_size, table_meta.default_partition_size_ );
    GDCHECK_EQ( read_partition_col_size, table_meta.default_column_size_ );
    GDCHECK_EQ( read_part_type, table_meta.default_partition_type_ );
    GDCHECK_EQ( read_num_records_in_chain, table_meta.num_records_in_chain_ );
    GDCHECK_EQ( read_num_records_in_snapshot_chain,
                table_meta.num_records_in_snapshot_chain_ );
    GDCHECK_EQ( read_table_name, table_meta.table_name_ );

    DVLOG( 20 ) << "Loading table metadata for table_id:" << table_id_
                << " okay!";
    return table_meta;
}

int32_t site_selector_table_reader::load_num_table_chunks() {
    DVLOG( 20 ) << "Loading table num table chunks for table_id:" << table_id_;

    int32_t num_chunks = 0;

    data_reader_.read_i32( (uint32_t*) &num_chunks );

    DVLOG( 40 ) << "Read num chunks:" << num_chunks;
    DVLOG( 20 ) << "Loading table num table chunks for table_id:" << table_id_
                << ", okay!";
    return num_chunks;
}

void site_selector_table_reader::load_persisted_table_data() {
    DVLOG( 20 ) << "Loading table persisted for table_id:" << table_id_;

    for( ;; ) {
        std::shared_ptr<partition_payload> payload = read_entry();
        if( !payload ) {
            break;
        }

        auto inserted = data_loc_tab_->insert_partition(
            payload, partition_lock_mode::no_lock );
        GDCHECK( inserted );
        GDCHECK_EQ( inserted->identifier_, payload->identifier_ );
    }

    DVLOG( 20 ) << "Loading table persisted for table_id:" << table_id_
                << " okay!";
}

std::shared_ptr<partition_payload> site_selector_table_reader::read_entry() {
    uint64_t             partition_start;
    uint64_t             partition_end;
    uint32_t             col_start;
    uint32_t             col_end;
    uint32_t             master_location;
    uint32_t             num_replicas;
    std::unordered_set<uint32_t> replica_locations;
    std::unordered_map<uint32_t, partition_type::type> partition_types;
    std::unordered_map<uint32_t, storage_tier_type::type> storage_types;
    uint32_t update_destination_slot;

    data_reader_.read_i64( (uint64_t*) &partition_start );
    if( partition_start == k_unassigned_key ) {
        return nullptr;
    }

    data_reader_.read_i64( (uint64_t*) &partition_end );
    data_reader_.read_i32( (uint32_t*) &col_start );
    data_reader_.read_i32( (uint32_t*) &col_end );

    partition_column_identifier pid = create_partition_column_identifier(
        table_id_, partition_start, partition_end, col_start, col_end );

    data_reader_.read_i32( (uint32_t*) &master_location );
    data_reader_.read_i32( (uint32_t*) &num_replicas );

    uint32_t             replica_loc;
    for( uint32_t i = 0; i < num_replicas; i++ ) {
        data_reader_.read_i32( (uint32_t*) &replica_loc );
        replica_locations.emplace( replica_loc );
    }

    uint32_t num_part_types;
    data_reader_.read_i32( (uint32_t*) &num_part_types );
    DCHECK_EQ( num_part_types, num_replicas + 1 );
    uint32_t             part_type_int;
    partition_type::type part_type;
    for( uint32_t i = 0; i < num_part_types; i++ ) {
        data_reader_.read_i32( (uint32_t*) &replica_loc );
        data_reader_.read_i32( (uint32_t*) &part_type_int );
        part_type = (partition_type::type) part_type_int;
        partition_types.emplace( replica_loc, part_type );
    }
    uint32_t num_storage_types;
    data_reader_.read_i32( (uint32_t*) &num_storage_types );
    DCHECK_EQ( num_storage_types, num_replicas + 1 );
    uint32_t             storage_type_int;
    storage_tier_type::type storage_type;
    for( uint32_t i = 0; i < num_storage_types; i++ ) {
        data_reader_.read_i32( (uint32_t*) &replica_loc );
        data_reader_.read_i32( (uint32_t*) &storage_type_int );
        storage_type = (storage_tier_type::type) storage_type_int;
        storage_types.emplace( replica_loc, storage_type );
    }
    data_reader_.read_i32( (uint32_t*) &update_destination_slot );

    auto loc_info = std::make_shared<partition_location_information>();
    loc_info->version_ = 1;
    loc_info->master_location_ = master_location;
    loc_info->replica_locations_ = replica_locations;
    loc_info->partition_types_ = partition_types;
    loc_info->storage_types_ = storage_types;
    loc_info->update_destination_slot_ = update_destination_slot;

    auto payload = std::make_shared<partition_payload>( pid );
    payload->lock();
    payload->set_location_information( loc_info );
    payload->unlock();

    DVLOG( 40 ) << "Load partition payload:" << *payload;

    return payload;
}

uint32_t site_selector_table_reader::get_table_id() const { return table_id_; }
multi_version_partition_data_location_table*
    site_selector_table_reader::get_data_location_table() const {
    return data_loc_tab_;
}
