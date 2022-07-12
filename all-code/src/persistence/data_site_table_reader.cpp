#include "data_site_table_reader.h"

#include <glog/logging.h>

#include "../common/gdcheck.h"
#include "../data-site/db/cell_identifier.h"

data_site_table_reader::data_site_table_reader(
    const std::string& input_file_name, const std::string& partition_directory,
    tables*                                       data_tables,
    std::shared_ptr<update_destination_generator> update_generator,
    const std::vector<std::vector<propagation_configuration>>&
             site_propagation_configs,
    uint32_t table_id, uint32_t site_id )
    : data_reader_( input_file_name ),
      partition_directory_( partition_directory ),
      data_tables_( data_tables ),
      update_generator_( update_generator ),
      site_propagation_configs_( site_propagation_configs ),
      table_id_( table_id ),
      site_id_( site_id ) {}

// open
void data_site_table_reader::init() { data_reader_.open(); }
// close
void data_site_table_reader::close() { data_reader_.close(); }

table_metadata data_site_table_reader::load_table_metadata() {
    DVLOG( 20 ) << "Loading table location metadata for table_id:" << table_id_;

    uint32_t                    read_table_id = 0;
    uint32_t                    read_num_columns = 0;
    std::vector<cell_data_type> col_types;
    uint64_t                    read_partition_size = 0;
    uint32_t                    read_partition_col_size = 0;
    partition_type::type        read_part_type;
    uint32_t                    read_num_records_in_snapshot_chain = 0;
    uint32_t                    read_num_records_in_chain = 0;
    uint32_t                    read_site_id = 0;
    std::string                 read_table_name;

    data_reader_.read_i32( (uint32_t*) &read_table_id );
    data_reader_.read_i32( (uint32_t*) &read_num_columns );
    for( uint32_t col = 0; col < read_num_columns; col++ ) {
        uint32_t col_type_int = 0;
        data_reader_.read_i32( (uint32_t*) &col_type_int );
        col_types.emplace_back( (cell_data_type) col_type_int );
    }

    data_reader_.read_i64( (uint64_t*) &read_partition_size );
    data_reader_.read_i32( (uint32_t*) &read_partition_col_size );
    uint32_t read_part_type_int = 0;
    data_reader_.read_i32( (uint32_t*) &read_part_type_int );
    read_part_type = (partition_type::type) read_part_type_int;
    data_reader_.read_i32( (uint32_t*) &read_num_records_in_chain );
    data_reader_.read_i32( (uint32_t*) &read_num_records_in_snapshot_chain );
    data_reader_.read_i32( (uint32_t*) &read_site_id );
    data_reader_.read_str( &read_table_name );

    DVLOG( 40 ) << "Read table_id:" << read_table_id;
    DVLOG( 40 ) << "Read num_columns:" << read_num_columns;
    DVLOG( 40 ) << "Read partition_size:" << read_partition_size;
    DVLOG( 40 ) << "Read partition_column_size:" << read_partition_col_size;
    DVLOG( 40 ) << "Read part_type:" << read_part_type;
    DVLOG( 40 ) << "Read num_records_in_chain:" << read_num_records_in_chain;
    DVLOG( 40 ) << "Read num_records_in_snapshot_chain:"
                << read_num_records_in_snapshot_chain;
    DVLOG( 40 ) << "Read site_id:" << read_site_id;
    DVLOG( 40 ) << "Read table_name:" << read_table_name;

    GDCHECK_EQ( table_id_, read_table_id );
    GDCHECK_EQ( site_id_, read_site_id );

    table_metadata table_meta =
        data_tables_->get_table( table_id_ )->get_metadata();

    GDCHECK_EQ( read_table_id, table_meta.table_id_ );
    GDCHECK_EQ( read_num_columns, table_meta.num_columns_ );
    GDCHECK_EQ( col_types, table_meta.col_types_ );
    GDCHECK_EQ( read_partition_size, table_meta.default_partition_size_ );
    GDCHECK_EQ( read_partition_col_size, table_meta.default_column_size_ );
    GDCHECK_EQ( read_part_type, table_meta.default_partition_type_ );
    GDCHECK_EQ( read_num_records_in_chain, table_meta.num_records_in_chain_ );
    GDCHECK_EQ( read_num_records_in_snapshot_chain,
                table_meta.num_records_in_snapshot_chain_ );
    GDCHECK_EQ( read_site_id, table_meta.site_location_ );
    GDCHECK_EQ( read_table_name, table_meta.table_name_ );

    DVLOG( 20 ) << "Loading table metadata for table_id:" << table_id_
                << " okay!";
    return table_meta;
}

int32_t data_site_table_reader::load_num_table_chunks() {
    DVLOG( 20 ) << "Loading table num table chunks for table_id:" << table_id_;

    int32_t num_chunks = 0;

    data_reader_.read_i32( (uint32_t*) &num_chunks );

    DVLOG( 40 ) << "Read num chunks:" << num_chunks;
    DVLOG( 20 ) << "Loading table num table chunks for table_id:" << table_id_
                << ", okay!";
    return num_chunks;
}

void data_site_table_reader::load_persisted_table_data() {
    DVLOG( 20 ) << "Loading table persisted for table_id:" << table_id_;

    for( ;; ) {
        auto                       entry = read_entry();
        std::shared_ptr<partition> part = std::get<0>( entry );
        if( !part ) {
            break;
        }

        auto inserted = data_tables_->insert_partition(
            part->get_metadata().partition_id_, part );

        if( part->get_master_location() != site_id_ ) {
            uint32_t update_slot = std::get<1>( entry );
            uint64_t update_offset = std::get<2>( entry );
            uint64_t version = std::get<3>( entry );

            start_subscription( part->get_metadata().partition_id_,
                                part->get_master_location(), update_slot,
                                update_offset, version );
        }

        part->unlock_partition();

        GDCHECK( inserted );
    }

    DVLOG( 20 ) << "Loading table persisted for table_id:" << table_id_
                << " okay!";
}

void data_site_table_reader::start_subscription(
    const partition_column_identifier& pid, uint32_t master,
    uint32_t update_slot, uint64_t offset, uint64_t version ) {

    snapshot_vector snapshot;
    set_snapshot_version( snapshot, pid, version );

    GDCHECK_LT( master, site_propagation_configs_.size() );
    GDCHECK_LT( update_slot, site_propagation_configs_.at( master ).size() );

    update_propagation_information up_info;
    up_info.propagation_config_ =
        site_propagation_configs_.at( master ).at( update_slot );
    up_info.propagation_config_.offset = offset;
    up_info.identifier_ = pid;
    up_info.do_seek_ = true;

    data_tables_->start_subscribing( {up_info}, snapshot,
                                     k_add_replica_cause_string );
}

std::shared_ptr<update_destination_interface>
    data_site_table_reader::get_update_destination( uint32_t update_slot ) {
    return update_generator_->get_update_destination_by_position( update_slot );
}

std::tuple<std::shared_ptr<partition>, uint32_t, uint64_t, uint64_t>
    data_site_table_reader::read_entry() {
    uint64_t partition_start;
    uint64_t partition_end;
    uint32_t column_start;
    uint32_t column_end;
    uint32_t master_location;
    uint8_t  is_master;
    uint32_t part_type;
    uint32_t storage_type;

    data_reader_.read_i64( (uint64_t*) &partition_start );
    if( partition_start == k_unassigned_key ) {
        return std::make_tuple<>( nullptr, 0, 0, 0 );
    }

    data_reader_.read_i64( (uint64_t*) &partition_end );
    data_reader_.read_i32( (uint32_t*) &column_start );
    data_reader_.read_i32( (uint32_t*) &column_end );

    partition_column_identifier pid = create_partition_column_identifier(
        table_id_, partition_start, partition_end, column_start, column_end );

    DVLOG( 40 ) << "Read partition:" << pid;

    data_reader_.read_i32( (uint32_t*) &master_location );

    data_reader_.read_i8( (uint8_t*) &is_master );

    data_reader_.read_i32( (uint32_t*) &part_type );
    data_reader_.read_i32( (uint32_t*) &storage_type );

    return read_partition( pid, master_location,
                           (partition_type::type) part_type,
                           (storage_tier_type::type) storage_type );
}

std::tuple<std::shared_ptr<partition>, uint32_t, uint64_t, uint64_t>
    data_site_table_reader::read_partition(
        const partition_column_identifier& pid, uint32_t master_location,
        const partition_type::type&    part_type,
        const storage_tier_type::type& storage_type ) {

    std::string infile = partition_directory_ + "/" +
                         std::to_string( pid.partition_start ) + "-" +
                         std::to_string( pid.partition_end ) + "-" +
                         std::to_string( pid.column_start ) + "-" +
                         std::to_string( pid.column_end ) + "-" +
                         std::to_string( master_location ) + ".p";

    DVLOG( 5 ) << "Reading part:" << pid << ", from:" << infile;

    data_reader part_reader( infile );

    uint32_t read_table_id;
    int64_t  read_p_start;
    int64_t  read_p_end;
    int32_t  read_p_col_start;
    int32_t  read_p_col_end;

    part_reader.open();

    part_reader.read_i32( (uint32_t*) &read_table_id );
    part_reader.read_i64( (uint64_t*) &read_p_start );
    part_reader.read_i64( (uint64_t*) &read_p_end );
    part_reader.read_i32( (uint32_t*) &read_p_col_start );
    part_reader.read_i32( (uint32_t*) &read_p_col_end );


    GDCHECK_EQ( read_table_id, table_id_ );
    GDCHECK_EQ( read_p_start, pid.partition_start );
    GDCHECK_EQ( read_p_end, pid.partition_end );
    GDCHECK_EQ( read_p_col_start, pid.column_start );
    GDCHECK_EQ( read_p_col_end, pid.column_end );

    partition_column_identifier read_pid = create_partition_column_identifier(
        read_table_id, read_p_start, read_p_end, read_p_col_start,
        read_p_col_end );
    GDCHECK_EQ( read_pid, pid );

    uint32_t master_loc;
    uint64_t read_version;

    part_reader.read_i32( (uint32_t*) &master_loc );
    part_reader.read_i64( (uint64_t*) &read_version );

    DVLOG( 5 ) << "Read master loc:" << master_loc
               << ", version:" << read_version;

    // HDB-PR translate
    uint32_t update_destination_slot;
    uint64_t update_destination_offset;
    part_reader.read_i32( (uint32_t*) &update_destination_slot );
    part_reader.read_i64( (uint64_t*) &update_destination_offset );

    std::shared_ptr<update_destination_interface> update_destination =
        get_update_destination( update_destination_slot );

    snapshot_vector snapshot;
    set_snapshot_version( snapshot, pid, read_version );

    uint64_t init_version = read_version;

    if( init_version > 0 ) {
        init_version = init_version - 1;
    }

    // HDB-PR add the update destination
    std::shared_ptr<partition> part(
        data_tables_->create_partition_without_adding_to_table(
            pid, part_type, storage_tier_type::type::MEMORY, false,
            init_version, update_destination ) );

    uint64_t pid_hash = part->get_metadata().partition_id_hash_;
    (void) pid_hash;


    snapshot_partition_column_state snap_pc_state;
    part->snapshot_partition_metadata( snap_pc_state );

    for( ;; ) {
        uint64_t read_record_key;
        uint64_t read_versioned_pid;
        uint64_t read_record_version;
        uint64_t read_current_pid;

        part_reader.read_i64( (uint64_t*) &read_record_key );
        if( read_record_key == k_unassigned_key ) {
            break;
        }

        DVLOG( 40 ) << "Read record:" << read_record_key;

        part_reader.read_i64( (uint64_t*) &read_versioned_pid );
        part_reader.read_i64( (uint64_t*) &read_record_version );
        part_reader.read_i64( (uint64_t*) &read_current_pid );

        GDCHECK_EQ( read_current_pid, pid_hash );

        if( read_versioned_pid != read_current_pid ) {
            read_record_version = read_version;
        }
        for( int32_t i = pid.column_start; i <= pid.column_end; i++ ) {
            int32_t col = i - pid.column_start;

            uint8_t read_is_present;
            part_reader.read_i8( (uint8_t*) &read_is_present );

            if( read_is_present ) {
                std::string data_state;
                part_reader.read_str( (std::string*) &data_state );

                DVLOG( 40 ) << "Read record:" << read_record_key
                            << ", col:" << i << ", translated col:" << col;

                DCHECK_LT( col, snap_pc_state.columns.size() );
                std::vector<int64_t> keys = {(int64_t) read_record_key};
                snap_pc_state.columns.at( col ).keys.emplace_back( keys );
                snap_pc_state.columns.at( col ).data.emplace_back( data_state );
            }
        }
    }
    part->change_storage_tier( storage_type );

    part_reader.close();

    DVLOG( 40 ) << "Done reading records, init_partition_from_snapshot for pid:"
                << pid;

    part->init_partition_from_snapshot( snapshot, snap_pc_state.columns,
                                        master_loc );

    return std::make_tuple<>( part, update_destination_slot,
                              update_destination_offset, init_version );
}

uint32_t    data_site_table_reader::get_table_id() const { return table_id_; }
std::string data_site_table_reader::get_partition_directory() const {
    return partition_directory_;
}
