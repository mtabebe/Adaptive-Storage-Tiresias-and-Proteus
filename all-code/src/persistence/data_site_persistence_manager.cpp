#include "data_site_persistence_manager.h"

#include <glog/logging.h>

#include "../common/gdcheck.h"
#include "../common/thread_utils.h"
#include "persistence_utils.h"

static const std::string db_location_file_name = "metadata.db";

data_site_persistence_manager::data_site_persistence_manager(
    const std::string& data_site_directory,
    const std::string& partition_directory, tables* data_tables,
    std::shared_ptr<update_destination_generator> update_generator,
    const std::vector<std::vector<propagation_configuration>>&
                               site_propagation_configs,
    const persistence_configs& configs, uint32_t site_id )
    : data_site_directory_( data_site_directory ),
      partition_directory_( partition_directory ),
      data_tables_( data_tables ),
      update_generator_( update_generator ),
      site_propagation_configs_( site_propagation_configs ),
      configs_( configs ),
      site_id_( site_id ) {}

void data_site_persistence_manager::load() {
    tables_metadata t_meta = load_tables_metadata();

    DVLOG( 5 ) << "Loading data location";

    load_tables( t_meta.expected_num_tables_ );

    DVLOG( 5 ) << "Loading data location okay!";
}

tables_metadata data_site_persistence_manager::load_tables_metadata() {
    std::string input_file_name =
        data_site_directory_ + "/" + db_location_file_name;
    DVLOG( 5 ) << "Loading data location metadata:" << input_file_name;
    data_reader reader( input_file_name );
    reader.open();

    uint32_t num_tables = 0;
    uint32_t site_location = 0;

    tables_metadata t_meta = data_tables_->get_tables_metadata();

    // num tables
    reader.read_i32( &num_tables );

    DVLOG( 5 ) << "Number of tables to load" << num_tables;
    GDCHECK_EQ( num_tables, data_tables_->get_num_tables() );
    t_meta.expected_num_tables_ = num_tables;

    // site location
    reader.read_i32( &site_location );

    DVLOG( 5 ) << "Site location:" << site_location;
    GDCHECK_EQ( site_location, t_meta.site_location_ );

    reader.close();

    DVLOG( 5 ) << "Loading data location metadata:" << input_file_name
               << " okay!";

    return t_meta;
}

void data_site_persistence_manager::load_tables( uint32_t num_tables ) {
    std::vector<data_site_table_reader*> loaders;
    for( uint32_t table_id = 0; table_id < num_tables; table_id++ ) {
        std::string input_file_name =
            data_site_directory_ + "/" + std::to_string( table_id ) + ".loc";
        std::string partition_table_directory =
            partition_directory_ + "/" + std::to_string( table_id );

        loaders.emplace_back( new data_site_table_reader(
            input_file_name, partition_table_directory, data_tables_,
            update_generator_, site_propagation_configs_, table_id,
            site_id_ ) );
    }

    std::vector<std::thread> loader_threads;
    for( data_site_table_reader* loader : loaders ) {
        std::thread loader_thread( &data_site_persistence_manager::load_table_data,
                                   this, loader );
        loader_threads.push_back( std::move( loader_thread ) );
    }

    join_threads( loader_threads );

    for( uint32_t table_id = 0; table_id < num_tables; table_id++ ) {
        delete loaders.at( table_id );
        loaders.at( table_id ) = nullptr;
    }
    loaders.clear();
}

void data_site_persistence_manager::load_table_data(
    data_site_table_reader* loader ) {
    DVLOG( 10 ) << "Loading table location data:" << loader->get_table_id();
    loader->init();

    loader->load_table_metadata();
    if( configs_.load_chunked_tables_ ) {
        int32_t num_table_chunks = loader->load_num_table_chunks();
        load_table_chunks( num_table_chunks, loader );
    } else {
        loader->load_persisted_table_data();
    }

    loader->close();
    DVLOG( 10 ) << "Loading table location data:" << loader->get_table_id()
                << " okay!";
}

void data_site_persistence_manager::load_chunk_of_table(
    data_site_table_reader* loader ) {
    DVLOG( 10 ) << "Loading data site table data, chunk, table:"
                << loader->get_table_id();
    loader->init();
    loader->load_persisted_table_data();
    loader->close();
    DVLOG( 10 ) << "Loading data site table data, chunk, table:"
                << loader->get_table_id() << " okay!";
}

void data_site_persistence_manager::load_table_chunks(
    int32_t num_table_chunks, data_site_table_reader* meta_loader ) {

    std::vector<data_site_table_reader*> loaders;
    for( int32_t chunk_id = 0; chunk_id < num_table_chunks; chunk_id++ ) {
        std::string input_file_name =
            data_site_directory_ + "/" +
            std::to_string( meta_loader->get_table_id() ) + "-" +
            std::to_string( chunk_id ) + ".loc";

        loaders.emplace_back( new data_site_table_reader(
            input_file_name, meta_loader->get_partition_directory(),
            data_tables_, update_generator_, site_propagation_configs_,
            meta_loader->get_table_id(), site_id_ ) );
    }

    std::vector<std::thread> loader_threads;
    for( data_site_table_reader* loader : loaders ) {
        std::thread loader_thread(
            &data_site_persistence_manager::load_chunk_of_table, this, loader );
        loader_threads.push_back( std::move( loader_thread ) );
    }

    join_threads( loader_threads );

    for( int32_t chunk_id = 0; chunk_id < num_table_chunks; chunk_id++ ) {
        delete loaders.at( chunk_id );
        loaders.at( chunk_id ) = nullptr;
    }
    loaders.clear();
}

void data_site_persistence_manager::persist() {
    create_directory( data_site_directory_ );
    create_directory( partition_directory_ );
    persist_data_site_metadata();
    persist_tables();
}

void data_site_persistence_manager::persist_data_site_metadata() {
    std::string output_file_name =
        data_site_directory_ + "/" + db_location_file_name;

    DVLOG( 5 ) << "Persisting location metadata:" << output_file_name;
    data_persister persister( output_file_name );

    persister.open();

    tables_metadata t_meta = data_tables_->get_tables_metadata();

    uint32_t num_tables = data_tables_->get_num_tables();
    uint32_t site_location = t_meta.site_location_;
    // num tables
    persister.write_i32( num_tables );
    persister.write_i32( site_location );

    persister.close();

    DVLOG( 5 ) << "Persisting location metadata:" << output_file_name
               << " okay!";
}

void data_site_persistence_manager::persist_tables() {
    DVLOG( 5 ) << "Persisting location tables";

    std::vector<std::thread> persistence_threads;
    for( uint32_t table_id = 0;
         table_id < data_tables_->get_num_tables(); table_id++ ) {
        std::thread persister_thread(
            &data_site_persistence_manager::persist_table, this, table_id );
        persistence_threads.push_back( std::move( persister_thread ) );
    }

    join_threads( persistence_threads );
}

void data_site_persistence_manager::persist_table( uint32_t table_id ) {
    std::string output_file_name =
        data_site_directory_ + "/" + std::to_string( table_id ) + ".loc";

    std::string partition_table_directory =
        partition_directory_ + "/" + std::to_string( table_id );
    create_directory( partition_table_directory );


    DVLOG( 5 ) << "Persisting data location table:" << table_id
               << " to:" << output_file_name;

    GDCHECK_LT( site_id_, site_propagation_configs_.size() );

    data_site_table_persister persister(
        output_file_name, partition_table_directory,
        site_propagation_configs_.at( site_id_ ), site_id_ );
    if( configs_.persist_chunked_tables_ ) {
        persister.set_chunk_information(
            data_tables_->get_approx_total_number_of_partitions( table_id ),
            data_site_directory_, table_id, configs_.chunked_table_size_ );
    }

    persister.init();

    data_tables_->persist_db_table( table_id, &persister );

    persister.finish();
    DVLOG( 5 ) << "Persisting table:" << table_id << " to:" << output_file_name
               << " okay!";
}

