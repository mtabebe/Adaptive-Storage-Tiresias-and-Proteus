#include "site_selector_persistence_manager.h"

#include <glog/logging.h>

#include "../common/gdcheck.h"
#include "../common/thread_utils.h"
#include "persistence_utils.h"

static const std::string db_location_file_name = "metadata.loc";

site_selector_persistence_manager::site_selector_persistence_manager(
    const std::string&                           directory,
    multi_version_partition_data_location_table* data_location_table,
    const std::vector<std::vector<propagation_configuration>>& prop_configs,
    const persistence_configs&                                 configs )
    : directory_( directory ),
      data_location_table_( data_location_table ),
      prop_configs_( prop_configs ),
      configs_( configs ) {}

void site_selector_persistence_manager::load() {
    int num_tables = load_num_tables();

    DVLOG( 5 ) << "Loading data location";

    load_tables( num_tables );

    DVLOG( 5 ) << "Loading data location okay!";
}

int site_selector_persistence_manager::load_num_tables() {
    std::string input_file_name = directory_ + "/" + db_location_file_name;
    DVLOG( 5 ) << "Loading data location metadata:" << input_file_name;
    data_reader reader( input_file_name );
    reader.open();

    uint32_t num_tables = 0;
    // num tables
    reader.read_i32( &num_tables );

    DVLOG( 5 ) << "Number of tables to load" << num_tables;
    GDCHECK_EQ( num_tables, data_location_table_->get_num_tables() );

    uint32_t num_sites = 0;
    reader.read_i32( &num_sites );
    GDCHECK_EQ( num_sites, prop_configs_.size() );

    for( uint32_t site_id = 0; site_id < num_sites; site_id++ ) {
        uint32_t num_props_for_site = 0;
        reader.read_i32( &num_props_for_site );
        GDCHECK_EQ( num_props_for_site, prop_configs_.at( site_id ).size() );
    }

    reader.close();

    DVLOG( 5 ) << "Loading data location metadata:" << input_file_name
               << " okay!";

    return num_tables;
}

void site_selector_persistence_manager::load_tables( uint32_t num_tables ) {
    std::vector<site_selector_table_reader*> loaders;
    for( uint32_t table_id = 0; table_id < num_tables; table_id++ ) {
        std::string input_file_name =
            directory_ + "/" + std::to_string( table_id ) + ".loc";
        loaders.emplace_back( new site_selector_table_reader(
            input_file_name, data_location_table_, table_id ) );
    }

    std::vector<std::thread> loader_threads;
    for( site_selector_table_reader* loader : loaders ) {
        std::thread loader_thread( &site_selector_persistence_manager::load_table_data,
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

void site_selector_persistence_manager::load_table_data(
    site_selector_table_reader* loader ) {
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

void site_selector_persistence_manager::load_chunk_of_table(
    site_selector_table_reader* loader ) {
    DVLOG( 10 ) << "Loading table location data, chunk, table:"
                << loader->get_table_id();
    loader->init();
    loader->load_persisted_table_data();
    loader->close();
    DVLOG( 10 ) << "Loading table location data, chunk, table:"
                << loader->get_table_id() << " okay!";
}

void site_selector_persistence_manager::load_table_chunks(
    int32_t num_table_chunks, site_selector_table_reader* meta_loader ) {

    std::vector<site_selector_table_reader*> loaders;
    for( int32_t chunk_id = 0; chunk_id < num_table_chunks; chunk_id++ ) {
        std::string input_file_name =
            directory_ + "/" + std::to_string( meta_loader->get_table_id() ) +
            "-" + std::to_string( chunk_id ) + ".loc";
        loaders.emplace_back( new site_selector_table_reader(
            input_file_name, data_location_table_,
            meta_loader->get_table_id() ) );
    }

    std::vector<std::thread> loader_threads;
    for( site_selector_table_reader* loader : loaders ) {
        std::thread loader_thread(
            &site_selector_persistence_manager::load_chunk_of_table, this,
            loader );
        loader_threads.push_back( std::move( loader_thread ) );
    }

    join_threads( loader_threads );

    for( int32_t chunk_id = 0; chunk_id < num_table_chunks; chunk_id++ ) {
        delete loaders.at( chunk_id );
        loaders.at( chunk_id ) = nullptr;
    }
    loaders.clear();
}

void site_selector_persistence_manager::persist() {
    create_directory( directory_ );
    persist_location_metadata();
    persist_tables();
}

void site_selector_persistence_manager::persist_location_metadata() {
    std::string output_file_name = directory_ + "/" + db_location_file_name;

    DVLOG( 5 ) << "Persisting location metadata:" << output_file_name;
    data_persister persister( output_file_name );

    persister.open();

    uint32_t num_tables = data_location_table_->get_num_tables();
    // num tables
    persister.write_i32( num_tables );

    persister.write_i32( prop_configs_.size() );
    for( uint32_t site_id = 0; site_id < prop_configs_.size(); site_id++ ) {
        persister.write_i32( prop_configs_.at( site_id ).size() );
    }

    persister.close();

    DVLOG( 5 ) << "Persisting location metadata:" << output_file_name
               << " okay!";
}

void site_selector_persistence_manager::persist_tables() {
    DVLOG( 5 ) << "Persisting location tables";

    std::vector<std::thread> persistence_threads;
    for( uint32_t table_id = 0;
         table_id < data_location_table_->get_num_tables(); table_id++ ) {
        std::thread persister_thread(
            &site_selector_persistence_manager::persist_table, this, table_id );
        persistence_threads.push_back( std::move( persister_thread ) );
    }

    join_threads( persistence_threads );
}

void site_selector_persistence_manager::persist_table( uint32_t table_id ) {
    std::string output_file_name =
        directory_ + "/" + std::to_string( table_id ) + ".loc";

    DVLOG( 5 ) << "Persisting data location table:" << table_id
               << " to:" << output_file_name;

    site_selector_table_persister persister( output_file_name );

    if( configs_.persist_chunked_tables_ ) {
        persister.set_chunk_information(
            data_location_table_->get_approx_total_number_of_partitions(),
            directory_, table_id, configs_.chunked_table_size_ );
    }

    persister.init();

    data_location_table_->persist_table_location( table_id, &persister );

    persister.finish();
    DVLOG( 5 ) << "Persisting table:" << table_id << " to:" << output_file_name
               << " okay!";
}

