#include "persistence_editor.h"

#include <glog/logging.h>

#include "../common/gdcheck.h"
#include "../common/thread_utils.h"
#include "../gen-cpp/gen-cpp/dynamic_mastering_constants.h"
#include "../site-selector/partition_data_location_table.h"
#include "persistence_utils.h"

persistence_editor::persistence_editor(
    data_location_rewriter_interface* rewriter,
    const std::string& ss_in_directory, const std::string& sm_in_directory,
    const std::string& part_in_directory, const std::string& ss_out_directory,
    const std::string& sm_out_directory, const std::string& part_out_directory )
    : rewriter_( rewriter ),
      ss_in_directory_( ss_in_directory ),
      sm_in_directory_( sm_in_directory ),
      part_in_directory_( part_in_directory ),
      ss_out_directory_( ss_out_directory ),
      sm_out_directory_( sm_out_directory ),
      part_out_directory_( part_out_directory ) {}

void persistence_editor::rewrite() {
    DVLOG( 5 ) << "Rewriting logs, input SS directory:" << ss_in_directory_
               << ", input SM directory:" << sm_in_directory_
               << ", input PART directory:" << part_in_directory_
               << ", output SS directory:" << ss_out_directory_
               << ", output SM directory:" << sm_out_directory_
               << ", output PART directory:" << part_out_directory_;
    GDCHECK_NE( 0, ss_in_directory_.compare( ss_out_directory_ ) );
    GDCHECK_NE( 0, sm_in_directory_.compare( sm_out_directory_ ) );
    GDCHECK_NE( 0, part_in_directory_.compare( part_out_directory_ ) );

    create_directory( ss_out_directory_ );
    create_directory( sm_out_directory_ );
    create_directory( part_out_directory_ );

    for( int site_id = 0; site_id < rewriter_->get_num_sites(); site_id++ ) {
        std::string out_dir =
            sm_out_directory_ + "/" + std::to_string( site_id );
        create_directory( out_dir );
    }

    //  read site selector info
    partition_locations_map old_partition_locations = get_in_partition_locations();

    partition_locations_map partition_locations;
    for( auto& table_entry : old_partition_locations ) {
        partition_column_identifier_map_t<partition_location_information> locations;
        for( const auto& part_entry : table_entry.second ) {
            locations.emplace( part_entry.first, part_entry.second );
        }
        partition_locations.emplace( table_entry.first, locations );
    }

    //  hand off to editor to tell us where to put each bucket
    rewriter_->rewrite_partition_locations( partition_locations );

    for( auto& table_entry : old_partition_locations ) {
        partition_column_identifier_map_t<partition_location_information> locations;
        for( const auto& part_entry : table_entry.second ) {
            auto new_part_entry = partition_locations.at( table_entry.first )
                                      .find( part_entry.first );
            GDCHECK( ( new_part_entry !=
                       partition_locations.at( table_entry.first ).end() ) );
            DVLOG( 5 ) << "part:" << part_entry.first
                       << ", old:" << part_entry.second
                       << ", new:" << new_part_entry->second;
        }
        partition_locations.emplace( table_entry.first, locations );
    }

    //  read each table and rewrite their localtion given the bucket
    write_tables_with_new_partitions( partition_locations,
                                      old_partition_locations );
}

partition_locations_map persistence_editor::get_in_partition_locations() {
    std::vector<table_metadata> tables_meta = rewriter_->get_tables_metadata();

    partition_locations_map locs;
    std::vector<std::vector<propagation_configuration>> prop_configs =
        rewriter_->get_propagation_configurations();

    multi_version_partition_data_location_table data_loc_tab;
    for( auto meta : tables_meta ) {
        data_loc_tab.create_table( meta );
    }

    site_selector_persistence_manager loader(
        ss_in_directory_, &data_loc_tab, prop_configs,
        rewriter_->get_persistence_configs() );

    uint32_t num_tables = loader.load_num_tables();
    GDCHECK_EQ( num_tables, tables_meta.size() );

    partition_locations_map partitions_locations;

    std::vector<site_selector_table_reader*> loaders;
    for( uint32_t table_id = 0; table_id < num_tables; table_id++ ) {
        std::string input_file_name =
            ss_in_directory_ + "/" + std::to_string( table_id ) + ".loc";
        loaders.emplace_back( new site_selector_table_reader(
            input_file_name, &data_loc_tab, table_id ) );
        partition_column_identifier_map_t<partition_location_information> locations;
        partitions_locations.emplace( table_id, locations );
    }

    std::vector<std::thread> loader_threads;
    for( site_selector_table_reader* loader : loaders ) {

        std::thread loader_thread(
            &persistence_editor::load_site_selector_table_data, this, loader,
            std::ref( partitions_locations.at( loader->get_table_id() ) ) );
        loader_threads.push_back( std::move( loader_thread ) );
    }
    join_threads( loader_threads );

    for( uint32_t table_id = 0; table_id < num_tables; table_id++ ) {
        delete loaders.at( table_id );
    }

    return partitions_locations;
}

void persistence_editor::write_tables_with_new_partitions(
    const partition_locations_map& partition_locations,
    const partition_locations_map& old_partition_locations ) {
    std::vector<table_metadata> tables_meta = rewriter_->get_tables_metadata();

    std::vector<std::vector<propagation_configuration>> prop_configs =
        rewriter_->get_propagation_configurations();

    multi_version_partition_data_location_table data_loc_tab;
    site_selector_persistence_manager           site_selector_persister(
        ss_out_directory_, &data_loc_tab, prop_configs,
        rewriter_->get_persistence_configs() );

    for( auto meta : tables_meta ) {
        data_loc_tab.create_table( meta );
    }

    site_selector_persister.persist_location_metadata();

    std::vector<tables*> site_data_tables;
    for( int site = 0; site < rewriter_->get_num_sites(); site++ ) {
      tables* t = new tables();
      tables_metadata ts_meta =
          create_tables_metadata( tables_meta.size(), site );
      t->init( ts_meta, rewriter_->get_update_generator(),
               rewriter_->get_update_enqueuers() );
      rewriter_->get_update_enqueuers()->set_tables( ts_meta, (void*) t );
      for( auto meta : tables_meta ) {
          t->create_table( meta );
      }
      site_data_tables.push_back( t );
    }

    for( int site = 0; site < rewriter_->get_num_sites(); site++ ) {
        std::string site_out_db_name =
            sm_out_directory_ + "/" + std::to_string( site );
        std::string site_in_db_name =
            sm_in_directory_ + "/" + std::to_string( site );

        data_site_persistence_manager site_reader(
            site_in_db_name, part_in_directory_, site_data_tables.at( site ),
            rewriter_->get_update_generator(), prop_configs,
            rewriter_->get_persistence_configs(), site );
        site_reader.load_tables_metadata();

        data_site_persistence_manager site_persister(
            site_out_db_name, part_out_directory_, site_data_tables.at( site ),
            rewriter_->get_update_generator(), prop_configs,
            rewriter_->get_persistence_configs(), site );
        site_persister.persist_data_site_metadata();
    }


    std::vector<std::thread> persistence_threads;
    for( unsigned int pos = 0; pos < tables_meta.size(); pos++ ) {
        std::thread persister_thread(
            &persistence_editor::persist_site_selector_table, this,
            std::cref( tables_meta.at( pos ) ),
            std::cref(
                partition_locations.at( tables_meta.at( pos ).table_id_ ) ) );
        persistence_threads.push_back( std::move( persister_thread ) );
    }

    for( unsigned int pos = 0; pos < tables_meta.size(); pos++ ) {
        std::thread persister_thread(
            &persistence_editor::rewrite_data_site_table, this, pos,
            std::cref( tables_meta ), std::cref( partition_locations.at( (
                                          tables_meta.at( pos ).table_id_ ) ) ),
            std::cref( old_partition_locations.at(
                ( tables_meta.at( pos ).table_id_ ) ) ),
            std::cref( site_data_tables ) );
        persistence_threads.push_back( std::move( persister_thread ) );
    }
    join_threads( persistence_threads );

    for( int site = 0; site < rewriter_->get_num_sites(); site++ ) {
        tables* t = site_data_tables.at( site );
        delete t;
        site_data_tables.at( site ) = nullptr;
    }

    site_data_tables.clear();

}

void persistence_editor::rewrite_data_site_table(
    uint32_t table_id, const std::vector<table_metadata>& tables_meta,
    const partition_column_identifier_map_t<partition_location_information>&
        partition_locations,
    const partition_column_identifier_map_t<partition_location_information>&
                                old_partition_locations,
    const std::vector<tables*>& site_data_tables ) {
    DVLOG( 5 ) << "Rewriting DB table:" << table_id;

    int num_sites = rewriter_->get_num_sites();

    std::vector<data_site_table_reader*> readers;
    std::vector<data_site_table_persister*> persisters;

    std::string part_table_out_dir =
        part_out_directory_ + "/" + std::to_string( table_id );
    create_directory( part_table_out_dir );

    std::string part_table_in_dir =
        part_in_directory_ + "/" + std::to_string( table_id );

    std::vector<std::vector<propagation_configuration>> prop_configs =
        rewriter_->get_propagation_configurations();

    // open
    for( int site_id = 0; site_id < num_sites; site_id++ ) {
        DVLOG( 10 ) << "Initializing for site:" << site_id;
        std::string in_dir = sm_in_directory_ + "/" + std::to_string( site_id );
        std::string out_dir = sm_out_directory_ + "/" + std::to_string(site_id);
        std::string input_file_name =
            in_dir + "/" + std::to_string( table_id ) + ".loc";
        std::string output_file_name =
            out_dir + "/" + std::to_string( table_id ) + ".loc";

        data_site_table_reader* reader = new data_site_table_reader(
            input_file_name, part_table_in_dir,
            site_data_tables.at( site_id ) /*tables*/,
            rewriter_->get_update_generator(), prop_configs, table_id,
            site_id );

        data_site_table_persister* persister = new data_site_table_persister(
            output_file_name, part_table_out_dir, prop_configs.at( site_id ),
            site_id );

        initialize_table( reader, persister, table_id,
                          partition_locations.size(), out_dir );

        readers.push_back(reader);
        persisters.push_back(persister);
    }

    for( const auto& part_location : partition_locations ) {
        const auto& pid = part_location.first;
        const auto& part_loc = part_location.second;

        auto part_lookup = old_partition_locations.find( pid );
        GDCHECK( part_lookup != old_partition_locations.end() );
        const auto& old_part_loc = part_lookup->second;

        int master_loc = old_part_loc.master_location_;
		if (master_loc == K_DATA_AT_ALL_SITES) {
            GDCHECK_EQ( part_loc.master_location_, K_DATA_AT_ALL_SITES );
            rewrite_at_all_site_partition( pid, part_loc, persisters, readers );
            continue;
        }

        partition_type::type master_type =
            std::get<1>( part_loc.get_partition_type( master_loc ) );

        std::tuple<std::shared_ptr<partition>, uint32_t /*slot*/,
                   uint64_t /*offset*/, uint64_t /*version*/>
            read_part_tuple =
                readers.at( master_loc )
                    ->read_partition( pid, master_loc, master_type,
                                      storage_tier_type::type::MEMORY );

        auto part = std::get<0>( read_part_tuple );
        GDCHECK( part );


        write_partition( persisters, part, part_loc );

        part->unlock_partition();
    }

    // close
    for( int site_id = 0; site_id < rewriter_->get_num_sites(); site_id++ ) {
        data_site_table_reader* reader = readers.at( site_id );
        data_site_table_persister* persister = persisters.at( site_id );
        finish_table( reader, persister );

        delete reader;
        delete persister;
    }

    DVLOG( 5 ) << "Rewriting DB table:" << table_id << " okay!";
}

void persistence_editor::rewrite_at_all_site_partition(
    const partition_column_identifier&              pid,
    const partition_location_information&    part_loc,
    std::vector<data_site_table_persister*>& persisters,
    std::vector<data_site_table_reader*>&    readers ) {
    partition_location_information rewrite_part_loc = part_loc;
    GDCHECK_EQ( rewrite_part_loc.master_location_, K_DATA_AT_ALL_SITES );
    for( int master_loc = 0; master_loc < rewriter_->get_num_sites();
         master_loc++ ) {
        partition_type::type master_type =
            std::get<1>( part_loc.get_partition_type( master_loc ) );

        std::tuple<std::shared_ptr<partition>, uint32_t /*slot*/,
                   uint64_t /*offset*/, uint64_t /*version*/>
            read_part_tuple =
                readers.at( master_loc )
                    ->read_partition( pid, master_loc, master_type,
                                      storage_tier_type::type::MEMORY );

        rewrite_part_loc.master_location_ = master_loc;
        rewrite_part_loc.partition_types_.emplace( master_loc, master_type );

        auto part = std::get<0>( read_part_tuple );
        GDCHECK( part );

        write_partition( persisters, part, rewrite_part_loc );

        part->unlock_partition();
    }
}

void persistence_editor::write_partition(
    std::vector<data_site_table_persister*>& persisters,
    std::shared_ptr<partition>               part,
    const partition_location_information&    part_loc ) {

    DVLOG( 40 ) << "Write partition:" << part->get_metadata().partition_id_
                << " to partition location information:" << part_loc;

    // set master location
    part->set_master_location( part_loc.master_location_ );
    // change the prop configs
    part->set_update_destination(
        rewriter_->get_update_generator()->get_update_destination_by_position(
            part_loc.update_destination_slot_ ) );

	int master_loc = part_loc.master_location_;
    persisters.at( master_loc )
        ->persist_partition(
            part, std::get<1>( part_loc.get_partition_type( master_loc ) ),
            std::get<1>( part_loc.get_storage_type( master_loc ) ) );

    for( const auto& replica_entry : part_loc.replica_locations_ ) {
        int replica = replica_entry;
        persisters.at( replica )->persist_partition(
            part, std::get<1>( part_loc.get_partition_type( replica ) ),
            std::get<1>( part_loc.get_storage_type( replica ) ) );
    }
}

void persistence_editor::initialize_table( data_site_table_reader*    reader,
                                           data_site_table_persister* persister,
                                           uint32_t                   table_id,
                                           uint64_t           num_partitions,
                                           const std::string& out_dir ) {
    reader->init();

    auto per_configs = rewriter_->get_persistence_configs();
    if( per_configs.persist_chunked_tables_ ) {
        persister->set_chunk_information( num_partitions, out_dir, table_id,
                                          per_configs.chunked_table_size_ );
    }
    persister->init();

    // read the table metadata
    table_metadata metadata = reader->load_table_metadata();
    GDCHECK_EQ( table_id, metadata.table_id_ );

    // write the table metadata
    persister->persist_table_info( metadata );
}

void persistence_editor::finish_table( data_site_table_reader*    reader,
                                       data_site_table_persister* persister ) {
    reader->close();
    persister->finish();
}

void persistence_editor::persist_site_selector_table(
    const table_metadata& table_meta,
    const partition_column_identifier_map_t<partition_location_information>&
        partition_locations ) {
    int         table_id = table_meta.table_id_;
    std::string output_file_name =
        ss_out_directory_ + "/" + std::to_string( table_id ) + ".loc";

    DVLOG( 5 ) << "Persisting site selector table:" << table_id
               << " to:" << output_file_name;

    auto        per_configs = rewriter_->get_persistence_configs();

    site_selector_table_persister persister( output_file_name );
    if( per_configs.persist_chunked_tables_ ) {
        persister.set_chunk_information( partition_locations.size(),
                                         ss_out_directory_, table_id,
                                         per_configs.chunked_table_size_ );
    }

    persister.init();

    persister.persist_table_info( table_meta );

    for( const auto& part_location : partition_locations ) {
        const auto& pid = part_location.first;
        const auto& existing_loc = part_location.second;

        auto part_payload = std::make_shared<partition_payload>( pid );

        auto part_loc = std::make_shared<partition_location_information>();
        part_loc->master_location_ = existing_loc.master_location_;
        part_loc->replica_locations_ = existing_loc.replica_locations_;
        part_loc->partition_types_ = existing_loc.partition_types_;
        part_loc->storage_types_ = existing_loc.storage_types_;
        part_loc->update_destination_slot_ =
            existing_loc.update_destination_slot_;
        part_loc->version_ = 1;

        part_payload->set_location_information( part_loc );

        persister.persist_partition_payload( part_payload );
    }

    persister.finish();
    DVLOG( 5 ) << "Persisting site selector table:" << table_id
               << " to:" << output_file_name << " okay!";
}

void persistence_editor::load_site_selector_table_data(
    site_selector_table_reader* loader,
    partition_column_identifier_map_t<partition_location_information>&
        partition_locations ) {
    loader->init();
    loader->load_table_metadata();

    load_site_selector_table_partitions( loader, partition_locations );

    loader->close();
}

void persistence_editor::load_site_selector_table_partitions(
    site_selector_table_reader* meta_loader,
    partition_column_identifier_map_t<partition_location_information>&
        partition_locations ) {
    int table_id = meta_loader->get_table_id();
    DVLOG( 20 ) << "Loading table persisted for table_id:" << table_id;

    std::vector<site_selector_table_reader*> loaders;
    int32_t                                  num_table_chunks = 0;

    auto per_configs = rewriter_->get_persistence_configs();
    if( per_configs.load_chunked_tables_ ) {
        num_table_chunks = meta_loader->load_num_table_chunks();
        for( int32_t chunk_id = 0; chunk_id < num_table_chunks; chunk_id++ ) {
            std::string input_file_name =
                ss_in_directory_ + "/" +
                std::to_string( meta_loader->get_table_id() ) + "-" +
                std::to_string( chunk_id ) + ".loc";
            loaders.emplace_back( new site_selector_table_reader(
                input_file_name, meta_loader->get_data_location_table(),
                meta_loader->get_table_id() ) );
            loaders.at( chunk_id )->init();
        }
    } else {
        loaders.push_back( meta_loader );
    }

    for( site_selector_table_reader* loader : loaders ) {
        for( ;; ) {

            std::shared_ptr<partition_payload> payload = loader->read_entry();
            if( !payload ) {
                break;
            }

            auto loc_info = payload->get_location_information();
            auto emplace_ret =
                partition_locations.emplace( payload->identifier_, *loc_info );

            if( !emplace_ret.second ) {
                LOG( WARNING )
                    << "Tried to insert the same partition but it has a "
                       "different location information, partition:"
                    << payload->identifier_ << " actual location information:"
                    << emplace_ret.first->second
                    << ", attempted location information:" << *loc_info;
            }
        }
    }

    if( per_configs.load_chunked_tables_ ) {
        for( int32_t chunk_id = 0; chunk_id < num_table_chunks; chunk_id++ ) {
            loaders.at( chunk_id )->close();
            delete loaders.at( chunk_id );
            loaders.at( chunk_id ) = nullptr;
        }
    }
    loaders.clear();

    DVLOG( 20 ) << "Loading table persisted for table_id:" << table_id
                << " okay!";
}

