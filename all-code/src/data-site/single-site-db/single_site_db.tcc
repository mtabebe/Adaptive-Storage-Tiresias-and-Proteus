#pragma once

#include <glog/logging.h>
#include <glog/stl_logging.h>

single_site_db_types single_site_db_templ::single_site_db()
    : db_(),
      loc_table_( nullptr ),
      prop_config_() {
    loc_table_ = make_partition_data_location_table(
        construct_partition_data_location_table_configs( 1 ) );
}
single_site_db_types single_site_db_templ::~single_site_db() {}

single_site_db_types void single_site_db_templ::init(
    std::shared_ptr<update_destination_generator> update_gen,
    std::shared_ptr<update_enqueuers>             enqueuers,
    const tables_metadata&                        t_meta ) {
    db_.init( update_gen, enqueuers, t_meta );
    prop_config_ = update_gen->get_propagation_configurations().at( 0 );
}

single_site_db_types uint32_t
    single_site_db_templ::create_table( const table_metadata& metadata ) {
    DVLOG( 5 ) << "Create table:" << metadata;
    uint32_t created_table_id = loc_table_->create_table( metadata );

    if( enable_db ) {
        uint32_t db_table_id = db_.get_tables()->create_table( metadata );
        DCHECK_EQ( created_table_id, db_table_id );
    }

    DCHECK_EQ( created_table_id, metadata.table_id_ );
    return created_table_id;
}

single_site_db_types transaction_partition_holder*
                     single_site_db_templ::get_partitions_with_begin(
        uint32_t client_id, const snapshot_vector& begin_timestamp,
        const std::vector<cell_key_ranges>& write_keys,
        const std::vector<cell_key_ranges>& read_keys, bool allow_missing ) {

    DVLOG( 40 ) << "Get Partitions with Begin, client_id:" << client_id
                << ", begin_timestamp:" << begin_timestamp
                << ", write_keys:" << write_keys << ", read_keys:" << read_keys;

    auto group_info = get_partitions_for_operations(
        client_id, write_keys, read_keys, partition_lock_mode::no_lock,
        allow_missing );
    DVLOG( 40 ) << "Got partitions for operations";

    partition_column_identifier_set write_pids =
        build_pid_set( group_info.existing_write_partitions_ );
    partition_column_identifier_set read_pids =
        build_pid_set( group_info.existing_read_partitions_ );
    DVLOG( 40 ) << "Got write partitions: " << write_pids
                << ", read partitions: " << read_pids << " for operations";

    transaction_partition_holder* holder = get_transaction_holder(
        client_id, write_pids, read_pids, {}, allow_missing );

    unlock_payloads( group_info.payloads_, partition_lock_mode::no_lock );

    if( enable_db ) {
        holder->begin_transaction( begin_timestamp );
    }
    return holder;
}

single_site_db_types transaction_partition_holder*
                     single_site_db_templ::get_partitions_with_begin(
        uint32_t client_id, const snapshot_vector& begin_timestamp,
        const partition_column_identifier_set& write_pids,
        const partition_column_identifier_set& read_pids,
        const partition_column_identifier_set& inflight_pids,
        bool                                   allow_missing ) {

    DVLOG( 40 ) << "Get Partitions with Begin, client_id:" << client_id
                << ", begin_timestamp:" << begin_timestamp
                << ", write_pids:" << write_pids << ", read_pids:" << read_pids;

    transaction_partition_holder* holder = get_transaction_holder(
        client_id, write_pids, read_pids, inflight_pids, allow_missing );
    if( enable_db ) {
        holder->begin_transaction( begin_timestamp );
    }
    return holder;
}

single_site_db_types grouped_partition_information
                     single_site_db_templ::get_partitions_for_operations(
        uint32_t client_id, const std::vector<cell_key_ranges>& write_cks,
        const std::vector<cell_key_ranges>& read_cks,
        const partition_lock_mode& mode, bool allow_missing ) {
    DVLOG( 40 ) << "Get Partitions for operations, client_id:" << client_id
                << ", write_cks:" << write_cks << ", read_cks:" << read_cks
                << ", mode:" << mode;

    std::vector<cell_key_ranges> sorted_ck_write_set( write_cks.begin(),
                                                      write_cks.end() );
    std::vector<cell_key_ranges> sorted_ck_read_set( read_cks.begin(),
                                                     read_cks.end() );
    std::sort( sorted_ck_write_set.begin(), sorted_ck_write_set.end() );
    std::sort( sorted_ck_read_set.begin(), sorted_ck_read_set.end() );

    grouped_partition_information group_info =
        loc_table_->get_partitions_and_group(
            sorted_ck_write_set, sorted_ck_read_set, mode, 1, allow_missing );
    if( group_info.payloads_.empty() and !allow_missing ) {
        group_info = loc_table_->get_or_create_partitions_and_group(
            sorted_ck_write_set, sorted_ck_read_set, partition_lock_mode::lock,
            1 );
        for( auto& payload : group_info.new_partitions_ ) {
            auto location_information =
                payload->get_location_information( partition_lock_mode::lock );
            if( location_information == nullptr ) {
                location_information =
                    std::make_shared<partition_location_information>();
                location_information->master_location_ =
                    db_.get_site_location();
                location_information->version_ = 1;
                payload->set_location_information( location_information );
            }
        }
        unlock_payloads( group_info.payloads_, partition_lock_mode::lock );
        group_info = loc_table_->get_partitions_and_group(
            sorted_ck_write_set, sorted_ck_read_set, mode, 1,
            false /* don't allow missing */ );
        DCHECK( !group_info.payloads_.empty() );
    }

    return group_info;
}

single_site_db_types grouped_partition_information
                     single_site_db_templ::get_partitions_for_operations(
        uint32_t client_id, const std::vector<cell_key>& write_cks,
        const std::vector<cell_key>& read_cks, const partition_lock_mode& mode,
        bool allow_missing ) {
    DVLOG( 40 ) << "Get Partitions for operations, client_id:" << client_id
                << ", write_cks:" << write_cks << ", read_cks:" << read_cks
                << ", mode:" << mode;

    std::vector<cell_key_ranges> write_ckrs;
    std::vector<cell_key_ranges> read_ckrs;

    // populate write and read ckrs
    for( const auto& ck : write_cks ) {
        cell_key_ranges ckr;
        ckr.table_id = ck.table_id;
        ckr.col_id_start = ck.col_id;
        ckr.col_id_end = ck.col_id;
        ckr.row_id_start = ck.row_id;
        ckr.row_id_end = ck.row_id;

        write_ckrs.emplace_back( ckr );
    }
    for( const auto& ck : read_cks ) {
        cell_key_ranges ckr;
        ckr.table_id = ck.table_id;
        ckr.col_id_start = ck.col_id;
        ckr.col_id_end = ck.col_id;
        ckr.row_id_start = ck.row_id;
        ckr.row_id_end = ck.row_id;

        read_ckrs.emplace_back( ckr );
    }

    return get_partitions_for_operations( client_id, write_ckrs, read_ckrs,
                                          mode, allow_missing );
}

single_site_db_types transaction_partition_holder*
                     single_site_db_templ::get_transaction_holder(
        uint32_t client_id, const partition_column_identifier_set& write_pids,
        const partition_column_identifier_set& read_pids,
        const partition_column_identifier_set& inflight_pids,
        bool                                   allow_missing ) {

    transaction_partition_holder* holder = nullptr;
    if( enable_db ) {
        partition_lookup_operation lookup_op =
            partition_lookup_operation::GET_OR_CREATE;
        if( allow_missing ) {
            lookup_op = partition_lookup_operation::GET_ALLOW_MISSING;
        }

        holder = db_.get_partition_holder( client_id, write_pids, read_pids,
                                           inflight_pids, lookup_op );
        DCHECK( holder );
    } else {
        holder = new transaction_partition_holder();
    }

    return holder;
}

single_site_db_types void single_site_db_templ::change_partition_types(
    uint32_t client_id, const std::vector<::partition_column_identifier>& pids,
    const std::vector<partition_type::type>&    part_types,
    const std::vector<storage_tier_type::type>& storage_types ) {
    DVLOG( 5 ) << "Change partition types, client_id:" << client_id
               << ", pids:" << pids << ", partition_types:" << part_types
               << ", storage_types:" << storage_types;

    DCHECK_EQ( pids.size(), part_types.size() );
    DCHECK_EQ( pids.size(), storage_types.size() );

    auto lock_mode = partition_lock_mode::lock;

    for( uint32_t pos = 0; pos < pids.size(); pos++ ) {
        std::vector<::partition_column_identifier> loc_pids = {pids.at( pos )};
        std::vector<partition_type::type> loc_p_type = {part_types.at( pos )};
        std::vector<storage_tier_type::type> loc_s_type = {
            storage_types.at( pos )};

        auto part = loc_table_->get_partition( pids.at( pos ), lock_mode );

        if( part ) {
            auto loc_info = part->get_location_information( lock_mode );
            DCHECK( loc_info );

            auto new_loc_info =
                std::make_shared<partition_location_information>();
            new_loc_info
                ->set_from_existing_location_information_and_increment_version(
                    loc_info );

            new_loc_info->partition_types_[new_loc_info->master_location_] =
                part_types.at( pos );
            new_loc_info->storage_types_[new_loc_info->master_location_] =
                storage_types.at( pos );

            part->set_location_information( new_loc_info );
        }

        if( enable_db ) {
            db_.change_partition_types( client_id, loc_pids, loc_p_type,
                                        loc_s_type );
        }

        if( part ) {
            part->unlock( lock_mode );
        }
    }
}

single_site_db_types void single_site_db_templ::add_partition(
    uint32_t client_id, const partition_column_identifier& pid,
    uint32_t master_location, const partition_type::type& p_type,
    const storage_tier_type::type& s_type ) {
    DVLOG( 5 ) << "Add partition, client_id:" << client_id << ", pid:" << pid
               << ", master_location:" << master_location
               << ", p_type:" << p_type
               << ", s_type:" << s_type;

    auto lock_mode = partition_lock_mode::lock;

    auto new_payload = std::make_shared<partition_payload>( pid );
    auto location_information =
        std::make_shared<partition_location_information>();
    location_information->version_ = 1;
    location_information->master_location_ = master_location;
    location_information->partition_types_[master_location] = p_type;
    location_information->storage_types_[master_location] = s_type;

    new_payload->set_location_information( location_information );

    auto inserted_payload =
        loc_table_->insert_partition( new_payload, lock_mode );
    if( !inserted_payload ) {
        return;
    }

    if( enable_db ) {
        db_.add_partition( client_id, pid, master_location, p_type, s_type );
    }

    inserted_payload->unlock( lock_mode );
}

single_site_db_types void single_site_db_templ::remove_partition(
    uint32_t client_id, std::shared_ptr<partition_payload> partition ) {

    DVLOG( 5 ) << "Remove partition, client_id:" << client_id
               << ", partition:" << *partition;

    auto pid = partition->identifier_;
    loc_table_->remove_partition( partition );
    if( enable_db ) {
        db_.remove_partition( client_id, pid );
    }
    partition->unlock( partition_lock_mode::lock );
}

single_site_db_types snapshot_vector single_site_db_templ::split_partition(
    uint32_t client_id, const snapshot_vector& snapshot,
    std::shared_ptr<partition_payload> partition, uint64_t row_split_point,
    uint32_t col_split_point, const partition_type::type& low_type,
    const partition_type::type&    high_type,
    const storage_tier_type::type& low_storage_type,
    const storage_tier_type::type& high_storage_type ) {

    DVLOG( 5 ) << "Split partition, client_id:" << client_id
               << ", partition:" << *partition
               << ", row_split_point:" << row_split_point
               << ", col_split_point:" << col_split_point
               << ", snapshot:" << snapshot;

    auto pid = partition->identifier_;
    auto lock_mode = partition_lock_mode::lock;

    if( (uint64_t) pid.partition_start == row_split_point ) {
        DVLOG( 40 ) << "Unable to split:" << pid
                    << ", at: row:" << row_split_point
                    << ", col:" << col_split_point;
        partition->unlock( lock_mode );
        return {};
    }
    if( (uint32_t) pid.column_start == col_split_point ) {
        DVLOG( 40 ) << "Unable to split:" << pid
                    << ", at: row:" << row_split_point
                    << ", col:" << col_split_point;
        partition->unlock( lock_mode );
        return {};
    }

    auto split_partitions = loc_table_->split_partition(
        partition, row_split_point, col_split_point, low_type, high_type,
        low_storage_type, high_storage_type, 0, 0, lock_mode );
    partition->unlock( lock_mode );
    if( split_partitions.size() != 2 ) {
        DVLOG( 40 ) << "Unable to split:" << pid
                    << ", at row:" << row_split_point
                    << ", col:" << col_split_point;
        return {};
    }
    DCHECK_EQ( 2, split_partitions.size() );

    snapshot_vector new_snapshot;

    if( enable_db ) {
        new_snapshot = std::get<1>( db_.split_partition(
            client_id, snapshot, pid, row_split_point, col_split_point,
            low_type, high_type, low_storage_type, high_storage_type,
            {prop_config_, prop_config_} ) );
    }

    unlock_payloads( split_partitions, lock_mode );

    return new_snapshot;
}

single_site_db_types snapshot_vector single_site_db_templ::merge_partition(
    uint32_t client_id, const snapshot_vector& snapshot,
    std::shared_ptr<partition_payload> low_partition,
    std::shared_ptr<partition_payload> high_partition,
    const partition_type::type&        merge_type,
    const storage_tier_type::type&     merge_storage_type ) {

    DVLOG( 5 ) << "Merge partition, client_id:" << client_id
               << ", low_partition:" << *low_partition
               << ", high_partition:" << *high_partition
               << ", snapshot:" << snapshot;

    auto low_pid = low_partition->identifier_;
    auto high_pid = high_partition->identifier_;

    auto lock_mode = partition_lock_mode::lock;

    auto merged_partition =
        loc_table_->merge_partition( low_partition, high_partition, merge_type,
                                     merge_storage_type, 0, lock_mode );
    low_partition->unlock( lock_mode );
    high_partition->unlock( lock_mode );
    if( !merged_partition ) {
        DVLOG( 40 ) << "Unable to merge:" << low_pid << ", " << high_pid;
        return {};
    }

    snapshot_vector new_snapshot;

    if( enable_db ) {
        new_snapshot = std::get<1>( db_.merge_partition(
            client_id, snapshot, low_pid, high_pid, merge_type,
            merge_storage_type, prop_config_ ) );
    }

    merged_partition->unlock( lock_mode );

    return new_snapshot;
}

single_site_db_types snapshot_vector single_site_db_templ::remaster_partitions(
    uint32_t client_id, const snapshot_vector& snapshot,
    std::vector<std::shared_ptr<partition_payload>>& partitions,
    uint32_t                                         new_master ) {

    DVLOG( 5 ) << "Remaster partitions, client_id:" << client_id
               << ", partitions:" << partitions
               << ", new_master:" << new_master;

    partition_column_identifier_set remaster_set;

    for( auto& payload : partitions ) {
        auto location_information = payload->get_location_information();
        auto new_location_information =
            std::make_shared<partition_location_information>();
        if( location_information ) {
            new_location_information
                ->set_from_existing_location_information_and_increment_version(
                    location_information );
        } else {
            new_location_information->version_ = 1;
        }
        location_information->master_location_ = new_master;
        payload->set_location_information( new_location_information );
        remaster_set.insert( payload->identifier_ );
    }

    partition_column_identifier_set empty_set;

    transaction_partition_holder* holder;
    if( enable_db ) {
        holder = db_.get_partition_holder( client_id, remaster_set, empty_set,
                                           empty_set,
                                           partition_lookup_operation::GET );
        DCHECK( holder );
    } else {
        holder = new transaction_partition_holder();
    }

    unlock_payloads( partitions, partition_lock_mode::lock );

    snapshot_vector commit_vv;
    if( enable_db ) {
        holder->begin_transaction( snapshot );
        holder->remaster_partitions( new_master );
        commit_vv = holder->commit_transaction();
        delete holder;
    }
    return commit_vv;
}

single_site_db_types db* single_site_db_templ::get_database() { return &db_; }

single_site_db_types std::shared_ptr<partition_data_location_table>
                     single_site_db_templ::get_data_location_table() {
    return loc_table_;
}

single_site_db_types partition_type::type
    single_site_db_templ::get_default_partition_type( uint32_t table_id ) {
    return loc_table_->get_partition_location_table()
        ->get_table_metadata( table_id )
        .default_partition_type_;
}

single_site_db_types storage_tier_type::type
    single_site_db_templ::get_default_storage_type( uint32_t table_id ) {
    return loc_table_->get_partition_location_table()
        ->get_table_metadata( table_id )
        .default_storage_type_;
}
