#include "db_abstraction.h"

#include <glog/logging.h>

#include "../../common/partition_funcs.h"
#include "../../gen-cpp/gen-cpp/dynamic_mastering_types.h"

plain_db_wrapper::plain_db_wrapper()
    : db_( nullptr ), own_( false ), prop_config_() {}
plain_db_wrapper::~plain_db_wrapper() {
    if( own_ ) {
        delete db_;
        own_ = false;
    }
}

void plain_db_wrapper::init(
    std::shared_ptr<update_destination_generator> update_gen,
    std::shared_ptr<update_enqueuers>             enqueuers,
    const tables_metadata&                        t_meta ) {
    db_ = new db();
    own_ = true;
    db_->init( update_gen, enqueuers, t_meta );

    prop_config_ = update_gen->get_propagation_configurations().at( 0 );
}
void plain_db_wrapper::init( db* db ) { db_ = db; }

uint32_t plain_db_wrapper::create_table( const table_metadata& metadata ) {
  auto ret = db_->get_tables()->create_table( metadata );
  return ret;
}

transaction_partition_holder* plain_db_wrapper::get_partitions_with_begin(
    uint32_t client_id, const snapshot_vector& begin_timestamp,
    std::vector<cell_key_ranges>& write_keys,
    std::vector<cell_key_ranges>& read_keys, bool allow_missing ) {
    auto write_pids = generate_partition_column_identifiers( write_keys );
    auto read_pids = generate_partition_column_identifiers( read_keys );
    partition_column_identifier_set inflight_pids;
    return get_partitions_with_begin( client_id, begin_timestamp, write_pids,
                                      read_pids, inflight_pids, allow_missing );
}

transaction_partition_holder* plain_db_wrapper::get_partitions_with_begin(
    uint32_t client_id, const snapshot_vector& begin_timestamp,
    const partition_column_identifier_set& write_pids,
    const partition_column_identifier_set& read_pids,
    const partition_column_identifier_set& inflight_pids, bool allow_missing ) {
    partition_lookup_operation lookup_op =
        partition_lookup_operation::GET_OR_CREATE;
    if( allow_missing ) {
        lookup_op = partition_lookup_operation::GET_ALLOW_MISSING;
    }
    auto ret =
        db_->get_partitions_with_begin( client_id, begin_timestamp, write_pids,
                                        read_pids, inflight_pids, lookup_op );
    DCHECK( ret );
    return ret;
}

void plain_db_wrapper::change_partition_types(
        uint32_t                                          client_id,
        const std::vector<::partition_column_identifier>& pids,
        const std::vector<partition_type::type>&          part_types,
        const std::vector<storage_tier_type::type>&       storage_types ) {
    db_->change_partition_types( client_id, pids, part_types, storage_types );
}

void plain_db_wrapper::add_partition( uint32_t client_id,
                                      const partition_column_identifier& pid,
                                      uint32_t master_location,
                                      const partition_type::type&    p_type,
                                      const storage_tier_type::type& s_type ) {
    return db_->add_partition( client_id, pid, master_location, p_type,
                               s_type );
}

void plain_db_wrapper::remove_partition( uint32_t           client_id,
                                         const cell_key& ck ) {

    return db_->remove_partition( client_id,
                                  generate_partition_column_identifier( ck ) );
}

snapshot_vector plain_db_wrapper::merge_partition(
    uint32_t client_id, const snapshot_vector& snapshot,
    const cell_key& merge_point, bool merge_vertically ) {
    cell_key low_ck = merge_point;
    if( merge_vertically ) {
        DCHECK_GT( low_ck.col_id, 0 );
        low_ck.col_id = low_ck.col_id - 1;
    } else {
        DCHECK_GT( low_ck.row_id, 0 );
        low_ck.row_id = low_ck.row_id - 1;
    }

    auto low_pid = generate_partition_column_identifier( low_ck );
    auto high_pid = generate_partition_column_identifier( merge_point );

    return std::get<1>(
        db_->merge_partition( client_id, snapshot, low_pid, high_pid,
                              db_->get_tables()
                                  ->get_table( low_pid.table_id )
                                  ->get_metadata()
                                  .default_partition_type_,
                              db_->get_tables()
                                  ->get_table( low_pid.table_id )
                                  ->get_metadata()
                                  .default_storage_type_,
                              prop_config_ ) );
}

snapshot_vector plain_db_wrapper::split_partition(
    uint32_t client_id, const snapshot_vector& snapshot,
    const cell_key& split_point, bool split_vertically ) {
    auto pid = generate_partition_column_identifier( split_point );

    uint64_t row_split_point = split_point.row_id;
    uint32_t col_split_point = split_point.col_id;
    if( split_vertically ) {
        row_split_point = k_unassigned_key;
    } else {
        col_split_point = k_unassigned_col;
    }

    auto part_type = db_->get_tables()
                         ->get_table( pid.table_id )
                         ->get_metadata()
                         .default_partition_type_;
    auto storage_type = db_->get_tables()
                            ->get_table( pid.table_id )
                            ->get_metadata()
                            .default_storage_type_;

    return std::get<1>( db_->split_partition(
        client_id, snapshot, pid, row_split_point, col_split_point, part_type,
        part_type, storage_type, storage_type, {prop_config_, prop_config_} ) );
}

snapshot_vector plain_db_wrapper::remaster_partitions(
    uint32_t client_id, const snapshot_vector& snapshot,
    std::vector<cell_key_ranges>& ckrs, uint32_t new_master ) {
    auto pids = generate_partition_column_identifiers( ckrs );
    std::vector<partition_column_identifier> pids_vec;
    for( const auto& pid : pids ) {
        pids_vec.push_back( pid );
    }

    auto ret = db_->remaster_partitions(
        client_id, snapshot, pids_vec, {}, new_master, false,
        partition_lookup_operation::GET_OR_CREATE );
    DCHECK( std::get<0>( ret ) );
    return std::get<1>( ret );
}

uint32_t plain_db_wrapper::get_site_location() {
    return db_->get_site_location();
}

partition_column_identifier_set
    plain_db_wrapper::generate_partition_column_identifiers(
        std::vector<cell_key>& cks ) {
    partition_column_identifier_set pids;
    if( cks.size() == 0 ) {
        return pids;
    }

    std::sort( cks.begin(), cks.end() );

    uint32_t             table_id = cks.at( 0 ).table_id;
    uint64_t                    partition_size = get_partition_size( table_id );
    uint32_t                    col_size = get_column_size( table_id );
    partition_column_identifier last_pid = generate_partition_column_identifier(
        cks.at( 0 ), partition_size, col_size );

    pids.emplace( last_pid );

    for( uint32_t pos = 1; pos < cks.size(); pos++ ) {
        auto& ck = cks.at( pos );
        if( !is_cid_within_pcid( ck, last_pid ) ) {
            if( ck.table_id != last_pid.table_id ) {
                table_id = ck.table_id;
                partition_size = get_partition_size( table_id );
                col_size = get_column_size( table_id );
            }
            last_pid = generate_partition_column_identifier( ck, partition_size,
                                                             col_size );
            pids.emplace( last_pid );
        }
    }

    return pids;
}

partition_column_identifier_set
    plain_db_wrapper::generate_partition_column_identifiers(
        std::vector<cell_key_ranges>& cks ) {
    partition_column_identifier_set pids;
    if( cks.size() == 0 ) {
        return pids;
    }

    std::sort( cks.begin(), cks.end() );

    uint32_t                    table_id = cks.at( 0 ).table_id;
    uint64_t                    partition_size = get_partition_size( table_id );
    uint32_t                    col_size = get_column_size( table_id );
    partition_column_identifier_set last_pids =
        generate_partition_column_identifier( cks.at( 0 ), partition_size,
                                              col_size );
    partition_column_identifier last_pid = *( last_pids.begin() );

    pids.insert( last_pids.begin(), last_pids.end() );

    for( uint32_t pos = 1; pos < cks.size(); pos++ ) {
        auto& ck = cks.at( pos );
        if( !is_ckr_fully_within_pcid( ck, last_pid ) ) {
            if( ck.table_id != last_pid.table_id ) {
                table_id = ck.table_id;
                partition_size = get_partition_size( table_id );
                col_size = get_column_size( table_id );
            }
            last_pids = generate_partition_column_identifier(
                ck, partition_size, col_size );
            pids.insert( last_pids.begin(), last_pids.end() );
            last_pid = *( last_pids.begin() );
        }
    }

    return pids;
}


partition_column_identifier plain_db_wrapper::generate_partition_column_identifier(
    const cell_key& ck ) {
    return generate_partition_column_identifier(
        ck, get_partition_size( ck.table_id ), get_column_size( ck.table_id ) );
}
uint64_t plain_db_wrapper::get_partition_size( uint32_t table_id ) {
    return db_->get_tables()
        ->get_table( table_id )
        ->get_metadata()
        .default_partition_size_;
}
uint32_t plain_db_wrapper::get_column_size( uint32_t table_id ) {

    auto ret = db_->get_tables()
                   ->get_table( table_id )
                   ->get_metadata()
                   .default_column_size_;
    return ret;
}

partition_column_identifier
    plain_db_wrapper::generate_partition_column_identifier(
        const cell_key& ck, uint64_t partition_size, uint32_t col_size ) {
    DVLOG( 40 ) << "Generate partition column identifier for ck:" << ck
                << ", partition size:" << partition_size
                << ", col_size:" << col_size;
    DCHECK_GT( partition_size, 0 );
    DCHECK_GT( col_size, 0 );
    uint64_t partition_start = partition_size * ( ck.row_id / partition_size );
    uint64_t partition_end = partition_start + partition_size - 1;
    uint32_t col_start = col_size * ( ck.col_id / col_size );
    uint32_t col_end =
        std::min( col_start + col_size - 1, db_->get_tables()
                                                    ->get_table( ck.table_id )
                                                    ->get_metadata()
                                                    .num_columns_ -
                                                1 );

    return create_partition_column_identifier(
        ck.table_id, partition_start, partition_end, col_start, col_end );
}

partition_column_identifier_set
    plain_db_wrapper::generate_partition_column_identifier(
        const cell_key_ranges& ckr, uint64_t partition_size,
        uint32_t col_size ) {
    partition_column_identifier_set pids;
    cell_key                        ck;
    ck.table_id = ckr.table_id;
    ck.row_id = ckr.row_id_start;
    ck.col_id = ckr.col_id_start;

    while( ck.row_id <= ckr.row_id_end ) {
        auto pid = generate_partition_column_identifier( ck, partition_size,
                                                         col_size );
        pids.insert( pid );
        ck.col_id = pid.column_end + 1;
        while( ck.col_id <= ckr.col_id_end ) {
            auto pid = generate_partition_column_identifier( ck, partition_size,
                                                             col_size );
            pids.insert( pid );

            ck.col_id = pid.column_end + 1;
        }
        ck.row_id = pid.partition_end + 1;
        ck.col_id = ckr.col_id_start;
    }

    return pids;
}

partition_type::type plain_db_wrapper::get_default_partition_type(
    uint32_t table_id ) {
    return db_->get_tables()
        ->get_table( table_id )
        ->get_metadata()
        .default_partition_type_;
}
storage_tier_type::type plain_db_wrapper::get_default_storage_type(
    uint32_t table_id ) {
    return db_->get_tables()
        ->get_table( table_id )
        ->get_metadata()
        .default_storage_type_;
}


db_abstraction_configs construct_db_abstraction_configs(
    const db_abstraction_type& type ) {
    db_abstraction_configs configs;
    configs.db_type_ = type;
    return configs;
}

db_abstraction* create_db_abstraction(
    const db_abstraction_configs& configs) {
    DVLOG( 1 ) << "Created DB abstraction: db_type:"
               << db_abstraction_type_string( configs.db_type_ );
    switch( configs.db_type_ ) {
        case SINGLE_SITE_DB:
            return new single_site_db_wrapper();
        case PLAIN_DB:
            return new plain_db_wrapper();
        case SS_DB:
            return new ss_db_wrapper();
        case UNKNOWN_DB:
            LOG( WARNING ) << "Trying to run unknown database type";
            return nullptr;
    }

    return nullptr;
}
