#define GTEST_HAS_TR1_TUPLE 0

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <chrono>

#include "../src/data-site/stored-procedures/stored_procedures_executor.h"
#include "../src/data-site/stored-procedures/ycsb/ycsb_prep_stmts.h"

class ycsb_scan_test : public ::testing::Test {};

partition_column_identifier_set get_pids( uint32_t start, uint32_t end,
                                          uint32_t part_size ) {

    partition_column_identifier pid =
        create_partition_column_identifier( 0, start, end, 0, 0 );

    partition_column_identifier_set pids;
    uint32_t pid_start = start;
    while( pid_start < end) {

        pid.partition_start = ( ( pid_start / part_size ) * part_size );
        pid.partition_end = pid.partition_start + ( part_size - 1 );

        pids.insert( pid );

        pid_start += part_size;
    }

    return pids;
}

void add_data( db* database, distributions* dist, uint32_t data_size,
               uint32_t part_size, uint32_t start, uint32_t end ) {
  partition_column_identifier_set empty_pids;
  snapshot_vector                 snap;

  DVLOG( 10 ) << "Add data:" << start << ", " << end;

  partition_column_identifier_set pids = get_pids( start, end, part_size );

  transaction_partition_holder* holder = database->get_partitions_with_begin(
      0, snap, pids, empty_pids, empty_pids,
      partition_lookup_operation::GET_OR_CREATE );

  DCHECK( holder );

  cell_identifier cid = create_cell_identifier( 0, 0, start );

  for( uint32_t i = start; i <= end; i++) {
      cid.key_ = i;
      std::string data = dist->write_uniform_str( data_size, data_size );
      holder->insert_string_data( cid, data );
  }

  holder->commit_transaction();

}

db* load_db( distributions* dist, uint32_t num_data_items,
             const partition_type::type&    part_type,
             const storage_tier_type::type& storage_type, uint32_t data_size,
             uint32_t part_size, bool enable_sec_storage,
             const std::string& sec_storage_dir ) {
    db* database = new db();

    uint32_t site_loc = 1;
    uint32_t num_tables = 1;
    uint32_t num_clients = 10;
    uint32_t gc_sleep_time = 10;

    database->init( make_no_op_update_destination_generator(),
                    make_update_enqueuers(),
                    create_tables_metadata( num_tables, site_loc, num_clients,
                                            gc_sleep_time, enable_sec_storage,
                                            sec_storage_dir ) );
    std::vector<cell_data_type> col_types = {cell_data_type::STRING};

    table_metadata t_meta = create_table_metadata(
        "ycsb", 0, 1, col_types, k_num_records_in_chain,
        k_num_records_in_snapshot_chain, site_loc, part_size, 1, part_size, 1,
        part_type, storage_type, enable_sec_storage, sec_storage_dir );

    database->get_tables()->create_table( t_meta );

    uint32_t start = 0;
    while( start < num_data_items ) {
        add_data( database, dist, data_size, part_size, start,
                  start + part_size - 1 );
        start += part_size;
    }

    if( storage_type != storage_tier_type::type::MEMORY ) {
        auto pids = get_pids( 0, num_data_items - 1, part_size );
        for( const auto& pid : pids ) {
            std::vector<partition_column_identifier> pid_vec = {pid};
            std::vector<storage_tier_type::type> storage_vec = {storage_type};
            std::vector<partition_type::type>    part_vec = {part_type};
            database->change_partition_types( 0, pid_vec, part_vec,
                                              storage_vec );
        }
    }

    DVLOG( 5 ) << "Done loading db";

    return database;
}
double perform_scan( db* database, distributions* dist, uint32_t num_data_items,
                     uint32_t part_size, const predicate_chain& pred,
                     uint32_t scan_size ) {

    uint32_t start_key = dist->get_uniform_int( 0, num_data_items - scan_size );
    uint32_t end_key = std::min( start_key + scan_size, num_data_items - 1 );

    partition_column_identifier_set empty_pids;
    std::vector<uint32_t>           project_cols = {0};

    partition_column_identifier_set pids =
        get_pids( start_key, end_key, part_size );
    snapshot_vector                 snap;
    for ( const auto& pid : pids) {
        set_snapshot_version( snap, pid, 1 );
    }

    transaction_partition_holder* holder = database->get_partitions_with_begin(
        0, snap, empty_pids, pids, empty_pids,
        partition_lookup_operation::GET );

    DCHECK( holder );

    std::chrono::high_resolution_clock::time_point time_start =
        std::chrono::high_resolution_clock::now();

    holder->scan( 0, start_key, end_key, project_cols, pred );

    std::chrono::high_resolution_clock::time_point time_end =
        std::chrono::high_resolution_clock::now();

    holder->commit_transaction();

    std::chrono::duration<double, std::nano> elapsed = time_end - time_start;
    double elapsed_count = elapsed.count();
    return elapsed_count;
}

TEST_F( ycsb_scan_test, scanner ) {

    std::vector<partition_type::type> part_types = {
        partition_type::type::ROW, partition_type::type::COLUMN,
        partition_type::type::SORTED_COLUMN, partition_type::type::MULTI_COLUMN,
        partition_type::type::SORTED_MULTI_COLUMN};
    std::vector<storage_tier_type::type> storage_types = {
        storage_tier_type::type::MEMORY, storage_tier_type::type::DISK};
    std::vector<uint32_t> data_sizes = {1, 10, 100, 1000};
    std::vector<uint32_t> part_sizes = { 10, 100, 1000, 10000};
    std::map<double, std::string> scan_selectivities = {
        {1.0, "z"}, {0.1, "E"}, {0.01, "AZ"}, {0.5, "Z"}, {0.05, "C"}};
    std::vector<uint32_t> scan_sizes = {
        1, 5, 10, 50, 100, 500, 1000};
    bool enable_sec_storage = true;

    uint32_t num_iters = 500;
    uint32_t num_data_items = 50000;

// #if 0
    data_sizes = { 5 };
    part_sizes = { 100 };
    scan_sizes = { 50 };
    scan_selectivities = {{1.0, "z"}};
    num_iters = 50;
    num_data_items = 1000;
    storage_types = {storage_tier_type::type::MEMORY,
                     storage_tier_type::type::DISK};
    // #endif


    DVLOG( 5 ) << "SCAN TIMER HEADER: Partition type, data size, partition "
                  "size, scan selectivity, scan size, timer";

    distributions dist( nullptr );

    for( const auto& part_type : part_types ) {
        for( const auto& data_size : data_sizes ) {
            for( const auto& part_size : part_sizes ) {
                for( const auto& store_type : storage_types ) {
                    std::string sec_storage_dir =
                        "/tmp/" +
                        std::to_string( (uint64_t) std::time( nullptr ) ) +
                        "-" + std::to_string( rand() % 1000 ) + "-sec-storage";

                    db* database = load_db(
                        &dist, num_data_items, part_type, store_type, data_size,
                        part_size, enable_sec_storage, sec_storage_dir );
                    for( const auto& scan_selectivity_entry :
                         scan_selectivities ) {

                        predicate_chain pred_chain;
                        cell_predicate  c_pred;
                        c_pred.table_id = 0;
                        c_pred.col_id = 0;
                        c_pred.type = data_type::type::STRING;
                        c_pred.data = scan_selectivity_entry.second;
                        c_pred.predicate =
                            predicate_type::type::LESS_THAN_OR_EQUAL;
                        predicate_chain pred;
                        pred.and_predicates.emplace_back( c_pred );

                        for( const auto& scan_size : scan_sizes ) {
                            for( uint32_t i = 0; i < num_iters; i++ ) {
                                double time_spent = perform_scan(
                                    database, &dist, num_data_items, part_size,
                                    pred, scan_size );
                                DVLOG( 5 )
                                    << "SCAN TIMER: " << part_type << ", "
                                    << data_size << ", " << part_size << ", "
                                    << scan_selectivity_entry.first << ", "
                                    << scan_size << ", " << time_spent;
                            }
                        }
                    }

                    delete database;
                }
            }
        }
    }
}
