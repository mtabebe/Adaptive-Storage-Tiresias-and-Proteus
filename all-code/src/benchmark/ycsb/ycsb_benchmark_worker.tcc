#pragma once

#include <glog/logging.h>
#include <glog/stl_logging.h>

#include "../../common/thread_utils.h"
#include "../benchmark_interface.h"

ycsb_benchmark_worker_types ycsb_benchmark_worker_templ::ycsb_benchmark_worker(
    uint32_t client_id, uint32_t table_id, db_abstraction* db,
    zipf_distribution_cdf*             z_cdf,
    const workload_operation_selector& op_selector, const ycsb_configs& configs,
    const db_abstraction_configs& abstraction_configs )
    : table_id_( table_id ),
      db_operators_( client_id, db, abstraction_configs,
                     configs.bench_configs_.limit_update_propagation_,
                     false /* store global snapshot state */ ),
      generator_( z_cdf, op_selector, configs, table_id ),
      statistics_(),
      worker_( nullptr ),
      done_( false ) {
    statistics_.init( k_ycsb_workload_operations );
}

ycsb_benchmark_worker_types
    ycsb_benchmark_worker_templ::~ycsb_benchmark_worker() {}

ycsb_benchmark_worker_types void
    ycsb_benchmark_worker_templ::start_timed_workload() {
    worker_ = std::unique_ptr<std::thread>(
        new std::thread( &ycsb_benchmark_worker_templ::run_workload, this ) );
}

ycsb_benchmark_worker_types void
    ycsb_benchmark_worker_templ::stop_timed_workload() {
    done_ = true;
    DCHECK( worker_ );
    join_thread( *worker_ );
    worker_ = nullptr;
}

ycsb_benchmark_worker_types void ycsb_benchmark_worker_templ::add_partitions(
    uint64_t start, uint64_t end ) {
    auto add_set =
        generate_partition_set( start, end, 0, k_ycsb_num_columns - 1 );
    db_operators_.add_partitions( add_set );
}

ycsb_benchmark_worker_types void ycsb_benchmark_worker_templ::run_workload() {
    // time me boys
    DVLOG( 10 ) << db_operators_.client_id_ << " starting workload";
    std::chrono::high_resolution_clock::time_point s =
        std::chrono::high_resolution_clock::now();

    // this will only every be true once so it's likely to not be done

    while( likely( !done_ ) ) {
        // we don't want to check this very often so we do a bunch of operations
        // in a loop
        for( uint32_t op_count = 0;
             op_count < generator_.get_num_ops_before_timer_check();
             op_count++ ) {
            do_workload_operation();
        }
    }
    std::chrono::high_resolution_clock::time_point e =
        std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::nano> elapsed = e - s;
    DVLOG( 10 ) << db_operators_.client_id_ << " ran for:" << elapsed.count()
                << " ns";
    statistics_.store_running_time( elapsed.count() );
}

ycsb_benchmark_worker_types void
    ycsb_benchmark_worker_templ::do_workload_operation() {

    double                  lat;
    workload_operation_enum op = generator_.get_operation();
    //    DCHECK_GE( op, YCSB_WRITE );
    //    DCHECK_LE( op, YCSB_MULTI_RMW );

    DVLOG( 30 ) << db_operators_.client_id_ << " performing "
                << workload_operation_string( op );
    start_timer( YCSB_WORKLOAD_OP_TIMER_ID );
    workload_operation_outcome_enum status = perform_workload_operation( op );
    stop_and_store_timer( YCSB_WORKLOAD_OP_TIMER_ID, lat );

    statistics_.add_outcome( op, status, lat );

    DVLOG( 30 ) << db_operators_.client_id_ << " performing "
                << k_workload_operations_to_strings.at( op )
                << " status:" << status;
}

ycsb_benchmark_worker_types workload_operation_outcome_enum
                            ycsb_benchmark_worker_templ::perform_workload_operation(
        const workload_operation_enum& op ) {
    uint64_t key = generator_.get_key();
    switch( op ) {
        case YCSB_WRITE:
            return do_write( key );
        case YCSB_READ:
            return do_read( key );
        case YCSB_RMW:
            return do_read_modify_write(
                key, generator_.get_rmw_bool() + ycsb_row::field0 );
        case YCSB_SCAN:
            return do_scan( key, generator_.get_scan_bool() + ycsb_row::field0 );
        case YCSB_MULTI_RMW:
            return do_multi_key_read_modify_write(
                key, generator_.get_rmw_bool() + ycsb_row::field0 );
        case DB_SPLIT:
            return do_db_split( key, generator_.get_column(),
                                generator_.get_bool() );
        case DB_MERGE:
            return do_db_merge( key, generator_.get_column(),
                                generator_.get_bool() );
        case DB_REMASTER:
            return do_db_remaster( key, generator_.get_column() );
    }
    // should be unreachable
    return WORKLOAD_OP_FAILURE;
}

ycsb_benchmark_worker_types inline workload_operation_outcome_enum
    ycsb_benchmark_worker_templ::do_write( uint64_t key ) {
    DVLOG( 10 ) << db_operators_.client_id_ << " do write :" << key;

    cell_identifier cid =
        create_cell_identifier( table_id_, ycsb_row::id, key );

    auto ckr = cell_key_ranges_from_cell_identifier( cid );
    ckr.col_id_end = ycsb_row::field1;


	return do_ckr_write(ckr);
}
ycsb_benchmark_worker_types workload_operation_outcome_enum
    ycsb_benchmark_worker_templ::do_ckr_write( const cell_key_ranges& ckr ) {
    check_ycsb_ckr( ckr );

    std::vector<cell_key_ranges> write_set = {ckr};
    std::vector<cell_key_ranges> read_set;


    if( do_begin_commit ) {
        db_operators_.begin_transaction( write_set, read_set );
        RETURN_AND_COMMIT_IF_SS_DB( WORKLOAD_OP_SUCCESS );
    }
    RETURN_IF_SS_DB( WORKLOAD_OP_SUCCESS );

	int64_t time =  generator_.get_current_time();

    cell_identifier cid =
        create_cell_identifier( table_id_, ckr.col_id_start, ckr.row_id_start );

    for( int64_t row = ckr.row_id_start; row <= ckr.row_id_end; row++ ) {
        cid.key_ = row;
        for( int32_t col = ckr.col_id_start; col <= ckr.col_id_end; col++ ) {
            cid.col_id_ = col;
            if( col == ycsb_row::id ) {
                db_operators_.write_uint64( cid, row );
            } else if( col == ycsb_row::timestamp ) {
                db_operators_.write_int64( cid, time );
            } else {
                db_operators_.write_string( cid, generator_.get_value() );
            }
        }
    }

    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }

    return WORKLOAD_OP_SUCCESS;
}

ycsb_benchmark_worker_types inline workload_operation_outcome_enum
    ycsb_benchmark_worker_templ::do_delete( uint64_t key ) {
    DVLOG( 10 ) << db_operators_.client_id_ << " do delete :" << key;

    cell_identifier cid =
        create_cell_identifier( table_id_, ycsb_row::id, key );

    auto ckr = cell_key_ranges_from_cell_identifier( cid );
    ckr.col_id_end = ycsb_row::field1;

    return do_delete_ckr( ckr );
}
ycsb_benchmark_worker_types workload_operation_outcome_enum
    ycsb_benchmark_worker_templ::do_delete_ckr( const cell_key_ranges& ckr ) {

    check_ycsb_ckr( ckr );

    if( do_begin_commit ) {
        std::vector<cell_key_ranges> write_set = {ckr};
        std::vector<cell_key_ranges> read_set;

        db_operators_.begin_transaction( write_set, read_set );
        RETURN_AND_COMMIT_IF_SS_DB( WORKLOAD_OP_SUCCESS );
    }

    RETURN_IF_SS_DB( WORKLOAD_OP_SUCCESS );

    cell_identifier cid =
        create_cell_identifier( table_id_, ckr.col_id_start, ckr.row_id_start );

    for( int64_t row = ckr.row_id_start; row <= ckr.row_id_end; row++ ) {
        cid.key_ = row;
        for( int32_t id = ckr.col_id_start; id <= ckr.col_id_end; id++ ) {
            cid.col_id_ = id;
            db_operators_.remove_data( cid );
        }
    }

    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }

    return WORKLOAD_OP_SUCCESS;
}

ycsb_benchmark_worker_types workload_operation_outcome_enum
                            ycsb_benchmark_worker_templ::change_partition_types(
        uint64_t start, uint64_t end, const partition_type::type& part_type,
        const storage_tier_type::type& storage_type ) {
    DVLOG( 10 ) << db_operators_.client_id_ << " do change partition types :["
                << start << ", " << end << ", partition type:" << part_type
                << ", storage type:" << storage_type << "]";

    RETURN_IF_SS_DB( WORKLOAD_OP_SUCCESS );

    auto change_set =
        generate_partition_set( start, end, 0, k_ycsb_num_columns - 1 );
    std::vector<partition_type::type> part_types( change_set.size(),
                                                  part_type );
    std::vector<storage_tier_type::type> storage_types( change_set.size(),
                                                        storage_type );

    db_operators_.change_partition_types( change_set, part_types,
                                          storage_types );

    return WORKLOAD_OP_SUCCESS;
}

ycsb_benchmark_worker_types workload_operation_outcome_enum
    ycsb_benchmark_worker_templ::do_range_insert( uint64_t start,
                                                  uint64_t end ) {
    DVLOG( 10 ) << db_operators_.client_id_ << " do insert :[" << start << ", "
                << end << "]";

    cell_identifier cid =
        create_cell_identifier( table_id_, ycsb_row::id, start );

    if( do_begin_commit ) {
        auto ckr = cell_key_ranges_from_cell_identifier( cid );
        ckr.col_id_end = ycsb_row::field1;
        ckr.row_id_end = end;

        std::vector<cell_key_ranges> write_set = {ckr};
        std::vector<cell_key_ranges> read_set;

        DVLOG( 10 ) << db_operators_.client_id_
                    << " calling begin:" << write_set;
        db_operators_.begin_transaction( write_set, read_set );
        DVLOG( 10 ) << db_operators_.client_id_ << " begin okay!";

        RETURN_AND_COMMIT_IF_SS_DB( WORKLOAD_OP_SUCCESS );
    }
    RETURN_IF_SS_DB( WORKLOAD_OP_SUCCESS );

    int64_t     insert_time = generator_.get_current_time();
    std::string field_1_value = generator_.get_value();
    std::string field_2_value = generator_.get_value();
    for( uint64_t key = start; key <= end; key++ ) {
        cid.key_ = key;

        cid.col_id_ = ycsb_row::id;
        db_operators_.insert_uint64( cid, key );
        cid.col_id_ = ycsb_row::timestamp;
        db_operators_.insert_int64( cid, insert_time );
        cid.col_id_ = ycsb_row::field0;
        db_operators_.insert_string( cid, field_1_value );
        cid.col_id_ = ycsb_row::field1;
        db_operators_.insert_string( cid, field_2_value );
    }
    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }
    return WORKLOAD_OP_SUCCESS;
}

ycsb_benchmark_worker_types workload_operation_outcome_enum
    ycsb_benchmark_worker_templ::do_ckr_insert( const cell_key_ranges& ckr ) {
    DVLOG( 10 ) << db_operators_.client_id_ << " do ckr insert [ ckr:" << ckr
                << "]";

    check_ycsb_ckr( ckr );

    if( do_begin_commit ) {

        std::vector<cell_key_ranges> write_set = {ckr};
        std::vector<cell_key_ranges> read_set;

        DVLOG( 10 ) << db_operators_.client_id_
                    << " calling begin:" << write_set;
        db_operators_.begin_transaction( write_set, read_set );
        DVLOG( 10 ) << db_operators_.client_id_ << " begin okay!";

        RETURN_AND_COMMIT_IF_SS_DB( WORKLOAD_OP_SUCCESS );
    }
    RETURN_IF_SS_DB( WORKLOAD_OP_SUCCESS );

    int64_t     insert_time = generator_.get_current_time();
    std::string field_1_value = generator_.get_value();
    std::string field_2_value = generator_.get_value();

    cell_identifier cid =
        create_cell_identifier( table_id_, ckr.col_id_start, ckr.row_id_start );

    for( int32_t col = ckr.col_id_start; col <= ckr.col_id_end; col++ ) {
        cid.col_id_ = col;
        for( int64_t row = ckr.row_id_start; row <= ckr.row_id_end; row++ ) {
            cid.key_ = row;
            if( col == ycsb_row::id ) {
                db_operators_.insert_uint64( cid, row );
            } else if( col == ycsb_row::timestamp ) {
                db_operators_.insert_int64( cid, insert_time );
            } else if( col == ycsb_row::field0 )  {
                db_operators_.insert_string( cid, field_1_value );
            } else if( col == ycsb_row::field1 ) {
                db_operators_.insert_string( cid, field_2_value );
            }
        }
    }

    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }
    return WORKLOAD_OP_SUCCESS;
}

ycsb_benchmark_worker_types workload_operation_outcome_enum
    ycsb_benchmark_worker_templ::do_db_split( uint64_t key, uint32_t col,
                                              bool split_vertically ) {
    cell_identifier cid = create_cell_identifier( table_id_, col, key );
    if( split_vertically ) {
        cid.col_id_ = std::max( cid.col_id_, (uint32_t) 1 );
    } else {
        cid.key_ = std::max( cid.key_, (uint64_t) 1 );
    }

    RETURN_IF_PLAIN_DB( WORKLOAD_OP_SUCCESS );
    db_operators_.split_partition( cid, split_vertically );
    return WORKLOAD_OP_SUCCESS;
}

ycsb_benchmark_worker_types workload_operation_outcome_enum
    ycsb_benchmark_worker_templ::do_db_merge( uint64_t key, uint32_t col,
                                              bool merge_vertically ) {
    cell_identifier cid = create_cell_identifier( table_id_, col, key );
    if( merge_vertically ) {
        cid.col_id_ = std::max( cid.col_id_, (uint32_t) 1 );
    } else {
        cid.key_ = std::max( cid.key_, (uint64_t) 1 );
    }
    RETURN_IF_PLAIN_DB( WORKLOAD_OP_SUCCESS );
    db_operators_.merge_partition( cid, merge_vertically );
    return WORKLOAD_OP_SUCCESS;
}

ycsb_benchmark_worker_types workload_operation_outcome_enum
    ycsb_benchmark_worker_templ::do_db_remaster( uint64_t key, uint32_t col ) {
    cell_identifier cid = create_cell_identifier( table_id_, col, key );
    RETURN_IF_PLAIN_DB( WORKLOAD_OP_SUCCESS );
    db_operators_.remaster_partition( cid, db_operators_.get_site_location() );
    return WORKLOAD_OP_SUCCESS;
}

ycsb_benchmark_worker_types workload_operation_enum
    ycsb_benchmark_worker_templ::do_ckr_read( const cell_key_ranges& ckr ) {
    check_ycsb_ckr( ckr );

    std::vector<cell_key_ranges>    ckrs = {ckr};
    workload_operation_outcome_enum outcome = WORKLOAD_OP_SUCCESS;

    if( do_begin_commit ) {
        std::vector<cell_key_ranges> empty_ckrs;
        db_operators_.begin_transaction( empty_ckrs, ckrs );
        RETURN_AND_COMMIT_IF_SS_DB( outcome );
    }

    RETURN_IF_SS_DB( outcome );

    cell_identifier cid =
        create_cell_identifier( table_id_, ckr.col_id_start, ckr.row_id_start );

    for( int64_t row = ckr.row_id_start; row <= ckr.row_id_end; row++ ) {
        cid.key_ = row;
        for( int32_t col = ckr.col_id_start; col <= ckr.col_id_end; col++ ) {
            cid.col_id_ = col;

            if( col == ycsb_row::id ) {
                auto read_id = db_operators_.lookup_nullable_uint64( cid );
                if( std::get<0>( read_id ) ) {
                    if( std::get<1>( read_id ) != (uint64_t) row ) {
                        outcome = WORKLOAD_OP_FAILURE;
                    }
                }
            } else if( col == ycsb_row::timestamp ) {
                auto read_time = db_operators_.lookup_nullable_int64( cid );
                if( std::get<0>( read_time ) ) {
                    if( std::get<1>( read_time ) < 0 ) {
                        outcome = WORKLOAD_OP_FAILURE;
                    }
                }
            } else {
                auto read_str = db_operators_.lookup_nullable_string( cid );
                if( std::get<0>( read_str ) ) {
                    if( std::get<1>( read_str ).size() !=
                        generator_.get_value_size() ) {
                        outcome = WORKLOAD_OP_FAILURE;
                    }
                }
            }
        }
    }

    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }

    return outcome;
}

ycsb_benchmark_worker_types workload_operation_outcome_enum
                            ycsb_benchmark_worker_templ::do_internal_scan(
        uint64_t start, uint64_t end, uint32_t scan_col,
        const predicate_chain& pred ) {
    cell_identifier cid = create_cell_identifier( table_id_, scan_col, start );
    auto            ckr = cell_key_ranges_from_cell_identifier( cid );
    ckr.row_id_end = end;
    std::vector<cell_key_ranges> ckrs = {ckr};
    return do_ckr_scan( ckrs, pred );
}

ycsb_benchmark_worker_types workload_operation_outcome_enum
                            ycsb_benchmark_worker_templ::do_ckr_scan(
        const std::vector<cell_key_ranges>& ckrs,
        const predicate_chain&              pred ) {
    std::vector<result_tuple> result;
    auto                      ret = do_ckr_scan( ckrs, pred, result );
    return ret;
}

ycsb_benchmark_worker_types workload_operation_outcome_enum
                            ycsb_benchmark_worker_templ::do_ckr_scan(
        const std::vector<cell_key_ranges>& ckrs, const predicate_chain& pred,
        std::vector<result_tuple>& result ) {
    if( do_begin_commit ) {
        std::vector<cell_key_ranges> read_ckrs( ckrs.begin(), ckrs.end() );

        db_operators_.begin_scan_transaction( read_ckrs );
        RETURN_AND_COMMIT_IF_SS_DB( WORKLOAD_OP_SUCCESS );
    }
    RETURN_IF_SS_DB( WORKLOAD_OP_SUCCESS );

    for( const auto& ckr : ckrs ) {
        check_ycsb_ckr( ckr );
        DCHECK_GE( ckr.col_id_start, ycsb_row::field0 );
        DCHECK_LE( ckr.col_id_end, ycsb_row::field1 );
        DCHECK_EQ( ckr.col_id_start, ckr.col_id_end );

        std::vector<uint32_t> project_cols;
        for( int32_t col = ckr.col_id_start; col <= ckr.col_id_end; col++ ) {
            project_cols.emplace_back( col );
        }
        db_operators_.scan( table_id_, ckr.row_id_start, ckr.row_id_end,
                            project_cols, pred, result );
        if( !generator_.store_scan_results() ) {
            result.clear();
        }
    }

    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }
    return WORKLOAD_OP_SUCCESS;
}

ycsb_benchmark_worker_types workload_operation_outcome_enum
                            ycsb_benchmark_worker_templ::do_internal_multi_key_read_modify_write(
        const std::vector<uint64_t>& keys, uint32_t field_col ) {
    return do_internal_multi_key_read_modify_write( keys.data(), keys.size(),
                                                    field_col );
}

ycsb_benchmark_worker_types workload_operation_outcome_enum
                            ycsb_benchmark_worker_templ::do_internal_multi_key_read_modify_write(
        const uint64_t* keys, uint32_t num_keys, uint32_t field_col ) {
    cell_identifier field_cid =
        create_cell_identifier( table_id_, field_col, 0 );
    cell_identifier time_cid =
        create_cell_identifier( table_id_, ycsb_row::timestamp, 0 );

    std::vector<cell_key_ranges> ckrs;
    for( uint32_t pos = 0; pos < num_keys; pos++ ) {
        uint64_t key = keys[pos];
        field_cid.key_ = key;
        time_cid.key_ = key;
        ckrs.push_back( cell_key_ranges_from_cell_identifier( field_cid ) );
        ckrs.push_back( cell_key_ranges_from_cell_identifier( time_cid ) );
    }

    return do_ckr_read_modify_write( ckrs, ckrs, true );
}
ycsb_benchmark_worker_types workload_operation_outcome_enum
                            ycsb_benchmark_worker_templ::do_ckr_read_modify_write(
        const std::vector<cell_key_ranges>& writeCKRs,
        const std::vector<cell_key_ranges>& readCKRs, bool do_prop ) {
    if( do_begin_commit ) {
        std::vector<cell_key_ranges> writeCKRSet( writeCKRs.begin(),
                                                  writeCKRs.end() );
        std::vector<cell_key_ranges> readCKRSet( readCKRs.begin(),
                                                 readCKRs.end() );
        db_operators_.begin_transaction( writeCKRSet, readCKRSet );
        RETURN_AND_COMMIT_IF_SS_DB( WORKLOAD_OP_SUCCESS );
    }
    RETURN_IF_SS_DB( WORKLOAD_OP_SUCCESS );

    int64_t update_time = generator_.get_current_time();
    auto    value = generator_.get_value();

    workload_operation_outcome_enum outcome = WORKLOAD_OP_SUCCESS;

    cell_identifier cid = create_cell_identifier( table_id_, 0, 0 );
    for( const auto& ckr : writeCKRs ) {
        check_ycsb_ckr( ckr );
        DCHECK_GE( ckr.col_id_start, ycsb_row::timestamp );
        DCHECK_LE( ckr.col_id_end, ycsb_row::field1 );

        for( int64_t row = ckr.row_id_start; row <= ckr.row_id_end; row++ ) {
            cid.key_ = row;
            for( int32_t col = ckr.col_id_start; col <= ckr.col_id_end;
                 col++ ) {
                cid.col_id_ = col;

                if( ( col == ycsb_row::field1 ) or
                    ( col == ycsb_row::field0 ) ) {

                    auto read_str =
                        db_operators_.lookup_latest_nullable_string( cid );
                    if( std::get<0>( read_str ) ) {
                        if( std::get<1>( read_str ).size() !=
                            generator_.get_value_size() ) {
                            outcome = WORKLOAD_OP_FAILURE;
                        }
                    }
                    db_operators_.write_string( cid, value );
                } else if( col == ycsb_row::timestamp ) {
                    db_operators_.write_int64( cid, update_time );
                }
            }
        }
    }

    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }

    return outcome;
}

ycsb_benchmark_worker_types void
    ycsb_benchmark_worker_templ::set_transaction_partition_holder(
        transaction_partition_holder* holder ) {
    db_operators_.set_transaction_partition_holder( holder );
}

// we don't maintain sessions here instead rely on the fact that this is local

ycsb_benchmark_worker_types inline workload_operation_outcome_enum
    ycsb_benchmark_worker_templ::do_read( uint64_t key ) {
    DVLOG( 10 ) << db_operators_.client_id_ << " do read :" << key;

    cell_identifier cid =
        create_cell_identifier( table_id_, ycsb_row::id, key );
    auto            ckr = cell_key_ranges_from_cell_identifier( cid );
    ckr.col_id_end = ycsb_row::field1;

    return do_ckr_read( ckr );
}

ycsb_benchmark_worker_types inline workload_operation_outcome_enum
    ycsb_benchmark_worker_templ::do_read_modify_write( uint64_t key,
                                                       uint32_t col ) {
    DVLOG( 10 ) << db_operators_.client_id_ << " do RMW :" << key;
    std::vector<uint64_t> keys = {key};
    return do_internal_multi_key_read_modify_write( keys, col );
}

ycsb_benchmark_worker_types inline workload_operation_outcome_enum
    ycsb_benchmark_worker_templ::do_scan( uint64_t start, uint32_t scan_col ) {
    uint64_t end = generator_.get_end_key( start );
    DVLOG( 10 ) << db_operators_.client_id_ << " do scan :[" << start << ", "
                << end << ", scan column:" << scan_col << "]";
    return do_internal_scan( start, end, scan_col,
                             generator_.get_scan_predicate( scan_col ) );
}

ycsb_benchmark_worker_types inline workload_operation_outcome_enum
    ycsb_benchmark_worker_templ::do_multi_key_read_modify_write(
        uint64_t start, uint32_t col ) {
    uint64_t end = generator_.get_end_key( start );
    std::vector<uint64_t> keys;
    for( uint64_t key = start; key < end; key++ ) {
        keys.emplace_back( key );
    }
    DVLOG( 10 ) << db_operators_.client_id_ << " do multi-key rmw :[" << start
                << ", " << end << "]";
    return do_internal_multi_key_read_modify_write( keys, col );
}

ycsb_benchmark_worker_types inline benchmark_statistics
    ycsb_benchmark_worker_templ::get_statistics() const {
    return statistics_;
}

ycsb_benchmark_worker_types inline std::vector<partition_column_identifier>
    ycsb_benchmark_worker_templ::generate_partition_set(
        uint64_t row_start, uint64_t row_end, uint32_t col_start,
        uint32_t col_end ) const {
    // give you the partitions from [start, end] (inclusive end)
    uint64_t r_start = row_start;
    uint32_t c_start = col_start;

    cell_identifier ck;
    ck.table_id_ = 0;
    ck.key_ = 0;
    ck.col_id_ = 0;

    std::vector<partition_column_identifier> pids;

    while( r_start <= row_end ) {
        ck.key_ = r_start;
        while( c_start <= col_end ) {
            ck.col_id_ = c_start;
            auto pid = generator_.generate_partition_column_identifier( ck );

            r_start = pid.partition_end + 1;
            c_start = pid.column_end + 1;

            pids.push_back( pid );
        }
        c_start = 0;
    }

    return pids;
}

ycsb_benchmark_worker_types inline void
    ycsb_benchmark_worker_templ::check_ycsb_ckr(
        const cell_key_ranges& ckr ) const {
    DCHECK_EQ( ckr.table_id, table_id_ );
    DCHECK_LE( ckr.col_id_start, ckr.col_id_end );
    DCHECK_LE( ckr.row_id_start, ckr.row_id_end );
    DCHECK_LE( ycsb_row::id, ckr.col_id_start );
    DCHECK_GE( ycsb_row::field1, ckr.col_id_end );
}
