#include "ycsb_prep_stmts.h"

#include <glog/logging.h>

#include "../../../benchmark/ycsb/ycsb_benchmark_worker.h"
#include "../../../benchmark/ycsb/ycsb_configs.h"
#include "../../../common/perf_tracking.h"
#include "../sproc_helpers.h"

uint32_t k_ycsb_table_id = 0;  // by default it's the only table

sproc_result ycsb_create_database(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges>& write_ckrs,
    const std::vector<cell_key_ranges>& read_ckrs,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque ) {
    DCHECK_EQ( codes.size(), values.size() );
    DCHECK_GE( codes.size(), 1 );
    ycsb_configs configs;
    if( codes.size() == 1 or values.at( 1 ) == nullptr ) {
        configs = construct_ycsb_configs();
    } else {
        configs = *(ycsb_configs *) values.at( 1 );
    }
    db *database = (db *) values.at( 0 );

    DCHECK( sproc_opaque );
    ycsb_sproc_helper_holder *sproc_helper =
        (ycsb_sproc_helper_holder *) sproc_opaque;

    DVLOG( 5 ) << "Creating database for ycsb_benchmark from configs:"
               << configs;
    DVLOG( 5 ) << "Database:" << database;
    k_ycsb_table_id = database->get_tables()->create_table(
        create_ycsb_table_metadata( configs, database->get_site_location() ) );

    DCHECK_EQ( 0, k_ycsb_table_id );

    sproc_helper->init( database );

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    return res;
}

sproc_result ycsb_read_record( transaction_partition_holder *partition_holder,
                               const clientid                id,
                               const std::vector<cell_key_ranges> &write_ckrs,
                               const std::vector<cell_key_ranges> &read_ckrs,
                               std::vector<arg_code> &             codes,
                               std::vector<void *> &               values,
                               void *sproc_opaque ) {

    start_timer( SPROC_YCSB_READ_RECORD_TIMER_ID );

    DCHECK( sproc_opaque );
    ycsb_sproc_helper_holder *sproc_helper =
        (ycsb_sproc_helper_holder *) sproc_opaque;

    ycsb_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_worker_and_set_holder( id, partition_holder );

    check_args( k_ycsb_read_arg_codes, codes, values );

    for( const auto &ckr : read_ckrs ) {
        worker->do_ckr_read( ckr );
    }

    sproc_result ret;
    ret.status = exec_status_type::COMMAND_OK;

    stop_timer( SPROC_YCSB_READ_RECORD_TIMER_ID );

    return ret;
}

sproc_result ycsb_delete_record( transaction_partition_holder *partition_holder,
                                 const clientid                id,
                                 const std::vector<cell_key_ranges> &write_ckrs,
                                 const std::vector<cell_key_ranges> &read_ckrs,
                                 std::vector<arg_code> &             codes,
                                 std::vector<void *> &               values,
                                 void *sproc_opaque ) {

    start_timer( SPROC_YCSB_DELETE_RECORD_TIMER_ID );

    DCHECK( sproc_opaque );
    ycsb_sproc_helper_holder *sproc_helper =
        (ycsb_sproc_helper_holder *) sproc_opaque;

    ycsb_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_worker_and_set_holder( id, partition_holder );

    check_args( k_ycsb_delete_arg_codes, codes, values );

    for( const auto &ckr : write_ckrs ) {
        worker->do_delete_ckr( ckr );
    }

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;
    res.return_code = 0;

    stop_timer( SPROC_YCSB_DELETE_RECORD_TIMER_ID );

    return res;
}

sproc_result ycsb_update_record( transaction_partition_holder *partition_holder,
                                 const clientid                id,
                                 const std::vector<cell_key_ranges> &write_ckrs,
                                 const std::vector<cell_key_ranges> &read_ckrs,
                                 std::vector<arg_code> &             codes,
                                 std::vector<void *> &               values,
                                 void *sproc_opaque ) {
    start_timer( SPROC_YCSB_UPDATE_RECORD_TIMER_ID );

    DCHECK( sproc_opaque );
    ycsb_sproc_helper_holder *sproc_helper =
        (ycsb_sproc_helper_holder *) sproc_opaque;

    ycsb_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_worker_and_set_holder( id, partition_holder );

    check_args( k_ycsb_update_arg_codes, codes, values );

    for( const auto &ckr : write_ckrs ) {
        worker->do_ckr_write( ckr );
    }

    sproc_result ret;
    ret.status = exec_status_type::COMMAND_OK;

    stop_timer( SPROC_YCSB_UPDATE_RECORD_TIMER_ID );

    return ret;
}

sproc_result ycsb_rmw_record( transaction_partition_holder *partition_holder,
                              const clientid                id,
                              const std::vector<cell_key_ranges> &write_ckrs,
                              const std::vector<cell_key_ranges> &read_ckrs,
                              std::vector<arg_code> &             codes,
                              std::vector<void *> &               values,
                              void *sproc_opaque ) {
    start_timer( SPROC_YCSB_RMW_RECORD_TIMER_ID );

    DCHECK( sproc_opaque );
    ycsb_sproc_helper_holder *sproc_helper =
        (ycsb_sproc_helper_holder *) sproc_opaque;

    ycsb_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_worker_and_set_holder( id, partition_holder );

    check_args( k_ycsb_rmw_arg_codes, codes, values );

    worker->do_ckr_read_modify_write( write_ckrs, read_ckrs,
                                      false /* do prop*/ );

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    stop_timer( SPROC_YCSB_RMW_RECORD_TIMER_ID );

    return res;
}

sproc_result ycsb_insert_records(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges> &write_ckrs,
    const std::vector<cell_key_ranges> &read_ckrs, std::vector<arg_code> &codes,
    std::vector<void *> &values, void *sproc_opaque ) {
    start_timer( SPROC_YCSB_INSERT_RECORDS_TIMER_ID );

    check_args( k_ycsb_insert_arg_codes, codes, values );

    DCHECK( sproc_opaque );
    ycsb_sproc_helper_holder *sproc_helper =
        (ycsb_sproc_helper_holder *) sproc_opaque;

    ycsb_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_worker_and_set_holder( id, partition_holder );

    unsigned num_keys = codes.at( 0 ).array_length;
    DVLOG( 20 ) << "YCSB INSERT:" << num_keys << " keys";
    uint64_t *keys = (uint64_t *) values.at( 0 );

    for( unsigned pos = 0; pos < num_keys; pos++ ) {
        uint64_t key = keys[pos];
        worker->do_range_insert( key, key );
    }
    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    stop_timer( SPROC_YCSB_INSERT_RECORDS_TIMER_ID );

    return res;
}

sproc_result ycsb_ckr_insert( transaction_partition_holder *partition_holder,
                              const clientid                id,
                              const std::vector<cell_key_ranges> &write_ckrs,
                              const std::vector<cell_key_ranges> &read_ckrs,
                              std::vector<arg_code> &             codes,
                              std::vector<void *> &               values,
                              void *sproc_opaque ) {
    start_timer( SPROC_YCSB_CKR_INSERTS_TIMER_ID );

    check_args( k_ycsb_ckr_insert_arg_codes, codes, values );

    DCHECK( sproc_opaque );
    ycsb_sproc_helper_holder *sproc_helper =
        (ycsb_sproc_helper_holder *) sproc_opaque;

    ycsb_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_worker_and_set_holder( id, partition_holder );

    uint32_t num_ckrs = write_ckrs.size();


    DVLOG( 20 ) << "YCSB INSERT CKRS:" << num_ckrs << " ckrs";
    for( unsigned pos = 0; pos < num_ckrs; pos++ ) {
        worker->do_ckr_insert( write_ckrs.at( pos ) );
    }
    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    stop_timer( SPROC_YCSB_CKR_INSERTS_TIMER_ID );

    return res;
}

sproc_result ycsb_mk_rmw( transaction_partition_holder *      partition_holder,
                          const clientid                      id,
                          const std::vector<cell_key_ranges> &write_ckrs,
                          const std::vector<cell_key_ranges> &read_ckrs,
                          std::vector<arg_code> &             codes,
                          std::vector<void *> &values, void *sproc_opaque ) {

    start_timer( SPROC_YCSB_MK_RMW_TIMER_ID );

    check_args( k_ycsb_mk_rmw_arg_codes, codes, values );
    DCHECK( sproc_opaque );
    ycsb_sproc_helper_holder *sproc_helper =
        (ycsb_sproc_helper_holder *) sproc_opaque;

    ycsb_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_worker_and_set_holder( id, partition_holder );


    unsigned num_keys = codes.at( 0 ).array_length;

    DVLOG( 20 ) << "YCSB MK RMW:" << num_keys << " keys";
    bool      do_prop_update = *( (uint8_t *) values.at( 0 ) );

    worker->do_ckr_read_modify_write( write_ckrs, read_ckrs, do_prop_update );

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    stop_timer( SPROC_YCSB_MK_RMW_TIMER_ID );

    return res;
}

sproc_result ycsb_scan_records( transaction_partition_holder *partition_holder,
                                const clientid                id,
                                const std::vector<cell_key_ranges> &write_ckrs,
                                const std::vector<cell_key_ranges> &read_ckrs,
                                std::vector<arg_code> &             codes,
                                std::vector<void *> &               values,
                                void *sproc_opaque ) {
    start_timer( SPROC_YCSB_SCAN_RECORDS_TIMER_ID );

    check_args( k_ycsb_scan_arg_codes, codes, values );

    DCHECK( sproc_opaque );
    ycsb_sproc_helper_holder *sproc_helper =
        (ycsb_sproc_helper_holder *) sproc_opaque;

    ycsb_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_worker_and_set_holder( id, partition_holder );

    // Start and end points have the same length
    uint32_t col = *( (uint32_t *) values.at( 0 ) );
    unsigned num_chars = codes.at( 1 ).array_length;
    char *   chars = (char *) values.at( 1 );

    predicate_chain pred_chain;
    cell_predicate  c_pred;
    c_pred.table_id = 0;
    c_pred.col_id = col;
    c_pred.type = data_type::type::STRING;
    c_pred.data = std::string( chars, num_chars );
    c_pred.predicate = predicate_type::type::LESS_THAN_OR_EQUAL;
    predicate_chain pred;
    pred.and_predicates.emplace_back( c_pred );

    worker->do_ckr_scan( read_ckrs, pred );

    sproc_result s_res;
    s_res.status = exec_status_type::COMMAND_OK;

    stop_timer( SPROC_YCSB_SCAN_RECORDS_TIMER_ID );

    return s_res;
}

void ycsb_scan_data( transaction_partition_holder *     partition_holder,
                     const clientid                     id,
                     const std::vector<scan_arguments> &scan_args,
                     std::vector<arg_code> &codes, std::vector<void *> &values,
                     void *sproc_opaque, scan_result &scan_res ) {
    start_timer( SPROC_YCSB_SCAN_RECORDS_TIMER_ID );

    check_args( k_ycsb_scan_data_arg_codes, codes, values );

    DCHECK( sproc_opaque );
    ycsb_sproc_helper_holder *sproc_helper =
        (ycsb_sproc_helper_holder *) sproc_opaque;

    ycsb_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_worker_and_set_holder( id, partition_holder );

    for ( const auto& scan_arg :scan_args) {
        std::vector<result_tuple> res_tuples;
        scan_res.res_tuples.emplace( scan_arg.label, res_tuples );

        worker->do_ckr_scan( scan_arg.read_ckrs, scan_arg.predicate,
                             scan_res.res_tuples.at( scan_arg.label ) );
    }

    scan_res.status = exec_status_type::COMMAND_OK;

    stop_timer( SPROC_YCSB_SCAN_RECORDS_TIMER_ID );
}

