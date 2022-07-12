#pragma once

#include <climits>
#include <glog/logging.h>
#include <glog/stl_logging.h>

#include "../../common/perf_tracking.h"
#include "../../common/thread_utils.h"
#include "smallbank_db_operators.h"

smallbank_benchmark_worker_types
    smallbank_benchmark_worker_templ::smallbank_benchmark_worker(
        uint32_t client_id, db_abstraction* db, zipf_distribution_cdf* z_cdf,
        const workload_operation_selector& op_selector,
        const smallbank_configs&           configs,
        const db_abstraction_configs&      abstraction_configs )
    : db_operators_( client_id, db, abstraction_configs,
                     configs.bench_configs_.limit_update_propagation_,
                     false /* store global snapshot state */ ),
      generator_( z_cdf, op_selector, client_id, configs ),
      statistics_(),
      worker_( nullptr ),
      done_( false ) {
    statistics_.init( k_smallbank_workload_operations );
}

smallbank_benchmark_worker_types
    smallbank_benchmark_worker_templ::~smallbank_benchmark_worker() {}

smallbank_benchmark_worker_types void
    smallbank_benchmark_worker_templ::start_timed_workload() {
    worker_ = std::unique_ptr<std::thread>( new std::thread(
        &smallbank_benchmark_worker_templ::run_workload, this ) );
}

smallbank_benchmark_worker_types void
    smallbank_benchmark_worker_templ::stop_timed_workload() {
    done_ = true;
    DCHECK( worker_ );
    join_thread( *worker_ );
    worker_ = nullptr;
}

smallbank_benchmark_worker_types void
    smallbank_benchmark_worker_templ::run_workload() {
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
    std::chrono::duration<int, std::nano> elapsed = e - s;
    DVLOG( 10 ) << db_operators_.client_id_ << " ran for:" << elapsed.count()
                << " ns";
    statistics_.store_running_time( elapsed.count() );
}

smallbank_benchmark_worker_types void
    smallbank_benchmark_worker_templ::do_workload_operation() {

    workload_operation_enum op = generator_.get_operation();
    DCHECK_GE( op, SMALLBANK_AMALGAMATE );
    DCHECK_LE( op, SMALLBANK_WRITE_CHECK );

    double lat;
    DVLOG( 30 ) << db_operators_.client_id_ << " performing "
                << workload_operation_string( op );
    start_timer( SMALLBANK_WORKLOAD_OP_TIMER_ID );
    workload_operation_outcome_enum status = perform_workload_operation( op );
    stop_and_store_timer( SMALLBANK_WORKLOAD_OP_TIMER_ID, lat );
    statistics_.add_outcome( op, status, lat );

    DVLOG( 30 ) << db_operators_.client_id_ << " performing "
                << k_workload_operations_to_strings.at( op )
                << " status:" << status;
}

smallbank_benchmark_worker_types workload_operation_outcome_enum
                                 smallbank_benchmark_worker_templ::perform_workload_operation(
        const workload_operation_enum& op ) {
    switch( op ) {
        case SMALLBANK_AMALGAMATE:
            return do_amalgamate();
        case SMALLBANK_BALANCE:
            return do_balance();
        case SMALLBANK_DEPOSIT_CHECKING:
            return do_deposit_checking();
        case SMALLBANK_SEND_PAYMENT:
            return do_send_payment();
        case SMALLBANK_TRANSACT_SAVINGS:
            return do_transact_savings();
        case SMALLBANK_WRITE_CHECK:
            return do_write_check();
    }
    // should be unreachable
    return false;
}

smallbank_benchmark_worker_types inline benchmark_statistics
    smallbank_benchmark_worker_templ::get_statistics() const {
    return statistics_;
}

smallbank_benchmark_worker_types workload_operation_outcome_enum
                                 smallbank_benchmark_worker_templ::do_amalgamate() {
    generator_.generate_customer_ids( true );
    return perform_amalgamate( generator_.customer_ids_.at( 0 ),
                               generator_.customer_ids_.at( 1 ) );
}
smallbank_benchmark_worker_types workload_operation_outcome_enum
                                 smallbank_benchmark_worker_templ::do_balance() {
    generator_.generate_customer_ids( false );
    return perform_balance( generator_.customer_ids_.at( 0 ),
                            false /*as write*/ );
}
smallbank_benchmark_worker_types workload_operation_outcome_enum
                                 smallbank_benchmark_worker_templ::do_deposit_checking() {
    generator_.generate_customer_ids( false );
    return perform_deposit_checking(
        generator_.customer_ids_.at( 0 ),
        smallbank_account::PARAM_DEPOSIT_CHECKING_AMOUNT );
}
smallbank_benchmark_worker_types workload_operation_outcome_enum
                                 smallbank_benchmark_worker_templ::do_send_payment() {
    generator_.generate_customer_ids( true );
    return perform_send_payment( generator_.customer_ids_.at( 0 ),
                                 generator_.customer_ids_.at( 1 ),
                                 smallbank_account::PARAM_SEND_PAYMENT_AMOUNT );
}
smallbank_benchmark_worker_types workload_operation_outcome_enum
                                 smallbank_benchmark_worker_templ::do_transact_savings() {
    generator_.generate_customer_ids( false );
    return perform_transact_savings(
        generator_.customer_ids_.at( 0 ),
        smallbank_account::PARAM_TRANSACT_SAVINGS_AMOUNT );
}
smallbank_benchmark_worker_types workload_operation_outcome_enum
                                 smallbank_benchmark_worker_templ::do_write_check() {
    generator_.generate_customer_ids( false );
    return perform_deposit_checking(
        generator_.customer_ids_.at( 0 ),
        smallbank_account::PARAM_WRITE_CHECK_AMOUNT );
}

smallbank_benchmark_worker_types workload_operation_outcome_enum
                                 smallbank_benchmark_worker_templ::perform_amalgamate(
        uint64_t source_cust_id, uint64_t dest_cust_id ) {
    // get source savings balance
    // zero source's savings
    // add sources savings to destination checking if < uint32_t
    DVLOG( 20 ) << "Amalgamate (source_cust_id:" << source_cust_id
                << ", dest_cust_id:" << dest_cust_id << ")";

    cell_identifier source_saving_ck =
        create_cell_identifier( k_smallbank_savings_table_id,
                                smallbank_saving::balance, source_cust_id );

    cell_identifier dest_checking_ck =
        create_cell_identifier( k_smallbank_checkings_table_id,
                                smallbank_checking::balance, dest_cust_id );

    cell_key_ranges source_saving_ckr =
        cell_key_ranges_from_cell_identifier( source_saving_ck );
    cell_key_ranges dest_checking_ckr =
        cell_key_ranges_from_cell_identifier( dest_checking_ck );

    std::vector<cell_key_ranges> ckrs = {source_saving_ckr, dest_checking_ckr};

    workload_operation_outcome_enum outcome = WORKLOAD_OP_SUCCESS;
    if( do_begin_commit ) {
        db_operators_.begin_transaction( ckrs, ckrs );
        RETURN_AND_COMMIT_IF_SS_DB( outcome );
    }

    RETURN_IF_SS_DB( outcome );

    auto source_saving_read =
        db_operators_.lookup_latest_nullable_int64( source_saving_ck );
    auto dest_checking_read =
        db_operators_.lookup_latest_nullable_int64( dest_checking_ck );

    if( ( !std::get<0>( source_saving_read ) ) or
        ( !std::get<0>( dest_checking_read ) ) ) {
        outcome = WORKLOAD_OP_FAILURE;
    } else if( !add_balance_valid( std::get<1>( source_saving_read ),
                                   std::get<1>( dest_checking_read ) ) ) {
        outcome = WORKLOAD_OP_EXPECTED_ABORT;
    } else {
        // zero out the balance
        int32_t source_saving_written = 0;
        int32_t dest_checking_written;
        dest_checking_written = std::get<1>( source_saving_read ) +
                                std::get<1>( dest_checking_read );

        db_operators_.write_int64( source_saving_ck, source_saving_written );
        db_operators_.write_int64( dest_checking_ck, dest_checking_written );
    }

    if( do_begin_commit ) {
        if( outcome == WORKLOAD_OP_SUCCESS ) {
            db_operators_.commit_transaction();
        } else {
            db_operators_.abort_transaction();
        }
    }

    DVLOG( 20 ) << "Amalgamate (source_cust_id:" << source_cust_id
                << ", dest_cust_id:" << dest_cust_id
                << "), outcome=" << outcome;
    return outcome;
}

smallbank_benchmark_worker_types workload_operation_outcome_enum
    smallbank_benchmark_worker_templ::perform_balance( uint64_t cust_id,
                                                       bool     as_read ) {
    // source checking balance - amount should be >= 0
    // return sum checkking and saving
    DVLOG( 20 ) << "Balance (cust_id:" << cust_id << ")";

    cell_identifier saving_ck = create_cell_identifier(
        k_smallbank_savings_table_id, smallbank_saving::balance, cust_id );

    cell_identifier checking_ck = create_cell_identifier(
        k_smallbank_checkings_table_id, smallbank_checking::balance, cust_id );

    std::vector<cell_key_ranges> ckrs = {
        cell_key_ranges_from_cell_identifier( saving_ck ),
        cell_key_ranges_from_cell_identifier( checking_ck )};
    workload_operation_outcome_enum outcome = WORKLOAD_OP_SUCCESS;

    if( do_begin_commit ) {
        std::vector<cell_key_ranges> empty_ckrs;
        if( as_read ) {
            db_operators_.begin_transaction( empty_ckrs, ckrs );
        } else {
            db_operators_.begin_transaction( ckrs, ckrs );
        }
        RETURN_AND_COMMIT_IF_SS_DB( outcome );
    }

    RETURN_IF_SS_DB( outcome );

    std::tuple<bool, int64_t> saving = std::make_tuple<>( false, 0 );
    std::tuple<bool, int64_t> checking = std::make_tuple<>( false, 0 );
    if( as_read ) {
        saving = db_operators_.lookup_nullable_int64( saving_ck );
        checking = db_operators_.lookup_nullable_int64( checking_ck );
    } else {
        saving = db_operators_.lookup_latest_nullable_int64( saving_ck );
        checking = db_operators_.lookup_latest_nullable_int64( checking_ck );
    }

    int ret = 0;
    if( ( !std::get<0>( saving ) ) or ( !std::get<0>( checking ) ) ) {
        outcome = WORKLOAD_OP_EXPECTED_ABORT;
        DVLOG( 0 ) << "null read, db_operators_.state_:"
                   << db_operators_.state_;

    } else {
        ret = std::get<1>( saving ) + std::get<1>( checking );
        if( ret < 0 ) {
            outcome = WORKLOAD_OP_EXPECTED_ABORT;
        }
    }

    if( do_begin_commit ) {
        if( outcome == WORKLOAD_OP_SUCCESS ) {
            db_operators_.commit_transaction();
        } else {
            db_operators_.abort_transaction();
        }
    }

    DVLOG( 20 ) << "Balance (cust_id:" << cust_id << "), outcome=" << outcome;

    return outcome;
}

smallbank_benchmark_worker_types workload_operation_outcome_enum
                                 smallbank_benchmark_worker_templ::perform_send_payment(
        uint64_t source_cust_id, uint64_t dest_cust_id, int amount ) {
    // source checking balance - amount should be >= 0
    // add amount to dest checking balance if less than uint32_t max

    DVLOG( 20 ) << "SendPayment (source_cust_id:" << source_cust_id
                << ", dest_cust_id:" << dest_cust_id << ", amount:" << amount
                << ")";

    workload_operation_outcome_enum outcome = WORKLOAD_OP_SUCCESS;

    cell_identifier source_ck =
        create_cell_identifier( k_smallbank_savings_table_id,
                                smallbank_saving::balance, source_cust_id );

    cell_identifier dest_ck =
        create_cell_identifier( k_smallbank_checkings_table_id,
                                smallbank_checking::balance, dest_cust_id );

    std::vector<cell_key_ranges> ckrs = {
        cell_key_ranges_from_cell_identifier( source_ck ),
        cell_key_ranges_from_cell_identifier( dest_ck )};

    if( do_begin_commit ) {
        db_operators_.begin_transaction( ckrs, ckrs );
        RETURN_AND_COMMIT_IF_SS_DB( outcome );
    }
    RETURN_IF_SS_DB( outcome );

    auto source_read = db_operators_.lookup_latest_nullable_int64( source_ck );
    auto dest_read = db_operators_.lookup_latest_nullable_int64( dest_ck );

    if( ( !std::get<0>( source_read ) ) or ( !std::get<0>( dest_read ) ) ) {
        outcome = WORKLOAD_OP_FAILURE;

    } else if( ( !add_balance_valid( std::get<1>( dest_read ), amount ) ) or
               ( !sub_balance_valid( std::get<1>( source_read ), amount ) ) ) {
        outcome = WORKLOAD_OP_EXPECTED_ABORT;
    } else {
        int64_t source_written;
        int64_t dest_written;

        // zero out the balance
        source_written = std::get<1>( source_read ) - amount;
        dest_written = std::get<1>( dest_read ) + amount;

        db_operators_.write_int64( source_ck, source_written );
        db_operators_.write_int64( dest_ck, dest_written );
    }

    if( do_begin_commit ) {
        if( outcome == WORKLOAD_OP_SUCCESS ) {
            db_operators_.commit_transaction();
        } else {
            db_operators_.abort_transaction();
        }
    }

    DVLOG( 20 ) << "SendPayment (source_cust_id:" << source_cust_id
                << ", dest_cust_id:" << dest_cust_id << ", amount:" << amount
                << "), outcome=" << outcome;

    return outcome;
}

smallbank_benchmark_worker_types template <
    class S, bool ( *check_op_fxn )( int a, int b )>
workload_operation_outcome_enum
    smallbank_benchmark_worker_templ::perform_rmw_on_table(
        uint32_t table_id, uint64_t cust_id, int amount, uint32_t multiplier,
        const std::string& name ) {
    DVLOG( 20 ) << name << "( cust_id:" << cust_id << ", table_id:" << table_id
                << ", amount:" << amount << " )";
    workload_operation_outcome_enum outcome = WORKLOAD_OP_SUCCESS;

    cell_identifier ck;
    ck.table_id_ = table_id;
    ck.key_ = cust_id;
    ck.col_id_ = S::balance;

    cell_key_ranges ckr = cell_key_ranges_from_cell_identifier( ck );
    std::vector<cell_key_ranges> ckrs = {ckr};

    if( do_begin_commit ) {
        db_operators_.begin_transaction( ckrs, ckrs );
        RETURN_AND_COMMIT_IF_SS_DB( outcome );
    }
    RETURN_IF_SS_DB( outcome );

    auto found = db_operators_.lookup_latest_nullable_int64( ck );

    if( !std::get<0>( found ) ) {
        outcome = WORKLOAD_OP_FAILURE;

    } else if( !( *check_op_fxn )( std::get<1>( found ), amount ) ) {
        outcome = WORKLOAD_OP_EXPECTED_ABORT;
    } else {
        int64_t new_balance = std::get<1>( found ) + ( multiplier * amount );
        db_operators_.write_int64( ck, new_balance );
    }

    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }

    DVLOG( 20 ) << name << "( cust_id:" << cust_id << ", table_id:" << table_id
                << ", amount:" << amount << " ), outcome=" << outcome;
    return outcome;
}

smallbank_benchmark_worker_types workload_operation_outcome_enum
                                 smallbank_benchmark_worker_templ::perform_deposit_checking(
        uint64_t cust_id, int amount ) {

    // add amount to checkings if the value is valid (>0 or < uint32_t max)
    workload_operation_outcome_enum outcome =
        perform_rmw_on_table<smallbank_checking, add_balance_valid>(
            k_smallbank_checkings_table_id, cust_id, amount, 1,
            "DepositChecking" );

    return outcome;
}

smallbank_benchmark_worker_types workload_operation_outcome_enum
                                 smallbank_benchmark_worker_templ::perform_transact_savings(
        uint64_t cust_id, int amount ) {
    // add amount to savings if the value is valid (>0 or < uint32_t max)
    workload_operation_outcome_enum outcome =
        perform_rmw_on_table<smallbank_saving, add_balance_valid>(
            k_smallbank_savings_table_id, cust_id, amount, 1,
            "TransactSavings" );

    return outcome;
}

smallbank_benchmark_worker_types workload_operation_outcome_enum
    smallbank_benchmark_worker_templ::perform_write_check( uint64_t cust_id,
                                                           int      amount ) {
    // subtract from checking if valid (>0 or < uint32_t max)
    workload_operation_outcome_enum outcome =
        perform_rmw_on_table<smallbank_saving, sub_balance_valid>(
            k_smallbank_savings_table_id, cust_id, amount, -1, "WriteCheck" );

    return outcome;
}

smallbank_benchmark_worker_types void
    smallbank_benchmark_worker_templ::set_transaction_partition_holder(
        transaction_partition_holder* holder ) {
    db_operators_.set_transaction_partition_holder( holder );
}

smallbank_benchmark_worker_types template <class S>
std::tuple<bool, int> smallbank_benchmark_worker_templ::get_balance(
    uint32_t table, uint64_t cust_id ) {

    DVLOG( 20 ) << "GetBalance( cust_id:" << cust_id << ", table:" << table
                << ")";

    cell_identifier ck = create_cell_identifier( table, S::balance, cust_id );

    auto found = db_operators_.lookup_nullable_int64( ck );

    DVLOG( 20 ) << "GetBalance( cust_id:" << cust_id << ", table:" << table
                << "), got balance?:" << std::get<0>( found )
                << ", balance:" << std::get<1>( found );

    return found;
}

smallbank_benchmark_worker_types std::tuple<bool, int>
    smallbank_benchmark_worker_templ::get_account_balance( uint32_t table_id,
                                                           uint64_t cust_id ) {
    if( table_id == k_smallbank_savings_table_id ) {
        return get_balance<smallbank_saving>( k_smallbank_savings_table_id,
                                              cust_id );
    } else if( table_id == k_smallbank_checkings_table_id ) {
        return get_balance<smallbank_checking>( k_smallbank_checkings_table_id,
                                                cust_id );
    }
    return std::make_tuple<>( false, 0 );
}

smallbank_benchmark_worker_types bool
    smallbank_benchmark_worker_templ::distributed_charge_account(
        uint32_t table_id, uint64_t cust_id, int amount, bool is_debit ) {

    workload_operation_outcome_enum outcome = WORKLOAD_OP_FAILURE;

    if( table_id == k_smallbank_savings_table_id ) {
        if( is_debit ) {
            outcome = perform_rmw_on_table<smallbank_saving, add_balance_valid>(
                k_smallbank_savings_table_id, cust_id, amount, 1,
                "DistributedCharge" );
        } else {
            outcome = perform_rmw_on_table<smallbank_saving, sub_balance_valid>(
                k_smallbank_savings_table_id, cust_id, amount, -1,
                "DistributedCharge" );
        }
    } else if( table_id == k_smallbank_checkings_table_id ) {
        if( is_debit ) {
            outcome =
                perform_rmw_on_table<smallbank_checking, add_balance_valid>(
                    k_smallbank_checkings_table_id, cust_id, amount, 1,
                    "DistributedCharge" );
        } else {
            outcome =
                perform_rmw_on_table<smallbank_checking, sub_balance_valid>(
                    k_smallbank_checkings_table_id, cust_id, amount, -1,
                    "DistributedCharge" );
        }
    }

    bool ret = false;
    if( outcome == WORKLOAD_OP_SUCCESS ) {
        ret = true;
    }
    return ret;
}

