#pragma once

#include <glog/logging.h>

#include "../../common/string_utils.h"
#include "../benchmark_interface.h"
#include "smallbank_db_operators.h"
#include "smallbank_record_types.h"
#include "smallbank_table_ids.h"

smallbank_loader_types smallbank_loader_templ::smallbank_loader(
    db_abstraction* db, const smallbank_configs& configs,
    const db_abstraction_configs& abstraction_configs, uint32_t client_id )
    : db_operators_( client_id, db, abstraction_configs,
                     configs.bench_configs_.limit_update_propagation_,
                     false /* store global snapshot state */ ),
      configs_( configs ),
      generator_( nullptr /*no need for zipf*/, client_id, configs ),
      dist_( nullptr /* no need for zipf*/ ) {}

smallbank_loader_types smallbank_loader_templ::~smallbank_loader() {}

smallbank_loader_types void smallbank_loader_templ::load_account(
    uint64_t account_number ) {

    load_account_with_args( account_number, generator_.dist_.write_uniform_nstr(
                                                smallbank_account::NAME_LEN,
                                                smallbank_account::NAME_LEN ) );
}

smallbank_loader_types void smallbank_loader_templ::load_account_with_args(
    uint64_t account_number, const std::string& name ) {
    DVLOG( 20 ) << "Load account:" << account_number;

    cell_identifier cid =
        create_cell_identifier( k_smallbank_accounts_table_id,
                                smallbank_account::cust_id, account_number );

    db_operators_.insert_uint64( cid, account_number );
    cid.col_id_ = smallbank_account::name;
    db_operators_.insert_string( cid, name );
    DVLOG( 20 ) << "Load account:" << account_number << ", okay!";
}

smallbank_loader_types void smallbank_loader_templ::load_saving(
    uint64_t account_number ) {
    load_saving_with_args(
        account_number,
        (int) generator_.dist_.get_uniform_int(
            smallbank_account::MIN_BALANCE, smallbank_account::MAX_BALANCE ) );
}
smallbank_loader_types void smallbank_loader_templ::load_saving_with_args(
    uint64_t account_number, int balance ) {
    DVLOG( 20 ) << "Load saving:" << account_number;

    cell_identifier cid =
        create_cell_identifier( k_smallbank_savings_table_id,
                                smallbank_saving::cust_id, account_number );

    db_operators_.insert_uint64( cid, account_number );
    cid.col_id_ = smallbank_saving::balance;
    db_operators_.insert_int64( cid, balance );

    DVLOG( 20 ) << "Load saving:" << account_number << ", okay!";
}

smallbank_loader_types void smallbank_loader_templ::load_checking(
    uint64_t account_number ) {
    load_checking_with_args(
        account_number,
        (int) generator_.dist_.get_uniform_int(
            smallbank_account::MIN_BALANCE, smallbank_account::MAX_BALANCE ) );
}
smallbank_loader_types void smallbank_loader_templ::load_checking_with_args(
    uint64_t account_number, int balance ) {
    DVLOG( 20 ) << "Load checking:" << account_number;

    cell_identifier cid =
        create_cell_identifier( k_smallbank_checkings_table_id,
                                smallbank_checking::cust_id, account_number );

    db_operators_.insert_uint64( cid, account_number );
    cid.col_id_ = smallbank_checking::balance;
    db_operators_.insert_int64( cid, balance );

    DVLOG( 20 ) << "Load checking:" << account_number << ", okay!";
}

smallbank_loader_types template <
    void ( smallbank_loader_templ::*load_op )( uint64_t account_number )>
void smallbank_loader_templ::do_range_load_helper( uint32_t           table_id,
                                                   const std::string& name,
                                                   uint64_t start, uint64_t end,
                                                   uint32_t num_columns ) {
    DVLOG( 20 ) << "Loading " << name << " range:" << start << " to " << end;
    cell_key_ranges ckr;
    ckr.table_id = table_id;
    ckr.row_id_start = start;
    ckr.row_id_end = end;
    ckr.col_id_start = 0;
    ckr.col_id_end = num_columns - 1;

    std::vector<cell_key_ranges> ckrs = {ckr};
    if( do_begin_commit ) {
        std::vector<cell_key_ranges> empty_ckrs;
        db_operators_.begin_transaction( ckrs, empty_ckrs );
    }

    RETURN_VOID_AND_COMMIT_IF_SS_DB();

    for( uint64_t row_id = start; row_id <= end; row_id++ ) {
        ( this->*load_op )( row_id );
    }

    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }

    DVLOG( 20 ) << "Loading " << name << " range:" << start << " to " << end
                << " okay!";
}

smallbank_loader_types void smallbank_loader_templ::add_partitions(
    uint32_t table_id, uint64_t account_start, uint64_t account_end,
    uint32_t num_columns ) {

    std::vector<partition_column_identifier> pids =
        generate_partition_column_identifiers( table_id, account_start,
                                               account_end, num_columns );
    db_operators_.add_partitions( pids );
}

smallbank_loader_types std::vector<partition_column_identifier>
                       smallbank_loader_templ::generate_partition_column_identifiers(
        uint32_t table_id, uint64_t account_start, uint64_t account_end,
        uint32_t num_columns ) {
    std::vector<partition_column_identifier> pids;
    uint64_t row_start = account_start;
    uint32_t col_start = 0;

    while( row_start <= account_end ) {
      uint64_t cur_start = row_start;
        while( col_start < num_columns ) {
            auto pid = generate_partition_column_identifier(
                table_id, cur_start, col_start, num_columns );

            row_start = pid.partition_end + 1;
            col_start = pid.column_end + 1;

            pids.push_back( pid );
        }
        col_start = 0;
    }

    return pids;
}

smallbank_loader_types partition_column_identifier
                       smallbank_loader_templ::generate_partition_column_identifier(
        uint32_t table_id, uint64_t account, uint32_t col_start,
        uint32_t num_columns ) {
    uint64_t p_start =
        ( account / configs_.partition_size_ ) * configs_.partition_size_;
    uint64_t p_end = p_start + ( configs_.partition_size_ - 1 );

    uint32_t col_part_size = configs_.accounts_col_size_;
    if( table_id != k_smallbank_accounts_table_id ) {
        col_part_size = configs_.banking_col_size_;
    }
    uint32_t c_start = ( col_start / col_part_size ) * col_part_size;
    uint32_t c_end = c_start + ( col_part_size - 1 );
    c_end = std::min( c_end, num_columns - 1 );

    return create_partition_column_identifier( table_id, p_start, p_end,
                                               c_start, c_end );
}

smallbank_loader_types void smallbank_loader_templ::add_partition_ranges(
    uint64_t account_start, uint64_t account_end ) {
    add_partitions( k_smallbank_accounts_table_id, account_start, account_end,
                    k_smallbank_accounts_num_columns );
    add_partitions( k_smallbank_savings_table_id, account_start, account_end,
                    k_smallbank_savings_num_columns );
    add_partitions( k_smallbank_checkings_table_id, account_start, account_end,
                    k_smallbank_checkings_num_columns );
}

smallbank_loader_types void smallbank_loader_templ::do_range_load_accounts(
    uint64_t account_start, uint64_t account_end ) {

    do_range_load_helper<&smallbank_loader_templ::load_account>(
        k_smallbank_accounts_table_id, std::string( "account" ), account_start,
        account_end, k_smallbank_accounts_num_columns );
}
smallbank_loader_types void smallbank_loader_templ::do_range_load_savings(
    uint64_t account_start, uint64_t account_end ) {
    do_range_load_helper<&smallbank_loader_templ::load_saving>(
        k_smallbank_savings_table_id, std::string( "savings" ), account_start,
        account_end, k_smallbank_savings_num_columns );
}
smallbank_loader_types void smallbank_loader_templ::do_range_load_checkings(
    uint64_t account_start, uint64_t account_end ) {
    do_range_load_helper<&smallbank_loader_templ::load_checking>(
        k_smallbank_checkings_table_id, std::string( "checkings" ),
        account_start, account_end, k_smallbank_checkings_num_columns );
}

smallbank_loader_types void smallbank_loader_templ::do_load_assorted_accounts(
    uint64_t* account_numbers, uint32_t num_accounts ) {
    for( uint32_t pos = 0; pos < num_accounts; pos++ ) {
        load_account( account_numbers[pos] );
    }
}
smallbank_loader_types void smallbank_loader_templ::do_load_assorted_savings(
    uint64_t* account_numbers, uint32_t num_accounts ) {
    for( uint32_t pos = 0; pos < num_accounts; pos++ ) {
        load_saving( account_numbers[pos] );
    }
}
smallbank_loader_types void smallbank_loader_templ::do_load_assorted_checkings(
    uint64_t* account_numbers, uint32_t num_accounts ) {
    for( uint32_t pos = 0; pos < num_accounts; pos++ ) {
        load_checking( account_numbers[pos] );
    }
}

smallbank_loader_types
void smallbank_loader_templ::set_transaction_partition_holder(
        transaction_partition_holder* holder ) {
    db_operators_.set_transaction_partition_holder( holder );
}
