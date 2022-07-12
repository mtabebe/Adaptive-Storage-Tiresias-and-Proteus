#include "smallbank_prep_stmts.h"

#include <glog/logging.h>
#include <glog/stl_logging.h>

#include "../sproc_helpers.h"

void serialize_smallbank_result( sproc_result &               res,
                                 std::vector<void *> &        result_values,
                                 const std::vector<arg_code> &result_codes ) {
    char * buff = NULL;
    size_t serialize_len =
        serialize_for_sproc( result_codes, result_values, &buff );
    DCHECK_GE( serialize_len, 4 );
    res.res_args.assign( buff, serialize_len );
}

void serialize_balance_result( sproc_result &res, bool is_okay, int amount ) {
    std::vector<void *>   values = {(void *) &is_okay, (void *) &amount};
    std::vector<arg_code> codes = {BOOL_CODE, INTEGER_CODE};

    serialize_smallbank_result( res, values, codes );
}

sproc_result smallbank_create_partition_holder(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges> &write_ckrs,
    const std::vector<cell_key_ranges> &read_ckrs, std::vector<arg_code> &codes,
    std::vector<void *> &values, void *sproc_opaque ) {
    smallbank_configs configs;

    DCHECK_EQ( codes.size(), values.size() );
    DCHECK_GE( codes.size(), 1 );
    if( codes.size() == 1 or values.at( 1 ) == nullptr ) {
        configs = construct_smallbank_configs();
    } else {
        configs = *(smallbank_configs *) values.at( 1 );
    }
    db *database = (db *) values.at( 0 );

    DVLOG( 5 ) << "Creating database for smallbank_benchmark from configs:"
               << configs;

    smallbank_create_tables( database, configs );

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    return res;
}

sproc_result smallbank_create_database(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges> &write_ckrs,
    const std::vector<cell_key_ranges> &read_ckrs, std::vector<arg_code> &codes,
    std::vector<void *> &values, void *sproc_opaque ) {
    DCHECK( sproc_opaque );
    smallbank_sproc_helper_holder *sproc_helper =
        (smallbank_sproc_helper_holder *) sproc_opaque;

    smallbank_configs configs;

    DCHECK_EQ( codes.size(), values.size() );
    DCHECK_GE( codes.size(), 1 );
    if( codes.size() == 1 or values.at( 1 ) == nullptr ) {
        configs = construct_smallbank_configs();
    } else {
        configs = *(smallbank_configs *) values.at( 1 );
    }
    db *database = (db *) values.at( 0 );

    DVLOG( 5 ) << "Creating database for smallbank_benchmark from configs:"
               << configs;

    smallbank_create_tables( database, configs );
    sproc_helper->init( database );

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    return res;
}

sproc_result smallbank_load_range_of_accounts(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges> &write_ckrs,
    const std::vector<cell_key_ranges> &read_ckrs, std::vector<arg_code> &codes,
    std::vector<void *> &values, void *sproc_opaque ) {
    DCHECK( sproc_opaque );
    smallbank_sproc_helper_holder *sproc_helper =
        (smallbank_sproc_helper_holder *) sproc_opaque;

    check_args( k_smallbank_load_range_of_accounts_arg_codes, codes, values );

    smallbank_loader_templ_no_commit *loader =
        sproc_helper->get_loader_and_set_holder( id, partition_holder );

    uint64_t start = *( (uint64_t *) values.at( 0 ) );
    uint64_t end = *( (uint64_t *) values.at( 1 ) );

    loader->do_range_load_accounts( start, end );
    loader->do_range_load_savings( start, end );
    loader->do_range_load_checkings( start, end );

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    return res;
}

sproc_result smallbank_load_assorted_accounts(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges> &write_ckrs,
    const std::vector<cell_key_ranges> &read_ckrs, std::vector<arg_code> &codes,
    std::vector<void *> &values, void *sproc_opaque ) {
    DCHECK( sproc_opaque );
    smallbank_sproc_helper_holder *sproc_helper =
        (smallbank_sproc_helper_holder *) sproc_opaque;

    check_args( k_smallbank_load_assorted_accounts_arg_codes, codes, values );

    smallbank_loader_templ_no_commit *loader =
        sproc_helper->get_loader_and_set_holder( id, partition_holder );

    uint64_t *accounts = (uint64_t *) values.at( 0 );
    int32_t num_accounts = codes.at( 0 ).array_length;

    loader->do_load_assorted_accounts( accounts, num_accounts );

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    return res;
}

sproc_result smallbank_load_assorted_savings(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges>& write_ckrs,
    const std::vector<cell_key_ranges>& read_ckrs,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque ) {
    DCHECK( sproc_opaque );
    smallbank_sproc_helper_holder *sproc_helper =
        (smallbank_sproc_helper_holder *) sproc_opaque;

    check_args( k_smallbank_load_assorted_savings_arg_codes, codes, values );

    smallbank_loader_templ_no_commit *loader =
        sproc_helper->get_loader_and_set_holder( id, partition_holder );

    uint64_t *accounts = (uint64_t *) values.at( 0 );
    int32_t num_accounts = codes.at( 0 ).array_length;

    loader->do_load_assorted_savings( accounts, num_accounts );

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    return res;
}

sproc_result smallbank_load_assorted_checkings(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges>& write_ckrs,
    const std::vector<cell_key_ranges>& read_ckrs,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque ) {
    DCHECK( sproc_opaque );
    smallbank_sproc_helper_holder *sproc_helper =
        (smallbank_sproc_helper_holder *) sproc_opaque;

    check_args( k_smallbank_load_assorted_checkings_arg_codes, codes, values );

    smallbank_loader_templ_no_commit *loader =
        sproc_helper->get_loader_and_set_holder( id, partition_holder );

    uint64_t *accounts = (uint64_t *) values.at( 0 );
    int32_t num_accounts = codes.at( 0 ).array_length;

    loader->do_load_assorted_checkings( accounts, num_accounts );

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    return res;
}

sproc_result smallbank_load_account_with_args(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges>& write_ckrs,
    const std::vector<cell_key_ranges>& read_ckrs,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque ) {
    DCHECK( sproc_opaque );
    smallbank_sproc_helper_holder *sproc_helper =
        (smallbank_sproc_helper_holder *) sproc_opaque;

    check_args( k_smallbank_load_account_with_args_arg_codes, codes, values );

    smallbank_loader_templ_no_commit *loader =
        sproc_helper->get_loader_and_set_holder( id, partition_holder );

    uint64_t account_id = *( (uint64_t *) values.at( 0 ) );
    char *   name = (char *) values.at( 1 );
    uint32_t name_len = codes.at( 1 ).array_length;

    std::string name_str( name, name_len );

    loader->load_account_with_args( account_id, name_str );

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    return res;
}
sproc_result smallbank_load_checking_with_args(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges>& write_ckrs,
    const std::vector<cell_key_ranges>& read_ckrs,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque ) {
    DCHECK( sproc_opaque );
    smallbank_sproc_helper_holder *sproc_helper =
        (smallbank_sproc_helper_holder *) sproc_opaque;

    check_args( k_smallbank_load_checking_with_args_arg_codes, codes, values );

    smallbank_loader_templ_no_commit *loader =
        sproc_helper->get_loader_and_set_holder( id, partition_holder );

    uint64_t account_id = *( (uint64_t *) values.at( 0 ) );
    int   amount = *( (int *) values.at( 1 ) );

    loader->load_checking_with_args( account_id, amount );

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    return res;
}
sproc_result smallbank_load_saving_with_args(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges>& write_ckrs,
    const std::vector<cell_key_ranges>& read_ckrs,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque ) {
    DCHECK( sproc_opaque );
    smallbank_sproc_helper_holder *sproc_helper =
        (smallbank_sproc_helper_holder *) sproc_opaque;

    check_args( k_smallbank_load_saving_with_args_arg_codes, codes, values );

    smallbank_loader_templ_no_commit *loader =
        sproc_helper->get_loader_and_set_holder( id, partition_holder );

    uint64_t account_id = *( (uint64_t *) values.at( 0 ) );
    int   amount = *( (int *) values.at( 1 ) );

    loader->load_saving_with_args( account_id, amount );

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    return res;
}



sproc_result smallbank_amalgamate(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges>& write_ckrs,
    const std::vector<cell_key_ranges>& read_ckrs,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque ) {
    DCHECK( sproc_opaque );
    smallbank_sproc_helper_holder *sproc_helper =
        (smallbank_sproc_helper_holder *) sproc_opaque;

    check_args( k_smallbank_amalgamate_arg_codes, codes, values );

    smallbank_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_worker_and_set_holder( id, partition_holder );

    uint64_t source = *( (uint64_t *) values.at( 0 ) );
    uint64_t dest = *( (uint64_t *) values.at( 1 ) );

    worker->perform_amalgamate( source, dest );

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    return res;
}

sproc_result smallbank_balance( transaction_partition_holder *partition_holder,
                                const clientid                id,
                                const std::vector<cell_key_ranges> &write_ckrs,
                                const std::vector<cell_key_ranges> &read_ckrs,
                                std::vector<arg_code> &             codes,
                                std::vector<void *> &               values,
                                void *sproc_opaque ) {
    DCHECK( sproc_opaque );
    smallbank_sproc_helper_holder *sproc_helper =
        (smallbank_sproc_helper_holder *) sproc_opaque;

    check_args( k_smallbank_balance_arg_codes, codes, values );

    smallbank_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_worker_and_set_holder( id, partition_holder );

    uint64_t cust = *( (uint64_t *) values.at( 0 ) );

    worker->perform_balance( cust, true /* as read*/ );

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    return res;
}

sproc_result smallbank_deposit_checking(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges>& write_ckrs,
    const std::vector<cell_key_ranges>& read_ckrs,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque ) {
    DCHECK( sproc_opaque );
    smallbank_sproc_helper_holder *sproc_helper =
        (smallbank_sproc_helper_holder *) sproc_opaque;

    check_args( k_smallbank_deposit_checking_arg_codes, codes, values );

    smallbank_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_worker_and_set_holder( id, partition_holder );

    uint64_t cust = *( (uint64_t *) values.at( 0 ) );
    int      amount = *( (int *) values.at( 1 ) );

    worker->perform_deposit_checking( cust, amount );

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    return res;
}

sproc_result smallbank_send_payment(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges>& write_ckrs,
    const std::vector<cell_key_ranges>& read_ckrs,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque ) {
    DCHECK( sproc_opaque );
    smallbank_sproc_helper_holder *sproc_helper =
        (smallbank_sproc_helper_holder *) sproc_opaque;

    check_args( k_smallbank_send_payment_arg_codes, codes, values );

    smallbank_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_worker_and_set_holder( id, partition_holder );

    uint64_t source = *( (uint64_t *) values.at( 0 ) );
    uint64_t dest = *( (uint64_t *) values.at( 1 ) );
    int      amount = *( (int *) values.at( 1 ) );

    worker->perform_send_payment( source, dest, amount );

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    return res;
}

sproc_result smallbank_transact_savings(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges>& write_ckrs,
    const std::vector<cell_key_ranges>& read_ckrs,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque ) {
    DCHECK( sproc_opaque );
    smallbank_sproc_helper_holder *sproc_helper =
        (smallbank_sproc_helper_holder *) sproc_opaque;

    check_args( k_smallbank_transact_savings_arg_codes, codes, values );

    smallbank_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_worker_and_set_holder( id, partition_holder );

    uint64_t cust = *( (uint64_t *) values.at( 0 ) );
    int      amount = *( (int *) values.at( 1 ) );

    worker->perform_transact_savings( cust, amount );

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    return res;
}

sproc_result smallbank_write_check(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges>& write_ckrs,
    const std::vector<cell_key_ranges>& read_ckrs,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque ) {
    DCHECK( sproc_opaque );
    smallbank_sproc_helper_holder *sproc_helper =
        (smallbank_sproc_helper_holder *) sproc_opaque;

    check_args( k_smallbank_write_check_arg_codes, codes, values );

    smallbank_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_worker_and_set_holder( id, partition_holder );

    uint64_t cust = *( (uint64_t *) values.at( 0 ) );
    int      amount = *( (int *) values.at( 1 ) );

    worker->perform_write_check( cust, amount );

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    return res;
}

sproc_result smallbank_account_balance(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges>& write_ckrs,
    const std::vector<cell_key_ranges>& read_ckrs,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque ) {
    DCHECK( sproc_opaque );
    smallbank_sproc_helper_holder *sproc_helper =
        (smallbank_sproc_helper_holder *) sproc_opaque;

    check_args( k_smallbank_account_balance_arg_codes, codes, values );

    smallbank_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_worker_and_set_holder( id, partition_holder );

    uint32_t table = *( (uint64_t *) values.at( 0 ) );
    uint64_t cust = *( (uint64_t *) values.at( 1 ) );

    auto get_res = worker->get_account_balance( table, cust );

    sproc_result res;
    serialize_balance_result( res, std::get<0>( get_res ),
                              std::get<1>( get_res ) );
    res.status = exec_status_type::COMMAND_OK;

    return res;
}

sproc_result smallbank_distributed_charge_account(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges>& write_ckrs,
    const std::vector<cell_key_ranges>& read_ckrs,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque ) {
    DCHECK( sproc_opaque );
    smallbank_sproc_helper_holder *sproc_helper =
        (smallbank_sproc_helper_holder *) sproc_opaque;

    check_args( k_smallbank_distributed_charge_account_arg_codes, codes,
                values );

    smallbank_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_worker_and_set_holder( id, partition_holder );

    uint32_t table_id = *( (uint32_t *) values.at( 0 ) );
    uint64_t cust = *( (uint64_t *) values.at( 1 ) );
    int      amount = *( (int *) values.at( 2 ) );
    bool     is_debit = *( (bool *) values.at( 3 ) );

    bool is_okay =
        worker->distributed_charge_account( table_id, cust, amount, is_debit );

    sproc_result res;

    std::vector<void *>   res_vals = {(void *) &is_okay};
    std::vector<arg_code> res_codes = {BOOL_CODE};

    serialize_smallbank_result( res, res_vals, res_codes );

    res.status = exec_status_type::NONFATAL_ERROR;
    if ( is_okay ) {
        res.status = exec_status_type::COMMAND_OK;
    }

    return res;
}

