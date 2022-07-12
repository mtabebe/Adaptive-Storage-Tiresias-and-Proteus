#pragma once

#include <memory>
#include <thread>
#include <unordered_map>

#include "../../common/hw.h"
#include "../../data-site/single-site-db/db_abstraction.h"
#include "../../templates/smallbank_benchmark_types.h"
#include "../benchmark_interface.h"
#include "../benchmark_statistics.h"
#include "smallbank_configs.h"
#include "smallbank_db_operators.h"
#include "smallbank_record_types.h"
#include "smallbank_table_ids.h"
#include "smallbank_workload_generator.h"

smallbank_benchmark_worker_types class smallbank_benchmark_worker {
   public:
    smallbank_benchmark_worker(
        uint32_t client_id, db_abstraction* db, zipf_distribution_cdf* z_cdf,
        const workload_operation_selector& op_selector,
        const smallbank_configs&           configs,
        const db_abstraction_configs&      abstraction_configs );
    ~smallbank_benchmark_worker();

    void start_timed_workload();
    void stop_timed_workload();

    ALWAYS_INLINE benchmark_statistics get_statistics() const;

    workload_operation_outcome_enum do_amalgamate();
    workload_operation_outcome_enum do_balance();
    workload_operation_outcome_enum do_deposit_checking();
    workload_operation_outcome_enum do_send_payment();
    workload_operation_outcome_enum do_transact_savings();
    workload_operation_outcome_enum do_write_check();

    workload_operation_outcome_enum perform_amalgamate( uint64_t source_cust_id,
                                                        uint64_t dest_cust_id );
    workload_operation_outcome_enum perform_balance( uint64_t cust_id,
                                                     bool     as_read );
    workload_operation_outcome_enum perform_deposit_checking( uint64_t cust_id,
                                                              int   amount );
    workload_operation_outcome_enum perform_send_payment(
        uint64_t source_cust_id, uint64_t dest_cust_id, int amount );
    workload_operation_outcome_enum perform_transact_savings( uint64_t cust_id,
                                                              int   amount );
    workload_operation_outcome_enum perform_write_check( uint64_t cust_id,
                                                         int   amount );

    void set_transaction_partition_holder(
        transaction_partition_holder* holder );

    std::tuple<bool, int> get_account_balance( uint32_t table_id,
                                               uint64_t cust_id );

    bool distributed_charge_account( uint32_t table_id, uint64_t cust_id,
                                     int amount, bool is_debit );

   private:
    void                            run_workload();
    void                            do_workload_operation();
    workload_operation_outcome_enum perform_workload_operation(
        const workload_operation_enum& op );

    template <class S>
    std::tuple<bool, int> get_balance( uint32_t table, uint64_t cust_id );

    template <class S, bool ( *check_op_fxn )( int a, int b )>
    workload_operation_outcome_enum perform_rmw_on_table(
        uint32_t table_id, uint64_t cust_id, int amount, uint32_t multiplier,
        const std::string& name );

    // payment

    benchmark_db_operators db_operators_;

    smallbank_workload_generator generator_;
    benchmark_statistics    statistics_;

    std::unique_ptr<std::thread> worker_;
    volatile bool                done_;
};

bool add_balance_valid( int a, int b );
bool sub_balance_valid( int a, int b );

#include "smallbank_benchmark_worker.tcc"
