#pragma once

#include "../../data-site/single-site-db/db_abstraction.h"
#include "../../distributions/distributions.h"
#include "../../distributions/non_uniform_distribution.h"
#include "../../templates/smallbank_benchmark_types.h"
#include "smallbank_configs.h"
#include "smallbank_record_types.h"

smallbank_loader_types class smallbank_loader {
   public:
    smallbank_loader( db_abstraction* db, const smallbank_configs& configs,
                      const db_abstraction_configs& abstraction_configs,
                      uint32_t                      client_id );
    ~smallbank_loader();

    void add_partition_ranges( uint64_t account_start, uint64_t account_end );

    void do_range_load_accounts( uint64_t account_start, uint64_t account_end );
    void do_range_load_savings( uint64_t account_start, uint64_t account_end );
    void do_range_load_checkings( uint64_t account_start,
                                  uint64_t account_end );

    void do_load_assorted_accounts( uint64_t* account_numbers,
                                    uint32_t  num_accounts );
    void do_load_assorted_savings( uint64_t* account_numbers,
                                   uint32_t  num_accounts );
    void do_load_assorted_checkings( uint64_t* account_numbers,
                                     uint32_t  num_accounts );

    void load_account_with_args( uint64_t           account_number,
                                 const std::string& data );
    void load_saving_with_args( uint64_t account_number, int balance );
    void load_checking_with_args( uint64_t account_number, int balance );

    std::vector<partition_column_identifier>
        generate_partition_column_identifiers( uint32_t table_id,
                                               uint64_t account_start,
                                               uint64_t account_end,
                                               uint32_t num_columns );
    partition_column_identifier generate_partition_column_identifier(
        uint32_t table_id, uint64_t account, uint32_t col_start,
        uint32_t num_columns );

    void set_transaction_partition_holder(
        transaction_partition_holder* holder );

   private:
    void load_account( uint64_t account_number );
    void load_saving( uint64_t account_number );
    void load_checking( uint64_t account_number );

    void add_partitions( uint32_t table_id, uint64_t account_start,
                         uint64_t account_end, uint32_t num_columns );

    template <
        void ( smallbank_loader_templ::*load_op )( uint64_t account_number )>
    void do_range_load_helper( uint32_t table_id, const std::string& name,
                               uint64_t start, uint64_t end,
                               uint32_t num_columns );

    void begin_transaction( const std::vector<cell_key_ranges>& write_set,
                            const std::vector<cell_key_ranges>& read_set );
    void commit_transaction();
    void abort_transaction();

    benchmark_db_operators db_operators_;

    smallbank_configs      configs_;
    db_abstraction_configs abstraction_configs_;

    smallbank_workload_generator generator_;

    distributions            dist_;
};

#include "smallbank_loader.tcc"
