#pragma once

#include <memory>
#include <thread>
#include <unordered_map>

#include "../../common/hw.h"
#include "../../templates/tpcc_benchmark_types.h"
#include "../../templates/tpch_benchmark_types.h"
#include "../benchmark_interface.h"
#include "../benchmark_statistics.h"
#include "record-types/tpcc_primary_key_generation.h"
#include "record-types/tpcc_record_types.h"
#include "tpcc_loader.h"
#include "tpcc_table_ids.h"
#include "tpcc_workload_generator.h"

#include "tpch_mr.h"

tpch_benchmark_worker_types class tpch_benchmark_worker {
   public:
    tpch_benchmark_worker( uint32_t client_id, db_abstraction* db,
                           zipf_distribution_cdf*             z_cdf,
                           const workload_operation_selector& op_selector,
                           const tpcc_configs&                configs,
                           const db_abstraction_configs& abstraction_configs,
                           const snapshot_vector&        init_state );
    ~tpch_benchmark_worker();

    void start_timed_workload();
    void stop_timed_workload();

    ALWAYS_INLINE benchmark_statistics get_statistics() const;

    workload_operation_outcome_enum do_all();

    workload_operation_outcome_enum do_q1();
    void do_q1_work_by_scan(
        const std::unordered_map<int, scan_arguments>& scan_args,
        scan_result& scan_res );
    std::vector<scan_arguments> generate_q1_scan_args(
        const order_line& low_key, const order_line& high_key,
        uint64_t delivery_time ) const;

    workload_operation_outcome_enum do_q2();
    void do_q2_work_by_scan(
        const std::unordered_map<int, scan_arguments>& scan_args,
        scan_result& scan_res );
    std::vector<scan_arguments> generate_q2_scan_args(
        const stock& low_stock, const stock& high_stock,
        const supplier& low_supplier, const supplier& high_supplier,
        const nation& low_nation, const nation& high_nation,
        int32_t region_id ) const;

    workload_operation_outcome_enum do_q3();
    void                            do_q3_work_by_scan(
        const std::unordered_map<int, scan_arguments>& scan_args,
        scan_result& scan_res );
    std::vector<scan_arguments> generate_q3_scan_args(
        const order& low_order, const order& high_order,
        const std::string& low_c_state, const std::string& high_c_state,
        uint64_t low_delivery_time ) const;

    workload_operation_outcome_enum do_q4();
    void                            do_q4_work_by_scan(
        const std::unordered_map<int, scan_arguments>& scan_args,
        scan_result& scan_res );
    std::vector<scan_arguments> generate_q4_scan_args(
        const order& low_order, const order& high_order,
        uint64_t low_delivery_time, uint64_t high_delivery_time ) const;

    workload_operation_outcome_enum do_q5();
    void                            do_q5_work_by_scan(
        const std::unordered_map<int, scan_arguments>& scan_args,
        scan_result& scan_res );
    std::vector<scan_arguments> generate_q5_scan_args(
        const order_line& low_order, const order_line& high_order,
        const item& low_item, const item& high_item,
        const supplier& low_supplier, const supplier& high_supplier,
        const nation& low_nation, const nation& high_nation,
        uint64_t low_delivery_time, int32_t region_id ) const;

    workload_operation_outcome_enum do_q6();
    void                            do_q6_work_by_scan(
        const std::unordered_map<int, scan_arguments>& scan_args,
        scan_result& scan_res );
    std::vector<scan_arguments> generate_q6_scan_args(
        const order_line& low_order, const order_line& high_order,
        uint64_t low_delivery_time, uint64_t high_delivery_time,
        int64_t low_quantity, int64_t high_quantity ) const;

    workload_operation_outcome_enum do_q7();
    void                            do_q7_work_by_scan(
        const std::unordered_map<int, scan_arguments>& scan_args,
        scan_result& scan_res );
    std::vector<scan_arguments> generate_q7_scan_args(
        const order& low_order, const order& high_order, const item& low_item,
        const item& high_item, const supplier& low_supplier,
        const supplier& high_supplier, int32_t nation_id_1, int32_t nation_id_2,
        uint64_t low_delivery_time, uint64_t high_delivery_time ) const;

    workload_operation_outcome_enum do_q8();
    void                            do_q8_work_by_scan(
        const std::unordered_map<int, scan_arguments>& scan_args,
        scan_result& scan_res );
    std::vector<scan_arguments> generate_q8_scan_args(
        const order& low_order, const order& high_order, const item& low_item,
        const item& high_item, const supplier& low_supplier,
        const supplier& high_supplier, const nation& low_nation,
        const nation& high_nation, uint64_t low_delivery_time,
        uint64_t high_delivery_time, int32_t region_id ) const;

    workload_operation_outcome_enum do_q9();
    void                            do_q9_work_by_scan(
        const std::unordered_map<int, scan_arguments>& scan_args,
        scan_result& scan_res );
    std::vector<scan_arguments> generate_q9_scan_args(
        const order& low_order, const order& high_order, const item& low_item,
        const item& high_item, const supplier& low_supplier,
        const supplier& high_supplier, const nation& low_nation,
        const nation& high_nation, const std::string& low_item_str,
        const std::string& high_item_str ) const;

    workload_operation_outcome_enum do_q10();
    void                            do_q10_work_by_scan(
        const std::unordered_map<int, scan_arguments>& scan_args,
        scan_result& scan_res );
    std::vector<scan_arguments> generate_q10_scan_args(
        const order& low_order, const order& high_order,
        uint64_t low_delivery_time, int32_t nation_id ) const;

    workload_operation_outcome_enum do_q11();
    void                            do_q11_work_by_scan(
        const std::unordered_map<int, scan_arguments>& scan_args,
        scan_result& scan_res, float order_sum_multiplier );
    std::vector<scan_arguments> generate_q11_scan_args(
        const stock& low_stock, const stock& high_stock,
        const supplier& low_supplier, const supplier& high_supplier,
        int32_t nation_id ) const;

    workload_operation_outcome_enum do_q12();
    void                            do_q12_work_by_scan(
        const std::unordered_map<int, scan_arguments>& scan_args,
        scan_result& scan_res );
    std::vector<scan_arguments> generate_q12_scan_args(
        const order& low_order, const order& high_order,
        uint64_t low_delivery_time ) const;

    workload_operation_outcome_enum do_q13();
    void                            do_q13_work_by_scan(
        const std::unordered_map<int, scan_arguments>& scan_args,
        scan_result& scan_res );
    std::vector<scan_arguments> generate_q13_scan_args(
        const order& low_order, const order& high_order,
        int32_t o_carrier_threshold ) const;

    workload_operation_outcome_enum do_q14();
    void                            do_q14_work_by_scan(
        const std::unordered_map<int, scan_arguments>& scan_args,
        scan_result& scan_res );
    std::vector<scan_arguments> generate_q14_scan_args(
        const order_line& low_order, const order_line& high_order,
        const item& low_item, const item& high_item, uint64_t low_delivery_time,
        uint64_t high_delivery_time ) const;

    workload_operation_outcome_enum do_q15();
    void                            do_q15_work_by_scan(
        const std::unordered_map<int, scan_arguments>& scan_args,
        scan_result& scan_res );
    std::vector<scan_arguments> generate_q15_scan_args(
        const order_line& low_order, const order_line& high_order,
        const item& low_item, const item& high_item,
        const supplier& low_supplier, const supplier& high_supplier,
        uint64_t low_delivery_time ) const;

    workload_operation_outcome_enum do_q16();
    void                            do_q16_work_by_scan(
        const std::unordered_map<int, scan_arguments>& scan_args,
        scan_result& scan_res );
    std::vector<scan_arguments> generate_q16_scan_args(
        const stock& low_stock, const stock& high_stock,
        const supplier& low_supplier, const supplier& high_supplier,
        const std::string& i_data_low, const std::string& i_data_high,
        const std::string& supplier_comment_low,
        const std::string& supplier_comment_high ) const;

    workload_operation_outcome_enum do_q17();
    void                            do_q17_work_by_scan(
        const std::unordered_map<int, scan_arguments>& scan_args,
        scan_result& scan_res );
    std::vector<scan_arguments> generate_q17_scan_args(
        const order_line& low_ol, const order_line& high_ol,
        const item& low_item, const item& high_item,
        const std::string& low_item_str,
        const std::string& high_item_str ) const;

    workload_operation_outcome_enum do_q18();
    void                            do_q18_work_by_scan(
        const std::unordered_map<int, scan_arguments>& scan_args,
        scan_result& scan_res, float ol_amount_threshold );
    std::vector<scan_arguments> generate_q18_scan_args(
        const order& low_order, const order& high_order ) const;

    workload_operation_outcome_enum do_q19();
    void                            do_q19_work_by_scan(
        const std::unordered_map<int, scan_arguments>& scan_args,
        scan_result& scan_res );
    std::vector<scan_arguments> generate_q19_scan_args(
        const order_line& low_ol, const order_line& high_ol,
        const item& low_item, const item& high_item, int32_t low_ol_quantity,
        int32_t high_ol_quantity, float low_i_price, float high_i_price,
        const std::string& low_i_data, const std::string& high_i_data ) const;

    workload_operation_outcome_enum do_q20();
    void                            do_q20_work_by_scan(
        const std::unordered_map<int, scan_arguments>& scan_args,
        scan_result& scan_res, float s_quant_multi );
    std::vector<scan_arguments> generate_q20_scan_args(
        const order_line& low_order, const order_line& high_order,
        const item& low_item, const item& high_item,
        const supplier& low_supplier, const supplier& high_supplier,
        int32_t nation_id, const std::string& low_i_data,
        const std::string& high_i_data, uint64_t low_delivery_time ) const;

    workload_operation_outcome_enum do_q21();
    void                            do_q21_work_by_scan(
        const std::unordered_map<int, scan_arguments>& scan_args,
        scan_result& scan_res );
    std::vector<scan_arguments> generate_q21_scan_args(
        const order_line& low_order, const order_line& high_order,
        const item& low_item, const item& high_item,
        const supplier& low_supplier, const supplier& high_supplier,
        int32_t nation_id ) const;

    workload_operation_outcome_enum do_q22();
    void                            do_q22_work_by_scan(
        const std::unordered_map<int, scan_arguments>& scan_args,
        scan_result& scan_res );
    std::vector<scan_arguments> generate_q22_scan_args(
        const order& low_order, const order& high_order,
        const std::string& low_c_phone, const std::string& high_c_phone ) const;

    void set_transaction_partition_holder(
        transaction_partition_holder* txn_holder );
    tpcc_configs get_configs() const;

   private:
    void                            run_workload();
    void                            do_workload_operation();
    void                            do_workload_operation_work(
        const workload_operation_outcome_enum& op );
    workload_operation_outcome_enum perform_workload_operation(
        const workload_operation_enum& op );

    workload_operation_outcome_enum do_q1_work( uint64_t          delivery_time,
                                                const order_line& low_key,
                                                const order_line& high_key );
    workload_operation_outcome_enum do_q2_work( const stock&    low_stock,
                                                const stock&    high_stock,
                                                const supplier& low_supplier,
                                                const supplier& high_supplier,
                                                const nation&   low_nation,
                                                const nation&   high_nation,
                                                int32_t         region_id );

    workload_operation_outcome_enum do_q3_work( const order&       low_order,
                                                const order&       high_order,
                                                const std::string& low_c_state,
                                                const std::string& high_c_state,
                                                uint64_t low_delivery_time );
    workload_operation_outcome_enum do_q4_work( const order& low_key,
                                                const order& high_key,
                                                uint64_t     low_delivery_time,
                                                uint64_t high_delivery_time );
    workload_operation_outcome_enum do_q5_work(
        const order_line& low_key, const order_line& high_key,
        const item& low_item, const item& high_item,
        const supplier& low_supplier, const supplier& high_supplier,
        const nation& low_nation, const nation& high_nation,
        uint64_t low_delivery_time, int32_t region_id );
    workload_operation_outcome_enum do_q6_work( const order_line& low_key,
                                                const order_line& high_key,
                                                uint64_t low_delivery_time,
                                                uint64_t high_delivery_time,
                                                int64_t  low_quantity,
                                                int64_t  high_quantity );

    workload_operation_outcome_enum do_q7_work(
        const order& low_order, const order& high_order, const item& low_item,
        const item& high_item, const supplier& low_supplier,
        const supplier& high_supplier, int32_t nation_id_1, int32_t nation_id_2,
        uint64_t low_delivery_time, uint64_t high_delivery_time );

    workload_operation_outcome_enum do_q8_work(
        const order& low_key, const order& high_key, const item& low_item,
        const item& high_item, const supplier& low_supplier,
        const supplier& high_supplier, const nation& low_nation,
        const nation& high_nation, uint64_t low_delivery_time,
        uint64_t high_delivery_time, int32_t region_id );

    workload_operation_outcome_enum do_q9_work(
        const order& low_order, const order& high_order, const item& low_item,
        const item& high_item, const supplier& low_supplier,
        const supplier& high_supplier, const nation& low_nation,
        const nation& high_nation, const std::string& low_item_str,
        const std::string& high_item_str );

    workload_operation_outcome_enum do_q10_work( const order& low_key,
                                                 const order& high_key,
                                                 uint64_t     low_delivery_time,
                                                 int32_t      nation_id );

    workload_operation_outcome_enum do_q11_work( const stock&    low_stock,
                                                 const stock&    high_stock,
                                                 const supplier& low_supplier,
                                                 const supplier& high_supplier,
                                                 int32_t         nation_id,
                                                 float order_sum_multiplier );

    workload_operation_outcome_enum do_q12_work( const order& low_key,
                                                 const order& high_key,
                                                 uint64_t low_delivery_time );

    workload_operation_outcome_enum do_q13_work( const order& low_key,
                                                 const order& high_key,
                                                 int32_t o_carrier_threshold );

    workload_operation_outcome_enum do_q14_work( const order_line& low_key,
                                                 const order_line& high_key,
                                                 const item&       low_item,
                                                 const item&       high_item,
                                                 uint64_t low_delivery_time,
                                                 uint64_t high_delivery_time );

    workload_operation_outcome_enum do_q15_work( const order_line& low_key,
                                                 const order_line& high_key,
                                                 const item&       low_item,
                                                 const item&       high_item,
                                                 const supplier&   low_supplier,
                                                 const supplier& high_supplier,
                                                 uint64_t low_delivery_time );

    workload_operation_outcome_enum do_q16_work(
        const stock& low_stock, const stock& high_stock,
        const supplier& low_supplier, const supplier& high_supplier,
        const std::string& i_data_low, const std::string& i_data_high,
        const std::string& supplier_comment_low,
        const std::string& supplier_comment_high );

    workload_operation_outcome_enum do_q17_work(
        const order_line& low_ol, const order_line& high_ol,
        const item& low_item, const item& high_item,
        const std::string& i_data_low, const std::string& i_data_high );

    workload_operation_outcome_enum do_q18_work( const order& low_key,
                                                 const order& high_key,
                                                 float ol_amount_threshold );

    workload_operation_outcome_enum do_q19_work(
        const order_line& low_ol, const order_line& high_ol,
        const item& low_item, const item& high_item, int32_t low_ol_quantity,
        int32_t high_ol_quantity, float low_i_price, float high_i_price,
        const std::string& low_i_data, const std::string& high_i_data );

    workload_operation_outcome_enum do_q20_work(
        const order_line& low_order, const order_line& high_order,
        const item& low_item, const item& high_item,
        const supplier& low_supplier, const supplier& high_supplier,
        int32_t nation_id, const std::string& low_i_data,
        const std::string& high_i_data, uint64_t low_delivery_time,
        float s_quant_mult );

    workload_operation_outcome_enum do_q21_work( const order_line& low_order,
                                                 const order_line& high_order,
                                                 const item&       low_item,
                                                 const item&       high_item,
                                                 const supplier&   low_supplier,
                                                 const supplier& high_supplier,
                                                 int32_t         nation_id );

    workload_operation_outcome_enum do_q22_work(
        const order& low_order, const order& high_order,
        const std::string& low_c_phone, const std::string& high_c_phone );

    std::vector<cell_key_ranges> generate_q1_ckrs(
        const order_line& low_key, const order_line& high_key ) const;

    std::vector<cell_key_ranges> generate_q2_stock_ckrs(
        const stock& low_stock, const stock& high_stock ) const;
    std::vector<cell_key_ranges> generate_q2_item_ckrs(
        const stock& low_stock, const stock& high_stock ) const;
    std::vector<cell_key_ranges> generate_q2_supplier_ckrs(
      const supplier& low_supplier, const supplier& high_supplier ) const;
    std::vector<cell_key_ranges> generate_q2_nation_ckrs(
        const nation& low_nation, const nation& high_nation ) const;

    std::vector<cell_key_ranges> generate_q3_order_ckrs(
        const order& low_key, const order& high_key ) const;
    std::vector<cell_key_ranges> generate_q3_order_line_ckrs(
        const order& low_key, const order& high_key ) const;
    std::vector<cell_key_ranges> generate_q3_customer_ckrs(
        const order& low_key, const order& high_key ) const;

    std::vector<cell_key_ranges> generate_q4_order_ckrs(
        const order& low_key, const order& high_key ) const;
    std::vector<cell_key_ranges> generate_q4_order_line_ckrs(
        const order& low_key, const order& high_key ) const;

    std::vector<cell_key_ranges> generate_q5_nation_ckrs(
        const nation& low_nation, const nation& high_nation ) const;
    std::vector<cell_key_ranges> generate_q5_supplier_ckrs(
        const supplier& low_supplier, const supplier& high_supplier ) const;
    std::vector<cell_key_ranges> generate_q5_stock_ckrs(
        const order_line& low_order, const order_line& high_order,
        const item& low_item, const item& high_item ) const;
    std::vector<cell_key_ranges> generate_q5_order_line_ckrs(
        const order_line& low_order, const order_line& high_order ) const;

    std::vector<cell_key_ranges> generate_q6_order_line_ckrs(
        const order_line& low_order, const order_line& high_order ) const;

    std::vector<cell_key_ranges> generate_q7_supplier_ckrs(
        const supplier& low_supplier, const supplier& high_supplier ) const;
    std::vector<cell_key_ranges> generate_q7_stock_ckrs(
        const item& low_item, const item& high_item, const order& low_order,
        const order& high_order ) const;
    std::vector<cell_key_ranges> generate_q7_customer_ckrs(
        const order& low_order, const order& high_order ) const;
    std::vector<cell_key_ranges> generate_q7_order_ckrs(
        const order& low_order, const order& high_order ) const;
    std::vector<cell_key_ranges> generate_q7_order_line_ckrs(
        const order& low_order, const order& high_order ) const;

    std::vector<cell_key_ranges> generate_q8_nation_ckrs(
        const nation& low_nation, const nation& high_nation ) const;
    std::vector<cell_key_ranges> generate_q8_supplier_ckrs(
        const supplier& low_supplier, const supplier& high_supplier ) const;
    std::vector<cell_key_ranges> generate_q8_stock_ckrs(
        const item& low_item, const item& high_item, const order& low_order,
        const order& high_order ) const;
    std::vector<cell_key_ranges> generate_q8_order_ckrs(
        const order& low_order, const order& high_order ) const;
    std::vector<cell_key_ranges> generate_q8_order_line_ckrs(
        const order& low_order, const order& high_order ) const;

    std::vector<cell_key_ranges> generate_q9_nation_ckrs(
        const nation& low_nation, const nation& high_nation ) const;
    std::vector<cell_key_ranges> generate_q9_item_ckrs(
        const item& low_item, const item& high_item ) const;
    std::vector<cell_key_ranges> generate_q9_stock_ckrs(
        const item& low_item, const item& high_item, const order& low_order,
        const order& high_order ) const;
    std::vector<cell_key_ranges> generate_q9_supplier_ckrs(
        const supplier& low_supplier, const supplier& high_supplier ) const;
    std::vector<cell_key_ranges> generate_q9_order_ckrs(
        const order& low_order, const order& high_order ) const;
    std::vector<cell_key_ranges> generate_q9_order_line_ckrs(
        const order& low_order, const order& high_order ) const;

    std::vector<cell_key_ranges> generate_q10_nation_ckrs(
        int32_t nation_id ) const;
    std::vector<cell_key_ranges> generate_q10_customer_ckrs(
        const order& low_order, const order& high_order ) const;
    std::vector<cell_key_ranges> generate_q10_order_ckrs(
        const order& low_order, const order& high_order ) const;
    std::vector<cell_key_ranges> generate_q10_order_line_ckrs(
        const order& low_order, const order& high_order ) const;

    std::vector<cell_key_ranges> generate_q11_stock_ckrs(
        const stock& low_stock, const stock& high_stock ) const;
    std::vector<cell_key_ranges> generate_q11_supplier_ckrs(
        const supplier& low_supplier, const supplier& high_supplier ) const;

    std::vector<cell_key_ranges> generate_q12_order_ckrs(
        const order& low_order, const order& high_order ) const;
    std::vector<cell_key_ranges> generate_q12_order_line_ckrs(
        const order& low_order, const order& high_order ) const;

    std::vector<cell_key_ranges> generate_q13_order_ckrs(
        const order& low_order, const order& high_order ) const;
    std::vector<cell_key_ranges> generate_q13_customer_ckrs(
        const order& low_order, const order& high_order ) const;

    std::vector<cell_key_ranges> generate_q14_order_line_ckrs(
        const order_line& low_order, const order_line& high_order ) const;
    std::vector<cell_key_ranges> generate_q14_item_ckrs(
        const item& low_item, const item& high_item ) const;

    std::vector<cell_key_ranges> generate_q15_order_line_ckrs(
        const order_line& low_order, const order_line& high_order ) const;
    std::vector<cell_key_ranges> generate_q15_stock_ckrs(
        const order_line& low_order, const order_line& high_order,
        const item& low_item, const item& high_item ) const;
    std::vector<cell_key_ranges> generate_q15_supplier_ckrs(
        const supplier& low_supplier, const supplier& high_supplier ) const;

    std::vector<cell_key_ranges> generate_q16_item_ckrs(
        const stock& low_stock, const stock& high_stock ) const;
    std::vector<cell_key_ranges> generate_q16_stock_ckrs(
        const stock& low_stock, const stock& high_stock ) const;
    std::vector<cell_key_ranges> generate_q16_supplier_ckrs(
        const supplier& low_supplier, const supplier& high_supplier ) const;

    std::vector<cell_key_ranges> generate_q17_order_line_ckrs(
        const order_line& low_ol, const order_line& high_ol ) const;
    std::vector<cell_key_ranges> generate_q17_item_ckrs(
        const item& low_item, const item& high_item ) const;

    std::vector<cell_key_ranges> generate_q18_order_line_ckrs(
        const order& low_order, const order& high_order ) const;
    std::vector<cell_key_ranges> generate_q18_order_ckrs(
        const order& low_order, const order& high_order ) const;
    std::vector<cell_key_ranges> generate_q18_customer_ckrs(
        const order& low_order, const order& high_order ) const;

    std::vector<cell_key_ranges> generate_q19_order_line_ckrs(
        const order_line& low_ol, const order_line& high_ol ) const;
    std::vector<cell_key_ranges> generate_q19_item_ckrs(
        const item& low_item, const item& high_item ) const;

    std::vector<cell_key_ranges> generate_q20_item_ckrs(
        const item& low_item, const item& high_item ) const;
    std::vector<cell_key_ranges> generate_q20_stock_ckrs(
        const order_line& low_ol, const order_line& high_ol,
        const item& low_item, const item& high_item ) const;
    std::vector<cell_key_ranges> generate_q20_order_line_ckrs(
        const order_line& low_ol, const order_line& high_ol ) const;
    std::vector<cell_key_ranges> generate_q20_supplier_ckrs(
        const supplier& low_supplier, const supplier& high_supplier ) const;

    std::vector<cell_key_ranges> generate_q21_supplier_ckrs(
        const supplier& low_supplier, const supplier& high_supplier ) const;
    std::vector<cell_key_ranges> generate_q21_order_ckrs(
        const order_line& low_ol, const order_line& high_ol ) const;
    std::vector<cell_key_ranges> generate_q21_order_line_ckrs(
        const order_line& low_ol, const order_line& high_ol ) const;
    std::vector<cell_key_ranges> generate_q21_stock_ckrs(
        const order_line& low_ol, const order_line& high_ol,
        const item& low_item, const item& high_item ) const;

    std::vector<cell_key_ranges> generate_q22_order_ckrs(
        const order& low_order, const order& high_order ) const;
    std::vector<cell_key_ranges> generate_q22_customer_ckrs(
        const order& low_order, const order& high_order ) const;

    std::vector<cell_key_ranges> generate_nation_ckrs(
        const nation& low_nation, const nation&            high_nation,
        const std::vector<std::tuple<uint32_t, uint32_t>>& cols ) const;
    std::vector<cell_key_ranges> generate_supplier_ckrs(
        const supplier& low_supplier, const supplier&      high_supplier,
        const std::vector<std::tuple<uint32_t, uint32_t>>& cols ) const;
    std::vector<cell_key_ranges> generate_item_ckrs(
        const item& low_item, const item&                  high_item,
        const std::vector<std::tuple<uint32_t, uint32_t>>& cols ) const;
    std::vector<cell_key_ranges> generate_stock_ckrs(
        const stock& low_stock, const stock&               high_stock,
        const std::vector<std::tuple<uint32_t, uint32_t>>& cols ) const;
    std::vector<cell_key_ranges> generate_customer_ckrs(
        const customer& low_cust, const customer&          high_cust,
        const std::vector<std::tuple<uint32_t, uint32_t>>& cols ) const;
    std::vector<cell_key_ranges> generate_order_line_ckrs(
        const order_line& low_key, const order_line&       high_key,
        const std::vector<std::tuple<uint32_t, uint32_t>>& cols ) const;
    std::vector<cell_key_ranges> generate_order_ckrs(
        const order& low_key, const order&                 high_key,
        const std::vector<std::tuple<uint32_t, uint32_t>>& cols ) const;

    benchmark_db_operators db_operators_;

    tpcc_loader<do_begin_commit> loader_;

    tpcc_workload_generator                  generator_;
    std::vector<table_partition_information> data_sizes_;
    benchmark_statistics                     statistics_;

    std::unique_ptr<std::thread> worker_;
    volatile bool                done_;

};

#include "tpch_benchmark_worker.tcc"
