#pragma once

#include <memory>
#include <thread>
#include <unordered_map>

#include "../../common/hw.h"
#include "../../data-site/single-site-db/db_abstraction.h"
#include "../../templates/tpcc_benchmark_types.h"
#include "../benchmark_db_operators.h"
#include "../benchmark_interface.h"
#include "../benchmark_statistics.h"
#include "record-types/tpcc_primary_key_generation.h"
#include "record-types/tpcc_record_types.h"
#include "tpcc_configs.h"
#include "tpcc_loader.h"
#include "tpcc_table_ids.h"
#include "tpcc_workload_generator.h"

tpcc_benchmark_worker_types class tpcc_benchmark_worker {
   public:
    tpcc_benchmark_worker( uint32_t client_id, db_abstraction* db,
                           zipf_distribution_cdf*             z_cdf,
                           const workload_operation_selector& op_selector,
                           const tpcc_configs&                configs,
                           const db_abstraction_configs& abstraction_configs );
    ~tpcc_benchmark_worker();

    void start_timed_workload();
    void stop_timed_workload();

    ALWAYS_INLINE benchmark_statistics get_statistics() const;

    workload_operation_outcome_enum do_delivery();
    workload_operation_outcome_enum do_new_order();
    workload_operation_outcome_enum do_order_status();
    workload_operation_outcome_enum do_payment();
    workload_operation_outcome_enum do_stock_level();

    district* fetch_and_set_next_order_on_district( int32_t w_id, int32_t d_id,
                                                    bool is_abort );
    customer_district* fetch_and_set_next_order_on_customer( int32_t w_id,
                                                             int32_t d_id,
                                                             int32_t c_id,
                                                             bool    is_abort );

    workload_operation_outcome_enum place_new_orders(
        int32_t o_id, int32_t c_id, int32_t d_id, int32_t w_id,
        int32_t o_ol_cnt, int o_all_local, const int32_t* item_ids,
        const int32_t* supplier_w_ids, const int32_t* order_quantities );
#if 0 // MTODO-TPCC
    void new_order_order_items( int32_t w_id, int32_t d_id, int32_t o_id,
                                float w_tax, float d_tax, float c_discount,
                                uint32_t num_orders, int32_t* order_lines,
                                int32_t* order_quantities, int32_t* item_ids,
                                int32_t* supplier_w_ids,
                                int32_t* stock_descr_lens, char* stock_descr );
    // have to free these tuples
    std::tuple<int32_t*, std::string> new_order_update_stocks(
        int32_t order_w_id, int32_t order_d_id, uint32_t num_orders,
        const int32_t* item_ids, const int32_t* supplier_w_ids,
        const int32_t* order_quantities );
#endif

    void populate_order_and_stock(
        std::vector<record_identifier>& order_line_rids,
        std::vector<order_line>&        order_lines,
        std::vector<record_identifier>& stock_rids, std::vector<stock>& stocks,
        int32_t o_id, int32_t d_id, int32_t w_id, int32_t o_ol_cnt,
        const int32_t* item_ids, const int32_t* supplier_w_ids,
        const int32_t* order_quantities );
    void populate_order( order& o, new_order& new_o, int32_t o_id, int32_t c_id,
                         int32_t d_id, int32_t w_id, int32_t o_ol_cnt,
                         int32_t o_all_local );
    ALWAYS_INLINE float lookup_warehouse_tax( int32_t w_id );
    ALWAYS_INLINE float lookup_district_tax( int32_t w_id, int32_t d_id );
    ALWAYS_INLINE float lookup_customer_discount( int32_t w_id, int32_t d_id,
                                                  int32_t c_id );
    float lookup_item_price( int32_t i_id );
#if 0 // MTODO-TPCC

    void new_order_set_order( int32_t o_id, int32_t w_id, int32_t d_id,
                              int32_t c_id, int32_t o_ol_cnt,
                              int32_t o_all_local );
    void new_order_set_new_order( int32_t o_id, int32_t w_id, int32_t d_id );
#endif

	// stock level operations
    int32_t perform_stock_level_work( int32_t w_id, int32_t d_id,
                                      int32_t stock_threshold );
    int32_t perform_stock_level_work_with_recent_items(
        int32_t w_id, int32_t d_id, int32_t stock_threshold,
        bool have_recent_items, const std::vector<int32_t>& recent_items );

    std::vector<int32_t> get_stock_below_threshold( int32_t  w_id,
                                                    int32_t  stock_threshold,
                                                    int32_t* item_ids,
                                                    uint32_t item_id_count );
    void get_stock_below_threshold_by_scan(
        const std::vector<scan_arguments>& scan_args, scan_result& scan_res );

    int32_t get_max_order_id_to_look_back_on( int32_t w_id, int32_t d_id );
    std::vector<int32_t> get_recent_items( int32_t w_id, int32_t d_id,
                                           int32_t min_o_id, int32_t max_o_id,
                                           int32_t max_ol_num );
    std::vector<int32_t> get_recent_items_by_ckrs(
        const std::vector<cell_key_ranges>& read_ckrs );

    // payment
    workload_operation_enum perform_payment_work( int32_t w_id, int32_t d_id,
                                                  int32_t c_w_id,
                                                  int32_t c_d_id, int32_t c_id,
                                                  float h_amount );

    void add_payment_to_warehouse( int32_t w_id, float h_amount );
    void add_payment_to_district( int32_t w_id, int32_t d_id, float h_amount );
    void add_payment_to_history( int32_t w_id, int32_t d_id, int32_t c_w_id,
                                 int32_t c_d_id, int32_t c_id, float h_amount );
    void add_payment_to_customer( int32_t w_id, int32_t d_id, int32_t c_w_id,
                                  int32_t c_d_id, int32_t c_id,
                                  float h_amount );

    void set_transaction_partition_holder(
        transaction_partition_holder* txn_holder );

    tpcc_configs get_configs() const;

   private:
    void                            run_workload();
    void                            do_workload_operation();
    workload_operation_outcome_enum perform_workload_operation(
        const workload_operation_enum& op );

    workload_operation_outcome_enum perform_new_order_work(
        int32_t w_id, int32_t d_id, int32_t c_id, int32_t o_ol_cnt,
        int o_all_local, const int32_t* item_ids, const int32_t* supplier_w_ids,
        const int32_t* order_quantities );

#if 0 // MTODO-TPCC
    void insert_order_and_new_order( order& o, record_identifier& o_rid,
                                     new_order&         new_o,
                                     record_identifier& new_o_rid );
#endif
    float order_item( int32_t w_id, int32_t                 d_id,
                      std::unordered_map<uint64_t, stock*>& seen_stocks,
                      order_line& o_line, record_identifier& o_line_rid,
                      stock& s, record_identifier& s_rid );
#if 0 // MTODO-TPCC

    ALWAYS_INLINE customer* lookup_customer( int32_t w_id, int32_t d_id,
                                             int32_t c_id );
    ALWAYS_INLINE warehouse* lookup_warehouse( int32_t w_id );
    ALWAYS_INLINE district* lookup_district( int32_t w_id, int32_t d_id );
    ALWAYS_INLINE item* lookup_item( int32_t i_id );
    ALWAYS_INLINE stock* lookup_stock( int32_t w_id, int32_t item_id );
    ALWAYS_INLINE order_line* lookup_order_line( int32_t w_id, int32_t d_id,
                                                 int32_t o_id, int32_t ol_num );

#endif

    ALWAYS_INLINE void new_order_write_stock(
        std::unordered_map<uint64_t, stock*>& seen_stocks, int32_t d_id );
    ALWAYS_INLINE float new_order_order_item(
        order_line& o_line, const record_identifier& o_line_rid,
        std::string& stock_descr );
    ALWAYS_INLINE void new_order_update_stock(
        int32_t order_w_id, int32_t supplier_w_id, int32_t ol_quantity,
        int32_t item_id, const record_identifier& s_rid,
        std::unordered_map<uint64_t, stock*>&     seen_stocks );

    bool is_stock_level_below_threshold( int32_t w_id, int32_t i_id,
                                         int32_t stock_threshold );

    // payment

    benchmark_db_operators db_operators_;

    tpcc_loader<do_begin_commit> loader_;

    tpcc_workload_generator                  generator_;
    std::vector<table_partition_information> data_sizes_;
    benchmark_statistics                     statistics_;

    std::unique_ptr<std::thread> worker_;
    volatile bool                done_;
};
#if 0 // MTODO-TPCC

std::vector<record_identifier> get_payment_rids( int32_t w_id, int32_t d_id,
                                                 int32_t c_w_id, int32_t c_d_id,
                                                 int32_t             c_id,
                                                 const tpcc_configs& configs );
#endif
std::vector<cell_key_ranges> get_payment_ckrs( int32_t w_id, int32_t d_id,
                                               int32_t c_w_id, int32_t c_d_id,
                                               int32_t             c_id,
                                               const tpcc_configs& configs );

#include "tpcc_benchmark_worker.tcc"
