#pragma once

#include <queue>
#include <unordered_map>
#include <vector>

#include "../../common/hw.h"
#include "../../distributions/distributions.h"
#include "../../distributions/non_uniform_distribution.h"
#include "../workload_operation_selector.h"
#include "tpcc_configs.h"

class tpcc_workload_generator {
   public:
    tpcc_workload_generator( zipf_distribution_cdf*             z_cdf,
                             const workload_operation_selector& op_selector,
                             int32_t client_id, const tpcc_configs& configs );
    ~tpcc_workload_generator();

    ALWAYS_INLINE workload_operation_enum get_operation();
    ALWAYS_INLINE uint64_t get_num_ops_before_timer_check() const;

    ALWAYS_INLINE int32_t generate_warehouse_id();
    ALWAYS_INLINE int32_t generate_district_id();
    ALWAYS_INLINE int32_t generate_customer_id();
    ALWAYS_INLINE int32_t generate_number_order_lines();
    ALWAYS_INLINE int32_t generate_item_id();
    ALWAYS_INLINE int32_t generate_supplier_id();
    ALWAYS_INLINE int32_t generate_warehouse_excluding( int32_t exclude_w_id );

    ALWAYS_INLINE int32_t generate_rand_warehouse_id();
    ALWAYS_INLINE int32_t generate_rand_district_id();
    ALWAYS_INLINE int32_t generate_rand_region_id();
    ALWAYS_INLINE int32_t generate_rand_nation_id();

    ALWAYS_INLINE std::tuple<int32_t, int32_t> generate_scan_warehouse_id();
    ALWAYS_INLINE std::tuple<int32_t, int32_t> generate_scan_district_id();
    ALWAYS_INLINE std::tuple<int32_t, int32_t> generate_scan_item_id();
    ALWAYS_INLINE std::tuple<int32_t, int32_t> generate_scan_supplier_id();

    ALWAYS_INLINE bool is_distributed_new_order();
    ALWAYS_INLINE bool is_distributed_payment();
    ALWAYS_INLINE float generate_payment_amount();

    void add_recent_order_items( const std::vector<int32_t>& items,
                                 int32_t num_recent_orders_to_store );
    std::vector<int32_t> get_recent_item_ids() const;

    std::tuple<order_line, order_line> get_q1_scan_range();
    uint64_t get_q1_delivery_time();

    std::tuple<std::string, std::string> get_q3_customer_state_range();

    std::tuple<order, order> get_q4_order_range();
    std::tuple<uint64_t, uint64_t> get_q4_delivery_time();

    std::tuple<supplier, supplier> get_q5_supplier_range();

    std::tuple<int64_t, int64_t> get_q6_quantity_range();

    std::tuple<item, item> get_q14_item_range();

    std::tuple<nation, nation> get_q8_nation_range();

    std::tuple<stock, stock> get_q11_stock_range();

    std::tuple<float, float>     get_q19_i_price();
    std::tuple<int32_t, int32_t> get_q19_ol_quantity();

    std::tuple<std::string, std::string> get_q22_phone_range();
    std::tuple<int32_t, int32_t>         get_q7_nation_pair();

    distributions        dist_;
    non_uniform_distribution nu_dist_;

    tpcc_configs configs_;

   private:
    workload_operation_selector op_selector_;
    int32_t                     client_id_;

    int32_t client_w_id_;
    int32_t lower_terminal_id_;
    int32_t upper_terminal_id_;

    std::queue<std::vector<int32_t>> recent_orders_;
    std::unordered_map<int32_t, int32_t> order_items_;
};

#include "tpcc_workload_generator-inl.h"
