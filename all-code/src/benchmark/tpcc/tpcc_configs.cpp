#include "tpcc_configs.h"

#include <glog/logging.h>

#include "tpcc_table_sizes.h"

tpcc_layout_configs construct_tpcc_layout_configs(
    uint32_t                       warehouse_column_partition_size,
    const partition_type::type&    warehouse_part_type,
    const storage_tier_type::type& warehouse_storage_type,
    uint32_t                       item_column_partition_size,
    const partition_type::type&    item_part_type,
    const storage_tier_type::type& item_storage_type,
    uint32_t                       stock_column_partition_size,
    const partition_type::type&    stock_part_type,
    const storage_tier_type::type& stock_storage_type,
    uint32_t                       district_column_partition_size,
    const partition_type::type&    district_part_type,
    const storage_tier_type::type& district_storage_type,
    uint32_t                       customer_column_partition_size,
    const partition_type::type&    customer_part_type,
    const storage_tier_type::type& customer_storage_type,
    uint32_t                       history_column_partition_size,
    const partition_type::type&    history_part_type,
    const storage_tier_type::type& history_storage_type,
    uint32_t                       order_line_column_partition_size,
    const partition_type::type&    order_line_part_type,
    const storage_tier_type::type& order_line_storage_type,
    uint32_t                       new_order_column_partition_size,
    const partition_type::type&    new_order_part_type,
    const storage_tier_type::type& new_order_storage_type,
    uint32_t                       order_column_partition_size,
    const partition_type::type&    order_part_type,
    const storage_tier_type::type& order_storage_type,
    uint32_t                       customer_district_column_partition_size,
    const partition_type::type&    customer_district_part_type,
    const storage_tier_type::type& customer_district_storage_type,
    uint32_t                       region_column_partition_size,
    const partition_type::type&    region_part_type,
    const storage_tier_type::type& region_storage_type,
    uint32_t                       nation_column_partition_size,
    const partition_type::type&    nation_part_type,
    const storage_tier_type::type& nation_storage_type,
    uint32_t                       supplier_column_partition_size,
    const partition_type::type&    supplier_part_type,
    const storage_tier_type::type& supplier_storage_type ) {
    tpcc_layout_configs configs;

    configs.warehouse_column_partition_size_ = warehouse_column_partition_size;
    configs.warehouse_part_type_ = warehouse_part_type;
    configs.warehouse_storage_type_ = warehouse_storage_type;

    configs.item_column_partition_size_ = item_column_partition_size;
    configs.item_part_type_ = item_part_type;
    configs.item_storage_type_ = item_storage_type;

    configs.stock_column_partition_size_ = stock_column_partition_size;
    configs.stock_part_type_ = stock_part_type;
    configs.stock_storage_type_ = stock_storage_type;

    configs.district_column_partition_size_ = district_column_partition_size;
    configs.district_part_type_ = district_part_type;
    configs.district_storage_type_ = district_storage_type;

    configs.customer_column_partition_size_ = customer_column_partition_size;
    configs.customer_part_type_ = customer_part_type;
    configs.customer_storage_type_ = customer_storage_type;

    configs.history_column_partition_size_ = history_column_partition_size;
    configs.history_part_type_ = history_part_type;
    configs.history_storage_type_ = history_storage_type;

    configs.order_line_column_partition_size_ =
        order_line_column_partition_size;
    configs.order_line_part_type_ = order_line_part_type;
    configs.order_line_storage_type_ = order_line_storage_type;

    configs.new_order_column_partition_size_ = new_order_column_partition_size;
    configs.new_order_part_type_ = new_order_part_type;
    configs.new_order_storage_type_ = new_order_storage_type;

    configs.order_column_partition_size_ = order_column_partition_size;
    configs.order_part_type_ = order_part_type;
    configs.order_storage_type_ = order_storage_type;

    configs.customer_district_column_partition_size_ =
        customer_district_column_partition_size;
    configs.customer_district_part_type_ = customer_district_part_type;
    configs.customer_district_storage_type_ = customer_district_storage_type;

    configs.region_column_partition_size_ = region_column_partition_size;
    configs.region_part_type_ = region_part_type;
    configs.region_storage_type_ = region_storage_type;

    configs.nation_column_partition_size_ = nation_column_partition_size;
    configs.nation_part_type_ = nation_part_type;
    configs.nation_storage_type_ = nation_storage_type;

    configs.supplier_column_partition_size_ = supplier_column_partition_size;
    configs.supplier_part_type_ = supplier_part_type;
    configs.supplier_storage_type_ = supplier_storage_type;

    return configs;
}

tpcc_configs construct_tpcc_configs(
    uint32_t num_warehouses, uint64_t num_items, uint64_t num_suppliers,
    uint32_t expected_num_orders_per_cust, uint32_t item_partition_size,
    uint32_t partition_size, uint32_t district_partition_size,
    uint32_t customer_partition_size, uint32_t supplier_partition_size,
    uint32_t new_order_within_warehouse_likelihood,
    uint32_t payment_within_warehouse_likelihood,
    bool track_and_use_recent_items, uint32_t c_num_clients,
    uint32_t h_num_clients, const benchmark_configs& bench_configs,
    uint32_t delivery_prob, uint32_t new_order_prob, uint32_t order_status_prob,
    uint32_t payment_prob, uint32_t stock_level_prob, uint32_t q1_prob,
    uint32_t q2_prob, uint32_t q3_prob, uint32_t q4_prob, uint32_t q5_prob,
    uint32_t q6_prob, uint32_t q7_prob, uint32_t q8_prob, uint32_t q9_prob,
    uint32_t q10_prob, uint32_t q11_prob, uint32_t q12_prob, uint32_t q13_prob,
    uint32_t q14_prob, uint32_t q15_prob, uint32_t q16_prob, uint32_t q17_prob,
    uint32_t q18_prob, uint32_t q19_prob, uint32_t q20_prob, uint32_t q21_prob,
    uint32_t q22_prob, uint32_t h_all_prob,
    uint32_t num_districts_per_warehouse, uint32_t num_customers_per_district,
    uint32_t max_num_order_lines_per_order,
    uint32_t initial_num_customers_per_district,
    bool     limit_number_of_records_propagated,
    uint64_t number_of_updates_needed_for_propagation, bool use_warehouse,
    bool use_district, bool h_scans_full_tables,
    const tpcc_layout_configs& layouts ) {

    tpcc_configs configs;

    configs.num_warehouses_ = num_warehouses;
    DCHECK_LE( num_items, item::NUM_ITEMS );

    configs.num_items_ = num_items;

    configs.num_suppliers_ = num_suppliers;

    DCHECK_LE( num_districts_per_warehouse,
               district::NUM_DISTRICTS_PER_WAREHOUSE );
    configs.num_districts_per_warehouse_ = num_districts_per_warehouse;
    DCHECK_LE( num_customers_per_district,
               customer::NUM_CUSTOMERS_PER_DISTRICT );
    configs.num_customers_per_district_ = num_customers_per_district;
    DCHECK_GE( max_num_order_lines_per_order, order::MIN_OL_CNT );
    DCHECK_LE( max_num_order_lines_per_order, order::MAX_OL_CNT );
    configs.max_num_order_lines_per_order_ = max_num_order_lines_per_order;

    DCHECK_LE( initial_num_customers_per_district,
               configs.num_customers_per_district_ );
    configs.initial_num_customers_per_district_ =
        initial_num_customers_per_district;

    configs.expected_num_orders_per_cust_ = expected_num_orders_per_cust;

    configs.item_partition_size_ = item_partition_size;
    configs.partition_size_ = partition_size;
	configs.district_partition_size_ = district_partition_size;
    configs.customer_partition_size_ = customer_partition_size;
    configs.supplier_partition_size_ = supplier_partition_size;

    configs.new_order_within_warehouse_likelihood_ =
        new_order_within_warehouse_likelihood;
    configs.payment_within_warehouse_likelihood_ =
        payment_within_warehouse_likelihood;
    configs.track_and_use_recent_items_ = track_and_use_recent_items;

    configs.limit_number_of_records_propagated_ =
        limit_number_of_records_propagated;
    configs.number_of_updates_needed_for_propagation_ =
        number_of_updates_needed_for_propagation;

    configs.use_warehouse_ = use_warehouse;
    configs.use_district_ = use_district;
    configs.h_scans_full_tables_ = h_scans_full_tables;

    configs.layouts_ = layouts;

    configs.c_num_clients_ = c_num_clients;
    configs.h_num_clients_ = h_num_clients;

    configs.bench_configs_ = bench_configs;
    configs.bench_configs_.num_clients_ =
        configs.c_num_clients_ + configs.h_num_clients_;

    configs.c_workload_probs_ = {delivery_prob, new_order_prob, order_status_prob,
                               payment_prob, stock_level_prob};
    uint32_t cum_workload_prob = 0;
    for( uint32_t prob : configs.c_workload_probs_ ) {
        DCHECK_GE( prob, 0 );
        cum_workload_prob += prob;
    }
    DCHECK_EQ( 100, cum_workload_prob );

    configs.h_workload_probs_ = {
        q1_prob,  q2_prob,  q3_prob,  q4_prob,  q5_prob,   q6_prob,
        q7_prob,  q8_prob,  q9_prob,  q10_prob, q11_prob,  q12_prob,
        q13_prob, q14_prob, q15_prob, q16_prob, q17_prob,  q18_prob,
        q19_prob, q20_prob, q21_prob, q22_prob, h_all_prob};
    cum_workload_prob = 0;
    for( uint32_t prob : configs.h_workload_probs_ ) {
        DCHECK_GE( prob, 0 );
        cum_workload_prob += prob;
    }
    DCHECK_EQ( 100, cum_workload_prob );

    DVLOG( 1 ) << "Created: " << configs;

    return configs;
}

std::ostream& operator<<( std::ostream&              os,
                          const tpcc_layout_configs& configs ) {
    os << "TPCC Layout Config: [ "
       << " warehouse_column_partition_size_: "
       << configs.warehouse_column_partition_size_
       << ", warehouse_part_type_:" << configs.warehouse_part_type_
       << ", warehouse_storage_type_:" << configs.warehouse_storage_type_
       << ", item_column_partition_size_:"
       << configs.item_column_partition_size_
       << ", item_part_type_:" << configs.item_part_type_
       << ", item_storage_type_:" << configs.item_storage_type_
       << " , stock_column_partition_size_:"
       << configs.stock_column_partition_size_
       << ", stock_part_type_:" << configs.stock_part_type_
       << ", stock_storage_type_:" << configs.stock_storage_type_
       << ", district_column_partition_size_:"
       << configs.district_column_partition_size_
       << ", district_part_type_:" << configs.district_part_type_
       << ", district_storage_type_:" << configs.district_storage_type_
       << ", customer_column_partition_size_:"
       << configs.customer_column_partition_size_
       << ", customer_part_type_:" << configs.customer_part_type_
       << ", customer_storage_type_:" << configs.customer_storage_type_
       << ", history_column_partition_size_:"
       << configs.history_column_partition_size_
       << ", history_part_type_:" << configs.history_part_type_
       << ", history_storage_type_:" << configs.history_storage_type_
       << " , order_column_partition_size_:"
       << configs.order_line_column_partition_size_
       << ", order_line_part_type_:" << configs.order_line_part_type_
       << ", order_line_storage_type_:" << configs.order_line_storage_type_
       << ", new_order_column_partition_size_:"
       << configs.new_order_column_partition_size_
       << ", new_order_part_type_:" << configs.new_order_part_type_
       << ", new_order_storage_type_:" << configs.new_order_storage_type_
       << ", order_column_partition_size_:"
       << configs.order_column_partition_size_
       << ", order_part_type_:" << configs.order_part_type_
       << ", order_storage_type_:" << configs.order_storage_type_
       << ", customer_district_column_partition_size_:"
       << configs.customer_district_column_partition_size_
       << ", customer_district_part_type_:"
       << configs.customer_district_part_type_
       << ", customer_district_storage_type_:"
       << configs.customer_district_storage_type_
       << " region_column_partition_size_: "
       << configs.region_column_partition_size_
       << ", region_part_type_:" << configs.region_part_type_
       << ", region_storage_type_:" << configs.region_storage_type_
       << " nation_column_partition_size_: "
       << configs.nation_column_partition_size_
       << ", nation_part_type_:" << configs.nation_part_type_
       << ", nation_storage_type_:" << configs.nation_storage_type_
       << " supplier_column_partition_size_: "
       << configs.supplier_column_partition_size_
       << ", supplier_part_type_:" << configs.supplier_part_type_
       << ", supplier_storage_type_:" << configs.supplier_storage_type_ << " ]";
    return os;
}

std::ostream& operator<<( std::ostream& os, const tpcc_configs& config ) {
    os << "TPCC Config: [ "
       << " Num Warehouses:" << config.num_warehouses_ << ","
       << ", Num Items:" << config.num_items_ << ","
       << ", Num Suppliers:" << config.num_suppliers_ << ","
       << ", Expected Num Orders Per Cust:"
       << config.expected_num_orders_per_cust_
       << ", Num Districts Per Warehouse:"
       << config.num_districts_per_warehouse_
       << ", Num Customers Per District:" << config.num_customers_per_district_
       << ", Num Order Lines Per Order:"
       << config.max_num_order_lines_per_order_
       << ", Initial Num Customers Per District:"
       << config.initial_num_customers_per_district_
       << ", Item Partition Size:" << config.item_partition_size_
       << ", Partition Size:" << config.partition_size_
       << ", District Partition Size:" << config.district_partition_size_
       << ", Customer Partition Size:" << config.customer_partition_size_
       << ", Supplier Partition Size:" << config.supplier_partition_size_
       << ", New Order Within Warehouse Likelihood:"
       << config.new_order_within_warehouse_likelihood_ << ", "
       << ", Track and Use Recent Items:" << config.track_and_use_recent_items_
       << "Limit number of records propagated:"
       << config.limit_number_of_records_propagated_ << ", "
       << "Number of updates needed for propagation:"
       << config.number_of_updates_needed_for_propagation_ << ", "
       << ", Payment Within Warehouse Likelihood:"
       << config.payment_within_warehouse_likelihood_
       << ", Use Warehouse:" << config.use_warehouse_
       << ", Use District:" << config.use_district_
       << ", H Scans Full Tables:" << config.h_scans_full_tables_ << ", "
       << config.layouts_ << ", C Num Clients:" << config.c_num_clients_
       << ", H Num Clients:" << config.h_num_clients_ << ", "
       << config.bench_configs_ << " ]";
    return os;
}

std::vector<table_metadata> create_tpcc_table_metadata(
    const tpcc_configs& configs, int32_t site ) {
    std::vector<table_metadata> table_metas;

    int32_t  chain_size = k_num_records_in_chain;
    int32_t  chain_size_for_snapshot = k_num_records_in_snapshot_chain;
    uint32_t site_location = site;

    std::vector<table_partition_information> table_info =
        get_tpcc_data_sizes( configs );

    std::vector<uint32_t> table_ids = {
        k_tpcc_warehouse_table_id,  k_tpcc_item_table_id,
        k_tpcc_stock_table_id,      k_tpcc_district_table_id,
        k_tpcc_customer_table_id,   k_tpcc_history_table_id,
        k_tpcc_order_line_table_id, k_tpcc_new_order_table_id,
        k_tpcc_order_table_id,      k_tpcc_customer_district_table_id,
        k_tpcc_region_table_id,     k_tpcc_nation_table_id,
        k_tpcc_supplier_table_id,
    };
    std::vector<std::string> table_names = {
        "tpcc_warehouse",         "tpcc_item",      "tpcc_stock",
        "tpcc_district",          "tpcc_customer",  "tpcc_history",
        "tpcc_order_line",        "tpcc_new_order", "tpcc_order",
        "tpcc_customer_district", "tpch_region",    "tpch_nation",
        "tpch_supplier"};

    DCHECK_EQ( table_info.size(), table_ids.size() );
    DCHECK_EQ( table_info.size(), table_names.size() );

    for( uint32_t table_id : table_ids ) {
        auto t_info = table_info.at( table_id );

        DCHECK_EQ( table_id, std::get<0>( t_info ) );
        // create tables
        table_metas.push_back( create_table_metadata(
            table_names.at( table_id ), table_id,
            std::get<4>( t_info ) /* num columns*/,
            std::get<5>( t_info ) /* col types*/, chain_size,
            chain_size_for_snapshot, site_location,
            /* partition size*/ std::get<2>( t_info ), /* col size*/
            std::get<3>( t_info ),
            /* partition tracking size*/ std::get<2>(
                t_info ), /* col tracking size*/
            std::get<3>( t_info ),
            /* part type*/ std::get<6>( t_info ),
            /* storage type*/ std::get<7>( t_info ) ) );
    }

    return table_metas;
}

