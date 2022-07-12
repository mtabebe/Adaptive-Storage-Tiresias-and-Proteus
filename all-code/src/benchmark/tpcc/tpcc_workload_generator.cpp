#include "tpcc_workload_generator.h"

#include "record-types/tpcc_primary_key_generation.h"

tpcc_workload_generator::tpcc_workload_generator(
    zipf_distribution_cdf*             z_cdf,
    const workload_operation_selector& op_selector, int32_t client_id,
    const tpcc_configs& configs )
    : dist_( z_cdf ),
      nu_dist_( 223 /* last name*/, 259 /*customer*/, 7911 /*item_id*/ ),
      configs_( configs ),
      op_selector_( op_selector ),
      client_id_( client_id ),
      client_w_id_( 0 ),
      lower_terminal_id_( 0 ),
      upper_terminal_id_( 0 ),
      recent_orders_(),
      order_items_() {

    client_w_id_ = client_id_ % configs_.num_warehouses_;
    int32_t num_cust_per_warehouses =
        configs_.bench_configs_.num_clients_ / configs_.num_warehouses_;

    if( configs_.bench_configs_.num_clients_ < configs_.num_warehouses_ ) {
        num_cust_per_warehouses = 1;
    }
    int32_t num_cust_per_district =
        num_cust_per_warehouses / configs_.num_districts_per_warehouse_;
    if( num_cust_per_warehouses <
        (int32_t) configs_.num_districts_per_warehouse_ ) {
        num_cust_per_district = 1;
    }

    int32_t cust_terminal_id = client_id_ % num_cust_per_district;
    lower_terminal_id_ = ( cust_terminal_id * num_cust_per_district ) %
                         configs.num_districts_per_warehouse_;
    upper_terminal_id_ = ( ( cust_terminal_id + 1 ) * num_cust_per_district ) %
                         configs.num_districts_per_warehouse_;
    if( lower_terminal_id_ > upper_terminal_id_ ) {
        int32_t tmp = lower_terminal_id_;
        lower_terminal_id_ = upper_terminal_id_;
        upper_terminal_id_ = tmp;
    }
}

tpcc_workload_generator::~tpcc_workload_generator() {}

void tpcc_workload_generator::add_recent_order_items(
    const std::vector<int32_t>& items, int32_t num_recent_orders_to_store ) {
    recent_orders_.push( items );
    for( int32_t item : items ) {
        int32_t count = 0;
        auto search = order_items_.find( item );
        if( search != order_items_.end() ) {
            count = search->second;
        }
        order_items_[item] = count + 1;
    }
    while( (int32_t) recent_orders_.size() > num_recent_orders_to_store ) {
        const auto& recent_items = recent_orders_.front();
        for( int32_t item : recent_items ) {
            int32_t count = order_items_[item];
			count = count - 1;
            if( count > 0 ) {
                order_items_[item] = count;
            } else {
                order_items_.erase( item );
            }
        }
        recent_orders_.pop();
    }
}
std::vector<int32_t> tpcc_workload_generator::get_recent_item_ids() const {
    std::vector<int32_t> items;
    for( const auto& entry : order_items_ ) {
        items.push_back( entry.first );
    }
    return items;
}

std::tuple<order_line, order_line>
    tpcc_workload_generator::get_q1_scan_range() {
    std::tuple<int32_t, int32_t> ws = generate_scan_warehouse_id();
    std::tuple<int32_t, int32_t> ds = generate_scan_district_id();

    order_line low_ol;
    low_ol.ol_w_id = std::get<0>( ws );
    low_ol.ol_d_id = std::get<0>( ds );
    low_ol.ol_o_id = 0;
    low_ol.ol_number = order::MIN_OL_CNT;

    order_line high_ol;
    high_ol.ol_w_id = std::get<1>( ws );  // max
    high_ol.ol_d_id = std::get<1>( ds );
    high_ol.ol_number = configs_.max_num_order_lines_per_order_ - 1;
    high_ol.ol_o_id = 0;

    DVLOG( 40 ) << "Generate q1 range:"
                << make_order_line_key( low_ol, configs_ ) << " , "
                << make_order_line_key( high_ol, configs_ )
                << ", low w_id:" << low_ol.ol_w_id
                << ", d_id:" << low_ol.ol_d_id
                << ", high w_id:" << high_ol.ol_w_id
                << ", d_id:" << high_ol.ol_d_id;

    return std::make_tuple<>( low_ol, high_ol );
}
uint64_t tpcc_workload_generator::get_q1_delivery_time() {
    datetime d = get_current_time();
    // go back 15 seconds from the current time
    d.c_since = d.c_since - 15;

    return d.c_since;
}

std::tuple<order, order> tpcc_workload_generator::get_q4_order_range() {

    std::tuple<int32_t, int32_t> ws = generate_scan_warehouse_id();
    std::tuple<int32_t, int32_t> ds = generate_scan_district_id();

    order low_o;
    low_o.o_w_id = std::get<0>( ws );
    low_o.o_d_id = std::get<0>( ds );
    low_o.o_c_id = 0;
    low_o.o_id = 0;

    order high_o;
    high_o.o_w_id = std::get<1>( ws );  // max
    high_o.o_d_id = std::get<1>( ds );
    high_o.o_c_id = configs_.num_customers_per_district_ - 1;
    high_o.o_id = configs_.expected_num_orders_per_cust_;

    DVLOG( 40 ) << "Generate q4 range:" << make_order_key( low_o, configs_ )
                << " , " << make_order_key( high_o, configs_ )
                << ", low w_id:" << low_o.o_w_id << ", d_id:" << low_o.o_d_id
                << ", high w_id:" << high_o.o_w_id
                << ", d_id:" << high_o.o_d_id;

    return std::make_tuple<>( low_o, high_o );
}
std::tuple<uint64_t, uint64_t> tpcc_workload_generator::get_q4_delivery_time() {
    datetime d = get_current_time();
    // look at the previous minute
    d.c_since = d.c_since - 60;
    uint64_t high = d.c_since;
    d.c_since = d.c_since - 60;
    uint64_t low = d.c_since;

    return std::make_tuple<>( low, high );
}

std::tuple<int64_t, int64_t> tpcc_workload_generator::get_q6_quantity_range() {
    int32_t a = dist_.get_uniform_int( 1, 100000 );
    int32_t b = dist_.get_uniform_int( 1, 100000 );
    int64_t   low = (int64_t) std::min( a, b );
    int64_t   high = (int64_t) std::max( a, b );
    return std::make_tuple<>( low, high );
}

std::tuple<item, item> tpcc_workload_generator::get_q14_item_range() {
    item     low;
    item     high;
    std::tuple<int32_t, int32_t>  is = generate_scan_item_id();
    low.i_id = std::get<0>( is );
    high.i_id = std::get<1>( is );
    return std::make_tuple<>( low, high );
}

std::tuple<supplier, supplier>
    tpcc_workload_generator::get_q5_supplier_range() {
    supplier low;
    supplier high;
    std::tuple<int32_t, int32_t> ss = generate_scan_supplier_id();
    low.s_id = std::get<0>( ss );
    high.s_id = std::get<1>( ss );
    return std::make_tuple<>( low, high );
}

std::tuple<std::string, std::string>
    tpcc_workload_generator::get_q3_customer_state_range() {
    int32_t pos = dist_.get_uniform_int( 0, k_all_chars.size() - 2 );
    return std::make_tuple<>( std::string( 1, k_all_chars.at( pos ) ),
                              std::string( 1, k_all_chars.at( pos ) ) );
}

std::tuple<nation, nation> tpcc_workload_generator::get_q8_nation_range() {
    nation low_nation;
    nation high_nation;

    low_nation.n_id = 0;
    high_nation.n_id = nation::NUM_NATIONS - 1;

    low_nation.r_id = 0;
    high_nation.r_id = region::NUM_REGIONS - 1;

    return std::make_tuple<>( low_nation, high_nation );
}

std::tuple<stock, stock> tpcc_workload_generator::get_q11_stock_range() {
    std::tuple<int32_t, int32_t>  is = generate_scan_item_id();
    std::tuple<int32_t, int32_t>  ws = generate_scan_warehouse_id();

    stock low;
    stock high;

    low.s_i_id = std::get<0>( is );
    high.s_i_id = std::get<1>( is );

    low.s_w_id = std::get<0>( ws );
    high.s_w_id = std::get<1>( ws );

    return std::make_tuple<>( low, high );
}

std::tuple<float, float> tpcc_workload_generator::get_q19_i_price() {
    float a = (float) dist_.get_uniform_int( 1, 40000 );
    float b = (float) dist_.get_uniform_int( 1, 40000 );
    return std::make_tuple<>( std::min( a, b ), std::max( a, b ) );
}

std::tuple<int32_t, int32_t> tpcc_workload_generator::get_q19_ol_quantity() {
    int32_t a = dist_.get_uniform_int( 1, 10 );
    int32_t b = dist_.get_uniform_int( 1, 10 );
    return std::make_tuple<>( std::min( a, b ), std::max( a, b ) );
}

std::tuple<std::string, std::string>
    tpcc_workload_generator::get_q22_phone_range() {
    int32_t a = dist_.get_uniform_int( 1, k_all_nums.size() );
    int32_t b = dist_.get_uniform_int( 1, k_all_nums.size() );

    std::string low = std::string( 1, k_all_nums.at( std::min( a, b ) ) );
    std::string high = std::string( 1, k_all_nums.at( std::max( a, b ) ) );
    return std::make_tuple<>( low, high );
}

std::tuple<int32_t, int32_t> tpcc_workload_generator::get_q7_nation_pair() {
    int32_t n1 = generate_rand_nation_id();
    int32_t n2 = generate_rand_nation_id();
    return std::make_tuple<>( std::min( n1, n2 ), std::max( n1, n2 ) );
}
