#include "tpch_mr.h"

#include "../../common/string_conversion.h"
#include "tpcc_db_operators.h"

q4_order_line_probe::q4_order_line_probe(
    const std::unordered_map<q4_order_kv_types>& probe,
    const tpcc_configs&                          cfg )
    : probe_( probe ), cfg_( cfg ) {}

q14_order_line_probe::q14_order_line_probe(
    const std::unordered_map<q14_item_kv_types>& probe,
    const tpcc_configs&                          cfg )
    : probe_( probe ), cfg_( cfg ) {}

q1_agg::q1_agg()
    : quantity_count_( 0 ),
      quantity_sum_( 0 ),
      amount_count_( 0 ),
      amount_sum_( 0 ) {}

void q1_agg::reduce( const q1_agg& val ) {
    quantity_count_ += val.quantity_count_;
    quantity_sum_ += val.quantity_sum_;
    amount_count_ += val.amount_count_;
    amount_sum_ += val.amount_sum_;
}

std::tuple<bool, int32_t, q1_agg> q1_map( const result_tuple& res ) {
    bool   found = false;
    q1_agg ret;

    order_line ol;

    auto read_cols = read_from_scan_order_line( res, &ol );

    found = read_cols.count( order_line_cols::ol_number ) == 1;

    if( read_cols.count( order_line_cols::ol_quantity ) == 1 ) {
        ret.quantity_count_ += 1;
        ret.quantity_sum_ += ol.ol_quantity;
    }
    if( read_cols.count( order_line_cols::ol_amount ) == 1 ) {
        ret.amount_count_ += 1;
        ret.amount_sum_ += ol.ol_amount;
    }

    return std::make_tuple<>( found, ol.ol_number, ret );
}

void q1_mapper( const std::vector<result_tuple>& res_tuples,
                std::unordered_map<q1_kv_types>& res, const EmptyProbe& p ) {
    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, int32_t /* ol number */, q1_agg>
            mapped_res = q1_map( res_tuple );
        DVLOG( 40 ) << "Q1 map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res )
                    << ", ol_number:" << std::get<1>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            } else {
                found->second.reduce( std::get<2>( mapped_res ) );
            }
        }
    }
}

void q1_reducer( const std::unordered_map<q1_kv_vec_types>& input,
                 std::unordered_map<q1_kv_types>& res, const EmptyProbe& p ) {
    for( const auto& entry : input ) {
        q1_agg merge_res;

        for( const auto& val : entry.second ) {
            merge_res.reduce( val );
        }

        res[entry.first] = merge_res;
        DVLOG( 40 ) << "Q1 reduce:" << entry.first
                    << ", quantity_count_:" << merge_res.quantity_count_
                    << ", quantity_sum_:" << merge_res.quantity_sum_
                    << ", amount_count_:" << merge_res.amount_count_
                    << ", amount_sum_:" << merge_res.amount_sum_;
    }
}

std::tuple<bool, uint64_t, order> q4_order_map( const result_tuple& res ) {
    bool  found = false;
    order o;
    o.o_id = -1;
    o.o_d_id = -1;
    o.o_w_id = -1;
    o.o_ol_cnt = -1;
    o.o_c_id = -1;
    o.o_entry_d.c_since = datetime::EMPTY_DATE;

    auto read_cols = read_from_scan_order( res, &o );

    found = !read_cols.empty();
    return std::make_tuple<>( found, res.row_id, o );
}

void q4_order_reduce( order& o, const order& other ) {
    if( o.o_id < 0 ) {
        o.o_id = other.o_id;
    }
    if( o.o_w_id < 0 ) {
        o.o_w_id = other.o_w_id;
    }
    if( o.o_d_id < 0 ) {
        o.o_d_id = other.o_d_id;
    }
    if( o.o_ol_cnt < 0 ) {
        o.o_ol_cnt = other.o_ol_cnt;
    }
    if( o.o_entry_d.c_since == datetime::EMPTY_DATE ) {
        o.o_entry_d = other.o_entry_d;
    }
}

std::tuple<bool, uint64_t, uint64_t> q4_order_line_map(
    const result_tuple& res, const tpcc_configs& tpcc_cfg ) {
    bool       found = false;
    order_line ol;
    ol.ol_delivery_d.c_since = datetime::EMPTY_DATE;

    auto read_cols = read_from_scan_order_line( res, &ol );

    found = !read_cols.empty();
    uint64_t key = 0;
    if( !found ) {
        return std::make_tuple<>( found, key, ol.ol_delivery_d.c_since );
    }

    found = ( read_cols.count( order_line_cols::ol_o_id ) == 1 ) and
            ( read_cols.count( order_line_cols::ol_w_id ) == 1 ) and
            ( read_cols.count( order_line_cols::ol_d_id ) == 1 );

    if( found ) {
        order o;
        o.o_id = ol.ol_o_id;
        o.o_w_id = ol.ol_w_id;
        o.o_d_id = ol.ol_d_id;
        o.o_c_id = 0;

        key = make_order_key( o, tpcc_cfg );
    }

    return std::make_tuple<>( found, key, ol.ol_delivery_d.c_since );
}

void q4_order_mapper( const std::vector<result_tuple>&       res_tuples,
                      std::unordered_map<q4_order_kv_types>& res,
                      const EmptyProbe&                      p ) {
    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, uint64_t /* o number */, order>
            mapped_res = q4_order_map( res_tuple );
        DVLOG( 40 ) << "Q4 map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            } else {
                q4_order_reduce( found->second, std::get<2>( mapped_res ) );
            }
        }
    }
}

void q4_order_reducer( const std::unordered_map<q4_order_kv_vec_types>& input,
                       std::unordered_map<q4_order_kv_types>&           res,
                       const EmptyProbe&                                p ) {
    for( const auto& entry : input ) {
        order merge_res;
        merge_res.o_id = -1;
        merge_res.o_d_id = -1;
        merge_res.o_w_id = -1;
        merge_res.o_ol_cnt = -1;
        merge_res.o_entry_d.c_since = datetime::EMPTY_DATE;

        for( const auto& val : entry.second ) {
            q4_order_reduce( merge_res, val );
        }

        res[entry.first] = merge_res;
        DVLOG( 40 ) << "Q4 reduce:" << entry.first;
    }
}

void q4_order_line_mapper( const std::vector<result_tuple>& res_tuples,
                           std::unordered_map<q4_order_line_kv_types>& res,
                           const q4_order_line_probe&                  probe ) {
    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, uint64_t /* o number */, uint64_t>
            mapped_res = q4_order_line_map( res_tuple, probe.cfg_ );
        DVLOG( 40 ) << "Q4 map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            } else {
                std::max( found->second, std::get<2>( mapped_res ) );
            }
        }
    }
}

void q4_order_line_reducer(
    const std::unordered_map<q4_order_line_kv_vec_types>& input,
    std::unordered_map<q4_order_line_kv_types>&           res,
    const q4_order_line_probe&                            probe ) {

    for( const auto& entry : input ) {
        uint64_t order_key = entry.first;
        auto     found = probe.probe_.find( order_key );
        if( ( found != probe.probe_.end() ) and ( entry.second.size() > 0 ) ) {
            uint64_t t =
                *std::max_element( entry.second.begin(), entry.second.end() );
            if( t >= found->second.o_entry_d.c_since ) {
                res[order_key] = t;
            }
        }
    }
}

std::tuple<bool, int32_t, float> q6_map( const result_tuple& res ) {
    bool   found = false;

    order_line ol;

    auto read_cols = read_from_scan_order_line( res, &ol );

    found = ( read_cols.count( order_line_cols::ol_amount ) == 1 );
    return std::make_tuple<>( found, 0, ol.ol_amount );
}

void q6_mapper( const std::vector<result_tuple>& res_tuples,
                std::unordered_map<q6_kv_types>& res, const EmptyProbe& p ) {
    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, int32_t /*  dummy key */, float>
            mapped_res = q6_map( res_tuple );
        DVLOG( 40 ) << "Q6 map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res )
                    << ", key:" << std::get<1>( mapped_res )
                    << ", amount:" << std::get<2>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            res[std::get<1>( mapped_res )] += std::get<2>( mapped_res );
        }
    }
}

void q6_reducer( const std::unordered_map<q6_kv_vec_types>& input,
                 std::unordered_map<q6_kv_types>& res, const EmptyProbe& p ) {
    for( const auto& entry : input ) {
        float merge_res = 0;

        for( const auto& val : entry.second ) {
            merge_res += val;
        }

        res[entry.first] = merge_res;
        DVLOG( 40 ) << "QQ reduce:" << entry.first
                    << ", ol_amount:" << merge_res;
    }
}

std::tuple<bool, int32_t, item> q14_item_map( const result_tuple& res ) {
    bool  found = false;
    item i;
    i.i_id = -1;
    i.i_data = "";

    auto read_cols = read_from_scan_item( res, &i );
    i.i_id = (int32_t) res.row_id;

    found = !read_cols.empty();
    return std::make_tuple<>( found, (int32_t) res.row_id, i );
}

void q14_item_reduce( item& i, const item& other ) {
    if( i.i_id < 0 ) {
        i.i_id = other.i_id;
    }
    if( i.i_data.empty() ) {
        i.i_data = other.i_data;
    }
}
void q14_order_line_reduce( order_line& ol, const order_line& other ) {
    if( ol.ol_i_id < 0 ) {
        ol.ol_i_id = other.ol_i_id;
    }
    ol.ol_amount += other.ol_amount;
}

void q14_item_mapper( const std::vector<result_tuple>&       res_tuples,
                      std::unordered_map<q14_item_kv_types>& res,
                      const EmptyProbe&                      p ) {
    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, int32_t /* i_id */, item> mapped_res =
            q14_item_map( res_tuple );
        DVLOG( 40 ) << "Q14 map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            } else {
                q14_item_reduce( found->second, std::get<2>( mapped_res ) );
            }
        }
    }
}

void q14_item_reducer( const std::unordered_map<q14_item_kv_vec_types>& input,
                       std::unordered_map<q14_item_kv_types>&           res,
                       const EmptyProbe&                                p ) {
    for( const auto& entry : input ) {
        item merge_res;
        merge_res.i_id = -1;
        merge_res.i_data = "";

        for( const auto& val : entry.second ) {
            q14_item_reduce( merge_res, val );
        }

        res[entry.first] = merge_res;
        DVLOG( 40 ) << "Q14 reduce:" << entry.first;
    }
}

std::tuple<bool, int32_t, order_line> q14_order_line_map(
    const result_tuple& res, const tpcc_configs& tpcc_cfg ) {
    bool       found = false;
    order_line ol;
    ol.ol_i_id = 0;
    ol.ol_amount = 0;

    auto read_cols = read_from_scan_order_line( res, &ol );

    found = ( read_cols.count( order_line_cols::ol_i_id ) == 1 );

    return std::make_tuple<>( found, ol.ol_i_id, ol );
}

void q14_order_line_mapper( const std::vector<result_tuple>& res_tuples,
                            std::unordered_map<q14_order_line_kv_types>& res,
                            const q14_order_line_probe& probe ) {
    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, int32_t /* ol_i_id */, order_line>
            mapped_res = q14_order_line_map( res_tuple, probe.cfg_ );
        DVLOG( 40 ) << "Q4 map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            } else {
                q14_order_line_reduce( found->second,
                                       std::get<2>( mapped_res ) );
            }
        }
    }
}

void q14_order_line_reducer(
    const std::unordered_map<q14_order_line_kv_vec_types>& input,
    std::unordered_map<q14_order_line_kv_types>&           res,
    const q14_order_line_probe&                            probe ) {
    for( const auto& entry : input ) {
        int32_t i_id = entry.first;
        auto    found = probe.probe_.find( i_id );
        if( ( found != probe.probe_.end() ) and ( entry.second.size() > 0 ) ) {
            order_line merge_res = entry.second.at( 0 );
            for( uint32_t pos = 1; pos < entry.second.size(); pos++ ) {
                q14_order_line_reduce( merge_res, entry.second.at( pos ) );
            }
            res[i_id] = merge_res;
        }
    }
}

double compute_q14_value( const item& i, const order_line& ol ) {
    double num = 0;
    if( i.i_data.size() >= 2 ) {
        if( i.i_data.at( 0 ) == 'P' and i.i_data.at( 1 ) == 'R' ) {
            num = ol.ol_amount;
        }
    }
    double denom = 1 + ol.ol_amount;

    return ( num / denom );
}
std::tuple<bool, uint64_t, float> q5_order_line_map(
    const result_tuple& res, const tpcc_configs& tpcc_cfg ) {
    bool       found = false;
    order_line ol;
    ol.ol_amount = 0;

    auto read_cols = read_from_scan_order_line( res, &ol );

    found = !read_cols.empty();
    uint64_t key = 0;
    if( !found ) {
        return std::make_tuple<>( found, key, ol.ol_amount );
    }

    found = ( read_cols.count( order_line_cols::ol_i_id ) == 1 ) and
            ( read_cols.count( order_line_cols::ol_supply_w_id ) == 1 );

    if( found ) {
        stock s;
        s.s_i_id = ol.ol_i_id;
        s.s_w_id = ol.ol_supply_w_id;

        key = make_stock_key( s, tpcc_cfg );
    }

    return std::make_tuple<>( found, key, ol.ol_amount );
}

void q5_order_line_mapper( const std::vector<result_tuple>& res_tuples,
                           std::unordered_map<q5_order_line_kv_types>& res,
                           const tpcc_configs&                         p ) {
    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, uint64_t /* stock key */, float>
            mapped_res = q5_order_line_map( res_tuple, p );
        DVLOG( 40 ) << "Q5 map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            } else {
                found->second += std::get<2>( mapped_res );
            }
        }
    }
}

void q5_order_line_reducer(
    const std::unordered_map<q5_order_line_kv_vec_types>& input,
    std::unordered_map<q5_order_line_kv_types>& res, const tpcc_configs& p ) {
    for( const auto& entry : input ) {
        float merge_res = 0;
        for( const auto& v : entry.second ) {
            merge_res += v;
        }
        res[entry.first] = merge_res;
    }
}

std::tuple<bool, int32_t, nation> q5_nation_map( const result_tuple& res ) {
    bool   found = false;
    nation n;
    n.n_id = -1;
    n.r_id = -1;
    n.n_name = "";

    auto read_cols = read_from_scan_nation( res, &n );
    n.n_id = (int32_t) res.row_id;

    found = !read_cols.empty();
    int32_t key = (int32_t) res.row_id;

    return std::make_tuple<>( found, key, n );
}

void q5_nation_reduce( nation& n, const nation& other ) {
    if( n.n_id < 0 ) {
        n.n_id = other.n_id;
    }
    if( n.r_id < 0 ) {
        n.r_id = other.r_id;
    }
    if( n.n_name.empty() ) {
        n.n_name = other.n_name;
    }
}

void q5_nation_mapper( const std::vector<result_tuple>&        res_tuples,
                       std::unordered_map<q5_nation_kv_types>& res,
                       const EmptyProbe&                       p ) {
    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, int32_t /* n_id */, nation> mapped_res =
            q5_nation_map( res_tuple );
        DVLOG( 40 ) << "Q5 map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            } else {
                q5_nation_reduce( found->second, std::get<2>( mapped_res ) );
            }
        }
    }
}

void q5_nation_reducer( const std::unordered_map<q5_nation_kv_vec_types>& input,
                        std::unordered_map<q5_nation_kv_types>&           res,
                        const EmptyProbe&                                 p ) {
    for( const auto& entry : input ) {
        nation merge_res;
        merge_res.n_id = -1;
        merge_res.r_id = -1;
        merge_res.n_name = "";

        for( const auto& val : entry.second ) {
            q5_nation_reduce( merge_res, val );
        }

        res[entry.first] = merge_res;
        DVLOG( 40 ) << "Q5 reduce:" << entry.first;
    }
}

q5_supplier_probe::q5_supplier_probe(
    const std::unordered_map<q5_nation_kv_types>& probe,
    const tpcc_configs&                           cfg )
    : probe_( probe ), cfg_( cfg ) {}

std::tuple<bool, int32_t, int32_t> q5_supplier_map(
    const result_tuple& res, const q5_supplier_probe& probe ) {
    bool     found = false;
    supplier s;

    auto read_cols = read_from_scan_supplier( res, &s );
    s.s_id = (int32_t) res.row_id;

    found = read_cols.count( supplier_cols::n_id ) == 1;
    int32_t key = (int32_t) res.row_id;

    if( found ) {
        found = probe.probe_.count( s.n_id ) == 1;
    }

    return std::make_tuple<>( found, key, 1 );
}

void q5_supplier_mapper( const std::vector<result_tuple>&          res_tuples,
                         std::unordered_map<q5_supplier_kv_types>& res,
                         const q5_supplier_probe&                  p ) {
    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, int32_t /* s_id */, int32_t> mapped_res =
            q5_supplier_map( res_tuple, p );
        DVLOG( 40 ) << "Q5 map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            }
        }
    }
}

void q5_supplier_reducer(
    const std::unordered_map<q5_supplier_kv_vec_types>& input,
    std::unordered_map<q5_supplier_kv_types>&           res,
    const q5_supplier_probe&                            p ) {
    for( const auto& entry : input ) {
        res[entry.first] = 1;
        DVLOG( 40 ) << "Q5 reduce:" << entry.first;
    }
}

q5_stock_probe::q5_stock_probe(
    const std::unordered_map<q5_supplier_kv_types>&   s_probe,
    const std::unordered_map<q5_order_line_kv_types>& ol_probe,
    const tpcc_configs&                               cfg )
    : s_probe_( s_probe ), ol_probe_( ol_probe ), cfg_( cfg ) {}

std::tuple<bool, uint64_t, stock> q5_stock_map( const result_tuple& res,
                                                const q5_stock_probe& probe ) {
    bool  found = false;
    stock s;
    s.s_i_id = -1;
    s.s_w_id = -1;
    s.s_s_id = -1;

    auto read_cols = read_from_scan_stock( res, &s );

    found = !read_cols.empty();
    uint64_t key = res.row_id;

    if( found ) {
        found = ( probe.ol_probe_.count( key ) == 1 );
    }

    return std::make_tuple<>( found, key, s );
}

void q5_stock_reduce( stock& s, const stock& other ) {
    if( s.s_i_id < 0 ) {
        s.s_i_id = other.s_i_id;
    }
    if( s.s_w_id < 0 ) {
        s.s_w_id = other.s_w_id;
    }
    if( s.s_s_id < 0 ) {
        s.s_s_id = other.s_s_id;
    }
}

void q5_stock_mapper( const std::vector<result_tuple>&       res_tuples,
                      std::unordered_map<q5_stock_kv_types>& res,
                      const q5_stock_probe&                  p ) {
    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, uint64_t /* stock supplier_id */, stock>
            mapped_res = q5_stock_map( res_tuple, p );
        DVLOG( 40 ) << "Q5 map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            } else {
                q5_stock_reduce( found->second, std::get<2>( mapped_res ) );
            }
        }
    }
}
void q5_stock_reducer( const std::unordered_map<q5_stock_kv_vec_types>& input,
                       std::unordered_map<q5_stock_kv_types>&           res,
                       const q5_stock_probe&                            p ) {
    for( const auto& entry : input ) {
        stock merge_res;
        merge_res.s_i_id = -1;
        merge_res.s_w_id = -1;
        merge_res.s_s_id = -1;

        for( const auto& val : entry.second ) {
            q5_stock_reduce( merge_res, val );
        }

        if( merge_res.s_s_id > 0 ) {
            if( p.s_probe_.count( merge_res.s_s_id ) == 1 ) {
                res[entry.first] = merge_res;
                DVLOG( 40 ) << "Q5 reduce:" << entry.first;
            }
        }
    }
}

q3_order_probe::q3_order_probe(
    const std::unordered_map<q3_customer_kv_types>& probe,
    const tpcc_configs&                             cfg )
    : probe_( probe ), cfg_( cfg ) {}
q3_order_line_probe::q3_order_line_probe(
    const std::unordered_map<q3_order_kv_types>& probe,
    const tpcc_configs&                          cfg )
    : probe_( probe ), cfg_( cfg ) {}

std::tuple<bool /* found */, uint64_t /* customer id */, int32_t>
    q3_customer_map( const result_tuple& res, const tpcc_configs& p ) {
    customer c;

    auto read_cols = read_from_scan_customer( res, &c );
    bool found = !read_cols.empty();

    return std::make_tuple<>( found, res.row_id, 0 );
}

void q3_customer_mapper( const std::vector<result_tuple>&          res_tuples,
                         std::unordered_map<q3_customer_kv_types>& res,
                         const tpcc_configs&                       p ) {
    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, uint64_t /* customer id */, int32_t>
            mapped_res = q3_customer_map( res_tuple, p );
        DVLOG( 40 ) << "Q3 map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            }
        }
    }
}

void q3_customer_reducer(
    const std::unordered_map<q3_customer_kv_vec_types>& input,
    std::unordered_map<q3_customer_kv_types>& res, const tpcc_configs& p ) {
    for( const auto& entry : input ) {
        res[entry.first] = 0;
        DVLOG( 40 ) << "Q1 reduce:" << entry.first;
    }
}

std::tuple<bool, uint64_t, order> q3_order_map( const result_tuple& res,
                                                const tpcc_configs& cfg ) {
    order o;
    o.o_id = -1;
    o.o_c_id = -1;
    o.o_d_id = -1;
    o.o_w_id = -1;
    o.o_c_id = -1;
    o.o_entry_d.c_since = datetime::EMPTY_DATE;

    auto read_cols = read_from_scan_order( res, &o );

    bool found = !read_cols.empty();
    return std::make_tuple<>( found, res.row_id, o );
}

void q3_order_reduce( order& o, const order& other ) {
    if( o.o_id < 0 ) {
        o.o_id = other.o_id;
    }
    if( o.o_c_id < 0 ) {
        o.o_c_id = other.o_c_id;
    }
    if( o.o_w_id < 0 ) {
        o.o_w_id = other.o_w_id;
    }
    if( o.o_d_id < 0 ) {
        o.o_d_id = other.o_d_id;
    }
    if( o.o_entry_d.c_since == datetime::EMPTY_DATE ) {
        o.o_entry_d = other.o_entry_d;
    }
}

void q3_order_mapper( const std::vector<result_tuple>&       res_tuples,
                      std::unordered_map<q3_order_kv_types>& res,
                      const q3_order_probe&                  p ) {
    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, uint64_t /* order id */, order>
            mapped_res = q3_order_map( res_tuple, p.cfg_ );
        DVLOG( 40 ) << "Q3 map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            } else {
                q3_order_reduce( found->second, std::get<2>( mapped_res ) );
            }
        }
    }
}
void q3_order_reducer( const std::unordered_map<q3_order_kv_vec_types>& input,
                       std::unordered_map<q3_order_kv_types>&           res,
                       const q3_order_probe&                            p ) {
    for( const auto& entry : input ) {
        order merge_res;
        merge_res.o_id = -1;
        merge_res.o_c_id = -1;
        merge_res.o_d_id = -1;
        merge_res.o_w_id = -1;
        merge_res.o_entry_d.c_since = datetime::EMPTY_DATE;

        for( const auto& val : entry.second ) {
            q3_order_reduce( merge_res, val );
        }

        if( ( merge_res.o_c_id > 0 ) and ( merge_res.o_d_id > 0 ) and
            ( merge_res.o_w_id > 0 ) ) {
            customer c;
            c.c_id = merge_res.o_c_id;
            c.c_d_id = merge_res.o_d_id;
            c.c_w_id = merge_res.o_w_id;

            uint64_t c_key = make_customer_key( c, p.cfg_ );

            if( p.probe_.count( c_key ) == 1 ) {
                res[entry.first] = merge_res;
                DVLOG( 40 ) << "Q3 reduce:" << entry.first;
            }
        }
    }
}

std::tuple<bool, uint64_t, order_line> q3_order_line_map(
    const result_tuple& res, const q3_order_line_probe& p ) {
    bool       found = false;
    uint64_t   key = 0;
    order_line ol;

    ol.ol_o_id = -1;
    ol.ol_d_id = -1;
    ol.ol_w_id = -1;
    ol.ol_amount = 0;

    auto read_cols = read_from_scan_order_line( res, &ol );

    found = ( ( read_cols.count( order_line_cols::ol_o_id ) == 1 ) and
              ( read_cols.count( order_line_cols::ol_d_id ) == 1 ) and
              ( read_cols.count( order_line_cols::ol_w_id ) == 1 ) );

    if( found ) {
        order o;
        o.o_id = ol.ol_o_id;
        o.o_c_id = 0;
        o.o_d_id = ol.ol_d_id;
        o.o_w_id = ol.ol_w_id;

        key = make_order_key( o, p.cfg_ );

        found = ( p.probe_.count( key ) == 1 );
    }

    return std::make_tuple<>( found, key, ol );
}
void q3_order_line_reduce( order_line& ol, const order_line& other ) {
    if( ol.ol_o_id < 0 ) {
        ol.ol_o_id = other.ol_o_id;
    }
    if( ol.ol_d_id < 0 ) {
        ol.ol_d_id = other.ol_d_id;
    }
    if( ol.ol_w_id < 0 ) {
        ol.ol_w_id = other.ol_w_id;
    }

    ol.ol_amount += other.ol_amount;
}

void q3_order_line_mapper( const std::vector<result_tuple>& res_tuples,
                           std::unordered_map<q3_order_line_kv_types>& res,
                           const q3_order_line_probe&                  p ) {

    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, uint64_t /* order id */, order_line>
            mapped_res = q3_order_line_map( res_tuple, p );
        DVLOG( 40 ) << "Q3 map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            } else {
                q3_order_line_reduce( found->second,
                                      std::get<2>( mapped_res ) );
            }
        }
    }
}
void q3_order_line_reducer(
    const std::unordered_map<q3_order_line_kv_vec_types>& input,
    std::unordered_map<q3_order_line_kv_types>&           res,
    const q3_order_line_probe&                            p ) {
    for( const auto& entry : input ) {
        order_line merge_res;
        merge_res.ol_o_id = -1;
        merge_res.ol_d_id = -1;
        merge_res.ol_w_id = -1;
        merge_res.ol_amount = 0;

        for( const auto& val : entry.second ) {
            q3_order_line_reduce( merge_res, val );
        }

        res[entry.first] = merge_res;
    }
}

void q10_nation_mapper( const std::vector<result_tuple>&         res_tuples,
                        std::unordered_map<q10_nation_kv_types>& res,
                        const EmptyProbe&                        p ) {
    q5_nation_mapper( res_tuples, res, p );
}
void q10_nation_reducer(
    const std::unordered_map<q10_nation_kv_vec_types>& input,
    std::unordered_map<q10_nation_kv_types>& res, const EmptyProbe& p ) {
    q5_nation_reducer( input, res, p );
}

std::tuple<bool /* found */, uint64_t /* customer id */, customer>
    q10_customer_map( const result_tuple& res ) {
    customer c;

    c.c_id = -1;
    c.c_d_id = -1;
    c.c_w_id = -1;
    c.c_n_id = -1;
    c.c_last = "";
    c.c_address.a_city = "";
    c.c_phone = "";

    auto read_cols = read_from_scan_customer( res, &c );
    bool found = !read_cols.empty();

    return std::make_tuple<>( found, res.row_id, c );
}

void q10_customer_reduce( customer& c, const customer& other ) {
    if( c.c_id < 0 ) {
        c.c_id = other.c_id;
    }
    if( c.c_d_id < 0 ) {
        c.c_d_id = other.c_d_id;
    }
    if( c.c_w_id < 0 ) {
        c.c_w_id = other.c_w_id;
    }
    if( c.c_n_id < 0 ) {
        c.c_n_id = other.c_n_id;
    }
    if( c.c_last.empty() ) {
        c.c_last = other.c_last;
    }
    if( c.c_address.a_city.empty() ) {
        c.c_address.a_city = other.c_address.a_city;
    }
    if( c.c_phone.empty() ) {
        c.c_phone = other.c_phone;
    }
}

void q10_customer_mapper( const std::vector<result_tuple>&           res_tuples,
                          std::unordered_map<q10_customer_kv_types>& res,
                          const EmptyProbe&                          p ) {
    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, uint64_t /* customer id */, customer>
            mapped_res = q10_customer_map( res_tuple );
        DVLOG( 40 ) << "Q10 map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            } else {
                q10_customer_reduce( found->second, std::get<2>( mapped_res ) );
            }
        }
    }
}

void q10_customer_reducer(
    const std::unordered_map<q10_customer_kv_vec_types>& input,
    std::unordered_map<q10_customer_kv_types>& res, const EmptyProbe& p ) {

    for( const auto& entry : input ) {
        customer merge_res;

        merge_res.c_id = -1;
        merge_res.c_d_id = -1;
        merge_res.c_w_id = -1;
        merge_res.c_n_id = -1;
        merge_res.c_last = "";
        merge_res.c_address.a_city = "";
        merge_res.c_phone = "";

        for( const auto& val : entry.second ) {
            q10_customer_reduce( merge_res, val );
        }

        res[entry.first] = merge_res;
    }
}

q10_order_probe::q10_order_probe(
    const std::unordered_map<q10_customer_kv_types>& probe,
    const tpcc_configs&                              cfg )
    : probe_( probe ), cfg_( cfg ) {}

std::tuple<bool, uint64_t, std::tuple<uint64_t, uint64_t>> q10_order_map(
    const result_tuple& res, const q10_order_probe& probe ) {

    order o;
    o.o_entry_d.c_since = datetime::EMPTY_DATE;

    auto read_cols = read_from_scan_order( res, &o );

    bool found = ( ( read_cols.count( order_cols::o_id ) == 1 ) and
                   ( read_cols.count( order_cols::o_c_id ) == 1 ) and
                   ( read_cols.count( order_cols::o_d_id ) == 1 ) and
                   ( read_cols.count( order_cols::o_w_id ) == 1 ) );

    uint64_t cust_id = 0;
    if( found ) {
        customer c;
        c.c_id = o.o_c_id;
        c.c_d_id = o.o_d_id;
        c.c_w_id = o.o_w_id;

        cust_id = make_customer_key( c, probe.cfg_ );

        found = ( probe.probe_.count( cust_id ) == 1 );
    }

    return std::make_tuple<>(
        found, res.row_id, std::make_tuple<>( cust_id, o.o_entry_d.c_since ) );
}

void q10_order_reduce( std::tuple<uint64_t, uint64_t>&      t,
                       const std::tuple<uint64_t, uint64_t> other ) {
    t = std::make_tuple<>( std::max( std::get<0>( other ), std::get<0>( t ) ),
                           std::max( std::get<1>( other ), std::get<1>( t ) ) );
}

void q10_order_mapper( const std::vector<result_tuple>&        res_tuples,
                       std::unordered_map<q10_order_kv_types>& res,
                       const q10_order_probe&                  p ) {
    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, uint64_t /* order id */,
                   std::tuple<uint64_t, uint64_t>>
            mapped_res = q10_order_map( res_tuple, p );
        DVLOG( 40 ) << "Q10 map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            } else {
                q10_order_reduce( found->second, std::get<2>( mapped_res ) );
            }
        }
    }
}
void q10_order_reducer( const std::unordered_map<q10_order_kv_vec_types>& input,
                        std::unordered_map<q10_order_kv_types>&           res,
                        const q10_order_probe&                            p ) {
    for( const auto& entry : input ) {
        std::tuple<uint64_t, uint64_t> merge_res =
            std::make_tuple<>( 0, datetime::EMPTY_DATE );
        for( const auto& val : entry.second ) {
            q10_order_reduce( merge_res, val );
        }

        res[entry.first] = merge_res;
    }
}

q10_order_line_probe::q10_order_line_probe(
    const std::unordered_map<q10_order_kv_types>& probe,
    const tpcc_configs&                           cfg )
    : probe_( probe ), cfg_( cfg ) {}

std::tuple<bool, uint64_t, float> q10_order_line_map(
    const result_tuple& res_tuple, const q10_order_line_probe& probe ) {

    order_line ol;
    ol.ol_amount = 0;

    auto read_cols = read_from_scan_order_line( res_tuple, &ol );

    bool found = ( ( read_cols.count( order_line_cols::ol_o_id ) == 1 ) and
                   ( read_cols.count( order_line_cols::ol_d_id ) == 1 ) and
                   ( read_cols.count( order_line_cols::ol_w_id ) == 1 ) );

    uint64_t key = 0;
    if( found ) {
        order o;
        o.o_id = ol.ol_o_id;
        o.o_d_id = ol.ol_d_id;
        o.o_w_id = ol.ol_w_id;
        o.o_c_id = 0;

        key = make_order_key( o, probe.cfg_ );

        found = ( probe.probe_.count( key ) == 1 );
    }

    return std::make_tuple<>( found, key, ol.ol_amount );
}

void q10_order_line_mapper( const std::vector<result_tuple>& res_tuples,
                            std::unordered_map<q10_order_line_kv_types>& res,
                            const q10_order_line_probe&                  p ) {
    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, uint64_t /* order id */, float>
            mapped_res = q10_order_line_map( res_tuple, p );
        DVLOG( 40 ) << "Q10 map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            } else {
                found->second += std::get<2>( mapped_res );
            }
        }
    }
}
void q10_order_line_reducer(
    const std::unordered_map<q10_order_line_kv_vec_types>& input,
    std::unordered_map<q10_order_line_kv_types>&           res,
    const q10_order_line_probe&                            p ) {
    for( const auto& entry : input ) {
        float merge_res = 0;
        for( const auto& val : entry.second ) {
            merge_res += val;
        }
        res[entry.first] = merge_res;
    }
}

std::tuple<bool /* found */, uint64_t /* customer id */, customer>
    q18_customer_map( const result_tuple& res ) {
    customer c;

    c.c_id = -1;
    c.c_d_id = -1;
    c.c_w_id = -1;
    c.c_last = "";

    auto read_cols = read_from_scan_customer( res, &c );
    bool found = !read_cols.empty();

    return std::make_tuple<>( found, res.row_id, c );
}

void q18_customer_reduce( customer& c, const customer& other ) {
    if( c.c_id < 0 ) {
        c.c_id = other.c_id;
    }
    if( c.c_d_id < 0 ) {
        c.c_d_id = other.c_d_id;
    }
    if( c.c_w_id < 0 ) {
        c.c_w_id = other.c_w_id;
    }
    if( c.c_last.empty() ) {
        c.c_last = other.c_last;
    }
}

void q18_customer_mapper( const std::vector<result_tuple>&           res_tuples,
                          std::unordered_map<q18_customer_kv_types>& res,
                          const EmptyProbe&                          p ) {
    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, uint64_t /* customer id */, customer>
            mapped_res = q18_customer_map( res_tuple );
        DVLOG( 40 ) << "Q18 map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            } else {
                q18_customer_reduce( found->second, std::get<2>( mapped_res ) );
            }
        }
    }
}

void q18_customer_reducer(
    const std::unordered_map<q18_customer_kv_vec_types>& input,
    std::unordered_map<q18_customer_kv_types>& res, const EmptyProbe& p ) {

    for( const auto& entry : input ) {
        customer merge_res;

        merge_res.c_id = -1;
        merge_res.c_d_id = -1;
        merge_res.c_w_id = -1;
        merge_res.c_last = "";

        for( const auto& val : entry.second ) {
            q18_customer_reduce( merge_res, val );
        }

        res[entry.first] = merge_res;
    }
}

q18_order_probe::q18_order_probe(
    const std::unordered_map<q18_customer_kv_types>& probe,
    const tpcc_configs&                              cfg )
    : probe_( probe ), cfg_( cfg ) {}

std::tuple<bool /* found */, uint64_t /* order id */,
           std::tuple<uint64_t /*cust id */, order>>
    q18_order_map( const result_tuple& res, const q18_order_probe& probe ) {
    order o;

    o.o_id = -1;
    o.o_c_id = -1;
    o.o_d_id = -1;
    o.o_w_id = -1;
    o.o_entry_d.c_since = datetime::EMPTY_DATE;

    auto read_cols = read_from_scan_order( res, &o );

    bool found = ( ( read_cols.count( order_cols::o_id ) == 1 ) and
                   ( read_cols.count( order_cols::o_c_id ) == 1 ) and
                   ( read_cols.count( order_cols::o_d_id ) == 1 ) and
                   ( read_cols.count( order_cols::o_w_id ) == 1 ) );

    uint64_t cust_id = 0;
    if( found ) {
        customer c;
        c.c_id = o.o_c_id;
        c.c_d_id = o.o_d_id;
        c.c_w_id = o.o_w_id;

        cust_id = make_customer_key( c, probe.cfg_ );

        found = ( probe.probe_.count( cust_id ) == 1 );
    }

    return std::make_tuple<>( found, res.row_id,
                              std::make_tuple<>( cust_id, o ) );
}

void q18_order_reduce( std::tuple<uint64_t, order>&       o,
                       const std::tuple<uint64_t, order>& other ) {
    uint64_t c_id = std::max( std::get<0>( o ), std::get<0>( other ) );
    order    new_o = std::get<1>( o );
    if( new_o.o_id < 0 ) {
        new_o.o_id = std::get<1>( other ).o_id;
    }
    if( new_o.o_c_id < 0 ) {
        new_o.o_c_id = std::get<1>( other ).o_c_id;
    }
    if( new_o.o_d_id < 0 ) {
        new_o.o_d_id = std::get<1>( other ).o_d_id;
    }
    if( new_o.o_w_id < 0 ) {
        new_o.o_w_id = std::get<1>( other ).o_w_id;
    }
    if( new_o.o_entry_d.c_since == datetime::EMPTY_DATE ) {
        new_o.o_entry_d.c_since = std::get<1>( other ).o_entry_d.c_since;
    }

    o = std::make_tuple<>( c_id, new_o );
}

void q18_order_mapper( const std::vector<result_tuple>&        res_tuples,
                       std::unordered_map<q18_order_kv_types>& res,
                       const q18_order_probe&                  p ) {
    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, uint64_t /* order id */,
                   std::tuple<uint64_t, order>>
            mapped_res = q18_order_map( res_tuple, p );
        DVLOG( 40 ) << "Q18 map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            } else {
                q18_order_reduce( found->second, std::get<2>( mapped_res ) );
            }
        }
    }
}
void q18_order_reducer( const std::unordered_map<q18_order_kv_vec_types>& input,
                        std::unordered_map<q18_order_kv_types>&           res,
                        const q18_order_probe&                            p ) {

    for( const auto& entry : input ) {
        order merge_o;

        merge_o.o_id = -1;
        merge_o.o_d_id = -1;
        merge_o.o_w_id = -1;
        merge_o.o_entry_d.c_since = datetime::EMPTY_DATE;

        uint64_t cust_id = 0;

        std::tuple<uint64_t, order> merge_res =
            std::make_tuple<>( cust_id, merge_o );

        for( const auto& val : entry.second ) {
            q18_order_reduce( merge_res, val );
        }

        res[entry.first] = merge_res;
    }
}

q18_order_line_probe::q18_order_line_probe(
    const std::unordered_map<q18_order_kv_types>& probe,
    const tpcc_configs&                           cfg )
    : probe_( probe ), cfg_( cfg ) {}

std::tuple<bool /* found */, uint64_t /* order id */, float> q18_order_line_map(
    const result_tuple& res, const q18_order_line_probe& probe ) {
    order_line ol;

    ol.ol_o_id = -1;
    ol.ol_d_id = -1;
    ol.ol_w_id = -1;
    ol.ol_amount = 0;

    auto read_cols = read_from_scan_order_line( res, &ol );

    bool found = ( ( read_cols.count( order_line_cols::ol_o_id ) == 1 ) and
                   ( read_cols.count( order_line_cols::ol_d_id ) == 1 ) and
                   ( read_cols.count( order_line_cols::ol_w_id ) == 1 ) );

    uint64_t key = 0;
    if( found ) {
        order o;
        o.o_id = ol.ol_o_id;
        o.o_d_id = ol.ol_d_id;
        o.o_w_id = ol.ol_w_id;
        o.o_c_id = 0;

        key = make_order_key( o, probe.cfg_ );

        found = ( probe.probe_.count( key ) == 1 );
    }

    return std::make_tuple<>( found, key, ol.ol_amount );
}

void q18_order_line_mapper( const std::vector<result_tuple>& res_tuples,
                            std::unordered_map<q18_order_line_kv_types>& res,
                            const q18_order_line_probe&                  p ) {
    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, uint64_t /* order id */, float>
            mapped_res = q18_order_line_map( res_tuple, p );
        DVLOG( 40 ) << "Q18 map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            } else {
                found->second += std::get<2>( mapped_res );
            }
        }
    }
}
void q18_order_line_reducer(
    const std::unordered_map<q18_order_line_kv_vec_types>& input,
    std::unordered_map<q18_order_line_kv_types>&           res,
    const q18_order_line_probe&                            p ) {

    for( const auto& entry : input ) {
        float merge_res = 0;

        for( const auto& val : entry.second ) {
            merge_res += val;
        }

        res[entry.first] = merge_res;
    }
}

std::tuple<bool /* found */, uint64_t /* order id */, order> q12_order_map(
    const result_tuple& res, const tpcc_configs& cfg ) {
    order o;

    o.o_id = -1;
    o.o_c_id = -1;
    o.o_d_id = -1;
    o.o_w_id = -1;
    o.o_carrier_id = -1;
    o.o_ol_cnt = -1;
    o.o_entry_d.c_since = datetime::EMPTY_DATE;

    auto read_cols = read_from_scan_order( res, &o );

    bool found = !read_cols.empty();

    return std::make_tuple<>( found, res.row_id, o );
}

void q12_order_reduce( order& o, const order& other ) {
    if( o.o_id < 0 ) {
        o.o_id = other.o_id;
    }
    if( o.o_c_id < 0 ) {
        o.o_c_id = other.o_c_id;
    }
    if( o.o_d_id < 0 ) {
        o.o_d_id = other.o_d_id;
    }
    if( o.o_w_id < 0 ) {
        o.o_w_id = other.o_w_id;
    }
    if( o.o_carrier_id < 0 ) {
        o.o_carrier_id = other.o_carrier_id;
    }
    if( o.o_ol_cnt < 0 ) {
        o.o_ol_cnt = other.o_ol_cnt;
    }
    if( o.o_entry_d.c_since == datetime::EMPTY_DATE ) {
        o.o_entry_d.c_since = other.o_entry_d.c_since;
    }
}

void q12_order_mapper( const std::vector<result_tuple>&        res_tuples,
                       std::unordered_map<q12_order_kv_types>& res,
                       const tpcc_configs& cfg) {

    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, uint64_t /* order id */, order>
            mapped_res = q12_order_map( res_tuple, cfg );
        DVLOG( 40 ) << "Q12 map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            } else {
                q12_order_reduce( found->second, std::get<2>( mapped_res ) );
            }
        }
    }
}
void q12_order_reducer( const std::unordered_map<q12_order_kv_vec_types>& input,
                        std::unordered_map<q12_order_kv_types>&           res,
                        const tpcc_configs& cfg ) {

    for( const auto& entry : input ) {
        order merge_res;

        merge_res.o_id = -1;
        merge_res.o_d_id = -1;
        merge_res.o_w_id = -1;
        merge_res.o_carrier_id = -1;
        merge_res.o_ol_cnt = -1;
        merge_res.o_entry_d.c_since = datetime::EMPTY_DATE;

        for( const auto& val : entry.second ) {
            q12_order_reduce( merge_res, val );
        }

        res[entry.first] = merge_res;
    }
}

q12_order_line_probe::q12_order_line_probe(
    const std::unordered_map<q12_order_kv_types>& probe,
    const tpcc_configs&                           cfg )
    : probe_( probe ), cfg_( cfg ) {}


std::tuple<bool /* found */, uint64_t /* order id */, uint64_t> q12_order_line_map(
    const result_tuple& res, const q12_order_line_probe& probe ) {
    order_line ol;

    ol.ol_o_id = -1;
    ol.ol_d_id = -1;
    ol.ol_w_id = -1;
    ol.ol_delivery_d.c_since = datetime::EMPTY_DATE;

    auto read_cols = read_from_scan_order_line( res, &ol );

    bool found = ( ( read_cols.count( order_line_cols::ol_o_id ) == 1 ) and
                   ( read_cols.count( order_line_cols::ol_d_id ) == 1 ) and
                   ( read_cols.count( order_line_cols::ol_w_id ) == 1 ) );

    uint64_t key = 0;
    if( found ) {
        order o;
        o.o_id = ol.ol_o_id;
        o.o_d_id = ol.ol_d_id;
        o.o_w_id = ol.ol_w_id;
        o.o_c_id = 0;

        key = make_order_key( o, probe.cfg_ );

        found = ( probe.probe_.count( key ) == 1 );
    }

    return std::make_tuple<>( found, key, ol.ol_delivery_d.c_since );
}

void q12_order_line_mapper( const std::vector<result_tuple>& res_tuples,
                            std::unordered_map<q12_order_line_kv_types>& res,
                            const q12_order_line_probe&                  p ) {
    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, uint64_t /* order id */,
                   uint64_t /* ol_delivery_d */>
            mapped_res = q12_order_line_map( res_tuple, p );
        DVLOG( 40 ) << "Q12 map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            } else {
                found->second =
                    std::max( found->second, std::get<2>( mapped_res ) );
            }
        }
    }
}
void q12_order_line_reducer(
    const std::unordered_map<q12_order_line_kv_vec_types>& input,
    std::unordered_map<q12_order_line_kv_types>&           res,
    const q12_order_line_probe&                            p ) {

    for( const auto& entry : input ) {
        uint64_t merge_ol_d = datetime::EMPTY_DATE;

        for( const auto& val : entry.second ) {
            merge_ol_d = std::max( merge_ol_d, val );
        }

        auto p_found = p.probe_.find( entry.first );
        if( p_found != p.probe_.end() ) {
            const auto& order = p_found->second;
            if( order.o_entry_d.c_since <= merge_ol_d ) {
                res[entry.first] = merge_ol_d;
            }
        }
    }
}

void q8_nation_mapper( const std::vector<result_tuple>&        res_tuples,
                       std::unordered_map<q8_nation_kv_types>& res,
                       const EmptyProbe&                       p ) {
    for( const auto& res_tuple : res_tuples ) {
        res[res_tuple.row_id] = 0;
    }
}
void q8_nation_reducer( const std::unordered_map<q8_nation_kv_vec_types>& input,
                        std::unordered_map<q8_nation_kv_types>&           res,
                        const EmptyProbe&                                 p ) {
    for( const auto& entry : input ) {
        res[entry.first] = 0;
    }
}

q8_supplier_probe::q8_supplier_probe(
    const std::unordered_map<q8_nation_kv_types>& probe,
    const tpcc_configs&                           cfg )
    : probe_( probe ), cfg_( cfg ) {}

std::tuple<bool, uint64_t, int32_t> q8_supplier_map(
    const result_tuple& res, const q8_supplier_probe& p ) {
    supplier s;
    s.n_id = -1;

    auto read_cols = read_from_scan_supplier( res, &s );
    s.s_id = (int32_t) res.row_id;

    bool found = ( read_cols.count( supplier_cols::n_id ) == 1 );

    int32_t nation = 0;
    if( found ) {
        auto nat_found = p.probe_.find( s.n_id );
        found = ( nat_found != p.probe_.end() );
        if( found ) {
            nation = nat_found->second;
        }
    }

    return std::make_tuple<>( found, res.row_id, nation );
}

void q8_supplier_mapper( const std::vector<result_tuple>&          res_tuples,
                         std::unordered_map<q8_supplier_kv_types>& res,
                         const q8_supplier_probe&                  p ) {
    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, uint64_t /* stock supplier_id */, int32_t>
            mapped_res = q8_supplier_map( res_tuple, p );
        DVLOG( 40 ) << "Q8 map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            } else {
                found->second =
                    std::max( found->second, std::get<2>( mapped_res ) );
            }
        }
    }
}
void q8_supplier_reducer(
    const std::unordered_map<q8_supplier_kv_vec_types>& input,
    std::unordered_map<q8_supplier_kv_types>&           res,
    const q8_supplier_probe&                            p ) {
    for( const auto& entry : input ) {
        int32_t c_id = 0;
        for( const auto& c : entry.second ) {
            c_id = std::max( c, c_id );
        }
        res[entry.first] = c_id;
    }
}

q8_stock_probe::q8_stock_probe(
    const std::unordered_map<q8_supplier_kv_types>& probe,
    const tpcc_configs&                             cfg )
    : probe_( probe ), cfg_( cfg ) {}

std::tuple<bool, uint64_t, int32_t> q8_stock_map( const result_tuple& res,
                                                const q8_stock_probe& probe ) {
    bool  found = false;
    stock s;
    s.s_i_id = -1;
    s.s_w_id = -1;
    s.s_s_id = -1;

    auto read_cols = read_from_scan_stock( res, &s );

    found = !read_cols.empty();

    return std::make_tuple<>( found, res.row_id, s.s_s_id );
}

void q8_stock_mapper( const std::vector<result_tuple>&       res_tuples,
                      std::unordered_map<q8_stock_kv_types>& res,
                      const q8_stock_probe&                  p ) {

    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, uint64_t /* stock supplier_id */, int32_t>
            mapped_res = q8_stock_map( res_tuple, p );
        DVLOG( 40 ) << "Q8 map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            } else {
                found->second =
                    std::max( found->second, std::get<2>( mapped_res ) );
            }
        }
    }
}
void q8_stock_reducer(
    const std::unordered_map<q8_stock_kv_vec_types>& input,
    std::unordered_map<q8_stock_kv_types>& res, const q8_stock_probe& p ) {
    for( const auto& entry : input ) {
        int32_t merge_sup = -1;
        for( const auto& val : entry.second ) {
            merge_sup = std::max( merge_sup, val );
        }
        if( p.probe_.count( merge_sup ) == 1 ) {
            res[entry.first] = merge_sup;
        }
    }
}

std::tuple<bool, uint64_t, uint64_t> q8_order_map( const result_tuple& res ) {
    order o;
    o.o_entry_d.c_since = datetime::EMPTY_DATE;

    auto read_cols = read_from_scan_order( res, &o );
    bool found = read_cols.count( order_cols::o_entry_d_c_since ) == 1;

    return std::make_tuple<>( found, res.row_id, o.o_entry_d.c_since );
}

void q8_order_mapper( const std::vector<result_tuple>&       res_tuples,
                      std::unordered_map<q8_order_kv_types>& res,
                      const EmptyProbe&                      p ) {

    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, uint64_t /* order id */, uint64_t>
            mapped_res = q8_order_map( res_tuple );
        DVLOG( 40 ) << "Q8 map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            } else {
                found->second =
                    std::max( found->second, std::get<2>( mapped_res ) );
            }
        }
    }
}
void q8_order_reducer( const std::unordered_map<q8_order_kv_vec_types>& input,
                        std::unordered_map<q8_order_kv_types>&           res,
                        const EmptyProbe& p ) {
    for( const auto& entry : input ) {
        uint64_t merge_sup = datetime::EMPTY_DATE;
        for( const auto& val : entry.second ) {
            merge_sup = std::max( merge_sup, val );
        }
        res[entry.first] = get_year_from_c_since( merge_sup );
    }
}

q8_order_line_probe::q8_order_line_probe(
    const std::unordered_map<q8_stock_kv_types>& s_probe,
    const std::unordered_map<q8_order_kv_types>& o_probe,
    const tpcc_configs&                          cfg )
    : s_probe_( s_probe ), o_probe_( o_probe ), cfg_( cfg ) {}

q8_order_line_key::q8_order_line_key( uint64_t stock_key, uint64_t year )
    : stock_key_( stock_key ), year_( year ) {}

std::tuple<bool, q8_order_line_key, float> q8_order_line_map(
    const result_tuple& res, const q8_order_line_probe& p ) {
    order_line        ol;
    ol.ol_o_id = -1;
    ol.ol_d_id = -1;
    ol.ol_w_id = -1;
    ol.ol_i_id = -1;
    ol.ol_amount = 0;

    auto read_cols = read_from_scan_order_line( res, &ol );

    bool found = ( read_cols.count( order_line_cols::ol_o_id ) == 1 ) and
                 ( read_cols.count( order_line_cols::ol_w_id ) == 1 ) and
                 ( read_cols.count( order_line_cols::ol_d_id ) == 1 ) and
                 ( read_cols.count( order_line_cols::ol_i_id ) == 1 );

    q8_order_line_key key( 0 /*stock*/, 0 /* year */ );

    if( found ) {
        stock s;
        s.s_i_id = ol.ol_i_id;
        s.s_w_id = ol.ol_w_id;

        order o;
        o.o_id = ol.ol_o_id;
        o.o_w_id = ol.ol_w_id;
        o.o_d_id = ol.ol_d_id;
        o.o_c_id = 0;

        uint64_t s_key = make_stock_key( s, p.cfg_ );
        uint64_t o_key = make_order_key( o, p.cfg_ );

        key.stock_key_ = s_key;

        found = p.s_probe_.count( s_key ) == 1;
        if( found ) {
            auto o_found = p.o_probe_.find( o_key );
            found = ( o_found != p.o_probe_.end() );
            if( found ) {
                key.year_ = o_found->second;
            }
        }
    }

    return std::make_tuple<>( found, key, ol.ol_amount );
}

void q8_order_line_mapper( const std::vector<result_tuple>& res_tuples,
                           std::unordered_map<q8_order_line_kv_types>& res,
                           const q8_order_line_probe&                  p ) {

    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, q8_order_line_key, float> mapped_res =
            q8_order_line_map( res_tuple, p );
        DVLOG( 40 ) << "Q8 map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            } else {
                found->second += std::get<2>( mapped_res );
            }
        }
    }
}
void q8_order_line_reducer(
    const std::unordered_map<q8_order_line_kv_vec_types>& input,
    std::unordered_map<q8_order_line_kv_types>&           res,
    const q8_order_line_probe&                            p ) {
    for( const auto& entry : input ) {
        float amount = 0;
        for( const auto& val : entry.second ) {
            amount += val;
        }
        res[entry.first] = amount;
    }
}

std::tuple<bool, int32_t, nation> q9_nation_map( const result_tuple& res ) {
    nation n;
    n.n_id = -1;
    n.n_name = "";

    auto read_cols = read_from_scan_nation( res, &n );
    bool    found = !read_cols.empty();
    n.n_id = (int32_t) res.row_id;
    int32_t key = (int32_t) res.row_id;

    return std::make_tuple<>( found, key, n );
}
void q9_nation_reduce( nation& n, const nation& other ) {
    if( n.n_id < 0 ) {
        n.n_id = other.n_id;
    }
    if( n.n_name.empty() ) {
        n.n_name = other.n_name;
    }
}

void q9_nation_mapper( const std::vector<result_tuple>&        res_tuples,
                       std::unordered_map<q9_nation_kv_types>& res,
                       const EmptyProbe&                       p ) {
    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, int32_t /* n_id */, nation> mapped_res =
            q9_nation_map( res_tuple );
        DVLOG( 40 ) << "Q9 map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            } else {
                q9_nation_reduce( found->second, std::get<2>( mapped_res ) );
            }
        }
    }
}

void q9_nation_reducer( const std::unordered_map<q9_nation_kv_vec_types>& input,
                        std::unordered_map<q9_nation_kv_types>&           res,
                        const EmptyProbe&                                 p ) {
    for( const auto& entry : input ) {
        nation merge_res;
        merge_res.n_id = -1;
        merge_res.n_name = "";

        for( const auto& val : entry.second ) {
            q9_nation_reduce( merge_res, val );
        }

        res[entry.first] = merge_res;
        DVLOG( 40 ) << "Q9 reduce:" << entry.first;
    }
}

std::tuple<bool, uint64_t, int32_t> q9_item_map( const result_tuple& res ) {
    item i;

    auto read_cols = read_from_scan_item( res, &i );
    i.i_id = (int32_t) res.row_id;
    bool found = !read_cols.empty();
    return std::make_tuple<>( found, res.row_id, 0 );
}

void q9_item_mapper( const std::vector<result_tuple>&      res_tuples,
                     std::unordered_map<q9_item_kv_types>& res,
                     const EmptyProbe&                     p ) {
    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, uint64_t, int32_t> mapped_res =
            q9_item_map( res_tuple );
        DVLOG( 40 ) << "Q9 map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            }
        }
    }
}
void q9_item_reducer( const std::unordered_map<q9_item_kv_vec_types>& input,
                      std::unordered_map<q9_item_kv_types>&           res,
                      const EmptyProbe&                               p ) {
    for( const auto& entry : input ) {
        res[entry.first] = 0;
    }
}

q9_supplier_probe::q9_supplier_probe(
    const std::unordered_map<q9_nation_kv_types>& probe,
    const tpcc_configs&                           cfg )
    : probe_( probe ), cfg_( cfg ) {}

std::tuple<bool, int32_t, int32_t> q9_supplier_map(
    const result_tuple& res_tuple, const q9_supplier_probe& p ) {
    supplier s;
    s.s_id = -1;
    s.n_id = -1;

    auto read_cols = read_from_scan_supplier( res_tuple, &s );
    s.s_id = (int32_t) res_tuple.row_id;
    bool found = ( read_cols.count( supplier_cols::n_id ) == 1 );
    if( found ) {
        found = p.probe_.count( s.n_id ) == 1;
    }

    return std::make_tuple<>( found, (int32_t) res_tuple.row_id, s.n_id );
}
void q9_supplier_mapper( const std::vector<result_tuple>&          res_tuples,
                         std::unordered_map<q9_supplier_kv_types>& res,
                         const q9_supplier_probe&                  p ) {
    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool, int32_t, int32_t> mapped_res =
            q9_supplier_map( res_tuple, p );

        DVLOG( 40 ) << "Q9 map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            } else {
                found->second =
                    std::max( found->second, std::get<2>( mapped_res ) );
            }
        }
    }
}
void q9_supplier_reducer(
    const std::unordered_map<q9_supplier_kv_vec_types>& input,
    std::unordered_map<q9_supplier_kv_types>&           res,
    const q9_supplier_probe&                            p ) {
    for( const auto& entry : input ) {
        int32_t nat_id = -1;
        for( const auto& val : entry.second ) {
            nat_id = std::max( nat_id, val );
        }
        res[entry.first] = nat_id;
        DVLOG( 40 ) << "Q9 reduce:" << entry.first;
    }
}

q9_stock_probe::q9_stock_probe(
    const std::unordered_map<q9_item_kv_types>&     i_probe,
    const std::unordered_map<q9_supplier_kv_types>& s_probe,
    const tpcc_configs&                             cfg )
    : i_probe_( i_probe ), s_probe_( s_probe ), cfg_( cfg ) {
}

std::tuple<bool, uint64_t, int32_t> q9_stock_map(
    const result_tuple& res, const q9_stock_probe& probe ) {
    stock s;
    s.s_i_id = -1;
    s.s_w_id = -1;
    s.s_s_id = -1;

    auto read_cols = read_from_scan_stock( res, &s );

    bool found = ( read_cols.count( stock_cols::s_i_id ) == 1 ) and
                 ( read_cols.count( stock_cols::s_s_id ) );
    uint64_t key = res.row_id;

    int32_t nat_id = -1;

    if( found ) {
        found = ( probe.i_probe_.count( s.s_i_id ) == 1 );
        if( found ) {
            auto sup_found = probe.s_probe_.find( s.s_s_id );
            if( sup_found == probe.s_probe_.end() ) {
                found = false;
            } else {
                nat_id = sup_found->second;
            }
        }
    }

    return std::make_tuple<>( found, key, nat_id );
}

void q9_stock_mapper( const std::vector<result_tuple>&       res_tuples,
                      std::unordered_map<q9_stock_kv_types>& res,
                      const q9_stock_probe&                  p ) {
    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, uint64_t, int32_t> mapped_res =
            q9_stock_map( res_tuple, p );
        DVLOG( 40 ) << "Q9 map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            } else {
                found->second =
                    std::max( found->second, std::get<2>( mapped_res ) );
            }
        }
    }
}
void q9_stock_reducer( const std::unordered_map<q9_stock_kv_vec_types>& input,
                       std::unordered_map<q9_stock_kv_types>&           res,
                       const q9_stock_probe&                            p ) {
    for( const auto& entry : input ) {
        int32_t nat_id = -1;
        for( const auto& val : entry.second ) {
            nat_id = std::max( nat_id, val );
        }
        res[entry.first] = nat_id;
    }
}

void q9_order_mapper( const std::vector<result_tuple>&       res_tuples,
                      std::unordered_map<q9_order_kv_types>& res,
                      const EmptyProbe&                      p ) {
    q8_order_mapper( res_tuples, res, p );
}
void q9_order_reducer( const std::unordered_map<q9_order_kv_vec_types>& input,
                        std::unordered_map<q9_order_kv_types>&           res,
                        const EmptyProbe& p ) {
    q8_order_reducer( input, res, p );
}

q9_order_line_probe::q9_order_line_probe(
    const std::unordered_map<q9_stock_kv_types>& s_probe,
    const std::unordered_map<q9_order_kv_types>& o_probe,
    const tpcc_configs&                          cfg )
    : s_probe_( s_probe ), o_probe_( o_probe ), cfg_( cfg ) {}

std::tuple<bool, q8_order_line_key, float> q9_order_line_map(
    const result_tuple& res, const q9_order_line_probe& p ) {
    order_line ol;
    ol.ol_o_id = -1;
    ol.ol_d_id = -1;
    ol.ol_w_id = -1;
    ol.ol_i_id = -1;
    ol.ol_amount = 0;

    auto read_cols = read_from_scan_order_line( res, &ol );

    bool found = ( read_cols.count( order_line_cols::ol_o_id ) == 1 ) and
                 ( read_cols.count( order_line_cols::ol_w_id ) == 1 ) and
                 ( read_cols.count( order_line_cols::ol_d_id ) == 1 ) and
                 ( read_cols.count( order_line_cols::ol_i_id ) == 1 );

    q8_order_line_key key( 0 /*stock*/, 0 /* year */ );

    if( found ) {
        stock s;
        s.s_i_id = ol.ol_i_id;
        s.s_w_id = ol.ol_w_id;

        order o;
        o.o_id = ol.ol_o_id;
        o.o_w_id = ol.ol_w_id;
        o.o_d_id = ol.ol_d_id;
        o.o_c_id = 0;

        uint64_t s_key = make_stock_key( s, p.cfg_ );
        uint64_t o_key = make_order_key( o, p.cfg_ );

        key.stock_key_ = s_key;

        found = p.s_probe_.count( s_key ) == 1;
        if( found ) {
            auto o_found = p.o_probe_.find( o_key );
            found = ( o_found != p.o_probe_.end() );
            if( found ) {
                key.year_ = o_found->second;
            }
        }
    }

    return std::make_tuple<>( found, key, ol.ol_amount );
}

void q9_order_line_mapper( const std::vector<result_tuple>& res_tuples,
                           std::unordered_map<q9_order_line_kv_types>& res,
                           const q9_order_line_probe&                  p ) {

    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, q8_order_line_key, float> mapped_res =
            q9_order_line_map( res_tuple, p );
        DVLOG( 40 ) << "Q9 map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            } else {
                found->second += std::get<2>( mapped_res );
            }
        }
    }
}

void q9_order_line_reducer(
    const std::unordered_map<q9_order_line_kv_vec_types>& input,
    std::unordered_map<q9_order_line_kv_types>&           res,
    const q9_order_line_probe&                            p ) {
    for( const auto& entry : input ) {
        float amount = 0;
        for( const auto& val : entry.second ) {
            amount += val;
        }
        res[entry.first] = amount;
    }
}

std::tuple<bool, int32_t, int32_t> q11_supplier_map( const result_tuple& res ) {
    int32_t key = (int32_t) res.row_id;
    bool found = true;

    return std::make_tuple<>( found, key, key );
}

void q11_supplier_mapper( const std::vector<result_tuple>&           res_tuples,
                          std::unordered_map<q11_supplier_kv_types>& res,
                          const EmptyProbe&                          p ) {

    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, int32_t /* s_id */, int32_t> mapped_res =
            q11_supplier_map( res_tuple );
        DVLOG( 40 ) << "Q11 map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            }
        }
    }
}
void q11_supplier_reducer(
    const std::unordered_map<q11_supplier_kv_vec_types>& input,
    std::unordered_map<q11_supplier_kv_types>& res, const EmptyProbe& p ) {
    for( const auto& entry : input ) {
        res[entry.first] = entry.first;
    }
}


q11_stock_probe::q11_stock_probe(
    const std::unordered_map<q11_supplier_kv_types>& probe,
    const tpcc_configs&                              cfg )
    : probe_( probe ), cfg_( cfg ) {}

std::tuple<bool, int32_t, int64_t> q11_stock_map(
    const result_tuple& res, const q11_stock_probe& probe ) {
    stock s;
    s.s_i_id = -1;
    s.s_s_id = -1;
    s.s_order_cnt = 0;

    auto read_cols = read_from_scan_stock( res, &s );
    bool found = read_cols.count( s.s_s_id ) == 1;

    int32_t i_id = get_item_from_key( res.table_id, res.row_id, probe.cfg_ );

    if( found ) {
        found = probe.probe_.count( s.s_s_id ) == 1;
    }

    return std::make_tuple<>( found, i_id, (int64_t) s.s_order_cnt );
}

void q11_stock_mapper( const std::vector<result_tuple>&        res_tuples,
                       std::unordered_map<q11_stock_kv_types>& res,
                       const q11_stock_probe&                  p ) {

    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, int32_t /* i_id*/, int64_t> mapped_res =
            q11_stock_map( res_tuple, p );
        DVLOG( 40 ) << "Q11 map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            } else {
                found->second += std::get<2>( mapped_res );
            }
        }
    }
}
void q11_stock_reducer( const std::unordered_map<q11_stock_kv_vec_types>& input,
                        std::unordered_map<q11_stock_kv_types>&           res,
                        const q11_stock_probe&                            p ) {
    for( const auto& entry : input ) {
        int64_t cnt = 0;
        for( const auto& val : entry.second ) {
            cnt += val;
        }
        res[entry.first] = cnt;
    }
}

std::tuple<bool, uint64_t, uint64_t> q13_order_map( const result_tuple& res,
                                                    const tpcc_configs& cfg ) {
    order o;
    o.o_c_id = -1;
    o.o_d_id = -1;
    o.o_w_id = -1;

    auto read_cols = read_from_scan_order( res, &o );
    bool found = ( read_cols.count( order_cols::o_c_id ) == 1 ) and
                 ( read_cols.count( order_cols::o_d_id ) == 1 ) and
                 ( read_cols.count( order_cols::o_w_id ) == 1 );

    uint64_t key = 0;
    if( found ) {
        customer c;
        c.c_id = o.o_c_id;
        c.c_d_id = o.o_d_id;
        c.c_w_id = o.o_w_id;

        key = make_customer_key( c, cfg );
    }

    return std::make_tuple<>( found, key, 1 );
}

void q13_order_mapper( const std::vector<result_tuple>&        res_tuples,
                       std::unordered_map<q13_order_kv_types>& res,
                       const tpcc_configs&                     cfg ) {

    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, uint64_t /* cust_key*/, uint64_t>
            mapped_res = q13_order_map( res_tuple, cfg );
        DVLOG( 40 ) << "Q13 map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            } else {
                found->second += std::get<2>( mapped_res );
            }
        }
    }
}
void q13_order_reducer( const std::unordered_map<q13_order_kv_vec_types>& input,
                        std::unordered_map<q13_order_kv_types>&           res,
                        const tpcc_configs& cfg ) {
    for( const auto& entry : input ) {
        uint64_t cnt = 0;
        for( const auto& val : entry.second ) {
            cnt += val;
        }
        res[entry.first] = cnt;
    }
}

q13_customer_probe::q13_customer_probe(
    const std::unordered_map<q13_order_kv_types>& probe,
    const tpcc_configs&                           cfg )
    : probe_( probe ), cfg_( cfg ) {}

std::tuple<bool, int32_t, uint64_t> q13_customer_map(
    const result_tuple& res, const q13_customer_probe& p ) {
    customer c;
    c.c_id = -1;

    auto read_cols = read_from_scan_customer( res, &c );
    bool found = ( read_cols.count( customer_cols::c_id ) == 1 );

    uint64_t ret = 0;
    if( found ) {
        auto p_found = p.probe_.find( res.row_id );
        if( p_found == p.probe_.end() ) {
            found = false;
        } else {
            ret = p_found->second;
        }
    }

    return std::make_tuple<>( found, c.c_id, ret );
}

void q13_customer_mapper( const std::vector<result_tuple>&           res_tuples,
                          std::unordered_map<q13_customer_kv_types>& res,
                          const q13_customer_probe&                  p ) {
    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, int32_t /* cust_key*/, uint64_t>
            mapped_res = q13_customer_map( res_tuple, p );
        DVLOG( 40 ) << "Q13 map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            } else {
                found->second += std::get<2>( mapped_res );
            }
        }
    }
}
void q13_customer_reducer(
    const std::unordered_map<q13_customer_kv_vec_types>& input,
    std::unordered_map<q13_customer_kv_types>&           res,
    const q13_customer_probe&                            p ) {
    for( const auto& entry : input ) {
        uint64_t cnt = 0;
        for( const auto& val : entry.second ) {
            cnt += val;
        }
        res[entry.first] = cnt;
    }
}

std::tuple<bool, uint64_t, float> q15_order_line_map(
    const result_tuple& res, const tpcc_configs& cfg ) {
    order_line ol;
    ol.ol_i_id = -1;
    ol.ol_supply_w_id = -1;
    ol.ol_amount = 0;

    auto read_cols = read_from_scan_order_line( res, &ol );
    bool found = ( read_cols.count( order_line_cols::ol_i_id ) == 1 ) and
                 ( read_cols.count( order_line_cols::ol_supply_w_id ) == 1 );

    uint64_t key = 0;
    if( found ) {
        stock s;
        s.s_i_id = ol.ol_i_id;
        s.s_w_id = ol.ol_supply_w_id;

        key = make_stock_key( s, cfg );
    }

    return std::make_tuple<>( found, key, ol.ol_amount);
}


void q15_order_line_mapper( const std::vector<result_tuple>& res_tuples,
                            std::unordered_map<q15_order_line_kv_types>& res,
                            const tpcc_configs& cfg ) {

    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, uint64_t /* stock key*/, float>
            mapped_res = q15_order_line_map( res_tuple, cfg );
        DVLOG( 40 ) << "Q15 map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            } else {
                found->second += std::get<2>( mapped_res );
            }
        }
    }
}
void q15_order_line_reducer(
    const std::unordered_map<q15_order_line_kv_vec_types>& input,
    std::unordered_map<q15_order_line_kv_types>&           res,
    const tpcc_configs&                                    cfg ) {
    for( const auto& entry : input ) {
        float merge = 0;
        for( const auto& val : entry.second ) {
            merge += val;
        }
        res[entry.first] = merge;
    }
}

q15_stock_probe::q15_stock_probe(
    const std::unordered_map<q15_order_line_kv_types>& probe,
    const tpcc_configs&                                cfg )
    : probe_( probe ), cfg_( cfg ) {}

std::tuple<bool, int32_t /* supplier */, float> q15_stock_map(
    const result_tuple& res, const q15_stock_probe& p ) {
    stock s;
    s.s_i_id = -1;
    s.s_w_id = -1;
    s.s_s_id = -1;

    auto read_cols = read_from_scan_stock( res, &s );
    bool found = ( read_cols.count( stock_cols::s_s_id ) == 1 );

    float ol_amount = 0;

    if( found ) {
        auto s_found = p.probe_.find( res.row_id );
        if( s_found == p.probe_.end() ) {
            found = false;
        } else {
            ol_amount = s_found->second;
        }
    }

    return std::make_tuple<>( found, s.s_s_id, ol_amount );
}

void q15_stock_mapper( const std::vector<result_tuple>&        res_tuples,
                       std::unordered_map<q15_stock_kv_types>& res,
                       const q15_stock_probe&                  p ) {
    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, int32_t /* supplier key*/, float>
            mapped_res = q15_stock_map( res_tuple, p );
        DVLOG( 40 ) << "Q15 map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            } else {
                found->second += std::get<2>( mapped_res );
            }
        }
    }
}
void q15_stock_reducer( const std::unordered_map<q15_stock_kv_vec_types>& input,
                        std::unordered_map<q15_stock_kv_types>&           res,
                        const q15_stock_probe&                            p ) {
    for( const auto& entry : input ) {
        float merge = 0;
        for( const auto& val : entry.second ) {
            merge += val;
        }
        res[entry.first] = merge;
    }
}

q15_supplier_probe::q15_supplier_probe(
    const std::unordered_map<q15_stock_kv_types>& probe,
    const tpcc_configs&                           cfg )
    : probe_( probe ), cfg_( cfg ) {}

std::tuple<bool, int32_t, supplier> q15_supplier_map(
    const result_tuple& res, const q15_supplier_probe& p ) {
    supplier s;
    s.s_name = "";
    s.s_phone = "";
    s.s_address.a_zip = "";

    auto read_cols = read_from_scan_supplier( res, &s );
    s.s_id = (int32_t) res.row_id;

    bool found = !read_cols.empty();
    if( found ) {
        found = ( p.probe_.count( s.s_id ) == 1 );
    }

    return std::make_tuple<>( found, s.s_id, s );
}

void q15_supplier_reduce( supplier& s, const supplier& other ) {
    if( s.s_id < 0 ) {
        s.s_id = other.s_id;
    }
    if( s.s_name.empty() ) {
        s.s_name = other.s_name;
    }
    if( s.s_phone.empty() ) {
        s.s_phone = other.s_phone;
    }
    if( s.s_address.a_zip.empty() ) {
        s.s_address.a_zip = other.s_address.a_zip;
    }
}

void q15_supplier_mapper( const std::vector<result_tuple>&           res_tuples,
                          std::unordered_map<q15_supplier_kv_types>& res,
                          const q15_supplier_probe&                  p ) {

    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, int32_t /* supplier key*/, supplier>
            mapped_res = q15_supplier_map( res_tuple, p );
        DVLOG( 40 ) << "Q15 map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            } else {
                q15_supplier_reduce( found->second, std::get<2>( mapped_res ) );
            }
        }
    }
}
void q15_supplier_reducer(
    const std::unordered_map<q15_supplier_kv_vec_types>& input,
    std::unordered_map<q15_supplier_kv_types>&           res,
    const q15_supplier_probe&                            p ) {

    for( const auto& entry : input ) {
        supplier merge_res;
        merge_res.s_id = -1;
        merge_res.s_name = "";
        merge_res.s_phone = "";
        merge_res.s_address.a_zip = "";

        for( const auto& val : entry.second ) {
            q15_supplier_reduce( merge_res, val );
        }
        res[entry.first] = merge_res;
    }
}

std::tuple<bool, int32_t, item> q16_item_map( const result_tuple& res ) {
    bool  found = false;
    item i;
    i.i_id = -1;
    i.i_price = -1;
    i.i_name = "";
    i.i_data = "";

    auto read_cols = read_from_scan_item( res, &i );
    i.i_id = (int32_t) res.row_id;

    found = !read_cols.empty();
    return std::make_tuple<>( found, (int32_t) res.row_id, i );
}

void q16_item_reduce( item& i, const item& other ) {
    if( i.i_id < 0 ) {
        i.i_id = other.i_id;
    }
    if( i.i_price < 0 ) {
        i.i_price = other.i_price;
    }
    if( i.i_name.empty() ) {
        i.i_name = other.i_name;
    }
    if( i.i_data.empty() ) {
        i.i_data = other.i_data;
    }
}

void q16_item_mapper( const std::vector<result_tuple>&       res_tuples,
                      std::unordered_map<q16_item_kv_types>& res,
                      const EmptyProbe&                      p ) {

    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, int32_t /* i_id */, item> mapped_res =
            q16_item_map( res_tuple );
        DVLOG( 40 ) << "Q16 map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            } else {
                q16_item_reduce( found->second, std::get<2>( mapped_res ) );
            }
        }
    }
}
void q16_item_reducer( const std::unordered_map<q16_item_kv_vec_types>& input,
                       std::unordered_map<q16_item_kv_types>&           res,
                       const EmptyProbe&                                p ) {

    for( const auto& entry : input ) {
        item merge_res;
        merge_res.i_id = -1;
        merge_res.i_price = -1;
        merge_res.i_name = "";
        merge_res.i_data = "";

        for( const auto& val : entry.second ) {
            q16_item_reduce( merge_res, val );
        }

        res[entry.first] = merge_res;
        DVLOG( 40 ) << "Q16 reduce:" << entry.first;
    }
}

std::tuple<bool, int32_t, int32_t> q16_supplier_map( const result_tuple& res ) {
    bool     found = false;
    supplier s;
    s.s_id = -1;

    auto read_cols = read_from_scan_supplier( res, &s );
    s.s_id = (int32_t) res.row_id;

    found = !read_cols.empty();
    return std::make_tuple<>( found, (int32_t) res.row_id, s.s_id );
}

void q16_supplier_mapper( const std::vector<result_tuple>&           res_tuples,
                          std::unordered_map<q16_supplier_kv_types>& res,
                          const EmptyProbe&                          p ) {
    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, int32_t /* s_id */, int32_t> mapped_res =
            q16_supplier_map( res_tuple );
        DVLOG( 40 ) << "Q16 map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            }
        }
    }
}
void q16_supplier_reducer(
    const std::unordered_map<q16_supplier_kv_vec_types>& input,
    std::unordered_map<q16_supplier_kv_types>& res, const EmptyProbe& p ) {
    for( const auto& entry : input ) {
        res[entry.first] = entry.first;
    }
}

q16_stock_probe::q16_stock_probe(
    const std::unordered_map<q16_item_kv_types>&     i_probe,
    const std::unordered_map<q16_supplier_kv_types>& s_probe,
    const tpcc_configs&                              cfg )
    : i_probe_( i_probe ), s_probe_( s_probe ), cfg_( cfg ) {}

std::tuple<bool, int32_t /*i_id*/, uint64_t> q16_stock_map(
    const result_tuple& res, const q16_stock_probe& p ) {
    stock s;
    s.s_i_id = -1;
    s.s_s_id = -1;

    auto read_cols = read_from_scan_stock( res, &s );

    bool found = ( read_cols.count( stock_cols::s_i_id ) == 1 ) and
                 ( read_cols.count( stock_cols::s_s_id ) == 1 );

    if( found ) {
        found = ( p.i_probe_.count( s.s_i_id ) == 1 ) and
                ( p.s_probe_.count( s.s_s_id ) );
    }

    return std::make_tuple<>( found, s.s_i_id, 1 );
}

void q16_stock_mapper( const std::vector<result_tuple>&        res_tuples,
                       std::unordered_map<q16_stock_kv_types>& res,
                       const q16_stock_probe&                  p ) {

    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, int32_t /* i_id */, uint64_t> mapped_res =
            q16_stock_map( res_tuple, p );
        DVLOG( 40 ) << "Q16 map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            } else {
                found->second += std::get<2>( mapped_res );
            }
        }
    }
}
void q16_stock_reducer( const std::unordered_map<q16_stock_kv_vec_types>& input,
                        std::unordered_map<q16_stock_kv_types>&           res,
                        const q16_stock_probe&                            p ) {
    for( const auto& entry : input ) {
        uint64_t merge = 0;
        for( const auto& val : entry.second ) {
            merge += val;
        }
        res[entry.first] = merge;
    }
}

std::tuple<bool, int32_t, int32_t> q19_item_map( const result_tuple& res ) {
    bool  found = false;
    item i;
    i.i_id = -1;
    i.i_price = -1;
    i.i_data = "";

    auto read_cols = read_from_scan_item( res, &i );
    i.i_id = (int32_t) res.row_id;

    found = !read_cols.empty();
    return std::make_tuple<>( found, (int32_t) res.row_id,
                              (int32_t) res.row_id );
}

void q19_item_mapper( const std::vector<result_tuple>&       res_tuples,
                      std::unordered_map<q19_item_kv_types>& res,
                      const EmptyProbe&                      p ) {

    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, int32_t /* i_id */, int32_t> mapped_res =
            q19_item_map( res_tuple );
        DVLOG( 40 ) << "Q19 map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            }
        }
    }
}
void q19_item_reducer( const std::unordered_map<q19_item_kv_vec_types>& input,
                       std::unordered_map<q19_item_kv_types>&           res,
                       const EmptyProbe&                                p ) {

    for( const auto& entry : input ) {
        res[entry.first] = entry.first;
        DVLOG( 40 ) << "Q19 reduce:" << entry.first;
    }
}

q19_order_line_probe::q19_order_line_probe(
    const std::unordered_map<q19_item_kv_types>& probe,
    const tpcc_configs&                          cfg )
    : probe_( probe ), cfg_( cfg ) {}

std::tuple<bool, int32_t, float> q19_order_line_map(
    const result_tuple& res, const q19_order_line_probe& p ) {
    order_line ol;
    ol.ol_i_id = -1;
    ol.ol_amount = 0;

    auto read_cols = read_from_scan_order_line( res, &ol );

    bool found = ( read_cols.count( order_line_cols::ol_i_id ) == 1 );
    if( found ) {
        found = ( p.probe_.count( ol.ol_i_id ) == 1 );
    }
    return std::make_tuple<>( found, ol.ol_i_id, ol.ol_amount );
}

void q19_order_line_mapper( const std::vector<result_tuple>& res_tuples,
                            std::unordered_map<q19_order_line_kv_types>& res,
                            const q19_order_line_probe&                  p ) {
    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, int32_t /* i_id */, float> mapped_res =
            q19_order_line_map( res_tuple, p );
        DVLOG( 40 ) << "Q19 map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            } else {
                found->second += std::get<2>( mapped_res );
            }
        }
    }
}
void q19_order_line_reducer(
    const std::unordered_map<q19_order_line_kv_vec_types>& input,
    std::unordered_map<q19_order_line_kv_types>&           res,
    const q19_order_line_probe&                            p ) {
    for( const auto& entry : input ) {
        float ol_amount = 0;
        for( const auto& val : entry.second ) {
            ol_amount += val;
        }
        res[entry.first] = ol_amount;
        DVLOG( 40 ) << "Q19 reduce:" << entry.first;
    }
}

std::tuple<bool, uint64_t, int32_t> q22_order_map( const result_tuple& res,
                                                   const tpcc_configs& cfg ) {
    order o;
    o.o_c_id = -1;
    o.o_d_id = -1;
    o.o_w_id = -1;

    auto read_cols = read_from_scan_order( res, &o );

    bool found = ( read_cols.count( order_cols::o_c_id ) == 1 ) and
                 ( read_cols.count( order_cols::o_d_id ) == 1 ) and
                 ( read_cols.count( order_cols::o_w_id ) == 1 );

    uint64_t key = 0;
    if( found ) {
        customer c;
        c.c_id = o.o_c_id;
        c.c_d_id = o.o_d_id;
        c.c_w_id = o.o_w_id;
        key = make_customer_key( c, cfg );
    }

    return std::make_tuple<>( found, key, 1 );
}
void q22_order_mapper( const std::vector<result_tuple>&        res_tuples,
                       std::unordered_map<q22_order_kv_types>& res,
                       const tpcc_configs&                     cfg ) {

    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, uint64_t /* i_id */, int32_t> mapped_res =
            q22_order_map( res_tuple, cfg );
        DVLOG( 40 ) << "Q22 map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            }
        }
    }
}
void q22_order_reducer( const std::unordered_map<q22_order_kv_vec_types>& input,
                        std::unordered_map<q22_order_kv_types>&           res,
                        const tpcc_configs& cfg ) {
    for( const auto& entry : input ) {
        res[entry.first] = 0;
    }
}

q22_customer_probe::q22_customer_probe(
    const std::unordered_map<q22_order_kv_types>& probe,
    const tpcc_configs&                           cfg )
    : probe_( probe ), cfg_( cfg ) {}

std::tuple<bool, uint64_t, customer> q22_customer_map(
    const result_tuple& res, const q22_customer_probe& p ) {
    customer c;
    c.c_id = -1;
    c.c_d_id = -1;
    c.c_w_id = -1;
    c.c_balance = 0;
    c.c_phone = "";

    auto     read_cols = read_from_scan_customer( res, &c );
    bool     found = !read_cols.empty();
    if( found ) {
        found = ( p.probe_.count( res.row_id ) == 0 );
    }

    return std::make_tuple<>( found, res.row_id, c );
}

void q22_customer_reduce( customer& c, const customer& other ) {
    if( c.c_id < 0 ) {
        c.c_id = other.c_id;
    }
    if( c.c_d_id < 0 ) {
        c.c_d_id = other.c_d_id;
    }
    if( c.c_w_id < 0 ) {
        c.c_w_id = other.c_w_id;
    }
    if( c.c_balance == 0 ) {
        c.c_balance = other.c_balance;
    }
    if( c.c_phone.empty() ) {
        c.c_phone = other.c_phone;
    }
}

void q22_customer_mapper( const std::vector<result_tuple>&           res_tuples,
                          std::unordered_map<q22_customer_kv_types>& res,
                          const q22_customer_probe&                  p ) {

    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, uint64_t /* i_id */, customer> mapped_res =
            q22_customer_map( res_tuple, p );
        DVLOG( 40 ) << "Q22 map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            } else {
                q22_customer_reduce( found->second, std::get<2>( mapped_res ) );
            }
        }
    }
}
void q22_customer_reducer(
    const std::unordered_map<q22_customer_kv_vec_types>& input,
    std::unordered_map<q22_customer_kv_types>&           res,
    const q22_customer_probe&                            p ) {
    for( const auto& entry : input ) {
        customer merge_res;
        merge_res.c_id = -1;
        merge_res.c_d_id = -1;
        merge_res.c_w_id = -1;
        merge_res.c_balance = 0;
        merge_res.c_phone = "";
        for( const auto& val : entry.second ) {
            q22_customer_reduce( merge_res, val );
        }
    }
}

void q20_item_mapper( const std::vector<result_tuple>&       res_tuples,
                      std::unordered_map<q20_item_kv_types>& res,
                      const EmptyProbe&                      p ) {
    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, int32_t /* i_id */, item> mapped_res =
            q14_item_map( res_tuple );
        DVLOG( 40 ) << "Q20 map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            } else {
                q14_item_reduce( found->second, std::get<2>( mapped_res ) );
            }
        }
    }
}
void q20_item_reducer( const std::unordered_map<q20_item_kv_vec_types>& input,
                       std::unordered_map<q20_item_kv_types>&           res,
                       const EmptyProbe&                                p ) {
    for( const auto& entry : input ) {
        item merge_res;
        merge_res.i_id = -1;
        merge_res.i_data = "";

        for( const auto& val : entry.second ) {
            q14_item_reduce( merge_res, val );
        }

        res[entry.first] = merge_res;
        DVLOG( 40 ) << "Q20 reduce:" << entry.first;
    }
}

q20_stock_probe::q20_stock_probe(
    const std::unordered_map<q20_item_kv_types>& probe,
    const tpcc_configs&                          cfg )
    : probe_( probe ), cfg_( cfg ) {}

std::tuple<bool, uint64_t, stock> q20_stock_map( const result_tuple& res,
                                                 const q20_stock_probe& p ) {
    stock s;
    s.s_i_id = -1;
    s.s_w_id = -1;
    s.s_s_id = -1;
    s.s_quantity = 0;

    auto read_cols = read_from_scan_stock( res, &s );
    bool found = ( read_cols.count( stock_cols::s_i_id ) == 1 );

    if( found ) {
        found = ( p.probe_.count( s.s_i_id ) == 1 );
    }

    return std::make_tuple<>( found, (int32_t) res.row_id, s );
}

void q20_stock_reduce( stock& s, const stock& other ) {
    if( s.s_i_id < 0 ) {
        s.s_i_id = other.s_i_id;
    }
    if( s.s_w_id < 0 ) {
        s.s_w_id = other.s_w_id;
    }
    if( s.s_s_id < 0 ) {
        s.s_s_id = other.s_s_id;
    }
    if( s.s_quantity < 0 ) {
        s.s_quantity = other.s_quantity;
    }
}

void q20_stock_mapper( const std::vector<result_tuple>&        res_tuples,
                       std::unordered_map<q20_stock_kv_types>& res,
                       const q20_stock_probe&                  p ) {

    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, uint64_t /* stock id */, stock>
            mapped_res = q20_stock_map( res_tuple, p );
        DVLOG( 40 ) << "Q20 map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            } else {
                q20_stock_reduce( found->second, std::get<2>( mapped_res ) );
            }
        }
    }
}
void q20_stock_reducer( const std::unordered_map<q20_stock_kv_vec_types>& input,
                        std::unordered_map<q20_stock_kv_types>&           res,
                        const q20_stock_probe&                            p ) {

    for( const auto& entry : input ) {
        stock merge_res;
        merge_res.s_i_id = -1;
        merge_res.s_w_id = -1;
        merge_res.s_s_id = -1;
        merge_res.s_quantity = 0;

        for( const auto& val : entry.second ) {
            q20_stock_reduce( merge_res, val );
        }

        res[entry.first] = merge_res;
        DVLOG( 40 ) << "Q20 reduce:" << entry.first;
    }
}

q20_order_line_probe::q20_order_line_probe(
    const std::unordered_map<q20_stock_kv_types>& probe,
    const tpcc_configs&                           cfg )
    : probe_( probe ), cfg_( cfg ) {}

std::tuple<bool, uint64_t, float> q20_order_line_map(
    const result_tuple& res, const q20_order_line_probe& p ) {
    order_line ol;
    ol.ol_w_id = -1;
    ol.ol_i_id = -1;
    ol.ol_quantity = 0;

    auto read_cols = read_from_scan_order_line( res, &ol );
    bool found = ( read_cols.count( order_line_cols::ol_w_id ) == 1 ) and
                 ( read_cols.count( order_line_cols::ol_i_id ) == 1 );

    uint64_t key = 0;

    if( found ) {
        stock s;
        s.s_i_id = ol.ol_i_id;
        s.s_w_id = ol.ol_w_id;

        key = make_stock_key( s, p.cfg_ );

        found = p.probe_.count( key ) == 1;
    }

    return std::make_tuple<>( found, key, ol.ol_quantity );
}

void q20_order_line_mapper( const std::vector<result_tuple>& res_tuples,
                            std::unordered_map<q20_order_line_kv_types>& res,
                            const q20_order_line_probe&                  p ) {

    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, uint64_t /* stock id */, float>
            mapped_res = q20_order_line_map( res_tuple, p );
        DVLOG( 40 ) << "Q20 map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            } else {
                found->second += std::get<2>( mapped_res );
            }
        }
    }
}
void q20_order_line_reducer(
    const std::unordered_map<q20_order_line_kv_vec_types>& input,
    std::unordered_map<q20_order_line_kv_types>&           res,
    const q20_order_line_probe&                            p ) {
    for( const auto& entry : input ) {
        float merge = 0;
        for( const auto& val : entry.second ) {
            merge += val;
        }

        res[entry.first] = merge;
    }
}

q20_supplier_probe::q20_supplier_probe(
    const std::unordered_map<q20_stock_kv_types>&      s_probe,
    const std::unordered_map<q20_order_line_kv_types>& ol_probe,
    const tpcc_configs&                                cfg )
    : s_probe_( s_probe ),
      ol_probe_( ol_probe ),
      cfg_( cfg ),
      supplier_ids_() {}
void q20_supplier_probe::compute( float s_quant_multi ) {
    for( const auto& stock_entry : s_probe_ ) {
        uint64_t stock_key = stock_entry.first;
        auto     ol_found = ol_probe_.find( stock_key );
        if( ol_found == ol_probe_.end() ) {
            continue;
        }

        if( ( s_quant_multi * stock_entry.second.s_quantity ) >
            ol_found->second ) {
            supplier_ids_.emplace( stock_entry.second.s_s_id );
        }
    }
}

std::tuple<bool, int32_t, supplier> q20_supplier_map(
    const result_tuple& res, const q20_supplier_probe& probe ) {
    bool     found = false;
    supplier s;
    s.s_id = -1;
    s.s_name = "";

    auto read_cols = read_from_scan_supplier( res, &s );
    s.s_id = (int32_t) res.row_id;

    found = !read_cols.empty();

    if( found ) {
        found = probe.supplier_ids_.count( s.s_id ) == 1;
    }

    return std::make_tuple<>( found, s.s_id, s );
}

void q20_supplier_reduce( supplier& s, const supplier& other ) {
    if( s.s_id < 0 ) {
        s.s_id = other.s_id;
    }
    if( s.s_name.empty() ) {
        s.s_name = other.s_name;
    }
}

void q20_supplier_mapper( const std::vector<result_tuple>&           res_tuples,
                          std::unordered_map<q20_supplier_kv_types>& res,
                          const q20_supplier_probe&                  p ) {

    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, int32_t /* supplier id */, supplier>
            mapped_res = q20_supplier_map( res_tuple, p );
        DVLOG( 40 ) << "Q20 map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            } else {
                q20_supplier_reduce( found->second, std::get<2>( mapped_res ) );
            }
        }
    }
}
void q20_supplier_reducer(
    const std::unordered_map<q20_supplier_kv_vec_types>& input,
    std::unordered_map<q20_supplier_kv_types>&           res,
    const q20_supplier_probe&                            p ) {

    for( const auto& entry : input ) {
        supplier merge_res;
        merge_res.s_id = -1;
        merge_res.s_name = "";

        for( const auto& val : entry.second ) {
            q20_supplier_reduce( merge_res, val );
        }

        res[entry.first] = merge_res;
        DVLOG( 40 ) << "Q20 reduce:" << entry.first;
    }
}

std::tuple<bool, int32_t, supplier> q21_supplier_map(
    const result_tuple& res ) {
    bool     found = false;
    supplier s;
    s.s_id = -1;
    s.s_name = "";

    auto read_cols = read_from_scan_supplier( res, &s );
    s.s_id = (int32_t) res.row_id;

    found = !read_cols.empty();

    return std::make_tuple<>( found, s.s_id, s );
}

void q21_supplier_reduce( supplier& s, const supplier& other ) {
    if( s.s_id < 0 ) {
        s.s_id = other.s_id;
    }
    if( s.s_name.empty() ) {
        s.s_name = other.s_name;
    }
}

void q21_supplier_mapper( const std::vector<result_tuple>&           res_tuples,
                          std::unordered_map<q21_supplier_kv_types>& res,
                          const EmptyProbe&                          p ) {

    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, int32_t /* supplier id */, supplier>
            mapped_res = q21_supplier_map( res_tuple );
        DVLOG( 40 ) << "Q21 map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            } else {
                q21_supplier_reduce( found->second, std::get<2>( mapped_res ) );
            }
        }
    }
}

void q21_supplier_reducer(
    const std::unordered_map<q21_supplier_kv_vec_types>& input,
    std::unordered_map<q21_supplier_kv_types>& res, const EmptyProbe& p ) {

    for( const auto& entry : input ) {
        supplier merge_res;
        merge_res.s_id = -1;
        merge_res.s_name = "";

        for( const auto& val : entry.second ) {
            q21_supplier_reduce( merge_res, val );
        }

        res[entry.first] = merge_res;
        DVLOG( 40 ) << "Q21 reduce:" << entry.first;
    }
}

q21_stock_probe::q21_stock_probe(
    const std::unordered_map<q21_supplier_kv_types>& probe,
    const tpcc_configs&                              cfg )
    : probe_( probe ), cfg_( cfg ) {}

std::tuple<bool, uint64_t, int32_t> q21_stock_map( const result_tuple& res,
                                                   const q21_stock_probe& p ) {
    stock s;
    s.s_i_id = -1;
    s.s_w_id = -1;
    s.s_s_id = -1;

    auto read_cols = read_from_scan_stock( res, &s );
    bool found = ( read_cols.count( stock_cols::s_s_id ) == 1 );

    if( found ) {
        found = ( p.probe_.count( s.s_s_id ) == 1 );
    }

    return std::make_tuple<>( found, res.row_id, s.s_s_id );
}

void q21_stock_mapper( const std::vector<result_tuple>&        res_tuples,
                       std::unordered_map<q21_stock_kv_types>& res,
                       const q21_stock_probe&                  p ) {

    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, uint64_t /* stock id */, int32_t>
            mapped_res = q21_stock_map( res_tuple, p );
        DVLOG( 40 ) << "Q21 map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            } else {
                found->second =
                    std::max( std::get<2>( mapped_res ), found->second );
            }
        }
    }
}
void q21_stock_reducer( const std::unordered_map<q21_stock_kv_vec_types>& input,
                        std::unordered_map<q21_stock_kv_types>&           res,
                        const q21_stock_probe&                            p ) {
    for( const auto& entry : input ) {
        int32_t s_id = -1;
        for( const auto& val : entry.second ) {
            s_id = std::max( s_id, val );
        }

        res[entry.first] = s_id;
    }
}

std::tuple<bool, uint64_t, uint64_t> q21_order_map( const result_tuple& res ) {
    bool  found = false;
    order o;
    o.o_id = -1;
    o.o_d_id = -1;
    o.o_w_id = -1;
    o.o_c_id = -1;
    o.o_entry_d.c_since = datetime::EMPTY_DATE;

    auto read_cols = read_from_scan_order( res, &o );

    found = read_cols.count( order_cols::o_entry_d_c_since ) == 1;
    return std::make_tuple<>( found, res.row_id, o.o_entry_d.c_since );
}

void q21_order_mapper( const std::vector<result_tuple>&        res_tuples,
                       std::unordered_map<q21_order_kv_types>& res,
                       const EmptyProbe&                       p ) {

    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, uint64_t /* order id */, uint64_t>
            mapped_res = q21_order_map( res_tuple );
        DVLOG( 40 ) << "Q21 map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            } else {
                found->second =
                    std::max( std::get<2>( mapped_res ), found->second );
            }
        }
    }
}
void q21_order_reducer( const std::unordered_map<q21_order_kv_vec_types>& input,
                        std::unordered_map<q21_order_kv_types>&           res,
                        const EmptyProbe&                                 p ) {
    for( const auto& entry : input ) {
        uint64_t c_since = datetime::EMPTY_DATE;
        for( const auto& val : entry.second ) {
            c_since = std::max( val, c_since );
        }
        res[entry.first] = c_since;
        DVLOG( 40 ) << "Q21 reduce:" << entry.first;
    }
}

q21_order_line_probe::q21_order_line_probe(
    const std::unordered_map<q21_stock_kv_types>& s_probe,
    const std::unordered_map<q21_order_kv_types>& o_probe,
    const tpcc_configs&                           cfg )
    : s_probe_( s_probe ), o_probe_( o_probe ), cfg_( cfg ) {}

std::tuple<bool, uint64_t, uint64_t> q21_order_line_map(
    const result_tuple& res, const q21_order_line_probe& p ) {
    order_line ol;
    ol.ol_o_id = -1;
    ol.ol_w_id = -1;
    ol.ol_d_id = -1;
    ol.ol_i_id = -1;
    ol.ol_delivery_d.c_since = datetime::EMPTY_DATE;

    auto read_cols = read_from_scan_order_line( res, &ol );

    bool found =
        ( read_cols.count( order_line_cols::ol_o_id ) == 1 ) and
        ( read_cols.count( order_line_cols::ol_w_id ) == 1 ) and
        ( read_cols.count( order_line_cols::ol_d_id ) == 1 ) and
        ( read_cols.count( order_line_cols::ol_i_id ) == 1 ) and
        ( read_cols.count( order_line_cols::ol_delivery_d_c_since ) == 1 );

    uint64_t stock_key = 0;

    if( found ) {
        order o;
        o.o_id = ol.ol_o_id;
        o.o_w_id = ol.ol_w_id;
        o.o_d_id = ol.ol_d_id;
        o.o_c_id = 0;

        uint64_t o_key = make_order_key( o, p.cfg_ );

        auto o_found = p.o_probe_.find( o_key );
        if( o_found == p.o_probe_.end() ) {
            found = false;
        } else {
            found = ( ol.ol_delivery_d.c_since > o_found->second );
        }

        if( found ) {

            stock s;
            s.s_i_id = ol.ol_i_id;
            s.s_w_id = ol.ol_w_id;

            stock_key = make_stock_key( s, p.cfg_ );

            found = ( p.s_probe_.count( stock_key ) == 1 );
        }
    }

    return std::make_tuple<>( found, res.row_id, stock_key );
}

void q21_order_line_mapper( const std::vector<result_tuple>& res_tuples,
                            std::unordered_map<q21_order_line_kv_types>& res,
                            const q21_order_line_probe&                  p ) {

    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, uint64_t /* ol key */,
                   uint64_t /* stock key */>
            mapped_res = q21_order_line_map( res_tuple, p );
        DVLOG( 40 ) << "Q21 map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            } else {
                found->second =
                    std::max( std::get<2>( mapped_res ), found->second );
            }
        }
    }
}

void q21_order_line_reducer(
    const std::unordered_map<q21_order_line_kv_vec_types>& input,
    std::unordered_map<q21_order_line_kv_types>&           res,
    const q21_order_line_probe&                            p ) {

    for( const auto& entry : input ) {
        uint64_t stock_key = 0;
        for( const auto& val : entry.second ) {
            stock_key = std::max( val, stock_key );
        }
        res[entry.first] = stock_key;
        DVLOG( 40 ) << "Q21 reduce:" << entry.first;
    }
}

void q17_item_mapper( const std::vector<result_tuple>&       res_tuples,
                      std::unordered_map<q17_item_kv_types>& res,
                      const EmptyProbe&                      p ) {

    for( const auto& res_tuple: res_tuples ) {
        DVLOG( 40 ) << "Q17 Map:" << res_tuple.row_id;
        res.emplace( (int32_t) res_tuple.row_id, 1 );
    }
}
void q17_item_reducer( const std::unordered_map<q17_item_kv_vec_types>& input,
                       std::unordered_map<q17_item_kv_types>&           res,
                       const EmptyProbe&                                p ) {
    for( const auto& entry : input ) {
        res[entry.first] = 0;
        DVLOG( 40 ) << "Q17 Reduce:" << entry.first;
    }
}

q17_order_line_probe::q17_order_line_probe(
    const std::unordered_map<q17_item_kv_types>& probe,
    const tpcc_configs&                          cfg )
    : probe_( probe ), cfg_( cfg ) {}

std::tuple<bool, int32_t, q1_agg> q17_order_line_map(
    const result_tuple& res, const q17_order_line_probe& p ) {

    q1_agg ret;

    order_line ol;
    ol.ol_i_id = -1;

    auto read_cols = read_from_scan_order_line( res, &ol );
    bool found = ( read_cols.count( order_line_cols::ol_i_id ) == 1 );

    if( found ) {
        found = p.probe_.count( ol.ol_i_id ) == 1;
    }

    if( read_cols.count( order_line_cols::ol_quantity ) ) {
        ret.quantity_count_ += 1;
        ret.quantity_sum_ += ol.ol_quantity;
    }
    if( read_cols.count( order_line_cols::ol_amount ) ) {
        ret.amount_count_ += 1;
        ret.amount_sum_ += ol.ol_amount;
    }

    return std::make_tuple<>( found, ol.ol_i_id, ret );
}

void q17_order_line_mapper( const std::vector<result_tuple>& res_tuples,
                            std::unordered_map<q17_order_line_kv_types>& res,
                            const q17_order_line_probe&                  p ) {

    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, int32_t /* i id */, q1_agg> mapped_res =
            q17_order_line_map( res_tuple, p );
        DVLOG( 40 ) << "Q17 map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res ) << ", "
                    << std::get<1>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            } else {
                found->second.reduce( std::get<2>( mapped_res ) );
            }
        }
    }
}
void q17_order_line_reducer(
    const std::unordered_map<q17_order_line_kv_vec_types>& input,
    std::unordered_map<q17_order_line_kv_types>&           res,
    const q17_order_line_probe&                            p ) {
    for( const auto& entry : input ) {
        q1_agg merge;
        for( const auto& val : entry.second ) {
            merge.reduce( val );
        }
        res[entry.first] = merge;
        DVLOG( 40 ) << "Q17 Reduce:" << entry.first;
    }
}

void q2_nation_mapper( const std::vector<result_tuple>&        res_tuples,
                       std::unordered_map<q2_nation_kv_types>& res,
                       const EmptyProbe&                       p ) {

    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, int32_t /* n_id */, nation> mapped_res =
            q5_nation_map( res_tuple );
        DVLOG( 40 ) << "Q2 map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            } else {
                q5_nation_reduce( found->second, std::get<2>( mapped_res ) );
            }
        }
    }
}
void q2_nation_reducer( const std::unordered_map<q2_nation_kv_vec_types>& input,
                        std::unordered_map<q2_nation_kv_types>&           res,
                        const EmptyProbe&                                 p ) {

    for( const auto& entry : input ) {
        nation merge_res;
        merge_res.n_id = -1;
        merge_res.r_id = -1;
        merge_res.n_name = "";

        for( const auto& val : entry.second ) {
            q5_nation_reduce( merge_res, val );
        }

        res[entry.first] = merge_res;
        DVLOG( 40 ) << "Q2 reduce:" << entry.first;
    }
}

q2_supplier_probe::q2_supplier_probe(
    const std::unordered_map<q2_nation_kv_types>& probe,
    const tpcc_configs&                           cfg )
    : probe_( probe ), cfg_( cfg ) {}

std::tuple<bool, int32_t, supplier> q2_supplier_map(
    const result_tuple& res, const q2_supplier_probe& p ) {
    bool     found = false;
    supplier s;
    s.s_id = -1;
    s.n_id = -1;
    s.s_name = "";
    s.s_comment = "";

    auto read_cols = read_from_scan_supplier( res, &s );
    s.s_id = (int32_t) res.row_id;

    int32_t key = (int32_t) res.row_id;
    found = read_cols.count( supplier_cols::n_id ) == 1;

    if( found ) {
        found = p.probe_.count( s.n_id ) == 1;
    }

    return std::make_tuple<>( found, key, s );
}

void q2_supplier_reduce( supplier& s, const supplier& other ) {
    if( s.s_id < 0 ) {
        s.s_id = other.s_id;
    }
    if( s.n_id < 0 ) {
        s.n_id = other.n_id;
    }
    if( s.s_name.empty() ) {
        s.s_name = other.s_name;
    }
    if( s.s_comment.empty() ) {
        s.s_comment = other.s_comment;
    }
}

void q2_supplier_mapper( const std::vector<result_tuple>&          res_tuples,
                         std::unordered_map<q2_supplier_kv_types>& res,
                         const q2_supplier_probe&                  p ) {

    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, int32_t /* s_id */, supplier> mapped_res =
            q2_supplier_map( res_tuple, p );
        DVLOG( 40 ) << "Q2 map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            } else {
                q2_supplier_reduce( found->second, std::get<2>( mapped_res ) );
            }
        }
    }
}
void q2_supplier_reducer(
    const std::unordered_map<q2_supplier_kv_vec_types>& input,
    std::unordered_map<q2_supplier_kv_types>& res, const q2_supplier_probe& p ) {

    for( const auto& entry : input ) {
        supplier merge_res;
        merge_res.s_id = -1;
        merge_res.n_id = -1;
        merge_res.s_name = "";
        merge_res.s_comment = "";

        for( const auto& val : entry.second ) {
            q2_supplier_reduce( merge_res, val );
        }

        res[entry.first] = merge_res;
        DVLOG( 40 ) << "Q2 reduce:" << entry.first;
    }
}

std::tuple<bool, int32_t, item> q2_item_map( const result_tuple& res ) {
    bool  found = false;
    item i;
    i.i_id = -1;
    i.i_name = "";
    i.i_data = "";

    auto read_cols = read_from_scan_item( res, &i );
    i.i_id = (int32_t) res.row_id;

    found = !read_cols.empty();
    return std::make_tuple<>( found, (int32_t) res.row_id, i );
}
void q2_item_reduce( item& i, const item& other ) {
    if( i.i_id < 0 ) {
        i.i_id = other.i_id;
    }
    if( i.i_data.empty() ) {
        i.i_data = other.i_data;
    }
    if( i.i_name.empty() ) {
        i.i_name = other.i_name;
    }
}

void q2_item_mapper( const std::vector<result_tuple>&      res_tuples,
                     std::unordered_map<q2_item_kv_types>& res,
                     const EmptyProbe&                     p ) {

    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, int32_t /* i_id */, item> mapped_res =
            q2_item_map( res_tuple );
        DVLOG( 40 ) << "Q2 map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            } else {
                q2_item_reduce( found->second, std::get<2>( mapped_res ) );
            }
        }
    }
}
void q2_item_reducer( const std::unordered_map<q2_item_kv_vec_types>& input,
                      std::unordered_map<q2_item_kv_types>&           res,
                      const EmptyProbe&                               p ) {

    for( const auto& entry : input ) {
        item merge_res;
        merge_res.i_id = -1;
        merge_res.i_name = "";
        merge_res.i_data = "";

        for( const auto& val : entry.second ) {
            q2_item_reduce( merge_res, val );
        }

        res[entry.first] = merge_res;
        DVLOG( 40 ) << "Q2 reduce:" << entry.first;
    }
}

q2_stock_probe::q2_stock_probe(
    const std::unordered_map<q2_supplier_kv_types>& s_probe,
    const std::unordered_map<q2_item_kv_types>&     i_probe,
    const tpcc_configs&                             cfg )
    : s_probe_( s_probe ), i_probe_( i_probe ), cfg_( cfg ) {}

std::tuple<bool, int32_t, stock> q2_stock_map( const result_tuple& res,
                                               const q2_stock_probe& probe ) {
    bool  found = false;
    stock s;
    s.s_i_id = -1;
    s.s_s_id = -1;
    s.s_quantity = INT32_MAX;

    auto read_cols = read_from_scan_stock( res, &s );

    found = ( read_cols.count( stock_cols::s_i_id ) == 1 ) and
            ( read_cols.count( stock_cols::s_s_id ) == 1 );
    int32_t key = s.s_i_id;

    if( found ) {
        found = ( probe.i_probe_.count( s.s_i_id ) == 1 ) and
                ( probe.s_probe_.count( s.s_s_id ) == 1 );
    }

    return std::make_tuple<>( found, key, s );
}

void q2_stock_reduce( stock& s, const stock& other ) {
    if( other.s_quantity < s.s_quantity ) {
        s.s_i_id = other.s_i_id;
        s.s_s_id = other.s_s_id;
        s.s_quantity = other.s_quantity;
    }
}

void q2_stock_mapper( const std::vector<result_tuple>&       res_tuples,
                      std::unordered_map<q2_stock_kv_types>& res,
                      const q2_stock_probe&                  p ) {

    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, int32_t /* i_id */, stock> mapped_res =
            q2_stock_map( res_tuple, p );
        DVLOG( 40 ) << "Q2 map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            } else {
                q2_stock_reduce( found->second, std::get<2>( mapped_res ) );
            }
        }
    }
}
void q2_stock_reducer( const std::unordered_map<q2_stock_kv_vec_types>& input,
                       std::unordered_map<q2_stock_kv_types>&           res,
                       const q2_stock_probe&                            p ) {

    for( const auto& entry : input ) {
        stock merge_res;
        merge_res.s_i_id = -1;
        merge_res.s_s_id = -1;
        merge_res.s_quantity = INT32_MAX;

        for( const auto& val : entry.second ) {
            q2_stock_reduce( merge_res, val );
        }

        res[entry.first] = merge_res;
        DVLOG( 40 ) << "Q2 reduce:" << entry.first;
    }
}

std::tuple<bool, int32_t, int32_t> q7_supplier_map( const result_tuple& res ) {
    bool     found = false;
    supplier s;
    s.n_id = -1;

    auto read_cols = read_from_scan_supplier( res, &s );
    s.s_id = (int32_t) res.row_id;

    found = read_cols.count( supplier_cols::n_id ) == 1;
    int32_t key = (int32_t) res.row_id;

    return std::make_tuple<>( found, key, s.n_id );
}

void q7_supplier_mapper( const std::vector<result_tuple>&          res_tuples,
                         std::unordered_map<q7_supplier_kv_types>& res,
                         const EmptyProbe&                         p ) {

    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, int32_t /* s_id */, int32_t> mapped_res =
            q7_supplier_map( res_tuple );
        DVLOG( 40 ) << "Q7 map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            }
        }
    }
}
void q7_supplier_reducer(
    const std::unordered_map<q7_supplier_kv_vec_types>& input,
    std::unordered_map<q7_supplier_kv_types>& res, const EmptyProbe& p ) {
    for( const auto& entry : input ) {
        int32_t n_id = -1;
        for( const auto& val : entry.second ) {
            n_id = std::max( n_id, val );
        }
        res[entry.first] = n_id;
        DVLOG( 40 ) << "Q7 Reduce:" << entry.first;
    }
}

q7_stock_probe::q7_stock_probe(
    const std::unordered_map<q7_supplier_kv_types>& probe,
    const tpcc_configs&                             cfg )
    : probe_( probe ), cfg_( cfg ) {}

std::tuple<bool, uint64_t, int32_t> q7_stock_map(
    const result_tuple& res, const q7_stock_probe& probe ) {
    bool  found = false;
    stock s;
    s.s_s_id = -1;

    auto read_cols = read_from_scan_stock( res, &s );

    found = !read_cols.empty();
    uint64_t key = res.row_id;

    if( found ) {
        found = ( probe.probe_.count( s.s_s_id ) == 1 );
    }

    return std::make_tuple<>( found, key, s.s_s_id );
}

void q7_stock_mapper( const std::vector<result_tuple>&       res_tuples,
                      std::unordered_map<q7_stock_kv_types>& res,
                      const q7_stock_probe&                  p ) {

    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, uint64_t /* stock_id */,
                   int32_t /* s_id */>
            mapped_res = q7_stock_map( res_tuple, p );
        DVLOG( 40 ) << "Q7 map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            }
        }
    }
}
void q7_stock_reducer( const std::unordered_map<q7_stock_kv_vec_types>& input,
                       std::unordered_map<q7_stock_kv_types>&           res,
                       const q7_stock_probe&                            p ) {
    for( const auto& entry : input ) {
        int32_t s_id = -1;
        for( const auto& val : entry.second ) {
            s_id = std::max( s_id, val );
        }
        res[entry.first] = s_id;
        DVLOG( 40 ) << "Q7 Reduce:" << entry.first;
    }
}

std::tuple<bool, uint64_t, int32_t> q7_customer_map( const result_tuple& res ) {
    bool     found = false;
    customer c;
    c.c_n_id = -1;

    auto read_cols = read_from_scan_customer( res, &c );
    found = read_cols.count( customer_cols::c_n_id ) == 1;
    return std::make_tuple<>( found, res.row_id, c.c_n_id );
}

void q7_customer_mapper( const std::vector<result_tuple>&          res_tuples,
                         std::unordered_map<q7_customer_kv_types>& res,
                         const EmptyProbe&                         p ) {

    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, uint64_t /* c_id */, int32_t> mapped_res =
            q7_customer_map( res_tuple );
        DVLOG( 40 ) << "Q7 map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            }
        }
    }
}
void q7_customer_reducer(
    const std::unordered_map<q7_customer_kv_vec_types>& input,
    std::unordered_map<q7_customer_kv_types>& res, const EmptyProbe& p ) {
    for( const auto& entry : input ) {
        int32_t n_id = -1;
        for( const auto& val : entry.second ) {
            n_id = std::max( n_id, val );
        }
        res[entry.first] = n_id;
        DVLOG( 40 ) << "Q7 Reduce:" << entry.first;
    }
}


q7_order_probe::q7_order_probe(
    const std::unordered_map<q7_customer_kv_types>& probe,
    const tpcc_configs&                             cfg )
    : probe_( probe ), cfg_( cfg ) {}

std::tuple<bool, uint64_t, uint64_t> q7_order_map( const result_tuple& res,
                                                   const q7_order_probe& p ) {
    bool  found = false;
    order o;
    o.o_id = -1;
    o.o_d_id = -1;
    o.o_w_id = -1;
    o.o_c_id = -1;

    auto read_cols = read_from_scan_order( res, &o );

    found = ( read_cols.count( order_cols::o_id ) == 1 ) and
            ( read_cols.count( order_cols::o_c_id ) == 1 ) and
            ( read_cols.count( order_cols::o_d_id ) == 1 ) and
            ( read_cols.count( order_cols::o_w_id ) == 1 );

    uint64_t key = 0;
    if( found ) {
        customer c;
        c.c_id = o.o_c_id;
        c.c_d_id = o.o_d_id;
        c.c_w_id = o.o_w_id;

        key = make_customer_key( c, p.cfg_ );
        found = p.probe_.count( key ) == 1;
    }

    return std::make_tuple<>( found, res.row_id, key );
}

void q7_order_mapper( const std::vector<result_tuple>&       res_tuples,
                      std::unordered_map<q7_order_kv_types>& res,
                      const q7_order_probe&                  p ) {
    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, uint64_t /* o_id */, uint64_t> mapped_res =
            q7_order_map( res_tuple, p );
        DVLOG( 40 ) << "Q7 map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            }
        }
    }
}
void q7_order_reducer( const std::unordered_map<q7_order_kv_vec_types>& input,
                       std::unordered_map<q7_order_kv_types>&           res,
                       const q7_order_probe&                            p ) {

    for( const auto& entry : input ) {
        uint64_t c_id = 0;
        for( const auto& val : entry.second ) {
            c_id = std::max( c_id, val );
        }
        res[entry.first] = c_id;
        DVLOG( 40 ) << "Q7 Reduce:" << entry.first;
    }
}

q7_order_line_key::q7_order_line_key( uint64_t stock_key, uint64_t customer_key,
                                      uint64_t year )
    : stock_key_( stock_key ), customer_key_( customer_key ), year_( year ) {}

q7_order_line_probe::q7_order_line_probe(
    const std::unordered_map<q7_order_kv_types>& o_probe,
    const std::unordered_map<q7_stock_kv_types>& s_probe,
    const tpcc_configs&                          cfg )
    : o_probe_( o_probe ), s_probe_( s_probe ), cfg_( cfg ) {}

std::tuple<bool, q7_order_line_key, float> q7_order_line_map(
    const result_tuple& res, const q7_order_line_probe& p ) {
    bool       found = false;
    order_line ol;
    ol.ol_o_id = -1;
    ol.ol_d_id = -1;
    ol.ol_w_id = -1;
    ol.ol_i_id = -1;
    ol.ol_supply_w_id = -1;
    ol.ol_amount = 0;
    ol.ol_delivery_d.c_since = datetime::EMPTY_DATE;

    auto read_cols = read_from_scan_order_line( res, &ol );

    found = ( read_cols.count( order_line_cols::ol_o_id ) == 1 ) and
            ( read_cols.count( order_line_cols::ol_d_id ) == 1 ) and
            ( read_cols.count( order_line_cols::ol_w_id ) == 1 ) and
            ( read_cols.count( order_line_cols::ol_i_id ) == 1 ) and
            ( read_cols.count( order_line_cols::ol_supply_w_id ) == 1 ) and
            ( order_line_cols::ol_delivery_d_c_since == 1 );

    q7_order_line_key key( 0, 0, 0 );
    if( found ) {
        order o;
        o.o_id = ol.ol_o_id;
        o.o_d_id = ol.ol_d_id;
        o.o_w_id = ol.ol_w_id;

        uint64_t o_key = make_order_key( o, p.cfg_ );
        auto     o_found = p.o_probe_.find( o_key );
        if( o_found == p.o_probe_.end() ) {
            found = false;
        } else {
            key.customer_key_ = o_found->second;
        }

        stock s;
        s.s_i_id = ol.ol_i_id;
        s.s_w_id = ol.ol_supply_w_id;

        uint64_t s_key = make_stock_key( s, p.cfg_ );
        auto     s_found = p.s_probe_.find( s_key );
        if( s_found == p.s_probe_.end() ) {
            found = false;
        } else {
            key.stock_key_ = s_found->second;
        }
        key.year_ = get_year_from_c_since( ol.ol_delivery_d.c_since );
    }

    return std::make_tuple<>( found, key, ol.ol_amount );
}

void q7_order_line_mapper( const std::vector<result_tuple>& res_tuples,
                           std::unordered_map<q7_order_line_kv_types>& res,
                           const q7_order_line_probe&                  p ) {

    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, q7_order_line_key /* o_id */, float>
            mapped_res = q7_order_line_map( res_tuple, p );
        DVLOG( 40 ) << "Q7 map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            } else {
                found->second += std::get<2>( mapped_res );
            }
        }
    }
}
void q7_order_line_reducer(
    const std::unordered_map<q7_order_line_kv_vec_types>& input,
    std::unordered_map<q7_order_line_kv_types>&           res,
    const q7_order_line_probe&                            p ) {
    for( const auto& entry : input ) {
        float ol_amount = 0;
        for( const auto& val : entry.second ) {
            ol_amount += val;
        }
        res[entry.first] = ol_amount;
        DVLOG( 40 ) << "Q7 Reduce:" << entry.first.stock_key_ << ", "
                    << entry.first.customer_key_ << "" << entry.first.year_;
    }
}

q7_res_key::q7_res_key( int32_t supplier_n_id, int32_t customer_n_id,
                        uint64_t year )
    : supplier_n_id_( supplier_n_id ),
      customer_n_id_( customer_n_id ),
      year_( year ) {}
