#pragma once

inline uint64_t make_w_s_key( int32_t w_id, int32_t i_id,
                              const tpcc_configs& configs ) {
    uint64_t id = i_id + ( w_id * configs.num_items_ );
    DVLOG( 50 ) << "make_w_s_key [ w_id:" << w_id << ", i_id:" << i_id
                << " ]=" << id;
    return id;
}

inline uint64_t make_w_d_key( int32_t d_id, int32_t w_id,
                              const tpcc_configs& configs ) {
    uint64_t id = d_id + ( w_id * configs.num_districts_per_warehouse_ );
    DVLOG( 50 ) << "make_w_d_key [ d_id:" << d_id << ", w_id:" << w_id
                << " ]=" << id;

    return id;
}

inline uint64_t make_w_d_c_key( int32_t c_id, int32_t d_id, int32_t w_id,
                                const tpcc_configs& configs ) {
    uint64_t id = ( make_w_d_key( d_id, w_id, configs ) *
                    configs.num_customers_per_district_ ) +
                  c_id;
    DVLOG( 50 ) << "make_w_d_c_key [ c_id:" << c_id << ", d_id:" << d_id
                << ", w_id:" << w_id << " ]=" << id;

    return id;
}

inline uint64_t make_w_d_c_o_key( int32_t o_id, int32_t c_id, int32_t d_id,
                                  int32_t w_id, const tpcc_configs& configs ) {
    uint64_t top_id = ( make_w_d_key( d_id, w_id, configs ) );
    uint64_t lower_id = o_id;
    uint64_t id = ( ( top_id ) << 32 ) | lower_id;
    DVLOG( 50 ) << "make_w_d_c_o_key [ o_id:" << o_id << ", c_id:" << c_id
                << ", d_id:" << d_id << ", w_id:" << w_id << " ]=" << id;
    return id;
}

inline uint64_t make_w_d_o_ol_key( int32_t ol_id, int32_t o_id, int32_t d_id,
                                   int32_t w_id, const tpcc_configs& configs ) {
    uint64_t id =
        ( make_w_d_c_o_key( o_id, 0 /* no customer*/, d_id, w_id, configs ) *
          configs.max_num_order_lines_per_order_ ) +
        ol_id;
    DVLOG( 50 ) << "make_w_d_c_l_o_key [ ol_id:" << ol_id << ", o_id:" << o_id
                << ", d_id:" << d_id << ", w_id:" << w_id << " ]=" << id;

    return id;
}

inline uint64_t make_stock_key( const stock& s, const tpcc_configs& configs ) {
    return make_w_s_key( s.s_w_id, s.s_i_id, configs );
}

inline uint64_t make_district_key( const district&     d,
                                   const tpcc_configs& configs ) {
    return make_w_d_key( d.d_id, d.d_w_id, configs );
}

inline uint64_t make_customer_key( const customer&     c,
                                   const tpcc_configs& configs ) {
    return make_w_d_c_key( c.c_id, c.c_d_id, c.c_w_id, configs );
}

inline uint64_t make_customer_district_key( const customer_district& cd,
                                            const tpcc_configs&      configs ) {
    return make_w_d_c_key( cd.c_id, cd.c_d_id, cd.c_w_id, configs );
}

inline uint64_t make_history_key( const history&      h,
                                  const tpcc_configs& configs ) {
    return make_w_d_c_key( h.h_c_id, h.h_c_d_id, h.h_c_w_id, configs );
}

inline uint64_t make_order_key( const order& o, const tpcc_configs& configs ) {
    return make_w_d_c_o_key( o.o_id, o.o_c_id, o.o_d_id, o.o_w_id, configs );
}

inline uint64_t make_new_order_key( const new_order&    new_o,
                                    const tpcc_configs& configs ) {
    uint64_t upper = make_w_d_key( new_o.no_d_id, new_o.no_w_id, configs )
                     << 32;
    uint64_t lower = ( (uint64_t) new_o.no_o_id );
    uint64_t key = upper | lower;
    DVLOG( 50 ) << "make_new_order_key [ no_o_id:" << new_o.no_o_id
                << ", d_id:" << new_o.no_d_id << ", w_id:" << new_o.no_w_id
                << "] = " << key;

    return key;
}

inline uint64_t make_order_line_key( const order_line&   o_line,
                                     const tpcc_configs& configs ) {
    return make_w_d_o_ol_key( o_line.ol_number, o_line.ol_o_id, o_line.ol_d_id,
                              o_line.ol_w_id, configs );
}

inline uint64_t make_warehouse_key( const warehouse&    w,
                                    const tpcc_configs& configs ) {
    return w.w_id;
}

inline uint64_t make_item_key( const item& i, const tpcc_configs& configs ) {
    return i.i_id;
}

inline uint64_t make_supplier_key( const supplier&     s,
                                   const tpcc_configs& configs ) {
    return s.s_id;
}
inline uint64_t make_region_key( const region&     r,
                                   const tpcc_configs& configs ) {
    return r.r_id;
}
inline uint64_t make_nation_key( const nation&     n,
                                   const tpcc_configs& configs ) {
    return n.n_id;
}

inline cell_key_ranges cell_key_ranges_from_cid( const cell_identifier& cid ) {
    return create_cell_key_ranges( cid.table_id_, cid.key_, cid.key_,
                                   cid.col_id_, cid.col_id_ );
}

inline uint32_t get_warehouse_from_warehouse_key(
    uint64_t key, const tpcc_configs& configs ) {
    return (uint32_t) key;
}

inline uint32_t get_warehouse_from_stock_key( uint64_t            key,
                                              const tpcc_configs& configs ) {
    uint32_t w_id = (uint32_t) key / configs.num_items_;
    // uint32_t i_id = key - (w_id * configs.num_items_);
    DVLOG( 50 ) << "get_warehouse_from_stock_key [ key:" << key
                << " ]=" << w_id;
    return w_id;
}

inline uint32_t get_item_from_item_key( uint64_t key, const tpcc_configs& configs) {
    uint32_t i_id = (uint32_t) key;
    DVLOG( 50 ) << "get_item_item_stock_key [ key:" << key << " ]=" << i_id;
    return i_id;
}

inline uint32_t get_item_from_stock_key( uint64_t            key,
                                         const tpcc_configs& configs ) {
    uint32_t w_id = (uint32_t) key / configs.num_items_;
    uint32_t i_id = key - (w_id * configs.num_items_);
    DVLOG( 50 ) << "get_item_from_stock_key [ key:" << key << " ]=" << i_id;
    return i_id;
}

inline uint32_t get_warehouse_from_district_key( uint64_t            key,
                                                 const tpcc_configs& configs ) {
    uint32_t w_id = (uint32_t) key / configs.num_districts_per_warehouse_;
    // uint32_t d_id = key - (w_id * configs.num_districts_per_warehouse_);
    DVLOG( 50 ) << "get_warehouse_from_district_key [ key:" << key
                << " ]=" << w_id;
    return w_id;
}
inline uint32_t get_district_from_district_key( uint64_t            key,
                                                const tpcc_configs& configs ) {
    uint32_t w_id = (uint32_t) key / configs.num_districts_per_warehouse_;
    uint32_t d_id = key - ( w_id * configs.num_districts_per_warehouse_ );
    DVLOG( 50 ) << "get_district_from_district_key [ key:" << key
                << " ]=" << d_id;
    return d_id;
}

inline uint32_t get_warehouse_from_customer_key( uint64_t            key,
                                                 const tpcc_configs& configs ) {
    uint32_t w_d_id = (uint32_t) key / configs.num_customers_per_district_;
    uint32_t w_id = get_warehouse_from_district_key( w_d_id, configs );
    // uint32_t c_id = key - (w_d_id * configs.num_customers_per_district_ );
    DVLOG( 50 ) << "get_warehouse_from_customer_key [ key:" << key
                << " ]=" << w_id;
    return w_id;
}
inline uint32_t get_customer_from_customer_key( uint64_t            key,
                                                 const tpcc_configs& configs ) {
    uint32_t w_d_id = (uint32_t) key / configs.num_customers_per_district_;
    // uint32_t w_id = get_warehouse_from_district_key( w_d_id, configs );
    uint32_t c_id = key - ( w_d_id * configs.num_customers_per_district_ );
    DVLOG( 50 ) << "get_customer_from_customer_key [ key:" << key
                << " ]=" << c_id;
    return c_id;
}

inline uint32_t get_warehouse_from_customer_district_key(
    uint64_t key, const tpcc_configs& configs ) {
    return get_warehouse_from_customer_key( key, configs );
}

inline uint32_t get_warehouse_from_history_key( uint64_t            key,
                                                const tpcc_configs& configs ) {
    return get_warehouse_from_customer_key( key, configs );
}

inline uint32_t get_warehouse_from_order_key(
    uint64_t key, const tpcc_configs& tpcc_configs ) {

    uint64_t top_id = key >> 32;
    uint32_t w_id = get_warehouse_from_district_key( top_id, tpcc_configs );

    DVLOG( 50 ) << "get_warehouse_from_order_key [ key:" << key
                << " ]=" << w_id;

    return w_id;

}

inline uint32_t get_warehouse_from_new_order_key(
    uint64_t key, const tpcc_configs& tpcc_configs ) {

    uint64_t top_id = key >> 32;
    uint32_t w_id = get_warehouse_from_district_key( top_id, tpcc_configs );

    DVLOG( 50 ) << "get_warehouse_from_new_order_key [ key:" << key
                << " ]=" << w_id;

    return w_id;
}

inline uint32_t get_warehouse_from_order_line_key(
    uint64_t key, const tpcc_configs& tpcc_configs ) {

    uint64_t w_d_c_o_key = key / tpcc_configs.max_num_order_lines_per_order_;
    uint32_t w_id = get_warehouse_from_order_key( w_d_c_o_key, tpcc_configs );

    DVLOG( 50 ) << "get_warehouse_from_new_order_key [ key:" << key
                << " ]=" << w_id;

    return w_id;
}

inline uint32_t get_warehouse_from_key( int32_t table_id, uint64_t key,
                                        const tpcc_configs& configs ) {
    if( table_id == k_tpcc_warehouse_table_id ) {
        return get_warehouse_from_warehouse_key( key, configs );
    } else if( table_id == k_tpcc_stock_table_id ) {
        return get_warehouse_from_stock_key( key, configs );
    } else if( table_id == k_tpcc_district_table_id ) {
        return get_warehouse_from_district_key( key, configs );
    } else if( table_id == k_tpcc_customer_table_id ) {
        return get_warehouse_from_customer_key( key, configs );
    } else if( table_id == k_tpcc_history_table_id ) {
        return get_warehouse_from_history_key( key, configs );
    } else if( table_id == k_tpcc_order_table_id ) {
        return get_warehouse_from_order_key( key, configs );
    } else if( table_id == k_tpcc_new_order_table_id ) {
        return get_warehouse_from_new_order_key( key, configs );
    } else if( table_id == k_tpcc_order_line_table_id ) {
        return get_warehouse_from_order_line_key( key, configs );
    } else if( table_id == k_tpcc_customer_district_table_id ) {
        return get_warehouse_from_customer_district_key( key, configs );
    }
    LOG( WARNING ) << "Could not get warehouse from table id:" << table_id
                   << ", key:" << key;
    return configs.num_warehouses_;
}

inline uint32_t get_district_from_key( int32_t table_id, uint64_t key,
                                       const tpcc_configs& configs ) {

    if( table_id == k_tpcc_district_table_id ) {
        return get_district_from_district_key( key, configs );
    }

    return configs.num_districts_per_warehouse_;
}

inline uint32_t get_item_from_key( int32_t table_id, uint64_t key,
                                       const tpcc_configs& configs ) {

  if ( table_id == k_tpcc_item_table_id ) {
      return get_item_from_item_key( key, configs );
  } else if( table_id == k_tpcc_stock_table_id ) {
      return get_item_from_stock_key( key, configs );
  }

  return configs.num_items_;
}
inline uint32_t get_customer_from_key( int32_t table_id, uint64_t key,
                                       const tpcc_configs& configs ) {
    if( table_id == k_tpcc_customer_table_id ) {
        return get_customer_from_customer_key( key, configs );
    } else if( table_id == k_tpcc_history_table_id ) {
        return get_customer_from_customer_key( key, configs );
    } else if( table_id == k_tpcc_customer_district_table_id ) {
        return get_customer_from_customer_key( key, configs );
    }
    return configs.num_customers_per_district_;
}
