#pragma once

#include "../../data-site/single-site-db/db_abstraction.h"
#include "../../distributions/distributions.h"
#include "../../distributions/non_uniform_distribution.h"
#include "../../templates/tpcc_benchmark_types.h"
#include "../benchmark_db_operators.h"
#include "record-types/tpcc_primary_key_generation.h"
#include "record-types/tpcc_record_types.h"
#include "tpcc_configs.h"

tpcc_loader_types class tpcc_loader {
   public:
    tpcc_loader( db_abstraction* db, const tpcc_configs& configs,
                 const db_abstraction_configs& abstraction_configs,
                 uint32_t                      client_id );
    ~tpcc_loader();

    snapshot_vector get_snapshot() const;

    void make_items_table();
    void make_nation_related_tables();
    void make_warehouse_related_tables( uint32_t w_id );

    void make_items_partitions();
    void make_nation_related_partitions();
    void make_warehouse_related_partitions( uint32_t w_id );

    static std::vector<cell_key_ranges> generate_write_set( uint32_t table_id,
                                                            uint64_t row_start,
                                                            uint64_t row_end,
                                                            uint32_t col_start,
                                                            uint32_t col_end );

    static std::vector<primary_key> generate_write_set_as_pks(
        uint32_t table_id, uint64_t start, uint64_t end );

    void load_item_range( const cell_key_ranges& ckr );
    void load_item( uint64_t id, const cell_key_ranges& ckr );

    void load_region_range(const cell_key_ranges& ckr );
    void load_region( uint64_t id, const cell_key_ranges& ckr );

    void load_nation_range(const cell_key_ranges& ckr );
    void load_nation( uint64_t id, const cell_key_ranges& ckr );

    void load_supplier_range( const cell_key_ranges& ckr );
    void load_supplier( uint64_t id, const cell_key_ranges& ckr );

    void make_warehouse( uint32_t w_id, const cell_key_ranges& ckr );
    void make_districts( uint32_t w_id, uint32_t d_id_start, uint32_t d_id_end,
                         const cell_key_ranges& ckr );
    void make_district( uint32_t w_id, uint32_t d_id,
                        const cell_key_ranges& ckr );

    void load_stock_range( uint32_t w_id, uint64_t start, uint64_t end,
                           const cell_key_ranges& ckr );
    void load_stock( uint32_t w_id, uint64_t item_id,
                     const cell_key_ranges& ckr );

    void load_customer_range( uint32_t w_id, uint32_t d_id, uint64_t start,
                              uint64_t end, const cell_key_ranges& ckr );
    void load_customer( uint32_t w_id, uint32_t d_id, uint32_t c_id,
                        const cell_key_ranges& ckr );

    void load_customer_district_range( uint32_t w_id, uint32_t d_id,
                                       uint64_t start, uint64_t end,
                                       const cell_key_ranges& ckr );
    void load_customer_district( uint32_t w_id, uint32_t d_id, uint32_t c_id,
                                 const cell_key_ranges& ckr );
    void generate_customer_district( customer_district* c, uint32_t c_id,
                                     uint32_t d_id, uint32_t w_id,
                                     uint32_t c_next_o_id );

    void load_history_range( uint32_t w_id, uint32_t d_id, uint64_t start,
                             uint64_t end, const cell_key_ranges& ckr );
    void load_history( uint32_t w_id, uint32_t d_id, uint32_t h_c_id,
                       const cell_key_ranges& ckr );

    void load_order( uint32_t w_id, uint32_t d_id, uint32_t c_id, uint32_t o_id,
                     uint32_t o_ol_cnt );
    void load_order( uint32_t w_id, uint32_t d_id, uint32_t c_id, uint32_t o_id,
                     uint32_t                            o_ol_cnt,
                     const std::vector<cell_key_ranges>& write_set );

    void load_only_order( uint32_t w_id, uint32_t d_id, uint32_t c_id,
                          uint32_t o_id, uint32_t o_ol_cnt,
                          const cell_key_ranges& ckr );
    void load_order_line( uint32_t w_id, uint32_t d_id, uint32_t o_id,
                          uint32_t ol_num, const cell_key_ranges& ckr );
    bool is_order_a_new_order( uint32_t o_id );
    void load_new_order( uint32_t w_id, uint32_t d_id, uint32_t o_id,
                         const cell_key_ranges& ckr );

    void set_transaction_partition_holder(
        transaction_partition_holder* txn_holder );

    void generate_warehouse( warehouse* w, uint32_t w_id );
    void generate_district( district* d, uint32_t w_id, uint32_t d_id );
    void generate_stock( stock* s, uint64_t item_id, uint32_t w_id,
                         bool is_original );
    int32_t generate_supplier_for_stock( const stock* s );

    void generate_customer( customer* c, uint32_t c_id, uint32_t d_id,
                            uint32_t w_id, bool is_bad_credit );
    int32_t generate_nation_for_customer( const customer* c );

    void generate_item( item* i, uint64_t id, bool is_original );
    void generate_history( history* h, uint32_t h_c_id, uint32_t d_id,
                           uint32_t w_id );
    void generate_order( order* o, uint32_t w_id, uint32_t d_id, uint32_t c_id,
                         uint32_t o_id, uint32_t o_ol_cnt, bool is_new_order );
    void generate_order_line( order_line* o_line, uint32_t w_id, uint32_t d_id,
                              uint32_t o_id, uint32_t ol_number,
                              bool is_new_order );
    void generate_new_order( new_order* new_o, uint32_t w_id, uint32_t d_id,
                             uint32_t o_id );

    void generate_nation( nation* n, uint32_t n_id );
    void generate_region( region* n, uint32_t r_id );
    void generate_supplier( supplier* s, uint32_t s_id );

    uint32_t generate_customer_order_id_start( uint32_t w_id, uint32_t d_id,
                                               uint32_t c_id );
    uint32_t generate_customer_order_id_end( uint32_t w_id, uint32_t d_id,
                                             uint32_t c_id );

    tpcc_configs get_configs() const;

   private:

    void generate_address( address* a );
    float generate_tax();

    void make_stock( uint32_t w_id, uint64_t start, uint64_t end,
                     const cell_key_ranges& ckr );

    void make_customers_and_history( uint32_t w_id, uint32_t d_id_start,
                                     uint32_t d_id_end );
    std::string make_last_name( uint32_t c_id );

    void make_orders( uint32_t w_id );

    std::string set_original( const std::string& s );

    benchmark_db_operators db_operators_;

    tpcc_configs                             configs_;
    std::vector<table_partition_information> data_sizes_;

    distributions            dist_;
    non_uniform_distribution nu_dist_;

    datetime now_;
};

#include "tpcc_loader.tcc"
