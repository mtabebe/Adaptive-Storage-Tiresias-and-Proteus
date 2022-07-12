#define GTEST_HAS_TR1_TUPLE 0

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../src/common/gdcheck.h"
#include "../src/data-site/stored-procedures/stored_procedures_executor.h"
#include "../src/data-site/stored-procedures/tpcc/tpcc_prep_stmts.h"

class tpcc_sproc_test : public ::testing::Test {};

uint32_t* generate_array( uint32_t low, uint32_t end ) {
    uint32_t  num_elements = ( end - low ) + 1;
    uint32_t* elems = new uint32_t[num_elements];

    for( uint32_t pos = 0; pos < num_elements; pos++ ) {
        elems[pos] = pos + low;
    }

    return elems;
}

snapshot_vector create_and_load_tpcc_table( db*                 database,
                                            sproc_lookup_table* sproc_table,
                                            const tpcc_configs& tpcc_cfg,
                                            void*               opaque_ptr ) {
    snapshot_vector c1_state;
    snapshot_vector accum_state;
    auto            data_sizes = get_tpcc_data_sizes( tpcc_cfg );

    std::vector<arg_code> create_table_args = {BIGINT_CODE, BIGINT_CODE};
    std::vector<void*>    create_table_vals = {(void*) database,
                                            (void*) &tpcc_cfg};
    function_identifier create_func_id( k_create_config_tables_sproc_name,
                                        create_table_args );

    sproc_result res;

    uint32_t cli_id = 0;

    function_skeleton create_function =
        sproc_table->lookup_function( create_func_id );

    std::vector<cell_key_ranges> write_ckrs;
    std::vector<cell_key_ranges> read_ckrs;

    res = create_function( nullptr, cli_id, write_ckrs, read_ckrs,
                           create_table_args, create_table_vals, opaque_ptr );
    EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );

    // just do an add here
    partition_lookup_operation lookup_op =
        partition_lookup_operation::GET_OR_CREATE;

    std::vector<arg_code> insert_items_arg_codes = {};
    uint64_t              i_start = 0;
    uint64_t              i_end = tpcc_cfg.num_items_ - 1;
    std::vector<void*>    insert_args = {};

    write_ckrs.emplace_back(
        create_cell_key_ranges( k_tpcc_item_table_id, i_start, i_end, 0,
                                k_tpcc_item_num_columns - 1 ) );

    function_identifier insert_item_func_id( k_tpcc_load_item_range_sproc_name,
                                             insert_items_arg_codes );
    function_skeleton insert_item_function =
        sproc_table->lookup_function( insert_item_func_id );

    std::vector<partition_column_identifier> pids =
        generate_partition_column_identifiers(
            // these should be contiguous
            k_tpcc_item_table_id, i_start, i_end, 0,
            k_tpcc_item_num_columns - 1, data_sizes );
    partition_column_identifier_set insert_pids;
    insert_pids.insert( pids.begin(), pids.end() );

    partition_column_identifier_set inflight_pids;
    partition_column_identifier_set read_pids;

    transaction_partition_holder* txn_holder =
        database->get_partitions_with_begin( cli_id, c1_state, insert_pids,
                                             read_pids, inflight_pids,
                                             lookup_op );
    res =
        insert_item_function( txn_holder, cli_id, write_ckrs, read_ckrs,
                              insert_items_arg_codes, insert_args, opaque_ptr );
    EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );
    c1_state = txn_holder->commit_transaction();
    merge_snapshot_versions_if_larger( accum_state, c1_state );
    delete txn_holder;
    txn_holder = nullptr;
    insert_pids.clear();
    write_ckrs.clear();

    std::vector<arg_code> insert_warehouse_arg_codes = {INTEGER_CODE};
    function_identifier   insert_warehouse_func_id(
        k_tpcc_load_warehouse_and_districts_sproc_name,
        insert_warehouse_arg_codes );
    function_skeleton insert_warehouse_function =
        sproc_table->lookup_function( insert_warehouse_func_id );

    std::vector<arg_code> insert_stock_arg_codes = {INTEGER_CODE, BIGINT_CODE,
                                                    BIGINT_CODE};
    function_identifier insert_stock_func_id(
        k_tpcc_load_stock_range_sproc_name, insert_stock_arg_codes );
    function_skeleton insert_stock_function =
        sproc_table->lookup_function( insert_stock_func_id );

    std::vector<arg_code> insert_c_h_arg_codes = {
        INTEGER_CODE /*w_id*/, INTEGER_CODE /*d_id*/, BIGINT_CODE /*start*/,
        BIGINT_CODE /*end*/};
    function_identifier insert_c_h_func_id(
        k_tpcc_load_customer_and_history_range_sproc_name,
        insert_c_h_arg_codes );
    function_skeleton insert_c_h_function =
        sproc_table->lookup_function( insert_c_h_func_id );

    std::vector<arg_code> insert_order_arg_codes = {
        INTEGER_CODE /*w_id*/, INTEGER_CODE /*d_id*/, INTEGER_CODE /*c_id*/,
        INTEGER_CODE /*o_id*/, INTEGER_CODE /*o_ol_cnt*/};
    function_identifier insert_order_func_id( k_tpcc_load_order_sproc_name,
                                              insert_order_arg_codes );
    function_skeleton insert_order_function =
        sproc_table->lookup_function( insert_order_func_id );

    for( uint32_t w_id = 0; w_id < tpcc_cfg.num_warehouses_; w_id++ ) {
        district d_start;
        d_start.d_id = 0;
        d_start.d_w_id = w_id;

        district d_end;
        d_end.d_id = tpcc_cfg.num_districts_per_warehouse_ - 1;
        d_end.d_w_id = w_id;

        std::vector<void*> insert_warehouse_args = {(void*) &w_id};

        write_ckrs.emplace_back(
            create_cell_key_ranges( k_tpcc_warehouse_table_id, w_id, w_id, 0,
                                    k_tpcc_warehouse_num_columns - 1 ) );
        write_ckrs.emplace_back( create_cell_key_ranges(
            k_tpcc_district_table_id, make_district_key( d_start, tpcc_cfg ),
            make_district_key( d_end, tpcc_cfg ), 0,
            k_tpcc_district_num_columns - 1 ) );

        pids = generate_partition_column_identifiers(
            k_tpcc_district_table_id, make_district_key( d_start, tpcc_cfg ),
            make_district_key( d_end, tpcc_cfg ), 0,
            k_tpcc_district_num_columns - 1, data_sizes );
        insert_pids.insert( pids.begin(), pids.end() );
        pids = generate_partition_column_identifiers(
            k_tpcc_warehouse_table_id, w_id, w_id, 0,
            k_tpcc_warehouse_num_columns - 1, data_sizes );
        insert_pids.insert( pids.begin(), pids.end() );

        txn_holder = database->get_partitions_with_begin(
            cli_id, c1_state, insert_pids, read_pids, inflight_pids,
            lookup_op );
        res = insert_warehouse_function( txn_holder, cli_id, write_ckrs,
                                         read_ckrs, insert_warehouse_arg_codes,
                                         insert_warehouse_args, opaque_ptr );

        EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );
        c1_state = txn_holder->commit_transaction();
        merge_snapshot_versions_if_larger( accum_state, c1_state );
        delete txn_holder;
        txn_holder = nullptr;
        insert_pids.clear();
        write_ckrs.clear();

        stock s_start;
        s_start.s_i_id = i_start;
        s_start.s_w_id = w_id;

        stock s_end;
        s_end.s_i_id = i_end;
        s_end.s_w_id = w_id;

        pids = generate_partition_column_identifiers(
            k_tpcc_stock_table_id, make_stock_key( s_start, tpcc_cfg ),
            make_stock_key( s_end, tpcc_cfg ), 0, k_tpcc_stock_num_columns - 1,
            data_sizes );
        insert_pids.insert( pids.begin(), pids.end() );

        write_ckrs.emplace_back( create_cell_key_ranges(
            k_tpcc_stock_table_id, make_stock_key( s_start, tpcc_cfg ),
            make_stock_key( s_end, tpcc_cfg ), 0,
            k_tpcc_stock_num_columns - 1 ) );

        txn_holder = database->get_partitions_with_begin(
            cli_id, c1_state, insert_pids, read_pids, inflight_pids,
            lookup_op );

        std::vector<void*> insert_stock_args = {(void*) &w_id, (void*) &i_start,
                                                (void*) &i_end};
        res = insert_stock_function( txn_holder, cli_id, write_ckrs, read_ckrs,
                                     insert_stock_arg_codes, insert_stock_args,
                                     opaque_ptr );
        EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );

        c1_state = txn_holder->commit_transaction();
        merge_snapshot_versions_if_larger( accum_state, c1_state );
        delete txn_holder;
        txn_holder = nullptr;
        insert_pids.clear();
        write_ckrs.clear();

        for( uint32_t d_id = 0; d_id < tpcc_cfg.num_districts_per_warehouse_;
             d_id++ ) {

            customer c_start;
            c_start.c_id = (int32_t) 0;
            c_start.c_d_id = (int32_t) d_id;
            c_start.c_w_id = (int32_t) w_id;
            customer c_end;
            c_end.c_id = (int32_t) tpcc_cfg.num_customers_per_district_ - 1;
            c_end.c_d_id = (int32_t) d_id;
            c_end.c_w_id = (int32_t) w_id;

            customer_district cd_start;
            cd_start.c_id = (int32_t) 0;
            cd_start.c_d_id = (int32_t) d_id;
            cd_start.c_w_id = (int32_t) w_id;
            customer_district cd_end;
            cd_end.c_id = (int32_t) tpcc_cfg.num_customers_per_district_ - 1;
            cd_end.c_d_id = (int32_t) d_id;
            cd_end.c_w_id = (int32_t) w_id;

            history h_start;
            h_start.h_c_id = (int32_t) 0;
            h_start.h_c_d_id = (int32_t) d_id;
            h_start.h_c_w_id = (int32_t) w_id;

            history h_end;
            h_end.h_c_id = (int32_t) c_end.c_id;
            h_end.h_c_d_id = (int32_t) d_id;
            h_end.h_c_w_id = (int32_t) w_id;

            auto c_pids = generate_partition_column_identifiers(
                k_tpcc_customer_table_id,
                make_customer_key( c_start, tpcc_cfg ),
                make_customer_key( c_end, tpcc_cfg ), 0,
                k_tpcc_customer_num_columns - 1, data_sizes );
            auto cd_pids = generate_partition_column_identifiers(
                k_tpcc_customer_district_table_id,
                make_customer_district_key( cd_start, tpcc_cfg ),
                make_customer_district_key( cd_end, tpcc_cfg ), 0,
                k_tpcc_customer_district_num_columns - 1, data_sizes );

            auto h_pids = generate_partition_column_identifiers(
                k_tpcc_history_table_id, make_history_key( h_start, tpcc_cfg ),
                make_history_key( h_end, tpcc_cfg ), 0,
                k_tpcc_history_num_columns - 1, data_sizes );

            insert_pids.insert( c_pids.begin(), c_pids.end() );
            insert_pids.insert( h_pids.begin(), h_pids.end() );
            insert_pids.insert( cd_pids.begin(), cd_pids.end() );

            write_ckrs.emplace_back(
                create_cell_key_ranges( k_tpcc_customer_table_id,
                                        make_customer_key( c_start, tpcc_cfg ),
                                        make_customer_key( c_end, tpcc_cfg ), 0,
                                        k_tpcc_customer_num_columns - 1 ) );
            write_ckrs.emplace_back( create_cell_key_ranges(
                k_tpcc_history_table_id, make_history_key( h_start, tpcc_cfg ),
                make_history_key( h_end, tpcc_cfg ), 0,
                k_tpcc_history_num_columns - 1 ) );
            write_ckrs.emplace_back( create_cell_key_ranges(
                k_tpcc_customer_district_table_id,
                make_customer_district_key( cd_start, tpcc_cfg ),
                make_customer_district_key( cd_end, tpcc_cfg ), 0,
                k_tpcc_customer_district_num_columns - 1 ) );

            std::vector<void*> insert_c_h_args = {(void*) &w_id, (void*) &d_id,
                                                  (void*) &c_start.c_id,
                                                  (void*) &c_end.c_id};

            txn_holder = database->get_partitions_with_begin(
                cli_id, c1_state, insert_pids, read_pids, inflight_pids,
                lookup_op );

            res = insert_c_h_function( txn_holder, cli_id, write_ckrs,
                                       read_ckrs, insert_c_h_arg_codes,
                                       insert_c_h_args, opaque_ptr );
            EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );

            c1_state = txn_holder->commit_transaction();
            merge_snapshot_versions_if_larger( accum_state, c1_state );
            delete txn_holder;
            txn_holder = nullptr;
            insert_pids.clear();
            write_ckrs.clear();

            uint32_t o_ol_cnt = tpcc_cfg.max_num_order_lines_per_order_;

            for( uint32_t c_id = 0; c_id < tpcc_cfg.num_customers_per_district_;
                 c_id++ ) {
                uint32_t o_id = c_id;  // not how things are loaded but w/e

                order o;
                o.o_id = (int32_t) o_id;
                o.o_c_id = (int32_t) c_id;
                o.o_d_id = (int32_t) d_id;
                o.o_w_id = (int32_t) w_id;
                o.o_ol_cnt = o_ol_cnt;
                uint64_t order_key = make_order_key( o, tpcc_cfg );
                pids = generate_partition_column_identifiers(
                    k_tpcc_order_table_id, order_key, order_key, 0,
                    k_tpcc_order_num_columns - 1, data_sizes );
                insert_pids.insert( pids.begin(), pids.end() );
                write_ckrs.emplace_back( create_cell_key_ranges(
                    k_tpcc_order_table_id, order_key, order_key, 0,
                    k_tpcc_order_num_columns - 1 ) );

                new_order new_o;
                new_o.no_w_id = (int32_t) w_id;
                new_o.no_d_id = (int32_t) d_id;
                new_o.no_o_id = (int32_t) o_id;
                uint64_t no_key = make_new_order_key( new_o, tpcc_cfg );
                pids = generate_partition_column_identifiers(
                    k_tpcc_new_order_table_id, no_key, no_key, 0,
                    k_tpcc_new_order_num_columns - 1, data_sizes );
                insert_pids.insert( pids.begin(), pids.end() );
                write_ckrs.emplace_back( create_cell_key_ranges(
                    k_tpcc_new_order_table_id, no_key, no_key, 0,
                    k_tpcc_new_order_num_columns - 1 ) );

                order_line o_line;
                o_line.ol_o_id = o_id;
                o_line.ol_d_id = d_id;
                o_line.ol_w_id = w_id;
                o_line.ol_number = 1;

                uint64_t ol_start = make_order_line_key( o_line, tpcc_cfg );

                o_line.ol_number = o.o_ol_cnt;
                uint64_t ol_end = make_order_line_key( o_line, tpcc_cfg );

                pids = generate_partition_column_identifiers(
                    k_tpcc_order_line_table_id, ol_start, ol_end, 0,
                    k_tpcc_order_line_num_columns - 1, data_sizes );
                write_ckrs.emplace_back( create_cell_key_ranges(
                    k_tpcc_order_line_table_id, ol_start, ol_end, 0,
                    k_tpcc_order_line_num_columns - 1 ) );
                insert_pids.insert( pids.begin(), pids.end() );

                std::vector<void*> insert_order_args = {
                    (void*) &w_id, (void*) &d_id, (void*) &c_id, (void*) &o_id,
                    (void*) &o_ol_cnt};

                txn_holder = database->get_partitions_with_begin(
                    cli_id, c1_state, insert_pids, read_pids, inflight_pids,
                    lookup_op );
                res = insert_order_function( txn_holder, cli_id, write_ckrs,
                                             read_ckrs, insert_order_arg_codes,
                                             insert_order_args, opaque_ptr );
                EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );
                c1_state = txn_holder->commit_transaction();
                merge_snapshot_versions_if_larger( accum_state, c1_state );
                delete txn_holder;
                txn_holder = nullptr;
                insert_pids.clear();
                write_ckrs.clear();

            }  // order
        }      // d_id
    }          // w_id

    std::vector<arg_code> insert_region_arg_codes;
    std::vector<arg_code> insert_nation_arg_codes;
    std::vector<arg_code> insert_supplier_arg_codes;
    insert_args = {};

    write_ckrs.emplace_back( create_cell_key_ranges(
        k_tpcc_region_table_id, 0, region::NUM_REGIONS - 1, 0,
        k_tpcc_region_num_columns - 1 ) );

    function_identifier insert_region_func_id(
        k_tpcc_load_region_range_sproc_name, insert_region_arg_codes );
    function_skeleton insert_region_function =
        sproc_table->lookup_function( insert_region_func_id );

    pids = generate_partition_column_identifiers(
        // these should be contiguous
        k_tpcc_region_table_id, 0, region::NUM_REGIONS - 1, 0,
        k_tpcc_region_num_columns - 1, data_sizes );
    insert_pids.insert( pids.begin(), pids.end() );

    txn_holder = database->get_partitions_with_begin(
        cli_id, c1_state, insert_pids, read_pids, inflight_pids, lookup_op );
    res = insert_region_function( txn_holder, cli_id, write_ckrs, read_ckrs,
                                  insert_region_arg_codes, insert_args,
                                  opaque_ptr );
    EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );
    c1_state = txn_holder->commit_transaction();
    merge_snapshot_versions_if_larger( accum_state, c1_state );
    delete txn_holder;
    txn_holder = nullptr;
    insert_pids.clear();
    write_ckrs.clear();

    write_ckrs.emplace_back( create_cell_key_ranges(
        k_tpcc_nation_table_id, 0, nation::NUM_NATIONS - 1, 0,
        k_tpcc_nation_num_columns - 1 ) );

    function_identifier insert_nation_func_id(
        k_tpcc_load_nation_range_sproc_name, insert_nation_arg_codes );
    function_skeleton insert_nation_function =
        sproc_table->lookup_function( insert_nation_func_id );

    pids = generate_partition_column_identifiers(
        // these should be contiguous
        k_tpcc_nation_table_id, 0, nation::NUM_NATIONS - 1, 0,
        k_tpcc_nation_num_columns - 1, data_sizes );
    insert_pids.insert( pids.begin(), pids.end() );

    txn_holder = database->get_partitions_with_begin(
        cli_id, c1_state, insert_pids, read_pids, inflight_pids, lookup_op );
    res = insert_nation_function( txn_holder, cli_id, write_ckrs, read_ckrs,
                                  insert_nation_arg_codes, insert_args,
                                  opaque_ptr );
    EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );
    c1_state = txn_holder->commit_transaction();
    merge_snapshot_versions_if_larger( accum_state, c1_state );
    delete txn_holder;
    txn_holder = nullptr;
    insert_pids.clear();
    write_ckrs.clear();

    write_ckrs.emplace_back( create_cell_key_ranges(
        k_tpcc_supplier_table_id, 0, tpcc_cfg.num_suppliers_ - 1, 0,
        k_tpcc_supplier_num_columns - 1 ) );

    function_identifier insert_supplier_func_id(
        k_tpcc_load_supplier_range_sproc_name, insert_supplier_arg_codes );
    function_skeleton insert_supplier_function =
        sproc_table->lookup_function( insert_supplier_func_id );

    pids = generate_partition_column_identifiers(
        // these should be contiguous
        k_tpcc_supplier_table_id, 0, tpcc_cfg.num_suppliers_ - 1, 0,
        k_tpcc_supplier_num_columns - 1, data_sizes );
    insert_pids.insert( pids.begin(), pids.end() );

    txn_holder = database->get_partitions_with_begin(
        cli_id, c1_state, insert_pids, read_pids, inflight_pids, lookup_op );
    res = insert_supplier_function( txn_holder, cli_id, write_ckrs, read_ckrs,
                                    insert_supplier_arg_codes, insert_args,
                                    opaque_ptr );
    EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );
    c1_state = txn_holder->commit_transaction();
    merge_snapshot_versions_if_larger( accum_state, c1_state );
    delete txn_holder;
    txn_holder = nullptr;
    insert_pids.clear();
    write_ckrs.clear();

    return accum_state;
}

#if 0  // MTODO-TPCC
snapshot_vector create_and_load_arg_based_tpcc_table(
    db* database, sproc_lookup_table* sproc_table, const tpcc_configs& tpcc_cfg,
    void* opaque_ptr ) {
    snapshot_vector c1_state;
    auto            data_sizes = get_tpcc_data_sizes( tpcc_cfg );

    std::vector<arg_code> create_table_args = {BIGINT_CODE, BIGINT_CODE};
    std::vector<void*>        create_table_vals = {(void*) database,
                                            (void*) &tpcc_cfg};
    function_identifier   create_func_id( k_create_config_tables_sproc_name,
                                        create_table_args );

    sproc_result res;

    uint32_t cli_id = 0;

    function_skeleton create_function =
        sproc_table->lookup_function( create_func_id );
    res = create_function( nullptr, cli_id, create_table_args,
                           create_table_vals, opaque_ptr );
    EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );

    // just do an add here
    partition_lookup_operation lookup_op =
        partition_lookup_operation::GET_OR_CREATE;

    std::vector<arg_code> insert_items_arg_codes;
    uint64_t              i_start = 0;
    uint64_t              i_end = tpcc_cfg.num_items_ - 1;

    uint32_t              num_items = i_end + 1;
    uint64_t*             stock_ids = new uint64_t[tpcc_cfg.num_items_];
    uint64_t*             item_ids = new uint64_t[num_items];
    int32_t*              item_im_ids = new int32_t[num_items];
    float*                item_prices = new float[num_items];
    int32_t*              name_lengths = new int32_t[num_items];
    int32_t*              data_lengths = new int32_t[num_items];
    std::string           names;
    std::string           data;
    for( uint32_t pos = 0; pos < num_items; pos++ ) {
        stock_ids[pos] = (uint64_t) pos;
        item_ids[pos] = (uint64_t) pos;
        item_im_ids[pos] =
            ( pos % ( item::MAX_IM - item::MIN_IM ) ) + item::MIN_IM;
        item_prices[pos] =
            ( pos % ( ( uint32_t )( item::MAX_PRICE - item::MIN_PRICE ) ) ) +
            item::MIN_PRICE;
        name_lengths[pos] =
            ( pos % ( item::MAX_NAME_LEN - item::MIN_NAME_LEN ) ) +
            item::MIN_NAME_LEN;
        data_lengths[pos] =
            ( pos % ( item::MAX_DATA_LEN - item::MIN_DATA_LEN ) ) +
            item::MIN_DATA_LEN;
        char c = 'a' + (char) ( pos % 26 );
        names.append( name_lengths[pos], c );
        data.append( data_lengths[pos], c );
    }
    arg_code array_code = BIGINT_ARRAY_CODE;
    array_code.array_length = num_items;
    insert_items_arg_codes.push_back( array_code );
    arg_code int_array_code = INTEGER_ARRAY_CODE;
    int_array_code.array_length = num_items;
    insert_items_arg_codes.push_back( int_array_code );
    array_code = FLOAT_ARRAY_CODE;
    array_code.array_length = num_items;
    insert_items_arg_codes.push_back( array_code );
    insert_items_arg_codes.push_back( int_array_code );
    array_code = STRING_CODE;
    array_code.array_length = names.size();
    insert_items_arg_codes.push_back( array_code );
    insert_items_arg_codes.push_back( int_array_code );
    array_code.array_length = data.size();
    insert_items_arg_codes.push_back( array_code );

    std::vector<void*> insert_args = {
        (void*) item_ids,     (void*) item_im_ids,   (void*) item_prices,
        (void*) name_lengths, (void*) names.c_str(), (void*) data_lengths,
        (void*) data.c_str()};
    function_identifier insert_item_func_id(
        k_tpcc_load_items_arg_range_sproc_name, insert_items_arg_codes );
    function_skeleton insert_item_function =
        sproc_table->lookup_function( insert_item_func_id );
    EXPECT_NE( insert_item_function, nullptr );

    std::vector<partition_identifier> pids = generate_partition_identifiers(
        // these should be contiguous
        k_tpcc_item_table_id, i_start, i_end, data_sizes );
    partition_identifier_set insert_pids;
    insert_pids.insert( pids.begin(), pids.end() );
    partition_identifier_set inflight_pids;

    transaction_partition_holder* txn_holder =
        database->get_partitions_with_begin( cli_id, c1_state, insert_pids, {},
                                             inflight_pids, lookup_op );
    res = insert_item_function( txn_holder, cli_id, insert_items_arg_codes,
                                insert_args, opaque_ptr );
    EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );
    c1_state = txn_holder->commit_transaction();
    delete txn_holder;
    txn_holder = nullptr;
    insert_pids.clear();

    delete[] item_ids;
    delete[] item_im_ids;
    delete[] item_prices;
    delete[] name_lengths;
    delete[] data_lengths;

    std::vector<arg_code> insert_warehouse_arg_codes = {INTEGER_CODE};
    function_identifier   insert_warehouse_func_id(
        k_tpcc_load_warehouse_sproc_name, insert_warehouse_arg_codes );
    function_skeleton insert_warehouse_function =
        sproc_table->lookup_function( insert_warehouse_func_id );
    EXPECT_NE( insert_warehouse_function, nullptr );

    std::vector<arg_code> insert_assorted_districts_arg_codes = {
        INTEGER_CODE, INTEGER_ARRAY_CODE};
    function_identifier insert_assorted_districts_func_id(
        k_tpcc_load_assorted_districts_sproc_name,
        insert_assorted_districts_arg_codes );
    function_skeleton insert_assorted_districts_function =
        sproc_table->lookup_function( insert_assorted_districts_func_id );
    EXPECT_NE( insert_assorted_districts_function, nullptr );

    std::vector<arg_code> insert_stock_arg_codes = {INTEGER_CODE,
                                                    BIGINT_ARRAY_CODE};
    function_identifier insert_stock_func_id(
        k_tpcc_load_assorted_stock_sproc_name, insert_stock_arg_codes );
    function_skeleton insert_stock_function =
        sproc_table->lookup_function( insert_stock_func_id );
    EXPECT_NE( insert_stock_function, nullptr );

    std::vector<arg_code> insert_c_h_arg_codes = {
        INTEGER_CODE /*w_id*/, INTEGER_CODE /*d_id*/, INTEGER_ARRAY_CODE};
    function_identifier insert_cust_func_id(
        k_tpcc_load_assorted_customers_sproc_name, insert_c_h_arg_codes );
    function_skeleton insert_cust_function =
        sproc_table->lookup_function( insert_cust_func_id );
    EXPECT_NE( insert_cust_function, nullptr );

    function_identifier insert_hist_func_id(
        k_tpcc_load_assorted_history_sproc_name, insert_c_h_arg_codes );
    function_skeleton insert_hist_function =
        sproc_table->lookup_function( insert_hist_func_id );
    EXPECT_NE( insert_hist_function, nullptr );

    std::vector<arg_code> insert_only_order_arg_codes = {
        INTEGER_CODE /*w_id*/, INTEGER_CODE /*d_id*/, INTEGER_CODE /*c_id*/,
        INTEGER_CODE /*o_id*/, INTEGER_CODE /*o_ol_cnt*/};
    function_identifier insert_only_order_func_id(
        k_tpcc_load_only_order_sproc_name, insert_only_order_arg_codes );
    function_skeleton insert_only_order_function =
        sproc_table->lookup_function( insert_only_order_func_id );
    EXPECT_NE( insert_only_order_function, nullptr );

    std::vector<arg_code> insert_order_lines_arg_codes = {
        INTEGER_CODE /*w_id*/, INTEGER_CODE /*d_id*/, INTEGER_CODE /*o_id*/,
        INTEGER_ARRAY_CODE /*o_ol_cnt*/};
    function_identifier insert_order_lines_func_id(
        k_tpcc_load_order_lines_sproc_name, insert_order_lines_arg_codes );
    function_skeleton insert_order_lines_function =
        sproc_table->lookup_function( insert_order_lines_func_id );
    EXPECT_NE( insert_order_lines_function, nullptr );

    std::vector<arg_code> insert_new_order_arg_codes = {
        INTEGER_CODE /*w_id*/, INTEGER_CODE /*d_id*/, INTEGER_CODE /*o_id*/};
    function_identifier insert_new_order_func_id(
        k_tpcc_load_new_order_sproc_name, insert_new_order_arg_codes );
    function_skeleton insert_new_order_function =
        sproc_table->lookup_function( insert_new_order_func_id );
    EXPECT_NE( insert_new_order_function, nullptr );

    uint32_t* d_ids =
        generate_array( 0, tpcc_cfg.num_districts_per_warehouse_ - 1 );
    uint32_t* c_ids =
        generate_array( 0, tpcc_cfg.num_customers_per_district_ - 1 );

    EXPECT_NE( opaque_ptr, nullptr );
    tpcc_sproc_helper_holder* sproc_helper =
        (tpcc_sproc_helper_holder*) opaque_ptr;
    tpcc_loader_templ_no_commit* loader =
        sproc_helper->get_loader_and_set_holder( cli_id, nullptr );
    EXPECT_NE( loader, nullptr );

    for( uint32_t w_id = 0; w_id < tpcc_cfg.num_warehouses_; w_id++ ) {
        record_identifier w_rid = {k_tpcc_warehouse_table_id, w_id};

        std::vector<void*> insert_warehouse_args = {(void*) &w_id};

        insert_pids.insert( generate_partition_identifier(
            k_tpcc_warehouse_table_id, w_rid.key_, data_sizes ) );

        txn_holder = database->get_partitions_with_begin(
            cli_id, c1_state, insert_pids, {}, inflight_pids, lookup_op );
        res = insert_warehouse_function( txn_holder, cli_id,
                                         insert_warehouse_arg_codes,
                                         insert_warehouse_args, opaque_ptr );

        EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );
        c1_state = txn_holder->commit_transaction();
        delete txn_holder;
        txn_holder = nullptr;
        insert_pids.clear();

        district d_start;
        d_start.d_id = 0;
        d_start.d_w_id = w_id;

        district d_end;
        d_end.d_id = tpcc_cfg.num_districts_per_warehouse_ - 1;
        d_end.d_w_id = w_id;

        pids = generate_partition_identifiers(
            k_tpcc_district_table_id, make_district_key( d_start, tpcc_cfg ),
            make_district_key( d_end, tpcc_cfg ), data_sizes );
        insert_pids.insert( pids.begin(), pids.end() );

        txn_holder = database->get_partitions_with_begin(
            cli_id, c1_state, insert_pids, {}, inflight_pids, lookup_op );

        std::vector<void*> insert_district_args = {(void*) &w_id,
                                                   (void*) d_ids};
        insert_assorted_districts_arg_codes.at( 1 ).array_length =
            tpcc_cfg.num_districts_per_warehouse_;

        res = insert_assorted_districts_function(
            txn_holder, cli_id, insert_assorted_districts_arg_codes,
            insert_district_args, opaque_ptr );

        EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );
        c1_state = txn_holder->commit_transaction();
        delete txn_holder;
        txn_holder = nullptr;
        insert_pids.clear();

        stock s_start;
        s_start.s_i_id = i_start;
        s_start.s_w_id = w_id;

        stock s_end;
        s_end.s_i_id = i_end;
        s_end.s_w_id = w_id;

        pids = generate_partition_identifiers(
            k_tpcc_stock_table_id, make_stock_key( s_start, tpcc_cfg ),
            make_stock_key( s_end, tpcc_cfg ), data_sizes );
        insert_pids.insert( pids.begin(), pids.end() );

        txn_holder = database->get_partitions_with_begin(
            cli_id, c1_state, insert_pids, {}, inflight_pids, lookup_op );

        std::vector<void*> insert_stock_args = {(void*) &w_id,
                                                (void*) stock_ids};
        insert_stock_arg_codes.at( 1 ).array_length = tpcc_cfg.num_items_;
        res = insert_stock_function( txn_holder, cli_id, insert_stock_arg_codes,
                                     insert_stock_args, opaque_ptr );
        EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );

        c1_state = txn_holder->commit_transaction();
        delete txn_holder;
        txn_holder = nullptr;
        insert_pids.clear();

        for( uint32_t d_id = 0; d_id < tpcc_cfg.num_districts_per_warehouse_;
             d_id++ ) {

            customer c_start;
            c_start.c_id = (int32_t) 0;
            c_start.c_d_id = (int32_t) d_id;
            c_start.c_w_id = (int32_t) w_id;
            customer c_end;
            c_end.c_id = (int32_t) tpcc_cfg.num_customers_per_district_ - 1;
            c_end.c_d_id = (int32_t) d_id;
            c_end.c_w_id = (int32_t) w_id;

            history h_start;
            h_start.h_c_id = (int32_t) 0;
            h_start.h_c_d_id = (int32_t) d_id;
            h_start.h_c_w_id = (int32_t) w_id;

            history h_end;
            h_end.h_c_id = (int32_t) c_end.c_id;
            h_end.h_c_d_id = (int32_t) d_id;
            h_end.h_c_w_id = (int32_t) w_id;

            auto c_pids = generate_partition_identifiers(
                k_tpcc_customer_table_id,
                make_customer_key( c_start, tpcc_cfg ),
                make_customer_key( c_end, tpcc_cfg ), data_sizes );
            auto h_pids = generate_partition_identifiers(
                k_tpcc_history_table_id, make_history_key( h_start, tpcc_cfg ),
                make_history_key( h_end, tpcc_cfg ), data_sizes );


            std::vector<void*> insert_c_h_args = {(void*) &w_id, (void*) &d_id,
                                                  (void*) c_ids};
            insert_c_h_arg_codes.at( 2 ).array_length =
                tpcc_cfg.num_customers_per_district_;

            insert_pids.insert( c_pids.begin(), c_pids.end() );
            txn_holder = database->get_partitions_with_begin(
                cli_id, c1_state, insert_pids, {}, inflight_pids, lookup_op );

            res =
                insert_cust_function( txn_holder, cli_id, insert_c_h_arg_codes,
                                      insert_c_h_args, opaque_ptr );

            EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );

            c1_state = txn_holder->commit_transaction();
            delete txn_holder;
            txn_holder = nullptr;
            insert_pids.clear();

            insert_pids.insert( h_pids.begin(), h_pids.end() );
            txn_holder = database->get_partitions_with_begin(
                cli_id, c1_state, insert_pids, {}, inflight_pids, lookup_op );

            res =
                insert_hist_function( txn_holder, cli_id, insert_c_h_arg_codes,
                                      insert_c_h_args, opaque_ptr );

            EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );

            c1_state = txn_holder->commit_transaction();
            delete txn_holder;
            txn_holder = nullptr;
            insert_pids.clear();

            uint32_t o_ol_cnt = tpcc_cfg.max_num_order_lines_per_order_;
            for( uint32_t c_id = 0; c_id < tpcc_cfg.num_customers_per_district_;
                 c_id++ ) {
                uint32_t o_id = c_id;  // not how things are loaded but w/e
                std::vector<record_identifier> order_rids;
                order_rids.reserve( o_ol_cnt + 2 );

                order o;
                o.o_id = (int32_t) o_id;
                o.o_c_id = (int32_t) c_id;
                o.o_d_id = (int32_t) d_id;
                o.o_w_id = (int32_t) w_id;
                o.o_ol_cnt = o_ol_cnt;
                order_rids.push_back(
                    {k_tpcc_order_table_id, make_order_key( o, tpcc_cfg )} );
                insert_pids.insert( generate_partition_identifier(
                    k_tpcc_order_table_id, order_rids.at( 0 ).key_,
                    data_sizes ) );

                new_order new_o;
                new_o.no_w_id = (int32_t) w_id;
                new_o.no_d_id = (int32_t) d_id;
                new_o.no_o_id = (int32_t) o_id;
                order_rids.push_back( {k_tpcc_new_order_table_id,
                                       make_new_order_key( new_o, tpcc_cfg )} );
                insert_pids.insert( generate_partition_identifier(
                    k_tpcc_new_order_table_id, order_rids.at( 1 ).key_,
                    data_sizes ) );

                order_line o_line;
                o_line.ol_o_id = o_id;
                o_line.ol_d_id = d_id;
                o_line.ol_w_id = w_id;

                uint32_t* ol_numbers = new uint32_t[o.o_ol_cnt - 1];

                for( int32_t ol_number = 1; ol_number <= o.o_ol_cnt;
                     ol_number++ ) {
                    ol_numbers[ol_number - 1] = ol_number;
                    o_line.ol_number = ol_number;
                    order_rids.push_back(
                        {k_tpcc_order_line_table_id,
                         make_order_line_key( o_line, tpcc_cfg )} );
                }  // ol_number
                pids = generate_partition_identifiers(
                    k_tpcc_order_line_table_id, order_rids.at( 2 ).key_,
                    order_rids.at( o_ol_cnt + 1 ).key_, data_sizes );
                insert_pids.insert( pids.begin(), pids.end() );

                txn_holder = database->get_partitions_with_begin(
                    cli_id, c1_state, insert_pids, {}, inflight_pids,
                    lookup_op );

                std::vector<void*> insert_only_order_args = {
                    (void*) &w_id, (void*) &d_id, (void*) &c_id, (void*) &o_id,
                    (void*) &o_ol_cnt};
                res = insert_only_order_function(
                    txn_holder, cli_id, insert_only_order_arg_codes,
                    insert_only_order_args, opaque_ptr );
                EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );

                std::vector<void*> insert_order_lines_args = {
                    (void*) &w_id, (void*) &d_id, (void*) &o_id,
                    (void*) ol_numbers};
                insert_order_lines_arg_codes.at( 3 ).array_length =
                    o.o_ol_cnt - 1;
                res = insert_order_lines_function(
                    txn_holder, cli_id, insert_order_lines_arg_codes,
                    insert_order_lines_args, opaque_ptr );
                EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );

                if( loader->is_order_a_new_order( o_id ) ) {
                    std::vector<void*> insert_new_order_args = {
                        (void*) &w_id, (void*) &d_id, (void*) &o_id};
                    res = insert_new_order_function(
                        txn_holder, cli_id, insert_new_order_arg_codes,
                        insert_new_order_args, opaque_ptr );
                    EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );
                }
                c1_state = txn_holder->commit_transaction();
                delete txn_holder;
                txn_holder = nullptr;
                insert_pids.clear();

                delete[] ol_numbers;
            }  // order
        }      // d_id
    }          // w_id

    delete[] stock_ids;
    delete[] d_ids;
    delete[] c_ids;


    return c1_state;
}
#endif

#define DO_TPCC_SCAN_SPROC_ARGS( _scan_args, _expected_scan_size,         \
                                 _expected_res, _sproc_name, _database,   \
                                 _c1_state, _data_sizes, _scan_arg_codes, \
                                 _scan_vals, _opaque_ptr )                \
    do {                                                                  \
        DCHECK_EQ( _expected_scan_size, _scan_args.size() );              \
        partition_column_identifier_set _write_pids;                      \
        partition_column_identifier_set _read_pids;                       \
        partition_column_identifier_set _inflight_pids;                   \
        for( const auto& _scan_arg : _scan_args ) {                       \
            std::vector<partition_column_identifier> _pids =              \
                generate_partition_column_identifiers_from_ckrs(          \
                    _scan_arg.read_ckrs, _data_sizes );                   \
            _read_pids.insert( _pids.begin(), _pids.end() );              \
        }                                                                 \
        function_identifier    _func_id( _sproc_name, _scan_arg_codes );  \
        scan_function_skeleton _scan_function =                           \
            sproc_table->lookup_scan_function( _func_id );                \
        EXPECT_NE( _scan_function, nullptr );                             \
        auto _txn_holder = _database.get_partitions_with_begin(           \
            1, _c1_state, _write_pids, _read_pids, _inflight_pids,        \
            partition_lookup_operation::GET_ALLOW_MISSING );              \
        EXPECT_NE( _txn_holder, nullptr );                                \
        scan_result _scan_res;                                            \
        _scan_function( _txn_holder, 1, _scan_args, _scan_arg_codes,      \
                        _scan_vals, _opaque_ptr, _scan_res );             \
        EXPECT_EQ( _scan_res.status, exec_status_type::COMMAND_OK );      \
        EXPECT_EQ( _scan_res.res_tuples.size(), 1 );                      \
        EXPECT_EQ( _scan_res.res_tuples.count( _expected_res ), 1 );      \
        EXPECT_GE( _scan_res.res_tuples.at( _expected_res ).size(), 0 );  \
        _c1_state = _txn_holder->commit_transaction();                    \
        delete _txn_holder;                                               \
        _txn_holder = nullptr;                                            \
        _read_pids.clear();                                               \
        _write_pids.clear();                                              \
        _inflight_pids.clear();                                           \
    } while( 0 )

#define DO_TPCC_SCAN_SPROC( _scan_args, _expected_scan_size, _expected_res,  \
                            _sproc_name, _database, _c1_state, _data_sizes,  \
                            _opaque_ptr )                                    \
    do {                                                                     \
        std::vector<arg_code> _codes;                                        \
        std::vector<void*>    _vals = {};                                    \
        DO_TPCC_SCAN_SPROC_ARGS(                                             \
            _scan_args, _expected_scan_size, _expected_res, _sproc_name,     \
            _database, _c1_state, _data_sizes, _codes, _vals, _opaque_ptr ); \
    } while( 0 )

void tpcc_sproc_execute_test( const partition_type::type& p_type ) {
    uint32_t site_loc = 1;
    uint32_t num_tables = 13;
    uint32_t num_clients = 10;
    uint32_t gc_sleep_time = 10;

    bool        enable_sec_storage = false;
    std::string sec_storage_dir = "/tmp";

    db database;
    database.init( make_no_op_update_destination_generator(),
                   make_update_enqueuers(),
                   create_tables_metadata( num_tables, site_loc, num_clients,
                                           gc_sleep_time, enable_sec_storage,
                                           sec_storage_dir ) );

    uint32_t cli_id = 0;

    snapshot_vector                     c1_state;
    std::unique_ptr<sproc_lookup_table> sproc_table =
        construct_tpcc_sproc_lookup_table();

    benchmark_configs b_cfg = construct_benchmark_configs(
        1 /*num clients*/, gc_sleep_time, 1 /*  bench time*/,
        1000 /* num operations before checking*/ );
    tpcc_configs tpcc_cfg = construct_tpcc_configs(
        2 /*num warehouses*/, 1000 /*num items*/, 1000 /* num suppliers */,
        10 /* expected num orders per cust*/, 1000 /*item partition size*/,
        2 /*partition size*/, 2 /*district partiion size */,
        2 /* customer partition size*/, 100 /* suppliers partition size */,
        1 /*new order distributed likelihood*/,
        15 /*payment distributed likelihood*/,
        false /*track_and_use_recent_items*/, 1 /* tpcc num clients */,
        1 /* tpch num clients */, b_cfg, 4 /*delivery prob*/, 45 /* new order*/,
        4 /*order status*/, 43 /* payment*/, 4 /*stock level*/, 0 /* q1 prob */,
        0 /* q2 prob */, 0 /* q3 prob */, 0 /* q4 prob */, 0 /* q5 prob */,
        0 /* q6 prob */, 0 /* q7 prob */, 0 /* q8 prob */, 0 /* q9 prob */,
        0 /* q10 prob */, 0 /* q11 prob */, 0 /* q12 prob */, 0 /* q13 prob */,
        0 /* q14 prob */, 0 /* q15 prob */, 0 /* q16 prob */, 0 /* q17 prob */,
        0 /* q18 prob */, 0 /* q19 prob */, 0 /* q20 prob */, 0 /* q21 prob */,
        0 /* q22 prob */, 100 /* all prob */, 2 /*  dist per whouse */,
        30 /*cust per warehouse*/, 6 /*order lines*/,
        9 /*initial num customers per district*/,
        false /*limit number of records propagated*/,
        1 /*number of updates needed for propagtion*/, true /*use warehouse*/,
        true /* use district*/, false /* h scans full tables */ );
    tpcc_cfg.layouts_.warehouse_part_type_ = p_type;
    tpcc_cfg.layouts_.item_part_type_ = p_type;
    tpcc_cfg.layouts_.stock_part_type_ = p_type;
    tpcc_cfg.layouts_.district_part_type_ = p_type;
    tpcc_cfg.layouts_.customer_part_type_ = p_type;
    tpcc_cfg.layouts_.history_part_type_ = p_type;
    tpcc_cfg.layouts_.order_line_part_type_ = p_type;
    tpcc_cfg.layouts_.new_order_part_type_ = p_type;
    tpcc_cfg.layouts_.order_part_type_ = p_type;
    tpcc_cfg.layouts_.customer_district_part_type_ = p_type;
    tpcc_cfg.layouts_.region_part_type_ = p_type;
    tpcc_cfg.layouts_.nation_part_type_ = p_type;
    tpcc_cfg.layouts_.supplier_part_type_ = p_type;

    void* opaque_ptr = construct_tpcc_opaque_pointer( tpcc_cfg );
    c1_state = create_and_load_tpcc_table( &database, sproc_table.get(),
                                           tpcc_cfg, opaque_ptr );

    tpcc_sproc_helper_holder* holder = (tpcc_sproc_helper_holder*) opaque_ptr;
    tpcc_benchmark_worker_templ_no_commit* worker =
        holder->get_c_worker_and_set_holder( 0, nullptr );
    tpch_benchmark_worker_templ_no_commit* h_worker =
        holder->get_h_worker_and_set_holder( 0, nullptr );

    sproc_result               res;
    partition_lookup_operation lookup_op = partition_lookup_operation::GET;
    partition_lookup_operation scan_lookup_op =
        partition_lookup_operation::GET_ALLOW_MISSING;

    std::vector<arg_code> no_op_arg_codes = {};
    std::vector<void*>    no_op_args = {};

    std::vector<cell_key_ranges> write_ckrs;
    std::vector<cell_key_ranges> read_ckrs;

    function_identifier no_op_func_id( k_tpcc_no_op_sproc_name,
                                       no_op_arg_codes );
    function_skeleton no_op_function =
        sproc_table->lookup_function( no_op_func_id );

    partition_column_identifier_set write_pids;
    partition_column_identifier_set read_pids;
    partition_column_identifier_set inflight_pids;
    auto data_sizes = get_tpcc_data_sizes( tpcc_cfg );

    transaction_partition_holder* txn_holder =
        database.get_partitions_with_begin(
            cli_id, c1_state, write_pids, read_pids, inflight_pids, lookup_op );
    res = no_op_function( txn_holder, cli_id, write_ckrs, read_ckrs,
                          no_op_arg_codes, no_op_args, opaque_ptr );
    EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );
    c1_state = txn_holder->commit_transaction();
    delete txn_holder;
    txn_holder = nullptr;

    // do a new order with an abort
    int32_t           w_id = 0;
    int32_t           d_id = 1;
    int32_t           c_id = 7;
    customer_district cd;
    cd.c_w_id = w_id;
    cd.c_d_id = d_id;
    cd.c_id = c_id;
    cell_identifier cd_cid = create_cell_identifier(
        k_tpcc_customer_district_table_id, customer_district_cols::c_next_o_id,
        make_customer_district_key( cd, tpcc_cfg ) );
    district d;
    d.d_w_id = w_id;
    d.d_id = d_id;

    bool is_abort = true;

    std::vector<arg_code> new_order_fetch_and_customer_arg_codes = {
        INTEGER_CODE, INTEGER_CODE, INTEGER_CODE, BOOL_CODE};
    std::vector<void*> new_order_fetch_and_customer_abort_args = {
        (void*) &w_id, (void*) &d_id, (void*) &c_id, (void*) &is_abort};

    function_identifier new_order_fetch_and_set_func_id(
        k_tpcc_new_order_fetch_and_set_next_order_on_customer_sproc_name,
        new_order_fetch_and_customer_arg_codes );
    function_skeleton new_order_fetch_and_set_function =
        sproc_table->lookup_function( new_order_fetch_and_set_func_id );

    write_pids.insert(
        generate_partition_column_identifier_from_cid( cd_cid, data_sizes ) );
    read_pids.insert(
        generate_partition_column_identifier_from_cid( cd_cid, data_sizes ) );

    txn_holder = database.get_partitions_with_begin(
        cli_id, c1_state, write_pids, read_pids, inflight_pids, lookup_op );

    cell_key_ranges cd_ckr = cell_key_ranges_from_cid( cd_cid );
    write_ckrs = {cd_ckr};
    read_ckrs = {cd_ckr};

    res = new_order_fetch_and_set_function(
        txn_holder, cli_id, write_ckrs, read_ckrs,
        new_order_fetch_and_customer_arg_codes,
        new_order_fetch_and_customer_abort_args, opaque_ptr );
    EXPECT_EQ( res.status, exec_status_type::NONFATAL_ERROR );
    std::vector<void*>    new_order_fetch_and_customer_results;
    std::vector<arg_code> new_order_fetch_and_customer_results_arg_codes;
    deserialize_result( res, new_order_fetch_and_customer_results,
                        new_order_fetch_and_customer_results_arg_codes );
    GASSERT_EQ( 2, new_order_fetch_and_customer_results_arg_codes.size() );
    EXPECT_EQ( BOOL_CODE.code,
               new_order_fetch_and_customer_results_arg_codes.at( 0 ).code );
    EXPECT_EQ( INTEGER_CODE.code,
               new_order_fetch_and_customer_results_arg_codes.at( 1 ).code );
    bool decision = *( (bool*) new_order_fetch_and_customer_results.at( 0 ) );
    EXPECT_FALSE( decision );
    // implicit abort
    c1_state = txn_holder->abort_transaction();

    delete txn_holder;
    txn_holder = nullptr;
    // don't clear write and read pids

    // do a new order with a sucess;
    is_abort = false;
    std::vector<void*> new_order_fetch_and_customer_success_args = {
        (void*) &w_id, (void*) &d_id, (void*) &c_id, (void*) &is_abort};

    txn_holder = database.get_partitions_with_begin(
        cli_id, c1_state, write_pids, read_pids, inflight_pids, lookup_op );
    res = new_order_fetch_and_set_function(
        txn_holder, cli_id, write_ckrs, read_ckrs,
        new_order_fetch_and_customer_arg_codes,
        new_order_fetch_and_customer_success_args, opaque_ptr );
    EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );

    new_order_fetch_and_customer_results_arg_codes.clear();
    new_order_fetch_and_customer_results.clear();
    deserialize_result( res, new_order_fetch_and_customer_results,
                        new_order_fetch_and_customer_results_arg_codes );
    EXPECT_EQ( 2, new_order_fetch_and_customer_results_arg_codes.size() );
    EXPECT_EQ( BOOL_CODE.code,
               new_order_fetch_and_customer_results_arg_codes.at( 0 ).code );
    EXPECT_EQ( INTEGER_CODE.code,
               new_order_fetch_and_customer_results_arg_codes.at( 1 ).code );

    decision = *( (bool*) new_order_fetch_and_customer_results.at( 0 ) );
    EXPECT_TRUE( decision );
    int32_t o_id = *( (int32_t*) new_order_fetch_and_customer_results.at( 1 ) );
    EXPECT_GT( o_id, 0 );

    c1_state = txn_holder->commit_transaction();

    delete txn_holder;
    txn_holder = nullptr;
    write_pids.clear();
    read_pids.clear();
    write_ckrs.clear();
    read_ckrs.clear();

    cd.c_next_o_id = o_id + 1;

    // now do a new_order
    // populate

    std::vector<record_identifier> read_set;

    int32_t  o_ol_cnt = tpcc_cfg.max_num_order_lines_per_order_;
    int32_t* item_ids = new int32_t[o_ol_cnt];
    int32_t* supplier_w_ids = new int32_t[o_ol_cnt];
    int32_t* order_quantities = new int32_t[o_ol_cnt];
    int32_t  all_local = 1;
    for( int32_t ol_i = 0; ol_i < o_ol_cnt; ol_i++ ) {
        item_ids[ol_i] = ( ol_i + 1 ) * 3;
        supplier_w_ids[ol_i] = w_id;
        order_quantities[ol_i] = ( ol_i % 9 ) + 1;

        read_ckrs.push_back( cell_key_ranges_from_cell_identifier(
            create_cell_identifier( k_tpcc_item_table_id, item_cols::i_price,
                                    (uint32_t) item_ids[ol_i] ) ) );
    }
    read_ckrs.push_back(
        cell_key_ranges_from_cell_identifier( create_cell_identifier(
            k_tpcc_warehouse_table_id, warehouse_cols::w_tax, w_id ) ) );
    read_ckrs.push_back( cell_key_ranges_from_cell_identifier(
        create_cell_identifier( k_tpcc_district_table_id, district_cols::d_tax,
                                make_w_d_key( d_id, w_id, tpcc_cfg ) ) ) );
    read_ckrs.push_back(
        cell_key_ranges_from_cell_identifier( create_cell_identifier(
            k_tpcc_customer_table_id, customer_cols::c_discount,
            make_w_d_c_key( c_id, d_id, w_id, tpcc_cfg ) ) ) );

    order     o;
    new_order new_o;
    worker->populate_order( o, new_o, o_id, c_id, d_id, w_id, o_ol_cnt,
                            all_local );

    uint64_t order_key = make_order_key( o, tpcc_cfg );
    auto     order_ckr =
        create_cell_key_ranges( k_tpcc_order_table_id, order_key, order_key, 0,
                                k_tpcc_order_num_columns - 1 );
    uint64_t new_order_key = make_new_order_key( new_o, tpcc_cfg );
    auto     new_order_ckr = create_cell_key_ranges(
        k_tpcc_new_order_table_id, new_order_key, new_order_key, 0,
        k_tpcc_new_order_num_columns - 1 );

    write_ckrs.emplace_back( order_ckr );
    write_ckrs.emplace_back( new_order_ckr );

    std::vector<order_line> order_lines( o_ol_cnt );
    std::vector<stock>      stocks( o_ol_cnt );

    std::vector<record_identifier> order_line_rids( o_ol_cnt );
    std::vector<record_identifier> stock_rids( o_ol_cnt );

    worker->populate_order_and_stock(
        order_line_rids, order_lines, stock_rids, stocks, o_id, d_id, w_id,
        o_ol_cnt, item_ids, supplier_w_ids, order_quantities );

    for( const auto& stock_rid : stock_rids ) {
        auto stock_ckr = create_cell_key_ranges(
            k_tpcc_stock_table_id, stock_rid.key_, stock_rid.key_,
            stock_cols::s_quantity, stock_cols::s_remote_cnt );
        auto stock_dist = create_cell_key_ranges(
            k_tpcc_stock_table_id, stock_rid.key_, stock_rid.key_,
            stock_cols::s_dist_0 + ( d_id - 1 ),
            stock_cols::s_dist_0 + ( d_id - 1 ) );

        read_ckrs.emplace_back( stock_ckr );
        write_ckrs.emplace_back( stock_ckr );
        write_ckrs.emplace_back( stock_dist );
    }

    auto order_line_ckrs = create_cell_key_ranges(
        k_tpcc_order_line_table_id, order_line_rids.at( 0 ).key_,
        order_line_rids.at( order_line_rids.size() - 1 ).key_, 0,
        k_tpcc_order_line_num_columns - 1 );

    write_ckrs.push_back( order_line_ckrs );

    auto no_w_pids = generate_partition_column_identifiers_from_ckrs(
        write_ckrs, data_sizes );
    auto no_r_pids = generate_partition_column_identifiers_from_ckrs(
        read_ckrs, data_sizes );
    write_pids.insert( no_w_pids.begin(), no_w_pids.end() );
    read_pids.insert( no_r_pids.begin(), no_r_pids.end() );

    std::vector<arg_code> place_new_order_arg_codes{
        INTEGER_CODE /*w_id*/, INTEGER_CODE /*d_id*/, INTEGER_CODE /*c_id*/,
        INTEGER_CODE /*o_id*/, INTEGER_CODE /*o_all_local*/};
    arg_code array_code = INTEGER_ARRAY_CODE;
    array_code.array_length = o_ol_cnt;
    place_new_order_arg_codes.push_back( array_code );  // item_ids
    place_new_order_arg_codes.push_back( array_code );  // w_ids
    place_new_order_arg_codes.push_back( array_code );  // order_quantities

    std::vector<void*> place_new_order_args = {
        (void*) &w_id,          (void*) &d_id,           (void*) &c_id,
        (void*) &o_id,          (void*) &all_local,      (void*) item_ids,
        (void*) supplier_w_ids, (void*) order_quantities};

    function_identifier place_new_order_func_id(
        k_tpcc_new_order_place_new_orders_sproc_name,
        place_new_order_arg_codes );
    function_skeleton place_new_order_function =
        sproc_table->lookup_function( place_new_order_func_id );

    txn_holder = database.get_partitions_with_begin(
        cli_id, c1_state, write_pids, read_pids, inflight_pids,
        partition_lookup_operation::GET_OR_CREATE );

    res = place_new_order_function( txn_holder, cli_id, write_ckrs, read_ckrs,
                                    place_new_order_arg_codes,
                                    place_new_order_args, opaque_ptr );
    EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );
    c1_state = txn_holder->commit_transaction();
    delete txn_holder;
    txn_holder = nullptr;
    write_pids.clear();
    read_pids.clear();
    write_ckrs.clear();
    read_ckrs.clear();

    delete[] item_ids;
    delete[] supplier_w_ids;
    delete[] order_quantities;

    // STOCK LEVEL

    int32_t stock_level_w_id = 0;
    int32_t stock_level_d_id = 1;
    int32_t stock_level_threshold = 20;

    auto district_sl_cid = create_cell_identifier(
        k_tpcc_district_table_id, district_cols::d_next_o_id,
        make_w_d_key( d_id, w_id, tpcc_cfg ) );
    read_pids.insert( generate_partition_column_identifier_from_cid(
        district_sl_cid, data_sizes ) );
    read_ckrs.emplace_back( cell_key_ranges_from_cid( district_sl_cid ) );

    txn_holder = database.get_partitions_with_begin(
        cli_id, c1_state, write_pids, read_pids, inflight_pids, lookup_op );

    std::vector<arg_code>
        stock_level_get_max_order_id_to_look_back_on_arg_codes = {INTEGER_CODE,
                                                                  INTEGER_CODE};
    std::vector<void*> stock_level_get_max_order_id_to_look_back_on_args = {
        (void*) &stock_level_w_id, (void*) &stock_level_d_id};

    function_identifier stock_level_get_max_order_id_to_look_back_on_func_id(
        k_tpcc_stock_level_get_max_order_id_to_look_back_on_sproc_name,
        stock_level_get_max_order_id_to_look_back_on_arg_codes );
    function_skeleton stock_level_get_max_order_id_to_look_back_on_function =
        sproc_table->lookup_function(
            stock_level_get_max_order_id_to_look_back_on_func_id );
    res = stock_level_get_max_order_id_to_look_back_on_function(
        txn_holder, cli_id, write_ckrs, read_ckrs,
        stock_level_get_max_order_id_to_look_back_on_arg_codes,
        stock_level_get_max_order_id_to_look_back_on_args, opaque_ptr );
    EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );
    std::vector<void*>    stock_level_get_order_id_results;
    std::vector<arg_code> stock_level_get_order_id_ret_arg_codes;
    deserialize_result( res, stock_level_get_order_id_results,
                        stock_level_get_order_id_ret_arg_codes );
    EXPECT_EQ( 1, stock_level_get_order_id_results.size() );
    EXPECT_EQ( 1, stock_level_get_order_id_ret_arg_codes.size() );
    EXPECT_EQ( INTEGER_CODE.as_bytes,
               stock_level_get_order_id_ret_arg_codes.at( 0 ).as_bytes );
    int32_t stock_level_o_id =
        *( (int32_t*) stock_level_get_order_id_results.at( 0 ) );
    free_args( stock_level_get_order_id_results );
    EXPECT_GE( stock_level_o_id, 0 );

    c1_state = txn_holder->commit_transaction();

    delete txn_holder;
    txn_holder = nullptr;
    write_pids.clear();
    read_pids.clear();
    write_ckrs.clear();
    read_ckrs.clear();

    int32_t              base_o_id = std::max( 0, stock_level_o_id - 5 );
    std::vector<int32_t> stock_level_o_ids;
    std::vector<int32_t> stock_level_ol_nums;
    int32_t max_ol_num = (int32_t) tpcc_cfg.max_num_order_lines_per_order_;
    DVLOG( 5 ) << "Looking at order ids in range:" << base_o_id << ", "
               << stock_level_o_id << ", max ol num:" << max_ol_num;

    auto sl_recent_items = create_cell_key_ranges(
        k_tpcc_order_line_table_id,
        make_w_d_o_ol_key( 1, base_o_id, d_id, w_id, tpcc_cfg ),
        make_w_d_o_ol_key( max_ol_num, stock_level_o_id, d_id, w_id, tpcc_cfg ),
        order_line_cols::ol_i_id, order_line_cols::ol_i_id );
    read_ckrs.emplace_back( sl_recent_items );
    auto sl_read_pids = generate_partition_column_identifiers_from_ckrs(
        read_ckrs, data_sizes );
    read_pids.insert( sl_read_pids.begin(), sl_read_pids.end() );

    scan_arguments sl_scan_arg;
    sl_scan_arg.label = 0;
    sl_scan_arg.read_ckrs = read_ckrs;

    std::vector<scan_arguments> sl_scan_args = {sl_scan_arg};

    std::vector<void*>    stock_level_get_recent_items_args = {};
    std::vector<arg_code> stock_level_get_recent_items_arg_codes = {};
    function_identifier   stock_level_get_recent_items_func_id(
        k_tpcc_stock_level_get_recent_items_sproc_name,
        stock_level_get_recent_items_arg_codes );
    scan_function_skeleton stock_level_get_recent_items_function =
        sproc_table->lookup_scan_function(
            stock_level_get_recent_items_func_id );

    txn_holder = database.get_partitions_with_begin(
        cli_id, c1_state, write_pids, read_pids, inflight_pids,
        scan_lookup_op );

    scan_result sl_res;
    stock_level_get_recent_items_function(
        txn_holder, cli_id, sl_scan_args,
        stock_level_get_recent_items_arg_codes,
        stock_level_get_recent_items_args, opaque_ptr, sl_res );
    EXPECT_EQ( sl_res.status, exec_status_type::COMMAND_OK );
    std::vector<int32_t> stock_level_get_recent_items;
    for( const auto& res : sl_res.res_tuples ) {
        for( const auto& rt : res.second ) {
            stock_level_get_recent_items.emplace_back( rt.row_id );
        }
    }
    int32_t num_stock_level_item_ids = stock_level_get_recent_items.size();
    EXPECT_GT( num_stock_level_item_ids, 0 );
    int32_t* stock_level_item_ids = stock_level_get_recent_items.data();

    c1_state = txn_holder->commit_transaction();

    delete txn_holder;
    txn_holder = nullptr;
    write_pids.clear();
    read_pids.clear();
    write_ckrs.clear();
    read_ckrs.clear();

    std::vector<arg_code> stock_level_get_stock_below_threshold_arg_codes = {};
    std::vector<void*>    stock_level_get_stock_below_threshold_args = {};
    function_identifier   stock_level_get_stock_below_func_id(
        k_tpcc_stock_level_get_stock_below_threshold_sproc_name,
        stock_level_get_stock_below_threshold_arg_codes );
    scan_function_skeleton stock_level_get_stock_below_threshold_function =
        sproc_table->lookup_scan_function(
            stock_level_get_stock_below_func_id );

    scan_arguments sl_threshold_scan_arg;
    sl_threshold_scan_arg.label = 0;

    for( int32_t pos = 0; pos < num_stock_level_item_ids; pos++ ) {
        auto sl_threshold_cid = create_cell_identifier(
            k_tpcc_stock_table_id, stock_cols::s_quantity,
            make_w_s_key( w_id, stock_level_item_ids[pos], tpcc_cfg ) );
        read_pids.insert( generate_partition_column_identifier_from_cid(
            sl_threshold_cid, data_sizes ) );
        sl_threshold_scan_arg.read_ckrs.emplace_back(
            cell_key_ranges_from_cid( sl_threshold_cid ) );
    }

    cell_predicate sl_c_threshold_pred;
    sl_c_threshold_pred.table_id = k_tpcc_stock_table_id;
    sl_c_threshold_pred.col_id = stock_cols::s_quantity;
    sl_c_threshold_pred.type = data_type::type::INT64;
    sl_c_threshold_pred.data = int64_to_string( stock_level_threshold );
    sl_c_threshold_pred.predicate = predicate_type::type::LESS_THAN;

    predicate_chain sl_threshold_pred;
    sl_threshold_pred.and_predicates.emplace_back( sl_c_threshold_pred );

    sl_threshold_scan_arg.predicate = sl_threshold_pred;

    std::vector<scan_arguments> sl_threshold_scan_args = {
        sl_threshold_scan_arg};

    txn_holder = database.get_partitions_with_begin(
        cli_id, c1_state, write_pids, read_pids, inflight_pids,
        scan_lookup_op );

    scan_result sl_threshold_res;

    stock_level_get_stock_below_threshold_function(
        txn_holder, cli_id, sl_threshold_scan_args,
        stock_level_get_stock_below_threshold_arg_codes,
        stock_level_get_stock_below_threshold_args, opaque_ptr,
        sl_threshold_res );
    EXPECT_EQ( sl_threshold_res.status, exec_status_type::COMMAND_OK );
    EXPECT_EQ( 1, sl_threshold_res.res_tuples.size() );
    EXPECT_EQ( 1, sl_threshold_res.res_tuples.count( 0 ) );
    int32_t num_stock_below_threshold =
        sl_threshold_res.res_tuples.at( 0 ).size();
    EXPECT_GE( num_stock_below_threshold, 0 );

    c1_state = txn_holder->commit_transaction();

    delete txn_holder;
    txn_holder = nullptr;
    write_pids.clear();
    read_pids.clear();
    write_ckrs.clear();
    read_ckrs.clear();
    (void) stock_level_threshold;
    (void) stock_level_item_ids;

    // payment

    w_id = 0;
    d_id = 1;
    int32_t c_w_id = 0;
    int32_t c_d_id = 0;
    c_id = 17;
    float h_amount = 53;

    std::vector<arg_code> payment_arg_codes = k_tpcc_payment_arg_codes;
    std::vector<void*>    payment_args = {(void*) &w_id,   (void*) &d_id,
                                       (void*) &c_w_id, (void*) &c_d_id,
                                       (void*) &c_id,   (void*) &h_amount};

    function_identifier payment_func_id( k_tpcc_payment_sproc_name,
                                         payment_arg_codes );
    function_skeleton payment_function =
        sproc_table->lookup_function( payment_func_id );

    std::vector<cell_key_ranges> payment_ckrs =
        get_payment_ckrs( w_id, d_id, c_w_id, c_d_id, c_id, tpcc_cfg );
    auto payment_pids = generate_partition_column_identifiers_from_ckrs(
        payment_ckrs, data_sizes );
    write_pids.insert( payment_pids.begin(), payment_pids.end() );
    read_pids.insert( payment_pids.begin(), payment_pids.end() );

    txn_holder = database.get_partitions_with_begin(
        cli_id, c1_state, write_pids, read_pids, inflight_pids, lookup_op );
    write_ckrs = payment_ckrs;
    read_ckrs = payment_ckrs;

    res = payment_function( txn_holder, cli_id, write_ckrs, read_ckrs,
                            payment_arg_codes, payment_args, opaque_ptr );
    EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );
    c1_state = txn_holder->commit_transaction();

    delete txn_holder;
    txn_holder = nullptr;
    write_pids.clear();
    read_pids.clear();
    read_ckrs.clear();
    write_ckrs.clear();

    order_line low_ol;
    order_line high_ol;
    low_ol.ol_w_id = 0;
    low_ol.ol_d_id = 0;
    high_ol.ol_w_id = tpcc_cfg.num_warehouses_ - 1;
    high_ol.ol_d_id = tpcc_cfg.num_districts_per_warehouse_ - 1;

    std::vector<scan_arguments> q1_scan_args =
        h_worker->generate_q1_scan_args( low_ol, high_ol, 0 );
    DO_TPCC_SCAN_SPROC( q1_scan_args, 1, 0, k_tpch_q1_sproc_name, database,
                        c1_state, data_sizes, opaque_ptr );

    order low_o;
    low_o.o_w_id = 0;
    low_o.o_d_id = 1;
    low_o.o_id = 0;

    order high_o;
    high_o.o_w_id = 1;
    high_o.o_d_id = tpcc_cfg.num_districts_per_warehouse_ - 1;
    high_o.o_id = 30;

    datetime q4_time = get_current_time();

    std::vector<scan_arguments> q4_scan_args = h_worker->generate_q4_scan_args(
        low_o, high_o, q4_time.c_since - 10, q4_time.c_since );

    DO_TPCC_SCAN_SPROC( q4_scan_args, 2, 0, k_tpch_q4_sproc_name, database,
                        c1_state, data_sizes, opaque_ptr );

    std::vector<scan_arguments> q6_scan_args = h_worker->generate_q6_scan_args(
        low_ol, high_ol, q4_time.c_since - 10, q4_time.c_since, 1, 1000 );
    DO_TPCC_SCAN_SPROC( q6_scan_args, 1, 0, k_tpch_q6_sproc_name, database,
                        c1_state, data_sizes, opaque_ptr );

    item low_item;
    low_item.i_id = 1;
    item high_item;
    high_item.i_id = tpcc_cfg.num_items_ / 2;

    std::vector<scan_arguments> q14_scan_args =
        h_worker->generate_q14_scan_args( low_ol, high_ol, low_item, high_item,
                                          q4_time.c_since - 10,
                                          q4_time.c_since );
    DO_TPCC_SCAN_SPROC( q14_scan_args, 2, 0, k_tpch_q14_sproc_name, database,
                        c1_state, data_sizes, opaque_ptr );

    int32_t  q5_region_id = 2;
    supplier low_supplier;
    supplier high_supplier;
    low_supplier.s_id = 3;
    high_supplier.s_id = tpcc_cfg.num_suppliers_ / 2;

    nation low_nation;
    nation high_nation;
    low_nation.n_id = 0;
    high_nation.n_id = nation::NUM_NATIONS - 1;

    std::vector<scan_arguments> q5_scan_args = h_worker->generate_q5_scan_args(
        low_ol, high_ol, low_item, high_item, low_supplier, high_supplier,
        low_nation, high_nation, q4_time.c_since - 10, q5_region_id );
    DO_TPCC_SCAN_SPROC( q5_scan_args, 4, 0, k_tpch_q5_sproc_name, database,
                        c1_state, data_sizes, opaque_ptr );

    std::vector<scan_arguments> q3_scan_args = h_worker->generate_q3_scan_args(
        low_o, high_o, "a", "b", q4_time.c_since - 10 );
    DO_TPCC_SCAN_SPROC( q3_scan_args, 3, 0, k_tpch_q3_sproc_name, database,
                        c1_state, data_sizes, opaque_ptr );

    int32_t q10_nation_id = 1;

    std::vector<scan_arguments> q10_scan_args =
        h_worker->generate_q10_scan_args( low_o, high_o, q4_time.c_since - 10,
                                          q10_nation_id );
    DO_TPCC_SCAN_SPROC( q10_scan_args, 4, 0, k_tpch_q10_sproc_name, database,
                        c1_state, data_sizes, opaque_ptr );

    std::vector<scan_arguments> q18_scan_args =
        h_worker->generate_q18_scan_args( low_o, high_o );
    std::vector<arg_code> q18_arg_codes = {FLOAT_CODE};
    float                 q18_threshold = 200.0;
    std::vector<void*>    q18_arg_vals = {(void*) &q18_threshold};
    DO_TPCC_SCAN_SPROC_ARGS( q18_scan_args, 3, 0, k_tpch_q18_sproc_name,
                             database, c1_state, data_sizes, q18_arg_codes,
                             q18_arg_vals, opaque_ptr );

    std::vector<scan_arguments> q12_scan_args =
        h_worker->generate_q12_scan_args( low_o, high_o, q4_time.c_since - 10 );
    DO_TPCC_SCAN_SPROC( q12_scan_args, 2, 0, k_tpch_q12_sproc_name, database,
                        c1_state, data_sizes, opaque_ptr );

    std::vector<scan_arguments> q8_scan_args = h_worker->generate_q8_scan_args(
        low_o, high_o, low_item, high_item, low_supplier, high_supplier,
        low_nation, high_nation, q4_time.c_since - 10, q4_time.c_since,
        0 /* region id */ );
    DO_TPCC_SCAN_SPROC( q8_scan_args, 5, 0, k_tpch_q8_sproc_name, database,
                        c1_state, data_sizes, opaque_ptr );

    std::vector<scan_arguments> q9_scan_args = h_worker->generate_q9_scan_args(
        low_o, high_o, low_item, high_item, low_supplier, high_supplier,
        low_nation, high_nation, "A", "B" );
    DO_TPCC_SCAN_SPROC( q9_scan_args, 6, 0, k_tpch_q9_sproc_name, database,
                        c1_state, data_sizes, opaque_ptr );

    stock low_stock;
    stock high_stock;

    low_stock.s_i_id = low_item.i_id;
    high_stock.s_i_id = high_item.i_id;

    low_stock.s_w_id = low_o.o_w_id;
    high_stock.s_w_id = high_o.o_w_id;

    std::vector<scan_arguments> q11_scan_args =
        h_worker->generate_q11_scan_args( low_stock, high_stock, low_supplier,
                                          high_supplier, q10_nation_id );
    std::vector<arg_code> q11_arg_codes = {FLOAT_CODE};
    float                 q11_threshold = 0.05;
    std::vector<void*>    q11_arg_vals = {(void*) &q11_threshold};
    DO_TPCC_SCAN_SPROC_ARGS( q11_scan_args, 2, 0, k_tpch_q11_sproc_name,
                             database, c1_state, data_sizes, q11_arg_codes,
                             q11_arg_vals, opaque_ptr );

    int32_t                     o_carrier_threshold = 8;
    std::vector<scan_arguments> q13_scan_args =
        h_worker->generate_q13_scan_args( low_o, high_o, o_carrier_threshold );
    DO_TPCC_SCAN_SPROC( q13_scan_args, 2, 0, k_tpch_q13_sproc_name, database,
                        c1_state, data_sizes, opaque_ptr );

    std::vector<scan_arguments> q15_scan_args =
        h_worker->generate_q15_scan_args( low_ol, high_ol, low_item, high_item,
                                          low_supplier, high_supplier,
                                          q4_time.c_since - 10 );
    DO_TPCC_SCAN_SPROC( q15_scan_args, 3, 0, k_tpch_q15_sproc_name, database,
                        c1_state, data_sizes, opaque_ptr );

    std::vector<scan_arguments> q16_scan_args =
        h_worker->generate_q16_scan_args( low_stock, high_stock, low_supplier,
                                          high_supplier, "a", "b", "e", "f" );
    DO_TPCC_SCAN_SPROC( q16_scan_args, 3, 0, k_tpch_q16_sproc_name, database,
                        c1_state, data_sizes, opaque_ptr );

    std::vector<scan_arguments> q19_scan_args =
        h_worker->generate_q19_scan_args(
            low_ol, high_ol, low_item, high_item, 1 /* low_ol_quantity */,
            10 /* high_ol_quantity */, 1 /* low_i_price */,
            40000 /* high_i_price */, "a" /* low_i_data */,
            "b" /* high_i_data */ );
    DO_TPCC_SCAN_SPROC( q19_scan_args, 2, 0, k_tpch_q19_sproc_name, database,
                        c1_state, data_sizes, opaque_ptr );

    std::vector<scan_arguments> q22_scan_args =
        h_worker->generate_q22_scan_args( low_o, high_o, "1" /* low c_phone */,
                                          "7" /* high c_phone */ );
    DO_TPCC_SCAN_SPROC( q22_scan_args, 2, 0, k_tpch_q22_sproc_name, database,
                        c1_state, data_sizes, opaque_ptr );

    std::vector<scan_arguments> q20_scan_args =
        h_worker->generate_q20_scan_args(
            low_ol, high_ol, low_item, high_item, low_supplier, high_supplier,
            2 /* nation id */, "c" /* low_i_data */, "d" /*high_i_data*/,
            q4_time.c_since - 10 );
    std::vector<arg_code> q20_arg_codes = {FLOAT_CODE};
    float                 q20_mult = 2;
    std::vector<void*>    q20_arg_vals = {(void*) &q20_mult};
    DO_TPCC_SCAN_SPROC_ARGS( q20_scan_args, 4, 0, k_tpch_q20_sproc_name,
                             database, c1_state, data_sizes, q20_arg_codes,
                             q20_arg_vals, opaque_ptr );

    std::vector<scan_arguments> q21_scan_args =
        h_worker->generate_q21_scan_args( low_ol, high_ol, low_item, high_item,
                                          low_supplier, high_supplier,
                                          2 /* nation id */ );
    DO_TPCC_SCAN_SPROC( q21_scan_args, 4, 0, k_tpch_q21_sproc_name, database,
                        c1_state, data_sizes, opaque_ptr );

    std::vector<scan_arguments> q17_scan_args =
        h_worker->generate_q17_scan_args( low_ol, high_ol, low_item, high_item,
                                          "b", "c" );
    DO_TPCC_SCAN_SPROC( q17_scan_args, 2, 0, k_tpch_q17_sproc_name, database,
                        c1_state, data_sizes, opaque_ptr );

    std::vector<scan_arguments> q2_scan_args = h_worker->generate_q2_scan_args(
        low_stock, high_stock, low_supplier, high_supplier, low_nation,
        high_nation, 0 /* region id */ );
    DO_TPCC_SCAN_SPROC( q2_scan_args, 4, 0, k_tpch_q2_sproc_name, database,
                        c1_state, data_sizes, opaque_ptr );

    std::vector<scan_arguments> q7_scan_args = h_worker->generate_q7_scan_args(
        low_o, high_o, low_item, high_item, low_supplier, high_supplier,
        0 /* low nation id */, 5 /* high nation id */, q4_time.c_since - 10,
        q4_time.c_since );
    DO_TPCC_SCAN_SPROC( q7_scan_args, 5, 0, k_tpch_q7_sproc_name, database,
                        c1_state, data_sizes, opaque_ptr );

    (void) worker;
}

TEST_F( tpcc_sproc_test, tpcc_sproc_execute_test_row ) {
    tpcc_sproc_execute_test( partition_type::type::ROW );
}
TEST_F( tpcc_sproc_test, tpcc_sproc_execute_test_column ) {
    tpcc_sproc_execute_test( partition_type::type::COLUMN );
}
TEST_F( tpcc_sproc_test, tpcc_sproc_execute_test_sorted_column ) {
    tpcc_sproc_execute_test( partition_type::type::SORTED_COLUMN );
}
TEST_F( tpcc_sproc_test, tpcc_sproc_execute_test_multi_column ) {
    tpcc_sproc_execute_test( partition_type::type::MULTI_COLUMN );
}
TEST_F( tpcc_sproc_test, tpcc_sproc_execute_test_sorted_multi_column ) {
    tpcc_sproc_execute_test( partition_type::type::SORTED_MULTI_COLUMN );
}

#if 0  // MTODO-TPCC
TEST_F(tpcc_sproc_test, tpcc_args_sproc_execute_test) {
    uint32_t site_loc = 1;
    uint32_t num_tables = 10;
    uint32_t num_clients = 10;
    uint32_t gc_sleep_time = 10;

    db database;
    database.init( site_loc, num_tables, num_clients, gc_sleep_time,
                   make_no_op_update_destination_generator(),
                   make_update_enqueuers() );

    uint32_t cli_id = 0;

    snapshot_vector                     c1_state;
    std::unique_ptr<sproc_lookup_table> sproc_table =
        construct_tpcc_sproc_lookup_table();

    benchmark_configs b_cfg = construct_benchmark_configs(
        1 /*num clients*/, gc_sleep_time, 1 /*  bench time*/,
        1000 /* num operations before checking*/ );
    tpcc_configs tpcc_cfg = construct_tpcc_configs(
        2 /*num warehouses*/, 1000 /*num items*/,
        10 /* expected num orders per cust*/, 1000 /*item partition size*/,
        2 /*partition size*/, 2 /*district partiion size */,
        2 /* customer partition size*/, 1 /*new order distributed likelihood*/,
        15 /*payment distributed likelihood*/,
        false /*track_and_use_recent_items*/, 1 /* tpcc num clients */,
        1 /* tpch num clients */, b_cfg, 4 /*delivery prob*/,
        45 /* new order*/, 4 /*order status*/, 43 /* payment*/,
        4 /*stock level*/, 100 /* q1 prob */, 2 /*  dist per whouse */, 30 /*cust per warehouse*/,
        6 /*order lines*/, 9 /*initial num customers per district*/,
        false /*limit number of records propagated*/,
        1 /*number of updates needed for propagtion*/, true /*use warehouse*/,
        true /* use district*/ );

    void* opaque_ptr = construct_tpcc_opaque_pointer( tpcc_cfg );
    c1_state = create_and_load_arg_based_tpcc_table(
        &database, sproc_table.get(), tpcc_cfg, opaque_ptr );

    tpcc_sproc_helper_holder* holder = (tpcc_sproc_helper_holder*) opaque_ptr;
    tpcc_benchmark_worker_templ_no_commit* worker =
        holder->get_c_worker_and_set_holder( 0, nullptr );

    sproc_result res;
    partition_lookup_operation lookup_op = partition_lookup_operation::GET;


    partition_identifier_set write_pids;
    partition_identifier_set read_pids;
    partition_identifier_set inflight_pids;
    auto                     data_sizes = get_tpcc_data_sizes( tpcc_cfg );
    transaction_partition_holder* txn_holder = nullptr;

    // do a new order with an abort
    int32_t  w_id = 0;
    int32_t  d_id = 1;
    district d;
    d.d_w_id = w_id;
    d.d_id = d_id;
    record_identifier d_rid = {k_tpcc_district_table_id,
                               make_district_key( d, tpcc_cfg )};

    bool is_abort = true;

    std::vector<arg_code> new_order_fetch_and_district_arg_codes = {
        INTEGER_CODE, INTEGER_CODE, BOOL_CODE};
    std::vector<void*> new_order_fetch_and_district_abort_args = {
        (void*) &w_id, (void*) &d_id, (void*) &is_abort};

    function_identifier new_order_fetch_and_set_func_id(
        k_tpcc_new_order_fetch_and_set_next_order_on_district_sproc_name,
        new_order_fetch_and_district_arg_codes );
    function_skeleton new_order_fetch_and_set_function =
        sproc_table->lookup_function( new_order_fetch_and_set_func_id );

    write_pids.insert( generate_partition_identifier(
        d_rid.table_id_, d_rid.key_, data_sizes ) );

    txn_holder = database.get_partitions_with_begin(
        cli_id, c1_state, write_pids, read_pids, inflight_pids, lookup_op );
    res = new_order_fetch_and_set_function(
        txn_holder, cli_id, new_order_fetch_and_district_arg_codes,
        new_order_fetch_and_district_abort_args, opaque_ptr );
    EXPECT_EQ( res.status, exec_status_type::NONFATAL_ERROR );
    std::vector<void*>    new_order_fetch_and_district_results;
    std::vector<arg_code> new_order_fetch_and_district_results_arg_codes;

    deserialize_result( res, new_order_fetch_and_district_results,
                        new_order_fetch_and_district_results_arg_codes );
    EXPECT_EQ( 2, new_order_fetch_and_district_results_arg_codes.size() );
    EXPECT_EQ( BOOL_CODE.code,
               new_order_fetch_and_district_results_arg_codes.at( 0 ).code );
    EXPECT_EQ( INTEGER_CODE.code,
               new_order_fetch_and_district_results_arg_codes.at( 1 ).code );
    bool decision = *( (bool*) new_order_fetch_and_district_results.at( 0 ) );
    EXPECT_FALSE( decision );
    // implicit abort
    c1_state = txn_holder->abort_transaction();

    // do a new order with a sucess;
    is_abort = false;
    std::vector<void*> new_order_fetch_and_district_success_args = {
        (void*) &w_id, (void*) &d_id, (void*) &is_abort};

    txn_holder = database.get_partitions_with_begin(
        cli_id, c1_state, write_pids, read_pids, inflight_pids, lookup_op );
    res = new_order_fetch_and_set_function(
        txn_holder, cli_id, new_order_fetch_and_district_arg_codes,
        new_order_fetch_and_district_abort_args, opaque_ptr );

    EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );

    new_order_fetch_and_district_results_arg_codes.clear();
    new_order_fetch_and_district_results.clear();
    deserialize_result( res, new_order_fetch_and_district_results,
                        new_order_fetch_and_district_results_arg_codes );
    EXPECT_EQ( 2, new_order_fetch_and_district_results_arg_codes.size() );
    EXPECT_EQ( BOOL_CODE.code,
               new_order_fetch_and_district_results_arg_codes.at( 0 ).code );
    EXPECT_EQ( INTEGER_CODE.code,
               new_order_fetch_and_district_results_arg_codes.at( 1 ).code );

    decision = *( (bool*) new_order_fetch_and_district_results.at( 0 ) );
    EXPECT_TRUE( decision );
    int32_t o_id = *( (int32_t*) new_order_fetch_and_district_results.at( 1 ) );
    EXPECT_GT( o_id, 0 );
    d.d_next_o_id = o_id + 1;

    c1_state = txn_holder->commit_transaction();

    delete txn_holder;
    txn_holder = nullptr;
    write_pids.clear();
    read_pids.clear();

    // now do a new_order
    // populate

    std::vector<record_identifier> read_set;

    int32_t  c_id = 27;
    int32_t  o_ol_cnt = tpcc_cfg.max_num_order_lines_per_order_;
    int32_t* item_ids = new int32_t[o_ol_cnt];
    int32_t* supplier_w_ids = new int32_t[o_ol_cnt];
    int32_t* order_quantities = new int32_t[o_ol_cnt];
    int32_t* order_line_ids = new int32_t[o_ol_cnt];
    int32_t  all_local = 1;
    for( int32_t ol_i = 0; ol_i < o_ol_cnt; ol_i++ ) {
        item_ids[ol_i] = ( ol_i + 1 ) * 3;
        supplier_w_ids[ol_i] = w_id;
        order_quantities[ol_i] = ( ol_i % 9 ) + 1;
        order_line_ids[ol_i] = ol_i + 1;

        read_set.push_back( {k_tpcc_item_table_id, (uint32_t) item_ids[ol_i]} );
    }
    read_set.push_back( {k_tpcc_warehouse_table_id, (uint64_t) d.d_w_id} );
    read_set.push_back(
        {k_tpcc_district_table_id, make_district_key( d, tpcc_cfg )} );
    read_set.push_back( {k_tpcc_customer_table_id,
                         make_w_d_c_key( c_id, d.d_id, d.d_w_id, tpcc_cfg )} );

    order     o;
    new_order new_o;
    worker->populate_order( o, new_o, o_id, c_id, d_id, w_id, o_ol_cnt,
                            all_local );

    record_identifier o_rid = {k_tpcc_order_table_id,
                               make_order_key( o, tpcc_cfg )};
    record_identifier new_o_rid = {k_tpcc_new_order_table_id,
                                   make_new_order_key( new_o, tpcc_cfg )};
    std::vector<order_line> order_lines( o_ol_cnt );
    std::vector<stock>      stocks( o_ol_cnt );

    std::vector<record_identifier> order_line_rids( o_ol_cnt );
    std::vector<record_identifier> stock_rids( o_ol_cnt );

    worker->populate_order_and_stock(
        order_line_rids, order_lines, stock_rids, stocks, o_id, d_id, w_id,
        o_ol_cnt, item_ids, supplier_w_ids, order_quantities );

    std::vector<record_identifier> write_set;
    write_set.reserve( 2 + ( o_ol_cnt * 2 ) );
    write_set.push_back( o_rid );
    write_set.push_back( new_o_rid );
    write_set.insert( write_set.begin() + 2, order_line_rids.begin(),
                      order_line_rids.end() );
    write_set.insert( write_set.begin() + 2 + o_ol_cnt, stock_rids.begin(),
                      stock_rids.end() );

    for( record_identifier write_pk : write_set ) {
        write_pids.insert( generate_partition_identifier(
            write_pk.table_id_, write_pk.key_, data_sizes ) );
    }
    for( record_identifier read_pk : read_set ) {
        read_pids.insert( generate_partition_identifier(
            read_pk.table_id_, read_pk.key_, data_sizes ) );
    }

    txn_holder = database.get_partitions_with_begin(
        cli_id, c1_state, write_pids, read_pids, inflight_pids,
        partition_lookup_operation::GET_OR_CREATE );

    std::vector<arg_code> new_order_get_warehouse_tax_arg_codes = {
        INTEGER_CODE /*w_id*/};
    std::vector<void*> new_order_get_warehouse_tax_args = {(void*) &w_id};

    function_identifier new_order_get_warehouse_tax_func_id(
        k_tpcc_new_order_get_warehouse_tax_sproc_name,
        new_order_get_warehouse_tax_arg_codes );
    function_skeleton new_order_get_warehouse_tax_function =
        sproc_table->lookup_function( new_order_get_warehouse_tax_func_id );
    res = new_order_get_warehouse_tax_function(
        txn_holder, cli_id, new_order_get_warehouse_tax_arg_codes,
        new_order_get_warehouse_tax_args, opaque_ptr );
    EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );

    std::vector<void*>    tax_discount_results;
    std::vector<arg_code> tax_discount_arg_codes;
    deserialize_result( res, tax_discount_results, tax_discount_arg_codes );
    EXPECT_EQ( 1, tax_discount_arg_codes.size() );
    EXPECT_EQ( FLOAT_CODE.code, tax_discount_arg_codes.at( 0 ).code );
    float w_tax = *( (float*) tax_discount_results.at( 0 ) );
    EXPECT_GE( w_tax, 0 );
    free_args( tax_discount_results );

    std::vector<arg_code> new_order_get_district_tax_arg_codes = {
        INTEGER_CODE /*w_id*/, INTEGER_CODE /*d_id*/};
    std::vector<void*> new_order_get_district_tax_args = {(void*) &w_id,
                                                          (void*) &d_id};

    function_identifier new_order_get_district_tax_func_id(
        k_tpcc_new_order_get_district_tax_sproc_name,
        new_order_get_district_tax_arg_codes );
    function_skeleton new_order_get_district_tax_function =
        sproc_table->lookup_function( new_order_get_district_tax_func_id );
    res = new_order_get_district_tax_function(
        txn_holder, cli_id, new_order_get_district_tax_arg_codes,
        new_order_get_district_tax_args, opaque_ptr );
    EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );

    tax_discount_results.clear();
    tax_discount_arg_codes.clear();
    deserialize_result( res, tax_discount_results, tax_discount_arg_codes );
    EXPECT_EQ( 1, tax_discount_arg_codes.size() );
    EXPECT_EQ( FLOAT_CODE.code, tax_discount_arg_codes.at( 0 ).code );
    float d_tax = *( (float*) tax_discount_results.at( 0 ) );
    EXPECT_GE( d_tax, 0 );
    free_args( tax_discount_results );

    std::vector<arg_code> new_order_get_customer_discount_arg_codes = {
        INTEGER_CODE /*w_id*/, INTEGER_CODE /*d_id*/, INTEGER_CODE /*c_id*/};
    std::vector<void*> new_order_get_customer_discount_args = {
        (void*) &w_id, (void*) &d_id, (void*) &c_id};

    function_identifier new_order_get_customer_discount_func_id(
        k_tpcc_new_order_get_customer_discount_sproc_name,
        new_order_get_customer_discount_arg_codes );
    function_skeleton new_order_get_customer_discount_function =
        sproc_table->lookup_function( new_order_get_customer_discount_func_id );
    res = new_order_get_customer_discount_function(
        txn_holder, cli_id, new_order_get_customer_discount_arg_codes,
        new_order_get_customer_discount_args, opaque_ptr );
    EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );

    tax_discount_results.clear();
    tax_discount_arg_codes.clear();
    deserialize_result( res, tax_discount_results, tax_discount_arg_codes );
    EXPECT_EQ( 1, tax_discount_arg_codes.size() );
    EXPECT_EQ( FLOAT_CODE.code, tax_discount_arg_codes.at( 0 ).code );
    float customer_discount = *( (float*) tax_discount_results.at( 0 ) );
    EXPECT_GE( customer_discount, 0 );
    free_args( tax_discount_results );

    std::vector<arg_code> new_order_update_stocks_arg_codes = {
        INTEGER_CODE /*order_w_id*/, INTEGER_CODE /*order_d_id*/,
        INTEGER_ARRAY_CODE /*item_ids*/, INTEGER_ARRAY_CODE /*supplier_w_ids*/,
        INTEGER_ARRAY_CODE /*order_quantities*/
    };
    new_order_update_stocks_arg_codes.at( 2 ).array_length = o_ol_cnt;
    new_order_update_stocks_arg_codes.at( 3 ).array_length = o_ol_cnt;
    new_order_update_stocks_arg_codes.at( 4 ).array_length = o_ol_cnt;

    std::vector<void*> new_order_update_stocks_args = {
        (void*) &w_id, (void*) &d_id, (void*) item_ids, (void*) supplier_w_ids,
        (void*) order_quantities};

    function_identifier new_order_update_stocks_func_id(
        k_tpcc_new_order_update_stocks_sproc_name,
        new_order_update_stocks_arg_codes );
    function_skeleton new_order_update_stocks_function =
        sproc_table->lookup_function( new_order_update_stocks_func_id );
    res = new_order_update_stocks_function(
        txn_holder, cli_id, new_order_update_stocks_arg_codes,
        new_order_update_stocks_args, opaque_ptr );
    EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );

    std::vector<void*>    update_stocks_results;
    std::vector<arg_code> update_stocks_arg_codes;

    deserialize_result( res, update_stocks_results, update_stocks_arg_codes );
    EXPECT_EQ( 4, update_stocks_arg_codes.size() );
    for( int32_t pos = 0; pos < 3; pos++ ) {
        EXPECT_EQ( INTEGER_ARRAY_CODE.code,
                   update_stocks_arg_codes.at( pos ).code );
        EXPECT_EQ( INTEGER_ARRAY_CODE.array_flag,
                   update_stocks_arg_codes.at( pos ).array_flag );
        EXPECT_EQ( o_ol_cnt, update_stocks_arg_codes.at( pos ).array_length );
    }

    EXPECT_EQ( STRING_CODE.code, update_stocks_arg_codes.at( 3 ).code );
    EXPECT_EQ( STRING_CODE.array_flag,
               update_stocks_arg_codes.at( 3 ).array_flag );
    EXPECT_GT( update_stocks_arg_codes.at( 3 ).array_length, 0 );

    int32_t* stock_supplier_w_ids = (int32_t*) update_stocks_results.at( 0 );
    int32_t* stock_item_ids = (int32_t*) update_stocks_results.at( 1 );

    for( int32_t pos = 0; pos < o_ol_cnt; pos++ ) {
        EXPECT_EQ( supplier_w_ids[pos], stock_supplier_w_ids[pos] );
        EXPECT_EQ( item_ids[pos], stock_item_ids[pos] );
    }

    int32_t* stock_descr_lens = (int32_t*) update_stocks_results.at( 2 );
    char*    stock_descrs = (char*) update_stocks_results.at( 3 );

    int32_t num_first_orders = 0;

    int32_t* later_order_line_ids = order_line_ids + num_first_orders;
    int32_t* later_item_ids = item_ids + num_first_orders;
    int32_t* later_supplier_w_ids = supplier_w_ids + num_first_orders;
    int32_t* later_order_quantities = order_quantities + num_first_orders;
    int32_t* later_stock_descr_lens = stock_descr_lens + num_first_orders;
    char*    later_stock_descrs = stock_descrs;
    int32_t  overall_stock_descrs_len =
        update_stocks_arg_codes.at( 1 ).array_length;
    for( int32_t pos = 0; pos < num_first_orders; pos++ ) {
        later_stock_descrs = later_stock_descrs + stock_descr_lens[pos];
        overall_stock_descrs_len =
            overall_stock_descrs_len - stock_descr_lens[pos];
    }

    std::vector<arg_code> new_order_order_items_no_reads_arg_codes = {
        INTEGER_CODE /*order_w_id*/,
        INTEGER_CODE /*d_id*/,
        INTEGER_CODE /*o_id*/,
        FLOAT_CODE /*w_tax*/,
        FLOAT_CODE /*d_tax*/,
        FLOAT_CODE /*c_discount*/,
        INTEGER_ARRAY_CODE /*order_lines*/,
        INTEGER_ARRAY_CODE /*item_ids*/,
        INTEGER_ARRAY_CODE /*supplier_w_ids*/,
        INTEGER_ARRAY_CODE /*order_quantities*/,
        INTEGER_ARRAY_CODE /*stock_descr_lens*/,
        STRING_CODE /*stock_descrs*/,
    };
    int32_t num_second_orders = o_ol_cnt - num_first_orders;
    new_order_order_items_no_reads_arg_codes.at( 6 ).array_length =
        num_second_orders;
    new_order_order_items_no_reads_arg_codes.at( 7 ).array_length =
        num_second_orders;
    new_order_order_items_no_reads_arg_codes.at( 8 ).array_length =
        num_second_orders;
    new_order_order_items_no_reads_arg_codes.at( 9 ).array_length =
        num_second_orders;
    new_order_order_items_no_reads_arg_codes.at( 10 ).array_length =
        num_second_orders;
    new_order_order_items_no_reads_arg_codes.at( 11 ).array_length =
        overall_stock_descrs_len;

    std::vector<void*> new_order_order_items_no_reads_args = {
        (void*) &w_id,
        (void*) &d_id,
        (void*) &o_id,
        (void*) &w_tax,
        (void*) &d_tax,
        (void*) &customer_discount,
        (void*) later_order_line_ids,
        (void*) later_item_ids,
        (void*) later_supplier_w_ids,
        (void*) later_order_quantities,
        (void*) later_stock_descr_lens,
        (void*) later_stock_descrs};

    function_identifier new_order_order_items_no_reads_func_id(
        k_tpcc_new_order_order_items_no_reads_sproc_name,
        new_order_order_items_no_reads_arg_codes );
    function_skeleton new_order_order_items_no_reads_function =
        sproc_table->lookup_function( new_order_order_items_no_reads_func_id );
    res = new_order_order_items_no_reads_function(
        txn_holder, cli_id, new_order_order_items_no_reads_arg_codes,
        new_order_order_items_no_reads_args, opaque_ptr );
    EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );

    std::vector<arg_code> set_order_arg_codes = {
        INTEGER_CODE /*w_id*/,     INTEGER_CODE /*d_id*/,
        INTEGER_CODE /*c_id*/,     INTEGER_CODE /*o_id*/,
        INTEGER_CODE /*o_ol_cnt*/, INTEGER_CODE /*o_all_local*/,
    };
    std::vector<void*> set_order_args = {(void*) &w_id,     (void*) &d_id,
                                         (void*) &c_id,     (void*) &o_id,
                                         (void*) &o_ol_cnt, (void*) &all_local};
    function_identifier new_order_set_order_func_id(
        k_tpcc_new_order_set_order_sproc_name, set_order_arg_codes );
    function_skeleton new_order_set_order_function =
        sproc_table->lookup_function( new_order_set_order_func_id );
    res = new_order_set_order_function( txn_holder, cli_id, set_order_arg_codes,
                                        set_order_args, opaque_ptr );
    EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );

    std::vector<arg_code> set_new_order_arg_codes = {
        INTEGER_CODE /*w_id*/, INTEGER_CODE /*d_id*/, INTEGER_CODE /* o_id*/
    };
    std::vector<void*> set_new_order_args = {(void*) &w_id, (void*) &d_id,
                                             (void*) &o_id};

    function_identifier new_order_set_new_order_func_id(
        k_tpcc_new_order_set_new_order_sproc_name, set_new_order_arg_codes );
    function_skeleton new_order_set_new_order_function =
        sproc_table->lookup_function( new_order_set_new_order_func_id );
    res = new_order_set_new_order_function( txn_holder, cli_id,
                                            set_new_order_arg_codes,
                                            set_new_order_args, opaque_ptr );
    EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );

    c1_state = txn_holder->commit_transaction();

    delete txn_holder;
    txn_holder = nullptr;
    write_pids.clear();
    read_pids.clear();


    free_args( update_stocks_results );

    delete[] item_ids;
    delete[] supplier_w_ids;
    delete[] order_quantities;
    delete[] order_line_ids;


    // STOCK LEVEL

    int32_t stock_level_w_id = 0;
    int32_t stock_level_d_id = 1;
    int32_t stock_level_threshold  = 20;

    read_pids.insert( generate_partition_identifier(
        k_tpcc_district_table_id, make_w_d_key( d_id, w_id, tpcc_cfg ),
        data_sizes ) );

    txn_holder = database.get_partitions_with_begin(
        cli_id, c1_state, write_pids, read_pids, inflight_pids, lookup_op );

    std::vector<arg_code>
        stock_level_get_max_order_id_to_look_back_on_arg_codes = {INTEGER_CODE,
                                                                  INTEGER_CODE};
    std::vector<void*> stock_level_get_max_order_id_to_look_back_on_args = {
        (void*) &stock_level_w_id, (void*) &stock_level_d_id};

    function_identifier stock_level_get_max_order_id_to_look_back_on_func_id(
        k_tpcc_stock_level_get_max_order_id_to_look_back_on_sproc_name,
        stock_level_get_max_order_id_to_look_back_on_arg_codes );
    function_skeleton stock_level_get_max_order_id_to_look_back_on_function =
        sproc_table->lookup_function(
            stock_level_get_max_order_id_to_look_back_on_func_id );
    res = stock_level_get_max_order_id_to_look_back_on_function(
        txn_holder, cli_id,
        stock_level_get_max_order_id_to_look_back_on_arg_codes,
        stock_level_get_max_order_id_to_look_back_on_args, opaque_ptr );
    EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );
    std::vector<void*>    stock_level_get_order_id_results;
    std::vector<arg_code> stock_level_get_order_id_ret_arg_codes;
    deserialize_result( res, stock_level_get_order_id_results,
                        stock_level_get_order_id_ret_arg_codes );
    EXPECT_EQ( 1, stock_level_get_order_id_results.size() );
    EXPECT_EQ( 1, stock_level_get_order_id_ret_arg_codes.size() );
    EXPECT_EQ( INTEGER_CODE.as_bytes,
               stock_level_get_order_id_ret_arg_codes.at( 0 ).as_bytes );
    int32_t stock_level_o_id =
        *( (int32_t*) stock_level_get_order_id_results.at( 0 ) );
    free_args( stock_level_get_order_id_results );
    EXPECT_GE( stock_level_o_id, 0 );

    c1_state = txn_holder->commit_transaction();

    delete txn_holder;
    txn_holder = nullptr;
    write_pids.clear();
    read_pids.clear();

    int32_t base_o_id = std::max( 0, stock_level_o_id - 5 );
    std::vector<int32_t> stock_level_o_ids;
    std::vector<int32_t> stock_level_ol_nums;
    int32_t max_ol_num = (int32_t) tpcc_cfg.max_num_order_lines_per_order_;
    DVLOG( 5 ) << "Looking at order ids in range:" << base_o_id << ", "
               << stock_level_o_id << ", max ol num:" << max_ol_num;

    for( int32_t o_id_to_lookup = base_o_id; o_id_to_lookup < stock_level_o_id;
         o_id_to_lookup++ ) {
        for( int32_t ol_num = 1; ol_num <= max_ol_num; ol_num++ ) {
            read_pids.insert( generate_partition_identifier(
                k_tpcc_order_line_table_id,
                make_w_d_o_ol_key( ol_num, o_id_to_lookup, d_id, w_id,
                                   tpcc_cfg ),
                data_sizes ) );
        }
    }

    std::vector<void*> stock_level_get_recent_items_args = {
        (void*) &stock_level_w_id, (void*) &stock_level_d_id,
        (void*) &base_o_id, (void*) &stock_level_o_id, (void*) &max_ol_num};
    std::vector<arg_code> stock_level_get_recent_items_arg_codes = {
        INTEGER_CODE, INTEGER_CODE, INTEGER_CODE, INTEGER_CODE, INTEGER_CODE};
    function_identifier stock_level_get_recent_items_func_id(
        k_tpcc_stock_level_get_recent_items_sproc_name,
        stock_level_get_recent_items_arg_codes );
    function_skeleton stock_level_get_recent_items_function =
        sproc_table->lookup_function( stock_level_get_recent_items_func_id );

    txn_holder = database.get_partitions_with_begin(
        cli_id, c1_state, write_pids, read_pids, inflight_pids, lookup_op );

    res = stock_level_get_recent_items_function(
        txn_holder, cli_id, stock_level_get_recent_items_arg_codes,
        stock_level_get_recent_items_args, opaque_ptr );
    EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );
    std::vector<void*>    stock_level_get_recent_items_results;
    std::vector<arg_code> stock_level_get_recent_items_ret_arg_codes;
    deserialize_result( res, stock_level_get_recent_items_results,
                        stock_level_get_recent_items_ret_arg_codes );
    EXPECT_EQ( 1, stock_level_get_recent_items_ret_arg_codes.size() );
    EXPECT_EQ( 1, stock_level_get_recent_items_results.size() );
    EXPECT_EQ( INTEGER_ARRAY_CODE.code,
               stock_level_get_recent_items_ret_arg_codes.at( 0 ).code );
    EXPECT_EQ( INTEGER_ARRAY_CODE.array_flag,
               stock_level_get_recent_items_ret_arg_codes.at( 0 ).array_flag );
    int32_t num_stock_level_item_ids =
        stock_level_get_recent_items_ret_arg_codes.at( 0 ).array_length;
    EXPECT_GT( num_stock_level_item_ids, 0 );
    // don't free until later
    //
    //

    int32_t* stock_level_item_ids =
        (int32_t*) stock_level_get_recent_items_results.at( 0 );

    c1_state = txn_holder->commit_transaction();

    delete txn_holder;
    txn_holder = nullptr;
    write_pids.clear();
    read_pids.clear();

    std::vector<arg_code> stock_level_get_stock_below_threshold_arg_codes =
        k_tpcc_stock_level_get_stock_below_threshold_arg_codes;
    std::vector<void*> stock_level_get_stock_below_threshold_args = {
};
    function_identifier stock_level_get_stock_below_func_id(
        k_tpcc_stock_level_get_stock_below_threshold_sproc_name,
        stock_level_get_stock_below_threshold_arg_codes );
    function_skeleton stock_level_get_stock_below_threshold_function =
        sproc_table->lookup_function( stock_level_get_stock_below_func_id );

    for( int32_t pos = 0; pos < num_stock_level_item_ids; pos++ ) {
        read_pids.insert( generate_partition_identifier(
            k_tpcc_stock_table_id,
            make_w_s_key( w_id, stock_level_item_ids[pos], tpcc_cfg ),
            data_sizes ) );
    }

    txn_holder = database.get_partitions_with_begin(
        cli_id, c1_state, write_pids, read_pids, inflight_pids, lookup_op );

    res = stock_level_get_stock_below_threshold_function(
        txn_holder, cli_id, stock_level_get_stock_below_threshold_arg_codes,
        stock_level_get_stock_below_threshold_args, opaque_ptr );
    EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );
    std::vector<void*>    stock_level_get_stock_below_threshold_results;
    std::vector<arg_code> stock_level_get_stock_below_threshold_ret_arg_codes;
    deserialize_result( res, stock_level_get_stock_below_threshold_results,
                        stock_level_get_stock_below_threshold_ret_arg_codes );
    EXPECT_EQ( 1, stock_level_get_stock_below_threshold_ret_arg_codes.size() );
    EXPECT_EQ( 1, stock_level_get_stock_below_threshold_results.size() );
    EXPECT_EQ(
        INTEGER_ARRAY_CODE.code,
        stock_level_get_stock_below_threshold_ret_arg_codes.at( 0 ).code );
    EXPECT_EQ( INTEGER_ARRAY_CODE.array_flag,
               stock_level_get_stock_below_threshold_ret_arg_codes.at( 0 )
                   .array_flag );
    int32_t num_stock_below_threshold =
        stock_level_get_stock_below_threshold_ret_arg_codes.at( 0 )
            .array_length;
    EXPECT_GE( num_stock_below_threshold, 0 );

    free_args( stock_level_get_recent_items_results );
    free_args( stock_level_get_stock_below_threshold_results );

    c1_state = txn_holder->commit_transaction();

    delete txn_holder;
    txn_holder = nullptr;
    write_pids.clear();
    read_pids.clear();

    // payment
    w_id = 0;
    d_id = 1;
    int32_t c_w_id = 0;
    int32_t c_d_id = 0;
    c_id = 17;
    float   h_amount = 53;

#endif
#if 0
    std::vector<arg_code> warehouse_payment_arg_codes =
        k_tpcc_add_payment_to_warehouse_arg_codes;
    std::vector<void*> warehouse_payment_args = {(void*) &w_id,
                                                 (void*) &h_amount};
    function_identifier warehouse_payment_func_id(
        k_tpcc_add_payment_to_warehouse_sproc_name,
        warehouse_payment_arg_codes );
    function_skeleton
        warehouse_payment_function =
            sproc_table->lookup_function( warehouse_payment_func_id );
#endif
#if 0  // MTODO-TPCC

    std::vector<arg_code> district_payment_arg_codes =
        k_tpcc_add_payment_to_district_arg_codes;
    std::vector<void*> district_payment_args = {(void*) &w_id, (void*) &d_id,
                                                (void*) &h_amount};
    function_identifier district_payment_func_id(
        k_tpcc_add_payment_to_district_sproc_name,
        district_payment_arg_codes );
    function_skeleton district_payment_function =
        sproc_table->lookup_function( district_payment_func_id );

#endif
#if 0
    std::vector<arg_code> history_payment_arg_codes =
        k_tpcc_add_payment_to_history_arg_codes;
    std::vector<void*> history_payment_args = {
        (void*) &w_id,   (void*) &d_id, (void*) &c_w_id,
        (void*) &c_d_id, (void*) &c_id, (void*) &h_amount};
    function_identifier history_payment_func_id(
        k_tpcc_add_payment_to_history_sproc_name,
        history_payment_arg_codes );
    function_skeleton
        history_payment_function =
            sproc_table->lookup_function( history_payment_func_id );
#endif
#if 0  // MTODO-TPCC

    std::vector<arg_code> customer_payment_arg_codes =
        k_tpcc_add_payment_to_customer_arg_codes;
    std::vector<void*> customer_payment_args = {
        (void*) &w_id,   (void*) &d_id, (void*) &c_w_id,
        (void*) &c_d_id, (void*) &c_id, (void*) &h_amount};
    function_identifier customer_payment_func_id(
        k_tpcc_add_payment_to_customer_sproc_name,
        customer_payment_arg_codes );
    function_skeleton customer_payment_function =
        sproc_table->lookup_function( customer_payment_func_id );

    std::vector<record_identifier> payment_rids =
        get_payment_rids( w_id, d_id, c_w_id, c_d_id, c_id, tpcc_cfg );

    for( record_identifier rid : payment_rids ) {
        write_pids.insert( generate_partition_identifier(
            rid.table_id_, rid.key_, data_sizes ) );
    }

    txn_holder = database.get_partitions_with_begin(
        cli_id, c1_state, write_pids, read_pids, inflight_pids, lookup_op );

#endif
#if 0
    res = warehouse_payment_function( txn_holder, cli_id,
                                      warehouse_payment_arg_codes,
                                      warehouse_payment_args, opaque_ptr );
    EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );
#endif
#if 0  // MTODO-TPCC
    res = district_payment_function( txn_holder, cli_id,
                                     district_payment_arg_codes,
                                     district_payment_args, opaque_ptr );
    EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );
#endif
#if 0
    res =
        history_payment_function( txn_holder, cli_id, history_payment_arg_codes,
                                  history_payment_args, opaque_ptr );
    EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );
#endif
#if 0  // MTODO-TPCC
    res = customer_payment_function( txn_holder, cli_id,
                                     customer_payment_arg_codes,
                                     customer_payment_args, opaque_ptr );
    EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );

    c1_state = txn_holder->commit_transaction();

    delete txn_holder;
    txn_holder = nullptr;
    write_pids.clear();
    read_pids.clear();



    (void) c1_state;
    (void) cli_id;
    (void) worker;
}

#endif
TEST_F( tpcc_sproc_test, tpcc_key_gen_test ) {
    benchmark_configs b_cfg = construct_benchmark_configs(
        1 /*num clients*/, 0 /*gc_sleep_time*/, 1 /*  bench time*/,
        1000 /* num operations before checking*/ );

    tpcc_configs tpcc_cfg = construct_tpcc_configs(
        2 /*num warehouses*/, 1000 /*num items*/, 1000 /* num suppliers */,
        10 /* expected num orders per cust*/, 1000 /*item partition size*/,
        2 /*partition size*/, 2 /*district partiion size */,
        2 /* customer partition size*/, 100 /* suppliers partition size */,
        1 /*new order distributed likelihood*/,
        15 /*payment distributed likelihood*/,
        false /*track_and_use_recent_items*/, 1 /* tpcc num clients */,
        1 /* tpch num clients */, b_cfg, 4 /*delivery prob*/, 45 /* new order*/,
        4 /*order status*/, 43 /* payment*/, 4 /*stock level*/, 0 /* q1 prob */,
        0 /* q2 prob */, 0 /* q3 prob */, 0 /* q4 prob */, 0 /* q5 prob */,
        0 /* q6 prob */, 0 /* q7 prob */, 0 /* q8 prob */, 0 /* q9 prob */,
        0 /* q10 prob */, 0 /* q11 prob */, 0 /* q12 prob */, 0 /* q13 prob */,
        0 /* q14 prob */, 0 /* q15 prob */, 0 /* q16 prob */, 0 /* q17 prob */,
        0 /* q18 prob */, 0 /* q19 prob */, 0 /* q20 prob */, 0 /* q21 prob */,
        0 /* q22 prob */, 100 /* all prob */, 2 /*  dist per whouse */,
        30 /*cust per warehouse*/, 6 /*order lines*/,
        9 /*initial num customers per district*/,
        false /*limit number of records propagated*/,
        1 /*number of updates needed for propagtion*/, true /*use warehouse*/,
        true /* use district*/, false /* h scans full tables */ );

    tpcc_loader_templ_no_commit loader(
        nullptr /*db abstraction*/, tpcc_cfg,
        construct_db_abstraction_configs( db_abstraction_type::PLAIN_DB ),
        0 /*client id*/ );

    workload_operation_selector           op_selector;
    tpcc_benchmark_worker_templ_no_commit worker(
        0 /*client*/, nullptr /*db abstraction*/, nullptr /*zipf*/, op_selector,
        tpcc_cfg,
        construct_db_abstraction_configs( db_abstraction_type::PLAIN_DB ) );

    for( int32_t w_id = 0; w_id < (int32_t) tpcc_cfg.num_warehouses_; w_id++ ) {
        VLOG( 10 ) << "W_ID:" << w_id;

        VLOG( 10 ) << "Warehouse PK:" << w_id;

        for( int32_t d_id = 0;
             d_id < (int32_t) tpcc_cfg.num_districts_per_warehouse_; d_id++ ) {
            VLOG( 10 ) << "D_ID:" << d_id << " (W_ID:" << w_id << ")";
            district d;
            d.d_w_id = w_id;
            d.d_id = d_id;

            VLOG( 10 ) << "District PK:" << make_district_key( d, tpcc_cfg );

            EXPECT_EQ( w_id, get_warehouse_from_district_key(
                                 make_district_key( d, tpcc_cfg ), tpcc_cfg ) );

            std::vector<int32_t> cids = {
                0, (int32_t) tpcc_cfg.num_customers_per_district_ / 2,
                (int32_t) tpcc_cfg.num_customers_per_district_ - 1};
            for( int32_t c_id : cids ) {
                VLOG( 10 ) << "C_ID:" << c_id << " (D_ID:" << d_id
                           << ", W_ID:" << w_id << ")";

                customer c;
                c.c_d_id = (int32_t) d_id;
                c.c_w_id = (int32_t) w_id;
                c.c_id = c_id;

                VLOG( 10 ) << "Customer PK:"
                           << make_customer_key( c, tpcc_cfg );

                EXPECT_EQ( w_id,
                           get_warehouse_from_customer_key(
                               make_customer_key( c, tpcc_cfg ), tpcc_cfg ) );

                history h;
                h.h_c_id = c_id;
                h.h_c_d_id = d_id;
                h.h_c_w_id = w_id;

                EXPECT_EQ( make_customer_key( c, tpcc_cfg ),
                           make_history_key( h, tpcc_cfg ) );

                EXPECT_EQ( w_id,
                           get_warehouse_from_history_key(
                               make_history_key( h, tpcc_cfg ), tpcc_cfg ) );

                customer_district cd;
                cd.c_id = c_id;
                cd.c_d_id = d_id;
                cd.c_w_id = w_id;

                EXPECT_EQ( make_customer_key( c, tpcc_cfg ),
                           make_customer_district_key( cd, tpcc_cfg ) );

                EXPECT_EQ( w_id, get_warehouse_from_customer_district_key(
                                     make_customer_district_key( cd, tpcc_cfg ),
                                     tpcc_cfg ) );

                int32_t c_o_id_start =
                    loader.generate_customer_order_id_start( w_id, d_id, c_id );
                int32_t c_o_id_end =
                    loader.generate_customer_order_id_end( w_id, d_id, c_id );
                int32_t c_o_id_mid =
                    c_o_id_start + ( ( c_o_id_end - c_o_id_start ) / 2 );

                VLOG( 10 ) << "C_O_ID_START:" << c_o_id_start
                           << ", C_O_ID_END:" << c_o_id_end;
                std::vector<int32_t> o_ids = {c_o_id_start, c_o_id_mid,
                                              c_o_id_end};
                for( int32_t o_id : o_ids ) {
                    VLOG( 10 ) << "O_ID:" << o_id << " (C_ID:" << c_id
                               << ", D_ID:" << d_id << ", W_ID:" << w_id << ")";

                    order     o;
                    new_order no;

                    worker.populate_order( o, no, o_id, c_id, d_id, w_id,
                                           0 /*o_ol_cnt */, 0 /*o_all_local*/ );

                    VLOG( 10 ) << "Order PK:" << make_order_key( o, tpcc_cfg );
                    VLOG( 10 ) << "NewOrder PK:"
                               << make_new_order_key( no, tpcc_cfg );

                    EXPECT_EQ( w_id,
                               get_warehouse_from_order_key(
                                   make_order_key( o, tpcc_cfg ), tpcc_cfg ) );

                    EXPECT_EQ( w_id, get_warehouse_from_new_order_key(
                                         make_new_order_key( no, tpcc_cfg ),
                                         tpcc_cfg ) );

                    std::vector<int32_t> ol_ids = {
                        0,
                        (int32_t) tpcc_cfg.max_num_order_lines_per_order_ / 2,
                        (int32_t) tpcc_cfg.max_num_order_lines_per_order_ - 1};

                    for( int32_t ol_id : ol_ids ) {
                        VLOG( 10 ) << "OL_NUM:" << ol_id << " (O_ID:" << o_id
                                   << ", C_ID:" << c_id << ", D_ID:" << d_id
                                   << ", W_ID:" << w_id << ")";

                        order_line ol;
                        ol.ol_o_id = o_id;
                        ol.ol_d_id = d_id;
                        ol.ol_w_id = w_id;
                        ol.ol_number = ol_id;

                        VLOG( 10 ) << "OrderLine PK:"
                                   << make_order_line_key( ol, tpcc_cfg );
                        EXPECT_EQ( w_id,
                                   get_warehouse_from_order_line_key(
                                       make_order_line_key( ol, tpcc_cfg ),
                                       tpcc_cfg ) );
                    }  // ol_num
                }      // o_id
            }          // c_id
        }              // d_id

        std::vector<int32_t> item_ids = {0, (int32_t) tpcc_cfg.num_items_ / 2,
                                         (int32_t) tpcc_cfg.num_items_ - 1};

        for( int32_t item_id : item_ids ) {
            VLOG( 10 ) << "W_ID:" << w_id << ", ITEM_ID:" << item_id;

            stock s;
            s.s_i_id = item_id;
            s.s_w_id = w_id;

            VLOG( 10 ) << "Stock PK:" << make_stock_key( s, tpcc_cfg );

            EXPECT_EQ( w_id, get_warehouse_from_stock_key(
                                 make_stock_key( s, tpcc_cfg ), tpcc_cfg ) );
        }  // i_id
    }

    snapshot_vector                       c1_state;
    tpch_benchmark_worker_templ_no_commit h_worker(
        0 /*client*/, nullptr /*db abstraction*/, nullptr /*zipf*/, op_selector,
        tpcc_cfg,
        construct_db_abstraction_configs( db_abstraction_type::PLAIN_DB ),
        c1_state );

    order_line low_ol;
    low_ol.ol_w_id = 0;
    low_ol.ol_d_id = 1;
    low_ol.ol_o_id = 0;
    low_ol.ol_number = order::MIN_OL_CNT;

    order_line high_ol;
    high_ol.ol_w_id = 1;
    high_ol.ol_d_id = tpcc_cfg.num_districts_per_warehouse_ - 1
        /* std::max( d1, d2 ) */;
    high_ol.ol_number = tpcc_cfg.max_num_order_lines_per_order_ - 1;
    high_ol.ol_o_id = 0;

    auto scan_args = h_worker.generate_q1_scan_args( low_ol, high_ol, 0 );
    EXPECT_EQ( 1, scan_args.size() );
    // (0,1) (1,0) (1, 1)
    EXPECT_EQ( scan_args.at(0).read_ckrs.size(), 3 );
    DVLOG( 40 ) << "Generated ckrs:" << scan_args.at( 0 ).read_ckrs;
}
