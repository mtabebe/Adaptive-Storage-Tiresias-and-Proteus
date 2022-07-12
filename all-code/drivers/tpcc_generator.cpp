#include <fstream>
#include <gflags/gflags.h>
#include <glog/logging.h>

#if 0 // MTODO-HTAP
#include "../src/benchmark/benchmark_executor.h"
#include "../src/benchmark/tpcc/tpcc_benchmark.h"
#include "../src/data-site/db/db.h"
#include "../src/data-site/stored-procedures/tpcc/tpcc_sproc_helper_holder.h"
#include "../src/persistence/persistence_editing_executor.h"
#endif

#include "../src/common/constants.h"
#include "../src/common/perf_tracking.h"
#include "../src/site-selector/site_selector_handler.h"

DEFINE_string( output_ss_dir, "~/persistent_ss_out_dir/",
               "The directory of persistent SS data to write" );
DEFINE_string( output_sm_dir, "~/persistent_sm_out_dir/",
               "The directory of persistent SM data to write" );
DEFINE_string( output_part_dir, "~/persistent_part_out_dir/",
               "The directory of persistent partition data to write" );
DEFINE_string( replica_locs_file, "~/.replica_locs_file.cfg",
               "The set of sites that should be replicated" );

DEFINE_bool( do_only_ss, false, "Should only load ss" );
DEFINE_bool( do_only_single_data_site, false,
             "Should only load a single data site" );
DEFINE_int32( data_site_id, 0, "Site id of the data site to load" );

#if 0 // MTODO-HTAP
void create_table_structures(
    multi_version_partition_data_location_table*  data_location_table,
    std::vector<db*>&                             site_dbs,
    std::shared_ptr<update_destination_generator> update_gen,
    std::shared_ptr<update_enqueuers> enqueuers, int num_sites,
    const tpcc_configs&                             tpcc_cfg,
    const std::vector<table_partition_information>& data_sizes,
    const std::vector<table_metadata>&              table_infos ) {
    for( int site = 0; site < num_sites; site++ ) {
        db* site_db = new db();
        site_db->init( site, 10, tpcc_cfg.bench_configs_.num_clients_,
                       k_gc_sleep_time, update_gen, enqueuers );
        tpcc_create_tables( site_db, tpcc_cfg );
        site_dbs.push_back( site_db );
    }
    for( auto table_info : table_infos ) {
        data_location_table->create_table( table_info );
    }
}
#endif

std::unordered_map<int, std::unordered_set<int>> build_replica_locs(
    const std::string& file_name, int num_sites ) {
    std::ifstream infile( file_name );

    std::unordered_map<int, std::unordered_set<int>> replica_locs;

    // line format:
    // replicas,

    std::string line_str;
    std::string site_replica_str;
    int site_id = 0;

    for( ;; ) {
        bool ok = (bool) std::getline( infile, line_str, '\n' );
        if( !ok ) {
            break;
        }

        DVLOG( 40 ) << "Line line_str:" << line_str;
        DCHECK( ok );

        std::unordered_set<int> replicas;

        std::stringstream linestream( line_str );

        for( ;; ) {
            bool ok = (bool) std::getline( linestream, site_replica_str, ',' );
            if ( !ok) {
                break;
            }
            DVLOG( 40 ) << "Replica str:" << site_replica_str;
            int replica = std::stoi( site_replica_str );
            DCHECK_LT( replica, num_sites );
            replicas.insert( replica );
        }

        DCHECK_LT( site_id, num_sites );
        replica_locs.emplace( site_id, replicas );

        site_id = site_id + 1;

    }

    return replica_locs;
}

#if 0 // MTODO-HTAP
std::vector<std::vector<propagation_configuration>> build_prop_configs(
    int num_sites, int num_dests ) {
    int partition = 0;

    std::vector<std::vector<propagation_configuration>> prop_configs;
    for( int site_id = 0; site_id < num_sites; site_id++ ) {
        std::vector<propagation_configuration> site_props;
        for( int dest = 0; dest < num_dests; dest++ ) {

            propagation_configuration config;
            config.type = propagation_type::VECTOR;
            config.partition = partition;

            site_props.push_back( config );

            partition = partition + 1;
        }
        prop_configs.push_back( site_props );
    }

    return prop_configs;
}

std::vector<tpcc_sproc_helper_holder*> generate_holders(
    const tpcc_configs& tpcc_cfg, std::vector<db*>& site_dbs ) {
    std::vector<tpcc_sproc_helper_holder*> holders;

    db_abstraction_configs abstraction_configs =
        construct_db_abstraction_configs( db_abstraction_type::PLAIN_DB );

    for( int site = 0; site < (int) site_dbs.size(); site++ ) {
        tpcc_sproc_helper_holder* holder =
            new tpcc_sproc_helper_holder( tpcc_cfg, abstraction_configs );
        holder->init( site_dbs.at( site ) );
        holders.push_back( holder );
    }

    return holders;
}

partition_identifier_set insert_pks(
    multi_version_partition_data_location_table* data_location_table,
    std::vector<primary_key>& pks, int master, int slot, int num_sites ) {

    std::sort( pks.begin(), pks.end() );
    partition_lock_mode lock_mode = partition_lock_mode::lock;

    auto grouped_info = data_location_table->get_or_create_partitions_and_group(
        pks, {} /*read set*/, lock_mode, num_sites );

    DCHECK_EQ( grouped_info.new_write_partitions_.size(),
               grouped_info.new_partitions_.size() );

    partition_identifier_set write_set;

    for( auto payload : grouped_info.new_write_partitions_ ) {
        auto location_information =
            payload->get_location_information( partition_lock_mode::lock );
        DCHECK( !location_information );
        location_information =
            std::make_shared<partition_location_information>();
        location_information->master_location_ = master;
        location_information->version_ = 1;
        location_information->update_destination_slot_ = slot;
        payload->set_location_information( location_information );

        write_set.insert( payload->identifier_ );
    }
    for( auto payload : grouped_info.existing_write_partitions_ ) {
        auto location_information =
            payload->get_location_information( partition_lock_mode::lock );
        DCHECK( location_information );
        DCHECK_EQ( location_information->master_location_, master );
        write_set.insert( payload->identifier_ );
    }

    unlock_payloads( grouped_info.existing_write_partitions_, lock_mode );
    unlock_payloads( grouped_info.new_write_partitions_, lock_mode );

    return write_set;
}

void load_items(
    multi_version_partition_data_location_table* data_location_table,
    std::vector<db*>&                            site_dbs,
    std::vector<tpcc_sproc_helper_holder*>& site_holders, int num_sites,
    const tpcc_configs& tpcc_cfg, int cid, bool load_only_ss,
    bool load_only_data_site, int load_data_site_id ) {

    DVLOG( 1 ) << "Load Items";

    int master = 0;
    int slot = 0;

    if( load_only_data_site ) {
        if( master != load_data_site_id ) {
            return;
        }
    }

    transaction_partition_holder* txn_holder = nullptr;

    uint64_t cur_start = 0;
    uint64_t cur_end = 0;
    uint64_t end = tpcc_cfg.num_items_;
    uint32_t batch_size = tpcc_cfg.item_partition_size_;
    while( cur_start < end ) {
        cur_end = std::min( end - 1, cur_start + batch_size );

        snapshot_vector c1_state;

        auto pks = tpcc_loader_templ_no_commit::generate_write_set_as_pks(
            k_tpcc_item_table_id, cur_start, cur_end );
        partition_identifier_set insert_pids =
            insert_pks( data_location_table, pks, master, slot, num_sites );

        if( !load_only_ss ) {

            txn_holder = site_dbs.at( master )->get_partitions_with_begin(
                cid, c1_state, insert_pids, {/*read_pids*/},
                {/*inflight_pids*/},
                partition_lookup_operation::GET_OR_CREATE );

            tpcc_loader_templ_no_commit* loader =
                site_holders.at( master )->get_loader_and_set_holder(
                    cid, txn_holder );

            loader->load_item_range( cur_start, cur_end );

            c1_state = txn_holder->commit_transaction();
            delete txn_holder;
            txn_holder = nullptr;
        }

        cur_start = cur_end + 1;
    }
}

void make_warehouse(
    multi_version_partition_data_location_table* data_location_table,
    std::vector<db*>&                            site_dbs,
    std::vector<tpcc_sproc_helper_holder*>& site_holders, int master, int slot,
    int num_sites, const tpcc_configs& tpcc_cfg, uint32_t w_id, int cid,
    bool load_only_ss ) {
    DVLOG( 2 ) << "Make warehouse for w_id:" << w_id;

    transaction_partition_holder* txn_holder = nullptr;
    snapshot_vector               c1_state;

    auto pks = tpcc_loader_templ_no_commit::generate_write_set_as_pks(
        k_tpcc_warehouse_table_id, w_id, w_id );
    partition_identifier_set insert_pids =
        insert_pks( data_location_table, pks, master, slot, num_sites );

    if( !load_only_ss ) {

        txn_holder = site_dbs.at( master )->get_partitions_with_begin(
            cid, c1_state, insert_pids, {/*read_pids*/}, {/*inflight_pids*/},
            partition_lookup_operation::GET_OR_CREATE );

        tpcc_loader_templ_no_commit* loader =
            site_holders.at( master )->get_loader_and_set_holder( cid,
                                                                  txn_holder );

        loader->make_warehouse( w_id );

        c1_state = txn_holder->commit_transaction();
        delete txn_holder;
        txn_holder = nullptr;
    }

    DVLOG( 2 ) << "Make warehouse for w_id:" << w_id << ", okay!";
}

void make_districts(
    multi_version_partition_data_location_table* data_location_table,
    std::vector<db*>&                            site_dbs,
    std::vector<tpcc_sproc_helper_holder*>& site_holders, int master, int slot,
    int num_sites, const tpcc_configs& tpcc_cfg, uint32_t w_id, int cid,
    bool load_only_ss ) {
    DVLOG( 2 ) << "Make districts for w_id:" << w_id;

    transaction_partition_holder* txn_holder = nullptr;
    snapshot_vector               c1_state;

    district d_start;
    d_start.d_id = 0;
    d_start.d_w_id = w_id;

    district d_end;
    d_end.d_id = tpcc_cfg.num_districts_per_warehouse_ - 1;
    d_end.d_w_id = w_id;

    auto pks = tpcc_loader_templ_no_commit::generate_write_set_as_pks(
        k_tpcc_district_table_id, make_district_key( d_start, tpcc_cfg ),
        make_district_key( d_end, tpcc_cfg ) );
    partition_identifier_set insert_pids =
        insert_pks( data_location_table, pks, master, slot, num_sites );

    if( !load_only_ss ) {
        txn_holder = site_dbs.at( master )->get_partitions_with_begin(
            cid, c1_state, insert_pids, {/*read_pids*/}, {/*inflight_pids*/},
            partition_lookup_operation::GET_OR_CREATE );

        tpcc_loader_templ_no_commit* loader =
            site_holders.at( master )->get_loader_and_set_holder( cid,
                                                                  txn_holder );

        for( uint32_t d_id = 0; d_id < tpcc_cfg.num_districts_per_warehouse_;
             d_id++ ) {
            loader->make_district( w_id, d_id );
        }

        c1_state = txn_holder->commit_transaction();
        delete txn_holder;
        txn_holder = nullptr;
    }

    DVLOG( 2 ) << "Make districts for w_id:" << w_id << ", okay!";
}

void make_stocks(
    multi_version_partition_data_location_table* data_location_table,
    std::vector<db*>&                            site_dbs,
    std::vector<tpcc_sproc_helper_holder*>& site_holders, int master, int slot,
    int num_sites, const tpcc_configs& tpcc_cfg, uint32_t w_id, int cid,
    bool load_only_ss ) {

    DVLOG( 2 ) << "Make stocks for w_id:" << w_id;

    transaction_partition_holder* txn_holder = nullptr;

    uint64_t cur_start = 0;
    uint64_t cur_end = 0;
    uint64_t end = tpcc_cfg.num_items_;
    uint32_t batch_size = tpcc_cfg.item_partition_size_;
    while( cur_start < end ) {
        snapshot_vector c1_state;

        cur_end = std::min( end - 1, cur_start + batch_size );

        stock s_start;
        s_start.s_i_id = cur_start;
        s_start.s_w_id = w_id;

        stock s_end;
        s_end.s_i_id = cur_end;
        s_end.s_w_id = w_id;

        std::vector<primary_key> pks =
            tpcc_loader_templ_no_commit::generate_write_set_as_pks(
                k_tpcc_stock_table_id, make_stock_key( s_start, tpcc_cfg ),
                make_stock_key( s_end, tpcc_cfg ) );

        partition_identifier_set insert_pids =
            insert_pks( data_location_table, pks, master, slot, num_sites );
        if( !load_only_ss ) {

            txn_holder = site_dbs.at( master )->get_partitions_with_begin(
                cid, c1_state, insert_pids, {/*read_pids*/},
                {/*inflight_pids*/},
                partition_lookup_operation::GET_OR_CREATE );

            tpcc_loader_templ_no_commit* loader =
                site_holders.at( master )->get_loader_and_set_holder(
                    cid, txn_holder );

            loader->load_stock_range( w_id, cur_start, cur_end );

            c1_state = txn_holder->commit_transaction();
            delete txn_holder;
            txn_holder = nullptr;
        }

        cur_start = cur_end + 1;
    }

    DVLOG( 2 ) << "Make stocks for w_id:" << w_id << ", okay!";
}

void make_custs(
    multi_version_partition_data_location_table* data_location_table,
    std::vector<db*>&                            site_dbs,
    std::vector<tpcc_sproc_helper_holder*>& site_holders, int master, int slot,
    int num_sites, const tpcc_configs& tpcc_cfg, uint32_t w_id, uint32_t d_id,
    int cid, bool load_only_ss ) {
    DVLOG( 2 ) << "Make customers for w_id:" << w_id << ", d_id:" << d_id;

    transaction_partition_holder* txn_holder = nullptr;

    uint64_t cur_start = 0;
    uint64_t cur_end = 0;
    uint64_t end = tpcc_cfg.num_customers_per_district_;
    uint32_t batch_size = tpcc_cfg.customer_partition_size_;
    while( cur_start < end ) {
        snapshot_vector c1_state;

        cur_end = std::min( end - 1, cur_start + batch_size );

        DVLOG( 3 ) << "Make customers for w_id:" << w_id << ", d_id:" << d_id
                   << ", start:" << cur_start << ", end:" << cur_end;

        customer c_start;
        c_start.c_id = cur_start;
        c_start.c_w_id = w_id;
        c_start.c_d_id = d_id;

        customer c_end;
        c_end.c_id = cur_end;
        c_end.c_w_id = w_id;
        c_end.c_d_id = d_id;

        customer_district cd_start;
        cd_start.c_id = cur_start;
        cd_start.c_w_id = w_id;
        cd_start.c_d_id = d_id;

        customer_district cd_end;
        cd_end.c_id = cur_end;
        cd_end.c_w_id = w_id;
        cd_end.c_d_id = d_id;

        history h_start;
        h_start.h_c_id = cur_start;
        h_start.h_c_d_id = d_id;
        h_start.h_c_w_id = w_id;

        history h_end;
        h_end.h_c_id = cur_end;
        h_end.h_c_d_id = d_id;
        h_end.h_c_w_id = w_id;

        std::vector<primary_key> cust_pks =
            tpcc_loader_templ_no_commit::generate_write_set_as_pks(
                k_tpcc_customer_table_id,
                make_customer_key( c_start, tpcc_cfg ),
                make_customer_key( c_end, tpcc_cfg ) );

        std::vector<primary_key> hist_pks =
            tpcc_loader_templ_no_commit::generate_write_set_as_pks(
                k_tpcc_history_table_id, make_history_key( h_start, tpcc_cfg ),
                make_history_key( h_end, tpcc_cfg ) );

        std::vector<primary_key> cust_dist_pks =
            tpcc_loader_templ_no_commit::generate_write_set_as_pks(
                k_tpcc_customer_district_table_id,
                make_customer_district_key( cd_start, tpcc_cfg ),
                make_customer_district_key( cd_end, tpcc_cfg ) );

        std::vector<primary_key> pks;
        pks.reserve( cust_pks.size() + hist_pks.size() + cust_dist_pks.size() );
        pks.insert( pks.end(), cust_pks.begin(), cust_pks.end() );
        pks.insert( pks.end(), hist_pks.begin(), hist_pks.end() );
        pks.insert( pks.end(), cust_dist_pks.begin(), cust_dist_pks.end() );

        partition_identifier_set insert_pids =
            insert_pks( data_location_table, pks, master, slot, num_sites );

        if( !load_only_ss ) {

            txn_holder = site_dbs.at( master )->get_partitions_with_begin(
                cid, c1_state, insert_pids, {/*read_pids*/},
                {/*inflight_pids*/},
                partition_lookup_operation::GET_OR_CREATE );

            tpcc_loader_templ_no_commit* loader =
                site_holders.at( master )->get_loader_and_set_holder(
                    cid, txn_holder );

            loader->load_customer_range( w_id, d_id, cur_start, cur_end );
            loader->load_customer_district_range( w_id, d_id, cur_start,
                                                  cur_end );
            loader->load_history_range( w_id, d_id, cur_start, cur_end );

            c1_state = txn_holder->commit_transaction();
            delete txn_holder;
            txn_holder = nullptr;
        }

        DVLOG( 3 ) << "Make customers for w_id:" << w_id << ", d_id:" << d_id
                   << ", start:" << cur_start << ", end:" << cur_end
                   << ", okay!";

        cur_start = cur_end + 1;
    }
    (void) txn_holder;

    DVLOG( 2 ) << "Make customers for w_id:" << w_id << ", d_id:" << d_id
               << ", okay!";
}

void insert_order(
    multi_version_partition_data_location_table* data_location_table,
    std::vector<db*>&                            site_dbs,
    std::vector<tpcc_sproc_helper_holder*>& site_holders, int master, int slot,
    int num_sites, const tpcc_configs& tpcc_cfg, uint32_t w_id, uint32_t d_id,
    uint32_t c_id, uint32_t o_id, uint32_t o_ol_cnt, int cid,
    bool load_only_ss ) {

    DVLOG( 3 ) << "Make order for w_id:" << w_id << ", d_id:" << d_id
               << ", c_id:" << c_id << ", o_id:" << o_id
               << ", o_ol_cnt:" << o_ol_cnt;

    auto gen_loader =
        site_holders.at( master )->get_loader_and_set_holder( cid, nullptr );

    std::vector<primary_key> write_pks;
    write_pks.reserve( o_ol_cnt + 2 );

    order o;
    o.o_id = o_id;
    o.o_c_id = c_id;
    o.o_d_id = d_id;
    o.o_w_id = w_id;
    o.o_ol_cnt = o_ol_cnt;

    primary_key o_pk;
    o_pk.table_id = k_tpcc_order_table_id;
    o_pk.row_id = make_order_key( o, tpcc_cfg );
    write_pks.push_back( o_pk );

    bool is_new_order = gen_loader->is_order_a_new_order( o_id );
    if( is_new_order ) {
        new_order new_o;
        new_o.no_w_id = w_id;
        new_o.no_d_id = d_id;
        new_o.no_o_id = o_id;

        primary_key no_pk;
        no_pk.table_id = k_tpcc_new_order_table_id;
        no_pk.row_id = make_new_order_key( new_o, tpcc_cfg );
        write_pks.push_back( no_pk );
    }

    order_line o_line;
    o_line.ol_o_id = o_id;
    o_line.ol_d_id = d_id;
    o_line.ol_w_id = w_id;

    primary_key ol_pk;
    ol_pk.table_id = k_tpcc_order_line_table_id;

    for( uint32_t ol_c = 1; ol_c <= o_ol_cnt; ol_c++ ) {
        o_line.ol_number = ol_c;  // ol_number
        ol_pk.row_id = make_order_line_key( o_line, tpcc_cfg );

        write_pks.push_back( ol_pk );
    }

    partition_identifier_set insert_pids =
        insert_pks( data_location_table, write_pks, master, slot, num_sites );

    if( !load_only_ss ) {

        snapshot_vector               c1_state;
        transaction_partition_holder* txn_holder =
            site_dbs.at( master )->get_partitions_with_begin(
                cid, c1_state, insert_pids, {/*read_pids*/},
                {/*inflight_pids*/},
                partition_lookup_operation::GET_OR_CREATE );

        tpcc_loader_templ_no_commit* loader =
            site_holders.at( master )->get_loader_and_set_holder( cid,
                                                                  txn_holder );

        loader->load_order( w_id, d_id, c_id, o_id, o_ol_cnt );

        c1_state = txn_holder->commit_transaction();
        delete txn_holder;
    }

    DVLOG( 3 ) << "Make order for w_id:" << w_id << ", d_id:" << d_id
               << ", c_id:" << c_id << ", o_id:" << o_id
               << ", o_ol_cnt:" << o_ol_cnt << ", okay!";
}

void insert_other_orders(
    multi_version_partition_data_location_table* data_location_table,
    std::vector<db*>&                            site_dbs,
    std::vector<tpcc_sproc_helper_holder*>& site_holders, int master, int slot,
    int num_sites, const tpcc_configs& tpcc_cfg, uint32_t w_id, uint32_t d_id,
    uint32_t c_id, int cid, bool load_only_ss ) {

    DVLOG( 3 ) << "Make other orders for w_id:" << w_id << ", d_id:" << d_id
               << ", c_id:" << c_id;

    auto gen_loader =
        site_holders.at( master )->get_loader_and_set_holder( cid, nullptr );

    int o_id_start =
        gen_loader->generate_customer_order_id_start( w_id, d_id, c_id );
    int o_id_end =
        gen_loader->generate_customer_order_id_end( w_id, d_id, c_id );
    int o_ol_cnt = tpcc_cfg.max_num_order_lines_per_order_ /*max*/;

    DVLOG( 4 ) << "Corresponding orders for w_id:" << w_id << ", d_id:" << d_id
               << ", c_id:" << c_id << ", o_id_start:" << o_id_start
               << ", o_id_end:" << o_id_end;

    std::vector<primary_key> write_pks;

    for( int o_id = o_id_start; o_id <= o_id_end; o_id++ ) {
        order o;
        o.o_id = o_id;
        o.o_c_id = c_id;
        o.o_d_id = d_id;
        o.o_w_id = w_id;
        o.o_ol_cnt = o_ol_cnt;

        primary_key o_pk;
        o_pk.table_id = k_tpcc_order_table_id;
        o_pk.row_id = make_order_key( o, tpcc_cfg );
        write_pks.push_back( o_pk );

        new_order new_o;
        new_o.no_w_id = w_id;
        new_o.no_d_id = d_id;
        new_o.no_o_id = o_id;

        primary_key no_pk;
        no_pk.table_id = k_tpcc_new_order_table_id;
        no_pk.row_id = make_new_order_key( new_o, tpcc_cfg );
        write_pks.push_back( no_pk );

        order_line o_line;
        o_line.ol_o_id = o_id;
        o_line.ol_d_id = d_id;
        o_line.ol_w_id = w_id;

        primary_key ol_pk;
        ol_pk.table_id = k_tpcc_order_line_table_id;

        for( int ol_c = 1; ol_c <= o_ol_cnt; ol_c++ ) {
            o_line.ol_number = ol_c;  // ol_number
            ol_pk.row_id = make_order_line_key( o_line, tpcc_cfg );

            write_pks.push_back( ol_pk );
        }
    }

    partition_identifier_set insert_pids =
        insert_pks( data_location_table, write_pks, master, slot, num_sites );

    if( !load_only_ss ) {

        snapshot_vector               c1_state;
        transaction_partition_holder* txn_holder =
            site_dbs.at( master )->get_partitions_with_begin(
                cid, c1_state, insert_pids, {/*read_pids*/},
                {/*inflight_pids*/},
                partition_lookup_operation::GET_OR_CREATE );

        c1_state = txn_holder->commit_transaction();
        delete txn_holder;
    }

    DVLOG( 3 ) << "Make other orders for w_id:" << w_id << ", d_id:" << d_id
               << ", c_id:" << c_id << ", okay!";
}

void make_orders(
    multi_version_partition_data_location_table* data_location_table,
    std::vector<db*>&                            site_dbs,
    std::vector<tpcc_sproc_helper_holder*>& site_holders, int master, int slot,
    int num_sites, const tpcc_configs& tpcc_cfg, uint32_t w_id, uint32_t d_id,
    int cid, bool load_only_ss ) {
    DVLOG( 2 ) << "Make orders for w_id:" << w_id << ", d_id:" << d_id;

    distributions dists( nullptr );

    std::vector<uint32_t> orders( tpcc_cfg.num_customers_per_district_, 0 );
    for( uint32_t o_id = 0; o_id < tpcc_cfg.num_customers_per_district_;
         o_id++ ) {
        orders.at( o_id ) = o_id;
    }
    std::mt19937 dist;
    std::shuffle( orders.begin(), orders.end(), std::move( dist ) );

    for( uint32_t c_id = 0; c_id < tpcc_cfg.num_customers_per_district_;
         c_id++ ) {
        uint32_t o_id = orders.at( c_id );
        uint32_t o_ol_cnt = dists.get_uniform_int(
            order::MIN_OL_CNT, tpcc_cfg.max_num_order_lines_per_order_ );

        insert_order( data_location_table, site_dbs, site_holders, master, slot,
                      num_sites, tpcc_cfg, w_id, d_id, c_id, o_id, o_ol_cnt,
                      cid, load_only_ss );
        insert_other_orders( data_location_table, site_dbs, site_holders,
                             master, slot, num_sites, tpcc_cfg, w_id, d_id,
                             c_id, cid, load_only_ss );
    }
    DVLOG( 2 ) << "Make orders for w_id:" << w_id << ", d_id:" << d_id
               << ", okay!";
}

void load_custs_and_orders(
    multi_version_partition_data_location_table* data_location_table,
    std::vector<db*>&                            site_dbs,
    std::vector<tpcc_sproc_helper_holder*>& site_holders, int master, int slot,
    int num_sites, const tpcc_configs& tpcc_cfg, uint32_t w_id, uint32_t d_id,
    int cid, bool load_only_ss ) {
    make_custs( data_location_table, site_dbs, site_holders, master, slot,
                num_sites, tpcc_cfg, w_id, d_id, cid, load_only_ss );
    make_orders( data_location_table, site_dbs, site_holders, master, slot,
                 num_sites, tpcc_cfg, w_id, d_id, cid, load_only_ss );
}

void load_warehouse(
    multi_version_partition_data_location_table* data_location_table,
    std::vector<db*>&                            site_dbs,
    std::vector<tpcc_sproc_helper_holder*>& site_holders, int num_sites,
    int num_dests, const tpcc_configs& tpcc_cfg, uint32_t w_id, int cid,
    bool load_only_ss, bool load_only_data_site, int load_data_site_id ) {
    DVLOG( 1 ) << "Load Warehouse:" << w_id;


    int                           master = w_id % num_sites;
    int                           slot = w_id % num_dests;

    if( load_only_data_site ) {
        if( master != load_data_site_id ) {
            return;
        }
    }

    make_warehouse( data_location_table, site_dbs, site_holders, master, slot,
                    num_sites, tpcc_cfg, w_id, cid, load_only_ss );
    make_districts( data_location_table, site_dbs, site_holders, master, slot,
                    num_sites, tpcc_cfg, w_id, cid, load_only_ss );
    make_stocks( data_location_table, site_dbs, site_holders, master, slot,
                 num_sites, tpcc_cfg, w_id, cid, load_only_ss );

    std::vector<std::thread> loader_threads;
    for( uint32_t d_id = 0; d_id < tpcc_cfg.num_districts_per_warehouse_;
         d_id++ ) {

        std::thread d_loader( load_custs_and_orders, data_location_table,
                              std::ref( site_dbs ), std::ref( site_holders ),
                              master, slot, num_sites, std::cref( tpcc_cfg ),
                              w_id, d_id, cid + d_id, load_only_ss );
        loader_threads.push_back( std::move( d_loader ) );
    }
    join_threads( loader_threads );
}


void load_data(
    multi_version_partition_data_location_table* data_location_table,
    std::vector<db*>& site_dbs, int num_sites, int num_dests,
    const tpcc_configs& tpcc_cfg, bool load_only_ss, bool load_only_data_site,
    int load_data_site_id ) {

    std::vector<tpcc_sproc_helper_holder*> site_holders =
        generate_holders( tpcc_cfg, site_dbs );

    std::vector<std::thread> loader_threads;

    std::thread i_loader( load_items, data_location_table, std::ref( site_dbs ),
                          std::ref( site_holders ), num_sites,
                          std::cref( tpcc_cfg ), 0, load_only_ss,
                          load_only_data_site, load_data_site_id );
    loader_threads.push_back( std::move( i_loader ) );

    for( uint32_t w_id = 0; w_id < tpcc_cfg.num_warehouses_; w_id++ ) {
        std::thread l( load_warehouse, data_location_table,
                       std::ref( site_dbs ), std::ref( site_holders ),
                       num_sites, num_dests, std::cref( tpcc_cfg ), w_id,
                       ( w_id + 1 ) * tpcc_cfg.num_districts_per_warehouse_,
                       load_only_ss, load_only_data_site, load_data_site_id );
        loader_threads.push_back( std::move( l ) );
    }
    join_threads( loader_threads );

    for (uint32_t pos = 0; pos < site_holders.size(); pos++) {
        delete site_holders.at( pos );
        site_holders.at( pos ) = nullptr;
    }
    site_holders.clear();
}

void persist_ss(
    multi_version_partition_data_location_table* data_location_table,
    const std::vector<std::vector<propagation_configuration>>& prop_configs,
    const std::string& out_ss, bool load_only_data_site ) {

    DVLOG( 1 ) << "Persist SS";

    if( !load_only_data_site ) {
        site_selector_persistence_manager manager(
            out_ss, data_location_table, prop_configs,
            construct_persistence_configs() );
        manager.persist();
    }

    DVLOG( 1 ) << "Persist SS, okay!";
}

void persist_ds(
    db* site_db, int site_id,
    const std::vector<std::vector<propagation_configuration>>& prop_configs,
    const std::string& out_sm, const std::string& out_part,
    bool load_only_data_site, int load_data_site_id ) {

    if( load_only_data_site ) {
        if( load_data_site_id != site_id ) {
            return;
        }
    }

    DVLOG( 1 ) << "Persist DS:" << site_id;
    std::string site_out_db_name = out_sm + "/" + std::to_string( site_id );

    data_site_persistence_manager manager(
        site_out_db_name, out_part, site_db->get_tables(),
        site_db->get_update_destination_generator(), prop_configs,
        construct_persistence_configs(), site_id );
    manager.persist();

    DVLOG( 1 ) << "Persist DS:" << site_id << ", okay!";
}

void persist_data(
    multi_version_partition_data_location_table* data_location_table,
    std::vector<db*>& site_dbs, int num_sites, const tpcc_configs& tpcc_cfg,
    const std::vector<std::vector<propagation_configuration>>& prop_configs,
    const std::string& out_ss, const std::string& out_sm,
    const std::string& out_part, bool load_only_ss, bool load_only_data_site,
    int load_data_site_id ) {

    std::vector<std::thread> loader_threads;

    std::thread s_persister( persist_ss, data_location_table,
                             std::cref( prop_configs ), std::cref( out_ss ),
                             load_only_data_site );
    loader_threads.push_back( std::move( s_persister ) );

    if( !load_only_ss ) {
        DCHECK_EQ( num_sites, site_dbs.size() );
        for( int32_t site_id = 0; site_id < num_sites; site_id++ ) {
            std::thread l( persist_ds, site_dbs.at( site_id ), site_id,
                           std::cref( prop_configs ), std::cref( out_sm ),
                           std::cref( out_part ), load_only_data_site,
                           load_data_site_id );
            loader_threads.push_back( std::move( l ) );
        }
    }
    join_threads( loader_threads );
}

#endif
void generate_tpcc() {
#if 0 // MTODO-HTAP

    DVLOG( 0 ) << "GENERATE TPCC";

    std::string out_ss = FLAGS_output_ss_dir;
    std::string out_sm = FLAGS_output_sm_dir;
    std::string out_part = FLAGS_output_part_dir;

    int data_site_id = FLAGS_data_site_id;
    bool load_only_ss = FLAGS_do_only_ss;
    bool load_only_data_site = FLAGS_do_only_single_data_site;

    int          num_sites = k_num_sites;
    uint32_t     num_dests = k_num_update_destinations;
    tpcc_configs tpcc_cfg = construct_tpcc_configs();
    auto         data_sizes = get_tpcc_data_sizes( tpcc_cfg );
    auto         table_infos = create_tpcc_table_metadata( tpcc_cfg, 0 );


    DVLOG( 0 ) << "TPCC_CONFIGS:" << tpcc_cfg;

    DVLOG( 0 ) << "Load only SS:" << load_only_ss;
    DVLOG( 0 ) << "Load only data site:" << load_only_data_site;
    DVLOG( 0 ) << "Data site id:" << data_site_id;

    DCHECK_LT( data_site_id, num_sites );

    DCHECK_GE( tpcc_cfg.bench_configs_.num_clients_,
               tpcc_cfg.num_warehouses_ + 1 );

    tpcc_cfg.bench_configs_.num_clients_ =
        tpcc_cfg.bench_configs_.num_clients_ *
        tpcc_cfg.num_districts_per_warehouse_;

    multi_version_partition_data_location_table* data_location_table =
        new multi_version_partition_data_location_table();
    std::vector<db*> site_dbs;

    std::unordered_map<int, std::unordered_set<int>> replica_locs =
        build_replica_locs( FLAGS_replica_locs_file, num_sites );

    std::vector<std::vector<propagation_configuration>> prop_configs =
        build_prop_configs( num_sites, num_dests );
    std::shared_ptr<update_destination_generator> update_generator =
        std::make_shared<update_destination_generator>(
            construct_update_destination_generator_configs( num_dests ) );
    for( int id = 0; id < (int) num_dests; id++ ) {
        update_generator->add_update_destination(
            std::make_shared<no_op_update_destination>( id ) );
    }
    std::shared_ptr<update_enqueuers> enqueuers = make_update_enqueuers();

    create_table_structures( data_location_table, site_dbs, update_generator,
                             enqueuers, num_sites, tpcc_cfg, data_sizes,
                             table_infos );

    load_data( data_location_table, site_dbs, num_sites, num_dests, tpcc_cfg,
               load_only_ss, load_only_data_site, data_site_id );
    persist_data( data_location_table, site_dbs, num_sites, tpcc_cfg,
                  prop_configs, out_ss, out_sm, out_part, load_only_ss,
                  load_only_data_site, data_site_id );

    delete data_location_table;
    for( uint32_t pos = 0; pos < site_dbs.size(); pos++ ) {
        delete site_dbs.at( pos );
        site_dbs.at( pos ) = 0;
    }
    site_dbs.clear();

    DVLOG( 0 ) << "GENERATE TPCC, OKAY!";
#endif
}

int main( int argc, char** argv ) {

    gflags::ParseCommandLineFlags( &argc, &argv, true );
    google::InitGoogleLogging( argv[0] );
    google::InstallFailureSignalHandler();

    init_global_state();

    generate_tpcc();

    return 0;
}
