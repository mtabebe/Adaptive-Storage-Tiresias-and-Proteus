#define GTEST_HAS_TR1_TUPLE 0

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../src/benchmark/ycsb/ycsb_configs.h"
#include "../src/common/scan_results.h"
#include "../src/data-site/db/partition.h"
#include "../src/data-site/db/partition_metadata.h"
#include "../src/site-selector/heuristic_site_evaluator.h"
#include "../src/site-selector/site_selector_executor.h"
#include "../src/site-selector/transaction_preparer.h"
#include "cost_modeller_types_test.h"
#include "mock_sm_client_conn.h"

using ::testing::_;
using ::testing::Return;
using namespace ::testing;

class transaction_preparer_test : public ::testing::Test {};

class mock_site_evaluator : public abstract_site_evaluator {
   public:


    int                  num_sites_ = 0;
    uint32_t             num_update_destinations_per_site_ = 0;
    int                  insert_only_destination_ = 0;
    uint32_t             update_destination_slot_ = 0;
    bool                 force_change_ = false;

    partition_type::type    insert_partition_type_ = partition_type::type::ROW;
    storage_tier_type::type insert_storage_type_ =
        storage_tier_type::type::MEMORY;

    heuristic_site_evaluator_configs configs_ =
        construct_heuristic_site_evaluator_configs( num_sites_ );

    std::shared_ptr<pre_transaction_plan> plan_;
    std::shared_ptr<pre_transaction_plan> reuse_plan_;

    std::shared_ptr<cost_modeller2>                cost_model2_ = nullptr;
    std::shared_ptr<partition_data_location_table> data_loc_tab_ = nullptr;

    std::shared_ptr<sites_partition_version_information>
        site_partition_version_info_ = nullptr;
    std::shared_ptr<stat_tracked_enumerator_holder> enumerator_holder_ =
        nullptr;
    std::shared_ptr<partition_tier_tracking> tier_tracking_ = nullptr;
    std::shared_ptr<query_arrival_predictor> query_predictor_ = nullptr;

    mock_site_evaluator(
        std::shared_ptr<cost_modeller2>                cost_model2,
        std::shared_ptr<partition_data_location_table> data_loc_tab,
        std::shared_ptr<sites_partition_version_information>
                                                        site_partition_version_info,
        std::shared_ptr<query_arrival_predictor>        query_predictor,
        std::shared_ptr<stat_tracked_enumerator_holder> enumerator_holder,
        std::shared_ptr<partition_tier_tracking>        tier_tracking ) {
        cost_model2_ = cost_model2;
        data_loc_tab_ = data_loc_tab;
        site_partition_version_info_ = site_partition_version_info;
        query_predictor_ = query_predictor;
        enumerator_holder_ = enumerator_holder;
        tier_tracking_ = tier_tracking;
    }

    bool get_should_force_change(
        const ::clientid                      id,
        const std::vector<::cell_key_ranges> &ckr_write_set,
        const std::vector<::cell_key_ranges> &ckr_read_set ) {
        return force_change_;
    }

    heuristic_site_evaluator_configs get_configs() const { return configs_; }

    void decay() {}

    int get_no_change_destination(
        const std::vector<int> &no_change_destinations,
        const std::vector<std::shared_ptr<partition_payload>> &write_partitions,
        const std::vector<std::shared_ptr<partition_payload>> &read_partitions,
        const partition_column_identifier_set &write_partitions_set,
        const partition_column_identifier_set &read_partitions_set,
        ::snapshot_vector &                    svv ) {
        return no_change_destinations.at( 0 );
    }

    std::shared_ptr<pre_transaction_plan> build_no_change_plan(
        int destination, const grouped_partition_information &grouped_info,
        std::shared_ptr<multi_query_plan_entry> &mq_plan_entry,
        ::snapshot_vector &                      svv ) const {
        auto heur_eval = std::make_shared<heuristic_site_evaluator>(
            configs_, cost_model2_, data_loc_tab_, site_partition_version_info_,
            query_predictor_, enumerator_holder_, tier_tracking_ );
        auto plan = heur_eval->build_no_change_plan( destination, grouped_info,
                                                     mq_plan_entry, svv );
        return plan;
    }

    std::shared_ptr<multi_site_pre_transaction_plan> build_no_change_scan_plan(
        const ::clientid id, const grouped_partition_information &grouped_info,
        const std::vector<scan_arguments> &scan_args,
        const std::unordered_map<uint32_t /* label*/,
                                 partition_column_identifier_to_ckrs>
            &labels_to_pids_and_ckrs,
        const partition_column_identifier_map_t<std::unordered_map<
            uint32_t /* label */, std::vector<cell_key_ranges>>>
            &pids_to_labels_and_ckrs,
        const std::unordered_map<uint32_t /* label */, uint32_t /* pos */>
            &                                     label_to_pos,
        const std::vector<site_load_information> &site_load_infos,
        const std::unordered_map<storage_tier_type::type, std::vector<double>>
            &                                    storage_sizes,
        std::shared_ptr<multi_query_plan_entry> &mq_plan_entry,
        ::snapshot_vector &                      svv ) {
        auto heur_eval = std::make_shared<heuristic_site_evaluator>(
            configs_, cost_model2_, data_loc_tab_, site_partition_version_info_,
            query_predictor_, enumerator_holder_, tier_tracking_ );
        auto plan = heur_eval->build_no_change_scan_plan(
            id, grouped_info, scan_args, labels_to_pids_and_ckrs,
            pids_to_labels_and_ckrs, label_to_pos, site_load_infos,
            storage_sizes, mq_plan_entry, svv );
        return plan;
    }

    std::shared_ptr<multi_site_pre_transaction_plan>
        generate_scan_plan_from_current_planners(
            const ::clientid                     id,
            const grouped_partition_information &grouped_info,
            const std::vector<scan_arguments> &  scan_args,
            const std::unordered_map<uint32_t /* label*/,
                                     partition_column_identifier_to_ckrs>
                &labels_to_pids_and_ckrs,
            const partition_column_identifier_map_t<std::unordered_map<
                uint32_t /* label */, std::vector<cell_key_ranges>>>
                &pids_to_labels_and_ckrs,
            const std::unordered_map<uint32_t /* label */, uint32_t /* pos */>
                &                                     label_to_pos,
            const std::vector<site_load_information> &site_load_infos,
            const std::unordered_map<storage_tier_type::type,
                                     std::vector<double>> &storage_sizes,
            std::shared_ptr<multi_query_plan_entry> &      mq_plan_entry,
            snapshot_vector &                              session ) {

        auto heur_eval = std::make_shared<heuristic_site_evaluator>(
            configs_, cost_model2_, data_loc_tab_, site_partition_version_info_,
            query_predictor_, enumerator_holder_, tier_tracking_ );
        auto plan = heur_eval->generate_scan_plan_from_current_planners(
            id, grouped_info, scan_args, labels_to_pids_and_ckrs,
            pids_to_labels_and_ckrs, label_to_pos, site_load_infos,
            storage_sizes, mq_plan_entry, session );
        return plan;
    }


    std::shared_ptr<multi_site_pre_transaction_plan> generate_scan_plan(
        const ::clientid id, const grouped_partition_information &grouped_info,
        const std::vector<scan_arguments> &scan_args,
        const std::unordered_map<uint32_t /* label*/,
                                 partition_column_identifier_to_ckrs>
            &labels_to_pids_and_ckrs,
        const partition_column_identifier_map_t<std::unordered_map<
            uint32_t /* label */, std::vector<cell_key_ranges>>>
            &pids_to_labels_and_ckrs,
        const std::unordered_map<uint32_t /* label */, uint32_t /* pos */>
            &                                     label_to_pos,
        const std::vector<site_load_information> &site_load_infos,
        const std::unordered_map<storage_tier_type::type, std::vector<double>>
            &                                    storage_sizes,
        std::shared_ptr<multi_query_plan_entry> &mq_plan_entry,
        snapshot_vector &                        session ) {
        auto heur_eval = std::make_shared<heuristic_site_evaluator>(
            configs_, cost_model2_, data_loc_tab_, site_partition_version_info_,
            query_predictor_, enumerator_holder_, tier_tracking_ );
        auto plan = heur_eval->generate_scan_plan(
            id, grouped_info, scan_args, labels_to_pids_and_ckrs,
            pids_to_labels_and_ckrs, label_to_pos, site_load_infos,
            storage_sizes, mq_plan_entry, session );
        return plan;
    }

    std::shared_ptr<pre_transaction_plan> get_plan_from_current_planners(
        const grouped_partition_information &    grouped_info,
        std::shared_ptr<multi_query_plan_entry> &mq_plan_entry,
        ::snapshot_vector &                      svv ) {

        mq_plan_entry->set_plan_destination( plan_->destination_,
                                             multi_query_plan_type::PLANNING );

        if ( reuse_plan_) {
            reuse_plan_->mq_plan_ = mq_plan_entry;
        }
        return reuse_plan_;
    }


    int generate_insert_only_partition_destination(
        std::vector<std::shared_ptr<partition_payload>> &insert_partitions ) {
        return insert_only_destination_;
    }
    std::tuple<std::vector<partition_type::type>,
               std::vector<storage_tier_type::type>>
        get_new_partition_types(
            int destination, std::shared_ptr<pre_transaction_plan> &plan,
            const site_load_information &site_load_info,
            const std::unordered_map<storage_tier_type::type,
                                     std::vector<double>> &storage_sizes,
            std::vector<std::shared_ptr<partition_payload>>
                &insert_partitions ) {
        std::vector<partition_type::type> p_types;
        std::vector<storage_tier_type::type> s_types;
        for( uint32_t i = 0; i < insert_partitions.size(); i++ ) {
            p_types.emplace_back( insert_partition_type_ );
            s_types.emplace_back( insert_storage_type_ );
        }
        return std::make_tuple<>( p_types, s_types );
    }

    std::shared_ptr<pre_transaction_plan> generate_multi_site_insert_only_plan(
        const ::clientid                          id,
        const std::vector<site_load_information> &site_load_infos,
        const std::unordered_map<storage_tier_type::type, std::vector<double>>
            &                                            storage_sizes,
        std::vector<std::shared_ptr<partition_payload>> &new_partitions,
        bool                                             is_fully_replicated ) {
        return plan_;
    }

    uint32_t get_new_partition_update_destination_slot(
        int                                              destination,
        std::vector<std::shared_ptr<partition_payload>> &insert_partitions ) {
        return update_destination_slot_;
    }

    std::shared_ptr<pre_transaction_plan> generate_plan(
        const ::clientid id, const std::vector<bool> &site_locations,
        const std::vector<std::shared_ptr<partition_payload>> &new_partitions,
        const std::vector<std::shared_ptr<partition_payload>> &write_partitions,
        const std::vector<std::shared_ptr<partition_payload>> &read_partitions,
        const partition_column_identifier_set &write_partitions_set,
        const partition_column_identifier_set &read_partitions_set,
        partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>> &partition_location_informations,
        const partition_column_identifier_to_ckrs &write_pids_to_ckrs,
        const partition_column_identifier_to_ckrs &read_pids_to_ckrs,
        const std::vector<site_load_information> & site_load_infos,
        const std::unordered_map<storage_tier_type::type, std::vector<double>>
            &                                    storage_sizes,
        std::shared_ptr<multi_query_plan_entry> &mq_plan_entry,
        const snapshot_vector &                  session ) {
        DVLOG( 10 ) << "Generate plan!";
        if ( plan_) {
            plan_->mq_plan_ = mq_plan_entry;
        }
        return plan_;
    }

    int      get_num_sites() { return num_sites_; }
    uint32_t get_num_update_destinations_per_site() {
        return num_update_destinations_per_site_;
    }

    void set_samplers(
        std::vector<std::unique_ptr<adaptive_reservoir_sampler>> *samplers ) {
        (void) samplers;
    }
};

TEST_F( transaction_preparer_test, test_transaction_preparation ) {
    int  num_sites = 2;
    auto static_configs = k_cost_model_test_configs;

    auto cost_model2 = std::make_shared<cost_modeller2>( static_configs );
    auto site_partition_version_info =
        std::make_shared<sites_partition_version_information>( cost_model2 );

    int partition_size = 10;
    k_ycsb_partition_size = partition_size;
    int num_cols_per_part = 2;
    k_ycsb_column_partition_size = num_cols_per_part;
    std::vector<storage_tier_type::type> acceptable_storage_types = {
        storage_tier_type::type::MEMORY, storage_tier_type::type::DISK};

    std::shared_ptr<partition_data_location_table> data_loc_tab =
        construct_partition_data_location_table(
            construct_partition_data_location_table_configs(
                num_sites, acceptable_storage_types ),
            workload_type::YCSB );

    auto stat_enumerator_holder =
        std::make_shared<stat_tracked_enumerator_holder>(
            construct_stat_tracked_enumerator_configs(
                false /* don't track stats*/ ) );

    auto query_predictor = std::make_shared<query_arrival_predictor>(
        construct_query_arrival_predictor_configs(
            true /* use query arrival predictor*/,
            query_arrival_predictor_type::SIMPLE_QUERY_PREDICTOR,
            0.5 /* threshold */, 30 /* time bucket */, 10 /* count bucket */,
            1.0 /* score scalar */, 0.001 /* default score */ ) );

    add_tables_to_query_arrival_predictor( query_predictor, data_loc_tab );

    auto tier_tracking = std::make_shared<partition_tier_tracking>(
        data_loc_tab, cost_model2, query_predictor );

    mock_site_evaluator *mk_s_eval = new mock_site_evaluator(
        cost_model2, data_loc_tab, site_partition_version_info, query_predictor,
        stat_enumerator_holder, tier_tracking );
    mk_s_eval->num_sites_ = num_sites;
    mk_s_eval->num_update_destinations_per_site_ = 1;
    mock_site_evaluator *mk_s_eval_copy = mk_s_eval;
    // we don't own this pointer, but we need a copy of it to do stuff in this
    // test

    std::unique_ptr<abstract_site_evaluator> evaluator(
        std::move( mk_s_eval_copy ) );

    transaction_preparer_configs configs =
        construct_transaction_preparer_configs(
            /* num threads*/ 1, /*num clients*/ 3, 1 /* astream_mod */,
            10 /* astream_empty_wait_ms */,
            10 /* astream_tracking_interval_ms */,
            10 /* astream_max_queue_size */, 0.5 /* max_write_blend_rate */,
            1 /* background cid*/, 2 /* periodic cid */,
            true /*use_background_worker */,
            1 /*num_background_work_items_to_execute*/,
            ss_mastering_type::ADAPT, 0 /* estore_clay_periodic_interval_ms */,
            0.025 /* estore_load_epsilon_threshold */,
            0.01 /* estore_hot_partition_pct_thresholds */,
            1.5 /*adr multiplier*/, true /* enable plan reuse */,
            true /* enable wait for plan reuse */,
            partition_lock_mode::no_lock /* optimistic*/,
            partition_lock_mode::try_lock /* physical adjustment*/,
            partition_lock_mode::try_lock /* force*/,
            construct_periodic_site_selector_operations_configs( 1, 10 ) );

    std::unique_ptr<std::vector<client_conn_pool>> pool(
        new std::vector<client_conn_pool>() );
    std::vector<mock_sm_client_conn *> client_conns;
    for( uint32_t cid = 0; cid < configs.num_clients_ - 1; cid++ ) {
        std::unique_ptr<std::vector<sm_client_ptr>> loc_clients(
            new std::vector<sm_client_ptr>() );
        for( int site = 0; site < num_sites; site++ ) {
            mock_sm_client_conn *cli_conn = new mock_sm_client_conn();
            mock_sm_client_conn *cli_conn_copy = cli_conn;
            client_conns.push_back( cli_conn );

            sm_client_ptr client( std::move( cli_conn_copy ) );
            loc_clients->push_back( std::move( client ) );
        }

        pool->push_back( std::move( loc_clients ) );
    }

    std::unique_ptr<std::vector<sm_client_ptr>> clients(
        new std::vector<sm_client_ptr>() );
    std::vector<propagation_configuration> site_prop_configs;
    for( int site = 0; site < num_sites; site++ ) {
        mock_sm_client_conn *cli_conn = new mock_sm_client_conn();

        site_statistics_results stat_result;
        stat_result.status = exec_status_type::COMMAND_OK;

        propagation_configuration prop_config;
        prop_config.type = propagation_type::VECTOR;
        prop_config.partition = site;
        prop_config.offset = 0;
        std::vector<propagation_configuration> prop_configs = {prop_config};
        stat_result.prop_configs = prop_configs;
        std::vector<int64_t> prop_counts = {0};
        stat_result.prop_counts = prop_counts;

        EXPECT_CALL( *cli_conn, rpc_get_site_statistics( _ ) )
            .WillRepeatedly( testing::SetArgReferee<0>( stat_result ) );

        mock_sm_client_conn *cli_conn_copy = cli_conn;
        client_conns.push_back( cli_conn );

        sm_client_ptr client( std::move( cli_conn_copy ) );
        clients->push_back( std::move( client ) );
        site_prop_configs.push_back( prop_config );
    }
    pool->push_back( std::move( clients ) );

    std::shared_ptr<client_conn_pools> conn_pool(
        new client_conn_pools( std::move( pool ), configs.num_clients_,
                               configs.num_worker_threads_per_client_ ) );

    cost_model_prediction_holder model_prediction;
    transaction_preparer         preparer(
        conn_pool, data_loc_tab, cost_model2, site_partition_version_info,
        std::move( evaluator ), query_predictor, stat_enumerator_holder,
        tier_tracking, configs );

    // insert
    mk_s_eval->insert_only_destination_ = 1;
    mk_s_eval->insert_partition_type_ = partition_type::type::COLUMN;
    mk_s_eval->insert_storage_type_ = storage_tier_type::type::DISK;
    ::clientid                   cid = 0;
    std::vector<cell_key_ranges> ckrs;
    ckrs.push_back(
        create_cell_key_ranges( 0, 0, 19, 0, num_cols_per_part - 1 ) );
    auto p_0_9 =
        create_partition_column_identifier( 0, 0, 9, 0, num_cols_per_part - 1 );
    auto p_10_19 = create_partition_column_identifier( 0, 10, 19, 0,
                                                       num_cols_per_part - 1 );

    commit_result cr;
    cr.status = exec_status_type::COMMAND_OK;
    EXPECT_CALL(
        *( client_conns.at( 1 ) ),
        rpc_add_partitions( _, cid, _, ElementsAre( p_0_9, p_10_19 ),
                            mk_s_eval->insert_only_destination_,
                            ElementsAre( partition_type::type::COLUMN,
                                         partition_type::type::COLUMN ),
                            ElementsAre( storage_tier_type::type::DISK,
                                         storage_tier_type::type::DISK ),
                            _ ) )
        .WillOnce( testing::SetArgReferee<0>( cr ) );

    begin_result br;
    br.status = exec_status_type::COMMAND_OK;
    EXPECT_CALL(
        *( client_conns.at( 1 ) ),
        rpc_begin_transaction( _, cid, _, ElementsAre( p_0_9, p_10_19 ),
                               IsEmpty(), IsEmpty() ) )
        .WillOnce( testing::SetArgReferee<0>( br ) );

    int insert_destination_site =
        preparer.prepare_transaction( cid, ckrs, {}, {} );
    EXPECT_EQ( insert_destination_site, mk_s_eval->insert_only_destination_ );

    // do a read
    ckrs.clear();
    ckrs.emplace_back(
        create_cell_key_ranges( 0, 15, 19, 0, num_cols_per_part - 1 ) );
    br.status = exec_status_type::COMMAND_OK;
    EXPECT_CALL( *( client_conns.at( 1 ) ),
                 rpc_begin_transaction( _, cid, _, IsEmpty(),
                                        ElementsAre( p_10_19 ), IsEmpty() ) )
        .WillOnce( testing::SetArgReferee<0>( br ) );

    int no_change_destination_site =
        preparer.prepare_transaction( cid, {}, ckrs, {} );
    EXPECT_EQ( no_change_destination_site,
               mk_s_eval->insert_only_destination_ );

    // insert partition 20-29
    mk_s_eval->insert_only_destination_ = 0;
    mk_s_eval->insert_partition_type_ = partition_type::type::ROW;
    mk_s_eval->insert_storage_type_ = storage_tier_type::type::MEMORY;
    ckrs.clear();
    ckrs.emplace_back(
        create_cell_key_ranges( 0, 20, 29, 0, num_cols_per_part - 1 ) );
    auto p_20_29 = create_partition_column_identifier( 0, 20, 29, 0, 1 );

    cr.status = exec_status_type::COMMAND_OK;
    EXPECT_CALL( *( client_conns.at( 0 ) ),
                 rpc_add_partitions(
                     _, cid, _, ElementsAre( p_20_29 ),
                     mk_s_eval->insert_only_destination_,
                     ElementsAre( partition_type::type::ROW ),
                     ElementsAre( storage_tier_type::type::MEMORY ), _ ) )
        .WillOnce( testing::SetArgReferee<0>( cr ) );

    br.status = exec_status_type::COMMAND_OK;
    EXPECT_CALL( *( client_conns.at( 0 ) ),
                 rpc_begin_transaction( _, cid, _, ElementsAre( p_20_29 ),
                                        IsEmpty(), IsEmpty() ) )
        .WillOnce( testing::SetArgReferee<0>( br ) );

    insert_destination_site = preparer.prepare_transaction( cid, ckrs, {}, {} );
    EXPECT_EQ( insert_destination_site, mk_s_eval->insert_only_destination_ );

    // now do a write on p_0_9 and p_20_22
    ckrs.clear();
    ckrs.emplace_back( create_cell_key_ranges( 0, 0, 0, 0, 0 ) );
    ckrs.emplace_back( create_cell_key_ranges( 0, 21, 21, 0, 0 ) );

    // make a plan, that consists of a split, an add replica, and then a
    // remaster
    std::shared_ptr<pre_transaction_plan> plan =
        std::make_shared<pre_transaction_plan>(
            stat_enumerator_holder->construct_decision_holder_ptr() );
    cell_key ck = create_cell_key( 0, k_unassigned_col, 23 );
    cell_key split_point = ck;
    auto     split_pids = construct_split_partition_column_identifiers(
        p_20_29, ck.row_id, ck.col_id );
    auto p_20_22 = std::get<0>( split_pids );
    auto p_23_29 = std::get<1>( split_pids );
    EXPECT_EQ( p_20_22.partition_start, 20 );
    EXPECT_EQ( p_20_22.partition_end, 22 );
    EXPECT_EQ( p_20_22.column_start, 0 );
    EXPECT_EQ( p_20_22.column_end, 1 );
    EXPECT_EQ( p_23_29.partition_start, 23 );
    EXPECT_EQ( p_23_29.partition_end, 29 );
    EXPECT_EQ( p_23_29.column_start, 0 );
    EXPECT_EQ( p_23_29.column_end, 1 );

    // update_destination
    plan->write_pids_.push_back( p_0_9 );
    plan->write_pids_.push_back( p_20_22 );

    std::vector<partition_column_identifier> pids;
    std::vector<std::shared_ptr<pre_action>> deps;

    auto split_action = create_split_partition_pre_action(
        0, p_20_29, split_point, partition_type::type::ROW,
        partition_type::type::COLUMN, storage_tier_type::type::MEMORY,
        storage_tier_type::type::DISK, deps, 1.0, model_prediction, 0, 0 );
    plan->add_pre_action_work_item( split_action );

    cr.status = exec_status_type::COMMAND_OK;
    EXPECT_CALL( *( client_conns.at( 0 ) ),
                 rpc_split_partition( _, cid, _, p_20_29, split_point,
                                      partition_type::type::ROW,
                                      partition_type::type::COLUMN,
                                      storage_tier_type::type::MEMORY,
                                      storage_tier_type::type::DISK, _ ) )
        .WillOnce( testing::SetArgReferee<0>( cr ) );

    deps = {split_action};
    pids = {p_20_22};
    std::vector<partition_type::type> add_types = {
        partition_type::type::COLUMN};
    std::vector<storage_tier_type::type> add_storage_types = {
        storage_tier_type::type::DISK};

    auto add_replica_action = create_add_replica_partition_pre_action(
        1, 0, pids, add_types, add_storage_types, deps, 1.0, model_prediction );
    plan->add_pre_action_work_item( add_replica_action );

    snapshot_partition_columns_results snap_results;
    snap_results.status = exec_status_type::COMMAND_OK;
    EXPECT_CALL( *( client_conns.at( 0 ) ),
                 rpc_snapshot_partitions( _, cid, ElementsAre( p_20_22 ) ) )
        .WillOnce( testing::SetArgReferee<0>( snap_results ) );

    cr.status = exec_status_type::COMMAND_OK;
    EXPECT_CALL( *( client_conns.at( 1 ) ),
                 rpc_add_replica_partitions( _, cid, _, _, _ ) )
        .WillOnce( testing::SetArgReferee<0>( cr ) );

    pids = {p_20_22};
    deps = {split_action, add_replica_action};

    auto remaster_action = create_remaster_partition_pre_action(
        0, 1, pids, deps, 1.0, model_prediction, {0} );
    plan->add_pre_action_work_item( remaster_action );
    plan->destination_ = 1;

    release_result rr;
    rr.status = exec_status_type::MASTER_CHANGE_OK;
    set_snapshot_version( rr.session_version_vector, p_20_22, 1 );
    EXPECT_CALL(
        *( client_conns.at( 0 ) ),
        rpc_release_mastership( _, cid, ElementsAre( p_20_22 ), 1,
                                ElementsAre( site_prop_configs.at( 1 ) ), _ ) )
        .WillOnce( testing::SetArgReferee<0>( rr ) );

    grant_result gr;
    set_snapshot_version( gr.session_version_vector, p_20_22, 2 );
    gr.status = exec_status_type::MASTER_CHANGE_OK;
    EXPECT_CALL( *( client_conns.at( 1 ) ),
                 rpc_grant_mastership( _, cid, ElementsAre( p_20_22 ), _, _ ) )
        .WillOnce( testing::SetArgReferee<0>( gr ) );

    mk_s_eval->plan_ = plan;

    br.status = exec_status_type::COMMAND_OK;
    EXPECT_CALL(
        *( client_conns.at( 1 ) ),
        rpc_begin_transaction( _, cid, _, ElementsAre( p_0_9, p_20_22 ),
                               IsEmpty(), IsEmpty() ) )
        .WillOnce( testing::SetArgReferee<0>( br ) );

    int destination_site = preparer.prepare_transaction( cid, ckrs, {}, {} );
    EXPECT_EQ( destination_site, plan->destination_ );

    // this should work, even with failure
    br.status = exec_status_type::DATA_NOT_LOCATED_AT_ONE_SITE;
    EXPECT_CALL(
        *( client_conns.at( 1 ) ),
        rpc_begin_transaction( _, cid, _, ElementsAre( p_0_9, p_20_22 ),
                               IsEmpty(), IsEmpty() ) )
        .Times( 2 )
        .WillRepeatedly( testing::SetArgReferee<0>( br ) );

    destination_site = preparer.prepare_transaction( cid, ckrs, {}, {} );
    EXPECT_EQ( destination_site, -1 );

    // a subsequent locking call should also work
    begin_result br2;
    br2.status = exec_status_type::COMMAND_OK;
    EXPECT_CALL(
        *( client_conns.at( 1 ) ),
        rpc_begin_transaction( _, cid, _, ElementsAre( p_0_9, p_20_22 ),
                               IsEmpty(), IsEmpty() ) )
        .WillOnce( testing::SetArgReferee<0>( br ) )
        .WillOnce( testing::SetArgReferee<0>( br2 ) );

    destination_site = preparer.prepare_transaction( cid, ckrs, {}, {} );
    EXPECT_EQ( destination_site, 1 );

    // now lets fail during a plan op

    // make a plan, that consists of a remaster and then a merge
    plan = std::make_shared<pre_transaction_plan>(
        stat_enumerator_holder->construct_decision_holder_ptr() );
    ckrs.clear();
    ckrs.emplace_back( create_cell_key_ranges( 0, 20, 29, 0, 1 ) );

    plan->write_pids_.push_back( p_20_29 );
    plan->destination_ = 0;

    pids = {p_20_22};
    deps = {};

    // remaster
    remaster_action = create_remaster_partition_pre_action(
        1, 0, pids, deps, 1.0, model_prediction, {0} );
    plan->add_pre_action_work_item( remaster_action );

    deps = {remaster_action};

    auto merge_action = create_merge_partition_pre_action(
        0, p_20_22, p_23_29, p_20_29, partition_type::type::ROW,
        storage_tier_type::type::MEMORY, deps, 1.0, model_prediction, 0 );
    plan->add_pre_action_work_item( merge_action );
    mk_s_eval->plan_ = plan;

    // lets fail the release
    rr.status = exec_status_type::MASTER_CHANGE_ERROR;
    EXPECT_CALL(
        *( client_conns.at( 1 ) ),
        rpc_release_mastership( _, cid, ElementsAre( p_20_22 ), 0,
                                ElementsAre( site_prop_configs.at( 0 ) ), _ ) )
        .WillOnce( testing::SetArgReferee<0>( rr ) );

    destination_site = preparer.prepare_transaction( cid, ckrs, {}, {} );
    EXPECT_EQ( -1, destination_site );

    previous_context_timer_information previous_contexts;
    previous_contexts.is_present = false;
    preparer.add_previous_context_timer_information( cid, previous_contexts );

    previous_contexts.is_present = true;
    previous_contexts.num_requests = 4;
    previous_contexts.site = 0;

    auto ckr_0_9 = create_cell_key_ranges( 0, 0, 9, 0, num_cols_per_part - 1 );
    auto ckr_10_19 =
        create_cell_key_ranges( 0, 10, 19, 0, num_cols_per_part - 1 );
    auto ckr_20_29 =
        create_cell_key_ranges( 0, 20, 29, 0, num_cols_per_part - 1 );

    previous_contexts.write_ckrs.push_back( ckr_0_9 );
    previous_contexts.write_ckrs.push_back( ckr_10_19 );
    previous_contexts.write_ckrs.push_back( ckr_20_29 );

    previous_contexts.write_ckr_counts.push_back( 3 );
    previous_contexts.write_ckr_counts.push_back( 4 );
    previous_contexts.write_ckr_counts.push_back( 2 );

    context_timer timer;
    timer.counter_id = PARTITION_BEGIN_WRITE_ACQUIRE_LOCKS_TIMER_ID;
    timer.counter_seen_count = 9;
    timer.timer_us_average = 300;
    previous_contexts.timers.push_back( timer );

    timer.counter_id = RPC_AT_DATA_SITE_SERVICE_TIME_TIMER_ID;
    timer.counter_seen_count = 3;
    timer.timer_us_average = 300;
    previous_contexts.timers.push_back( timer );

    timer.counter_id = PARTITION_BUILD_COMMIT_SNAPSHOT_TIMER_ID;
    timer.counter_seen_count = 9;
    timer.timer_us_average = 1700;
    previous_contexts.timers.push_back( timer );

    preparer.add_previous_context_timer_information( cid, previous_contexts );
}

TEST_F( transaction_preparer_test, test_pre_action_executor ) {
    int  num_sites = 2;
    auto static_configs = k_cost_model_test_configs;

    auto cost_model2 = std::make_shared<cost_modeller2>( static_configs );

    transaction_preparer_configs configs =
        construct_transaction_preparer_configs(
            /* num threads*/ 1, /*num clients*/ 3, 1 /* astream_mod */,
            10 /* astream_empty_wait_ms */,
            10 /* astream_tracking_interval_ms */,
            10 /* astream_max_queue_size */, 0.5 /* max_write_blend_rate */,
            1 /* background cid*/, 2 /* periodic cid */,
            true /*use_background_worker */,
            3 /*num_background_work_items_to_execute*/,
            ss_mastering_type::ADAPT, 0 /* estore_clay_periodic_interval_ms */,
            0.025 /* estore_load_epsilon_threshold */,
            0.01 /* estore_hot_partition_pct_thresholds */,
            1.5 /*adr multiplier*/, true /* enable plan reuse */,
            true /* enable wait for plan reuse */,
            partition_lock_mode::no_lock /* optimistic*/,
            partition_lock_mode::try_lock /* physical adjustment*/,
            partition_lock_mode::try_lock /* force*/,
            construct_periodic_site_selector_operations_configs( 1, 10 ) );

    std::unique_ptr<std::vector<client_conn_pool>> pool(
        new std::vector<client_conn_pool>() );
    std::vector<mock_sm_client_conn *> client_conns;
    for( uint32_t cid = 0; cid < configs.num_clients_ - 1; cid++ ) {
        std::unique_ptr<std::vector<sm_client_ptr>> loc_clients(
            new std::vector<sm_client_ptr>() );
        for( int site = 0; site < num_sites; site++ ) {
            mock_sm_client_conn *cli_conn = new mock_sm_client_conn();
            mock_sm_client_conn *cli_conn_copy = cli_conn;
            client_conns.push_back( cli_conn );

            sm_client_ptr client( std::move( cli_conn_copy ) );
            loc_clients->push_back( std::move( client ) );
        }

        pool->push_back( std::move( loc_clients ) );
    }

    std::unique_ptr<std::vector<sm_client_ptr>> clients(
        new std::vector<sm_client_ptr>() );
    std::vector<propagation_configuration> site_prop_configs;
    for( int site = 0; site < num_sites; site++ ) {
        mock_sm_client_conn *cli_conn = new mock_sm_client_conn();

        site_statistics_results stat_result;
        stat_result.status = exec_status_type::COMMAND_OK;

        propagation_configuration prop_config;
        prop_config.type = propagation_type::VECTOR;
        prop_config.partition = site;
        prop_config.offset = 0;
        std::vector<propagation_configuration> prop_configs = {prop_config};
        stat_result.prop_configs = prop_configs;
        std::vector<int64_t> prop_counts = {0};
        stat_result.prop_counts = prop_counts;

        EXPECT_CALL( *cli_conn, rpc_get_site_statistics( _ ) )
            .WillRepeatedly( testing::SetArgReferee<0>( stat_result ) );

        mock_sm_client_conn *cli_conn_copy = cli_conn;
        client_conns.push_back( cli_conn );

        sm_client_ptr client( std::move( cli_conn_copy ) );
        clients->push_back( std::move( client ) );
        site_prop_configs.push_back( prop_config );
    }
    pool->push_back( std::move( clients ) );

    std::shared_ptr<client_conn_pools> conn_pool(
        new client_conn_pools( std::move( pool ), configs.num_clients_,
                               configs.num_worker_threads_per_client_ ) );

    int partition_size = 10;
    k_ycsb_partition_size = partition_size;
    int num_cols_per_part = 2;
    k_ycsb_column_partition_size = num_cols_per_part;

    std::unique_ptr<site_selector_query_stats> query_stats =
        std::make_unique<site_selector_query_stats>();
    query_stats->create_table( create_ycsb_table_metadata() );

    std::vector<storage_tier_type::type> acceptable_storage_types = {
        storage_tier_type::type::MEMORY, storage_tier_type::type::DISK};

    std::shared_ptr<partition_data_location_table> data_loc_tab =
        construct_partition_data_location_table(
            construct_partition_data_location_table_configs(
                num_sites, acceptable_storage_types ),
            workload_type::YCSB );
    auto site_partition_version_info =
        std::make_shared<sites_partition_version_information>( cost_model2 );
    site_partition_version_info->init_partition_states(
        num_sites, data_loc_tab->get_num_tables() );

    auto query_predictor = std::make_shared<query_arrival_predictor>(
        construct_query_arrival_predictor_configs(
            true /*use query arrival predictor*/,
            query_arrival_predictor_type::SIMPLE_QUERY_PREDICTOR,
            0.5 /* threshold */, 30 /* time bucket */, 10 /* count bucket */,
            1.0 /* score scalar */, 0.001 /* default score */ ) );
    add_tables_to_query_arrival_predictor( query_predictor, data_loc_tab );

    auto tier_tracking = std::make_shared<partition_tier_tracking>(
        data_loc_tab, cost_model2, query_predictor );

    periodic_site_selector_operations periodic_site_ops(
        conn_pool, cost_model2, site_partition_version_info, data_loc_tab,
        query_stats.get(), tier_tracking, query_predictor,
        data_loc_tab->get_site_read_access_counts(),
        data_loc_tab->get_site_write_access_counts(), num_sites,
        configs.periodic_operations_cid_,
        configs.periodic_site_selector_operations_configs_ );
    periodic_site_ops.start_polling();

    pre_action_executor executor(
        conn_pool, data_loc_tab, cost_model2, &periodic_site_ops,
        site_partition_version_info, tier_tracking, configs );

    auto split_pid = create_partition_column_identifier( 0, 0, 10, 0, 1 );
    auto split_part = std::make_shared<partition_payload>( split_pid );
    auto split_loc_info = std::make_shared<partition_location_information>();
    split_loc_info->master_location_ = 0;
    split_loc_info->version_ = 1;
    bool compared_and_swapped =
        split_part->compare_and_swap_location_information( split_loc_info,
                                                           nullptr );
    EXPECT_TRUE( compared_and_swapped );

    cell_key split_point;
    split_point.table_id = 0;
    split_point.row_id = 5;
    split_point.col_id = k_unassigned_col;

    cost_model_prediction_holder split_prediction;

    std::shared_ptr<pre_action> split_partition_action =
        create_split_partition_pre_action(
            0 /*master*/, split_part, split_point, 5 /*cost*/, split_prediction,
            partition_type::type::ROW, partition_type::type::COLUMN,
            storage_tier_type::type::MEMORY, storage_tier_type::type::DISK,
            0 /*low slot*/, 0 /*high slot*/ );

    std::vector<std::shared_ptr<pre_action>> actions;
    actions.push_back( split_partition_action );
    executor.execute_background_actions( 0 /*tid*/, actions );

    data_loc_tab->insert_partition( split_part, partition_lock_mode::no_lock );
    split_part->lock();

    // shouldn't do anything can't get lock
    executor.execute_background_actions( 0 /*tid*/, actions );

    split_part->unlock();

    // now should be able to do something
    commit_result cr;
    cr.status = exec_status_type::COMMAND_OK;
    EXPECT_CALL(
        *( client_conns.at( ( num_sites * configs.background_executor_cid_ ) +
                            0 ) ),
        rpc_split_partition( _, configs.background_executor_cid_, _, split_pid,
                             split_point, partition_type::type::ROW,
                             partition_type::type::COLUMN,
                             storage_tier_type::type::MEMORY,
                             storage_tier_type::type::DISK, _ ) )
        .WillOnce( testing::SetArgReferee<0>( cr ) );
    executor.execute_background_actions( 0 /*tid*/, actions );

    EXPECT_FALSE( data_loc_tab->get_partition( split_pid,
                                               partition_lock_mode::no_lock ) );
    auto split_pids = construct_split_partition_column_identifiers(
        split_pid, split_point.row_id, split_point.col_id );

    auto left_pid = std::get<0>( split_pids );
    auto right_pid = std::get<1>( split_pids );

    auto left_part =
        data_loc_tab->get_partition( left_pid, partition_lock_mode::lock );
    auto right_part =
        data_loc_tab->get_partition( right_pid, partition_lock_mode::lock );
    EXPECT_TRUE( left_part );
    EXPECT_TRUE( right_part );
    EXPECT_EQ( left_part->identifier_, left_pid );
    EXPECT_EQ( right_part->identifier_, right_pid );

    left_part->unlock( partition_lock_mode::lock );
    right_part->unlock( partition_lock_mode::lock );

    std::vector<std::shared_ptr<partition_payload>> modify_payloads;
    modify_payloads.push_back( right_part );
    modify_payloads.push_back( left_part );

    DVLOG( 40 ) << "Making three new actions";

    // remaster
    cost_model_prediction_holder remaster_prediction;
    std::shared_ptr<pre_action>  first_remaster_action =
        create_remaster_partition_pre_action(
            0, 1, modify_payloads, 5 /* cost*/, remaster_prediction, {1, 1} );
    std::shared_ptr<pre_action> add_replica_action =
        create_add_replica_partition_pre_action(
            1, 0, modify_payloads,
            {partition_type::type::ROW, partition_type::type::ROW},
            {storage_tier_type::type::MEMORY, storage_tier_type::type::MEMORY},
            4 /* cost*/, remaster_prediction );
    std::shared_ptr<pre_action> second_remaster_action =
        create_remaster_partition_pre_action(
            0, 1, modify_payloads, 3 /* cost*/, remaster_prediction, {1, 1} );

    actions.clear();
    actions.push_back( add_replica_action );
    actions.push_back( second_remaster_action );
    actions.push_back( first_remaster_action );

    DVLOG( 40 ) << "Adding background work items";
    executor.add_background_work_items( actions );

    snapshot_partition_columns_results snap_results;
    snap_results.status = exec_status_type::COMMAND_OK;
    EXPECT_CALL( *( client_conns.at(
                     ( num_sites * configs.background_executor_cid_ ) + 0 ) ),
                 rpc_snapshot_partitions( _, configs.background_executor_cid_,
                                          ElementsAre( left_pid, right_pid ) ) )
        .WillOnce( testing::SetArgReferee<0>( snap_results ) );

    cr.status = exec_status_type::COMMAND_OK;
    EXPECT_CALL( *( client_conns.at(
                     ( num_sites * configs.background_executor_cid_ ) + 1 ) ),
                 rpc_add_replica_partitions(
                     _, configs.background_executor_cid_, _, _, _ ) )
        .WillOnce( testing::SetArgReferee<0>( cr ) );

    release_result rr;
    rr.status = exec_status_type::MASTER_CHANGE_OK;
    set_snapshot_version( rr.session_version_vector, left_pid, 1 );
    set_snapshot_version( rr.session_version_vector, right_pid, 1 );

    EXPECT_CALL(
        *( client_conns.at( ( num_sites * configs.background_executor_cid_ ) +
                            0 ) ),
        rpc_release_mastership(
            _, configs.background_executor_cid_,
            ElementsAre( left_pid, right_pid ), 1,
            ElementsAre( site_prop_configs.at( 1 ), site_prop_configs.at( 1 ) ),
            _ ) )
        .WillOnce( testing::SetArgReferee<0>( rr ) );

    grant_result gr;
    set_snapshot_version( gr.session_version_vector, left_pid, 2 );
    set_snapshot_version( gr.session_version_vector, right_pid, 2 );
    gr.status = exec_status_type::MASTER_CHANGE_OK;
    EXPECT_CALL(
        *( client_conns.at( ( num_sites * configs.background_executor_cid_ ) +
                            1 ) ),
        rpc_grant_mastership( _, configs.background_executor_cid_,
                              ElementsAre( left_pid, right_pid ), _, _ ) )
        .WillOnce( testing::SetArgReferee<0>( gr ) );

    executor.start_background_executor_threads();
    std::this_thread::yield();
    std::this_thread::sleep_for( std::chrono::milliseconds( 1 ) );
    executor.stop_background_executor_threads();

    left_part =
        data_loc_tab->get_partition( left_pid, partition_lock_mode::lock );
    EXPECT_TRUE( left_part );
    EXPECT_EQ( left_pid, left_part->identifier_ );
    auto left_loc = left_part->get_location_information();
    EXPECT_TRUE( left_loc );
    EXPECT_EQ( 1, left_loc->master_location_ );
    EXPECT_EQ( 1, left_loc->replica_locations_.size() );
    EXPECT_EQ( 1, left_loc->replica_locations_.count( 0 ) );
    left_part->unlock( partition_lock_mode::lock );

    periodic_site_ops.stop_polling();
}

TEST_F( transaction_preparer_test, test_multi_site_transaction_preparation ) {
    int num_sites = 2;

    auto static_configs = k_cost_model_test_configs;

    auto cost_model2 = std::make_shared<cost_modeller2>( static_configs );
    auto site_partition_version_info =
        std::make_shared<sites_partition_version_information>( cost_model2 );

    int partition_size = 10;
    k_ycsb_partition_size = partition_size;
    int num_cols_per_part = 2;
    k_ycsb_column_partition_size = num_cols_per_part;

    std::vector<storage_tier_type::type> acceptable_storage_types = {
        storage_tier_type::type::MEMORY, storage_tier_type::type::DISK};

    std::shared_ptr<partition_data_location_table> data_loc_tab =
        construct_partition_data_location_table(
            construct_partition_data_location_table_configs(
                num_sites, acceptable_storage_types ),
            workload_type::YCSB );
    auto stat_enumerator_holder =
        std::make_shared<stat_tracked_enumerator_holder>(
            construct_stat_tracked_enumerator_configs(
                false /* don't track stats*/ ) );

    auto query_predictor = std::make_shared<query_arrival_predictor>(
        construct_query_arrival_predictor_configs(
            true /*use query arrival predictor*/,
            query_arrival_predictor_type::SIMPLE_QUERY_PREDICTOR,
            0.5 /* threshold */, 30 /* time bucket */, 10 /* arrival count */,
            1.0 /* score scalar */, 0.001 /* default score */ ) );
    add_tables_to_query_arrival_predictor( query_predictor, data_loc_tab );

    auto tier_tracking = std::make_shared<partition_tier_tracking>(
        data_loc_tab, cost_model2, query_predictor );

    mock_site_evaluator *mk_s_eval = new mock_site_evaluator(
        cost_model2, data_loc_tab, site_partition_version_info, query_predictor,
        stat_enumerator_holder, tier_tracking );
    mk_s_eval->num_sites_ = num_sites;
    mk_s_eval->num_update_destinations_per_site_ = 1;
    mock_site_evaluator *mk_s_eval_copy = mk_s_eval;
    // we don't own this pointer, but we need a copy of it to do stuff in this
    // test

    std::unique_ptr<abstract_site_evaluator> evaluator(
        std::move( mk_s_eval_copy ) );

    transaction_preparer_configs configs =
        construct_transaction_preparer_configs(
            /* num threads*/ 1, /*num clients*/ 3, 1 /* astream_mod */,
            10 /* astream_empty_wait_ms */,
            10 /* astream_tracking_interval_ms */,
            10 /* astream_max_queue_size */, 0.5 /* max_write_blend_rate */,
            1 /* background cid*/, 2 /* periodic cid */,
            true /*use_background_worker */,
            1 /*num_background_work_items_to_execute*/, ss_mastering_type::ADAPT,
            0 /* estore_clay_periodic_interval_ms */,
            0.025 /* estore_load_epsilon_threshold */,
            0.01 /* estore_hot_partition_pct_thresholds */,
            1.5 /*adr multiplier*/, true /* enable plan reuse */,
            true /* enable wait for plan reuse */,
            partition_lock_mode::no_lock /* optimistic*/,
            partition_lock_mode::try_lock /* physical adjustment*/,
            partition_lock_mode::try_lock /* force*/,
            construct_periodic_site_selector_operations_configs( 1, 10 ) );

    std::unique_ptr<std::vector<client_conn_pool>> pool(
        new std::vector<client_conn_pool>() );
    std::vector<mock_sm_client_conn *> client_conns;
    for( uint32_t cid = 0; cid < configs.num_clients_ - 1; cid++ ) {
        std::unique_ptr<std::vector<sm_client_ptr>> loc_clients(
            new std::vector<sm_client_ptr>() );
        for (int site = 0; site < num_sites; site++) {
            mock_sm_client_conn *cli_conn = new mock_sm_client_conn();
            mock_sm_client_conn *cli_conn_copy = cli_conn;
            client_conns.push_back( cli_conn );

            sm_client_ptr client( std::move( cli_conn_copy ) );
            loc_clients->push_back( std::move( client ) );
        }

        pool->push_back( std::move( loc_clients ) );
    }

    std::unique_ptr<std::vector<sm_client_ptr>> clients(
        new std::vector<sm_client_ptr>() );
    std::vector<propagation_configuration> site_prop_configs;
    for( int site = 0; site < num_sites; site++ ) {
        mock_sm_client_conn *cli_conn = new mock_sm_client_conn();

        site_statistics_results stat_result;
        stat_result.status = exec_status_type::COMMAND_OK;

        propagation_configuration prop_config;
        prop_config.type = propagation_type::VECTOR;
        prop_config.partition = site;
        prop_config.offset = 0;
        std::vector<propagation_configuration> prop_configs = {prop_config};
        stat_result.prop_configs = prop_configs;
        std::vector<int64_t> prop_counts = {0};
        stat_result.prop_counts = prop_counts;

        EXPECT_CALL( *cli_conn, rpc_get_site_statistics( _ ) )
            .WillRepeatedly( testing::SetArgReferee<0>( stat_result ) );

        mock_sm_client_conn *cli_conn_copy = cli_conn;
        client_conns.push_back( cli_conn );

        sm_client_ptr client( std::move( cli_conn_copy ) );
        clients->push_back( std::move( client ) );
        site_prop_configs.push_back( prop_config );
    }
    pool->push_back( std::move( clients ) );

    std::shared_ptr<client_conn_pools> conn_pool(
        new client_conn_pools( std::move( pool ), configs.num_clients_,
                               configs.num_worker_threads_per_client_ ) );

    cost_model_prediction_holder model_prediction;
    transaction_preparer         preparer(
        conn_pool, data_loc_tab, cost_model2, site_partition_version_info,
        std::move( evaluator ), query_predictor, stat_enumerator_holder,
        tier_tracking, configs );
    // insert
    mk_s_eval->insert_only_destination_ = 1;
    mk_s_eval->insert_partition_type_ = partition_type::type::COLUMN;
    ::clientid cid = 0;
    std::vector<cell_key_ranges> ckrs;
    ckrs.emplace_back( create_cell_key_ranges( 0, 0, 19, 0, 1 ) );
    auto p_0_9 = create_partition_column_identifier( 0, 0, 9, 0, 1 );
    auto p_10_19 = create_partition_column_identifier( 0, 10, 19, 0, 1 );

    std::shared_ptr<pre_transaction_plan> plan =
        std::make_shared<pre_transaction_plan>(
            stat_enumerator_holder->construct_decision_holder_ptr() );
    // update_destination
    std::vector<partition_column_identifier>       pids = {p_0_9};
    std::vector<partition_type::type>              add_types = {
        partition_type::type::COLUMN};
    std::vector<storage_tier_type::type> add_storage_types = {
        storage_tier_type::type::DISK};

    std::vector<std::shared_ptr<pre_action>> deps;


    cost_model_prediction_holder add_model_prediction;

    auto add_action = create_add_partition_pre_action(
        0, 0, pids, add_types, add_storage_types, deps, 0.0 /* cost*/,
        add_model_prediction, 0 /*slot*/ );
    plan->add_pre_action_work_item( add_action );

    pids = {p_10_19};
    add_action = create_add_partition_pre_action(
        1, 1, pids, add_types, add_storage_types, deps, 0.0 /* cost*/,
        add_model_prediction, 0 /*slot*/ );
    plan->add_pre_action_work_item( add_action );

    mk_s_eval->plan_ = plan;

    commit_result cr;
    cr.status = exec_status_type::COMMAND_OK;
    EXPECT_CALL(
        *( client_conns.at( 0 ) ),
        rpc_add_partitions( _, cid, _, ElementsAre( p_0_9 ), 0,
                            ElementsAre( partition_type::type::COLUMN ),
                            ElementsAre( storage_tier_type::type::DISK ), _ ) )
        .WillOnce( testing::SetArgReferee<0>( cr ) );
    EXPECT_CALL(
        *( client_conns.at( 1 ) ),
        rpc_add_partitions( _, cid, _, ElementsAre( p_10_19 ), 1,
                            ElementsAre( partition_type::type::COLUMN ),
                            ElementsAre( storage_tier_type::type::DISK ), _ ) )
        .WillOnce( testing::SetArgReferee<0>( cr ) );

    begin_result br;
    br.status = exec_status_type::COMMAND_OK;
    EXPECT_CALL( *( client_conns.at( 0 ) ),
                 rpc_begin_transaction( _, cid, _, ElementsAre( p_0_9 ),
                                        IsEmpty(), IsEmpty() ) )
        .WillOnce( testing::SetArgReferee<0>( br ) );
    EXPECT_CALL( *( client_conns.at( 1 ) ),
                 rpc_begin_transaction( _, cid, _, ElementsAre( p_10_19 ),
                                        IsEmpty(), IsEmpty() ) )
        .WillOnce( testing::SetArgReferee<0>( br ) );

    std::unordered_map<int, std::vector<::cell_key_ranges>> preparer_result =
        preparer.prepare_multi_site_transaction(
            cid, ckrs /*write*/, {} /*read*/, {} /*full replica*/,
            {} /*snapshot*/ );
    EXPECT_EQ( 2, preparer_result.size() );
    auto ckr_0 = preparer_result.at( 0 );
    auto ckr_1 = preparer_result.at( 1 );
    EXPECT_EQ( ckr_0.size(), 1);
    EXPECT_EQ( ckr_0.at( 0 ), create_cell_key_ranges( 0, 0, 9, 0, 1 ) );
    EXPECT_EQ( ckr_1.size(), 1 );
    EXPECT_EQ( ckr_1.at( 0 ), create_cell_key_ranges( 0, 10, 19, 0, 1 ) );

    std::vector<cell_key_ranges> replica_ckrs;
    replica_ckrs.emplace_back( create_cell_key_ranges( 0, 20, 29, 0, 1 ) );
    auto p_20_29 = create_partition_column_identifier( 0, 20, 29, 0, 1 );

    std::shared_ptr<pre_transaction_plan> replica_plan =
        std::make_shared<pre_transaction_plan>(
            stat_enumerator_holder->construct_decision_holder_ptr() );
    // update_destination
    std::vector<partition_column_identifier>       replica_pids = {p_20_29};
    std::vector<std::shared_ptr<pre_action>> replica_deps;
    std::vector<partition_type::type>              replica_types = {
        partition_type::type::SORTED_COLUMN};
    std::vector<storage_tier_type::type> replica_storage_types = {
        storage_tier_type::type::DISK};

    cost_model_prediction_holder replica_add_model_prediction;

    auto replica_add_action = create_add_partition_pre_action(
        K_DATA_AT_ALL_SITES, K_DATA_AT_ALL_SITES, replica_pids, replica_types,
        replica_storage_types, replica_deps, 0.0 /* cost*/,
        replica_add_model_prediction, 0 /*slot*/ );
    replica_plan->add_pre_action_work_item( replica_add_action );
    mk_s_eval->plan_ = replica_plan;

    commit_result replica_cr;
    replica_cr.status = exec_status_type::COMMAND_OK;
    EXPECT_CALL(
        *( client_conns.at( 0 ) ),
        rpc_add_partitions( _, cid, _, ElementsAre( p_20_29 ), 0,
                            ElementsAre( partition_type::type::SORTED_COLUMN ),
                            ElementsAre( storage_tier_type::type::DISK ), _ ) )
        .WillOnce( testing::SetArgReferee<0>( replica_cr ) );
    EXPECT_CALL(
        *( client_conns.at( 1 ) ),
        rpc_add_partitions( _, cid, _, ElementsAre( p_20_29 ), 1,
                            ElementsAre( partition_type::type::SORTED_COLUMN ),
                            ElementsAre( storage_tier_type::type::DISK ), _ ) )
        .WillOnce( testing::SetArgReferee<0>( replica_cr ) );

    begin_result replica_br;
    br.status = exec_status_type::COMMAND_OK;
    EXPECT_CALL( *( client_conns.at( 0 ) ),
                 rpc_begin_transaction( _, cid, _, ElementsAre( p_20_29 ),
                                        IsEmpty(), IsEmpty() ) )
        .WillOnce( testing::SetArgReferee<0>( replica_br ) );
    EXPECT_CALL( *( client_conns.at( 1 ) ),
                 rpc_begin_transaction( _, cid, _, ElementsAre( p_20_29 ),
                                        IsEmpty(), IsEmpty() ) )
        .WillOnce( testing::SetArgReferee<0>( replica_br ) );

    preparer_result = preparer.prepare_multi_site_transaction(
        cid, {} /*write*/, {} /*read*/, replica_ckrs /*full replica*/,
        {} /*snapshot*/ );
    EXPECT_EQ( 2, preparer_result.size() );
    ckr_0 = preparer_result.at( 0 );
    ckr_1 = preparer_result.at( 1 );
    EXPECT_EQ( ckr_0.size(), 1);
    EXPECT_EQ( ckr_0.at( 0 ), create_cell_key_ranges( 0, 20, 29, 0, 1 ) );
    EXPECT_EQ( ckr_1.size(), 1 );
    EXPECT_EQ( ckr_1.at( 0 ), create_cell_key_ranges( 0, 20, 29, 0, 1 ) );

	std::vector<::cell_key_ranges> write_ckrs;
    write_ckrs.push_back( create_cell_key_ranges( 0, 0, 0, 0, 0 ) );
    std::vector<::cell_key_ranges> read_ckrs;
    read_ckrs.push_back( create_cell_key_ranges( 0, 20, 20, 0, 0 ) );

    begin_result single_br;
    single_br.status = exec_status_type::COMMAND_OK;
    EXPECT_CALL( *( client_conns.at( 0 ) ),
                 rpc_begin_transaction( _, cid, _, ElementsAre( p_0_9 ),
                                        ElementsAre( p_20_29 ), IsEmpty() ) )
        .WillOnce( testing::SetArgReferee<0>( single_br ) );

    preparer_result = preparer.prepare_multi_site_transaction(
        cid, write_ckrs, read_ckrs, {} /*full replica*/, {} /*snapshot*/ );

    EXPECT_EQ( 1, preparer_result.size() );
    ckr_0 = preparer_result.at( 0 );
    EXPECT_EQ( 2, ckr_0.size() );
    EXPECT_EQ( ckr_0.at( 0 ), create_cell_key_ranges( 0, 0, 0, 0, 0 ) );
    EXPECT_EQ( ckr_0.at( 1 ), create_cell_key_ranges( 0, 20, 20, 0, 0 ) );

    one_shot_sproc_result sproc_ret;
    sproc_ret.status = exec_status_type::COMMAND_OK;

    EXPECT_CALL(
        *( client_conns.at( 0 ) ),
        rpc_one_shot_sproc( _, cid, _, ElementsAre( p_0_9 ),
                            ElementsAre( p_20_29 ), IsEmpty(), _ /*name*/,
                            _ /* write ckrs*/, _ /*read_ckrs*/, _ /*args*/ ) )
        .WillOnce( testing::SetArgReferee<0>( sproc_ret ) );

    one_shot_sproc_result sproc_res;
    int dest = preparer.execute_one_shot_sproc_for_multi_site_system(
        sproc_res, cid, {} /*snapshot vector*/, write_ckrs, read_ckrs,
        "sproc_name", "args" );
    EXPECT_EQ( 0, dest );
    EXPECT_EQ( sproc_res.status, exec_status_type::COMMAND_OK );

    write_ckrs.push_back( create_cell_key_ranges( 0, 10, 10, 0, 0 ) );

    dest = preparer.execute_one_shot_sproc_for_multi_site_system(
        sproc_res, cid, {} /*snapshot vector*/, write_ckrs, read_ckrs,
        "sproc_name", "args" );
    EXPECT_EQ( -1, dest );
}

TEST_F( transaction_preparer_test, join_results ) {
    one_shot_scan_result              res;
    std::vector<one_shot_scan_result> results;
    std::vector<scan_arguments>       scan_args;

    // default empty should be all good
    join_scan_results( res, results, scan_args );
    EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );

    // error
    one_shot_scan_result err_res;
    err_res.status = exec_status_type::DATA_NOT_LOCATED_AT_ONE_SITE;
    results = {err_res};
    join_scan_results( res, results, scan_args );
    EXPECT_EQ( res.status, exec_status_type::DATA_NOT_LOCATED_AT_ONE_SITE );

    // now do some things
    scan_arguments scan_arg;
    scan_arg.label = 5;
    scan_args.emplace_back( scan_arg );

    one_shot_scan_result s1;
    one_shot_scan_result s2;
    s1.status = exec_status_type::COMMAND_OK;
    s2.status = exec_status_type::COMMAND_OK;

    result_cell c_0;
    result_cell c_1;
    result_cell c_2;

    c_0.col_id = 0;
    c_1.col_id = 1;
    c_2.col_id = 2;

    result_tuple r_0_0;
    result_tuple r_0_1;
    result_tuple r_0_3;
    result_tuple r_1_1_a;
    result_tuple r_1_1_b;

    r_0_0.table_id = 0;
    r_0_0.row_id = 0;
    r_0_0.cells = {c_0, c_2, c_1};

    r_0_1.table_id = 0;
    r_0_1.row_id = 1;
    r_0_1.cells = {c_2, c_1, c_0};

    r_0_3.table_id = 0;
    r_0_3.row_id = 3;
    r_0_3.cells = {c_1, c_0, c_2};

    r_1_1_a.table_id = 1;
    r_1_1_a.row_id = 1;
    r_1_1_a.cells = {c_1};

    r_1_1_b.table_id = 1;
    r_1_1_b.row_id = 1;
    r_1_1_b.cells = {c_0, c_2};

    s1.res_tuples[5] = {r_0_0, r_0_1, r_1_1_a};
    s2.res_tuples[5] = {r_0_0, r_0_3, r_1_1_b};

    results = {s1, s2};
    join_scan_results( res, results, scan_args );

    EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );
    EXPECT_EQ( 1, res.res_tuples.size() );
    EXPECT_EQ( 1, res.res_tuples.count( 5 ) );
    // r_0_0, r_0_1, r_1_1, r_0_3
    EXPECT_EQ( 4, res.res_tuples.at( 5 ).size() );

    std::vector<int32_t> expected_tables = {0, 0, 1, 0};
    std::vector<int64_t> expected_rows = {0, 1, 1, 3};
    std::vector<result_cell> expected_cells = {c_0, c_1, c_2};

    EXPECT_EQ( expected_tables.size(), expected_rows.size() );
    EXPECT_EQ( expected_tables.size(), res.res_tuples.at( 5 ).size() );

    for( uint32_t pos = 0; pos < expected_tables.size(); pos++ ) {
        const auto &res_tuple = res.res_tuples.at( 5 ).at( pos );
        EXPECT_EQ( expected_tables.at( pos ), res_tuple.table_id );
        EXPECT_EQ( expected_rows.at( pos ), res_tuple.row_id );
        EXPECT_EQ( expected_cells.size(), res_tuple.cells.size() );
        for( uint32_t cell_pos = 0; cell_pos < expected_cells.size();
             cell_pos++ ) {
            EXPECT_EQ( expected_cells.at( cell_pos ).col_id,
                       res_tuple.cells.at( cell_pos ).col_id );
        }
    }
}

TEST_F( transaction_preparer_test, test_multi_site_scan ) {
    int  num_sites = 2;
    auto static_configs = k_cost_model_test_configs;

    auto cost_model2 = std::make_shared<cost_modeller2>( static_configs );
    auto site_partition_version_info =
        std::make_shared<sites_partition_version_information>( cost_model2 );

    int partition_size = 10;
    k_ycsb_partition_size = partition_size;
    int num_cols_per_part = 2;
    k_ycsb_column_partition_size = num_cols_per_part;
    std::vector<storage_tier_type::type> acceptable_storage_types = {
        storage_tier_type::type::MEMORY, storage_tier_type::type::DISK};

    std::shared_ptr<partition_data_location_table> data_loc_tab =
        construct_partition_data_location_table(
            construct_partition_data_location_table_configs(
                num_sites, acceptable_storage_types ),
            workload_type::YCSB );

    auto stat_enumerator_holder =
        std::make_shared<stat_tracked_enumerator_holder>(
            construct_stat_tracked_enumerator_configs(
                false /* don't track stats*/ ) );

    auto query_predictor = std::make_shared<query_arrival_predictor>(
        construct_query_arrival_predictor_configs(
            true /* use query arrival predictor*/,
            query_arrival_predictor_type::SIMPLE_QUERY_PREDICTOR,
            0.5 /* threshold */, 30 /* time bucket */, 10 /* count bucket */,
            1.0 /* score scalar */, 0.001 /* default score */ ) );

    add_tables_to_query_arrival_predictor( query_predictor, data_loc_tab );

    auto tier_tracking = std::make_shared<partition_tier_tracking>(
        data_loc_tab, cost_model2, query_predictor );

    mock_site_evaluator *mk_s_eval = new mock_site_evaluator(
        cost_model2, data_loc_tab, site_partition_version_info, query_predictor,
        stat_enumerator_holder, tier_tracking );
    mk_s_eval->num_sites_ = num_sites;
    mk_s_eval->configs_ =
        construct_heuristic_site_evaluator_configs( num_sites );
    mk_s_eval->configs_.plan_scan_in_cost_order_ = true;
    mk_s_eval->configs_.plan_scan_limit_changes_ = true;
    mk_s_eval->configs_.plan_scan_limit_ratio_ =
        1 /* this is somewhat dumb, but we need to set it high to get the change */;

    mk_s_eval->num_update_destinations_per_site_ = 1;
    mock_site_evaluator *mk_s_eval_copy = mk_s_eval;
    // we don't own this pointer, but we need a copy of it to do stuff in this
    // test

    std::unique_ptr<abstract_site_evaluator> evaluator(
        std::move( mk_s_eval_copy ) );

    transaction_preparer_configs configs =
        construct_transaction_preparer_configs(
            /* num threads*/ 1, /*num clients*/ 3, 1 /* astream_mod */,
            10 /* astream_empty_wait_ms */,
            10 /* astream_tracking_interval_ms */,
            10 /* astream_max_queue_size */, 0.5 /* max_write_blend_rate */,
            1 /* background cid*/, 2 /* periodic cid */,
            true /*use_background_worker */,
            1 /*num_background_work_items_to_execute*/,
            ss_mastering_type::ADAPT, 0 /* estore_clay_periodic_interval_ms */,
            0.025 /* estore_load_epsilon_threshold */,
            0.01 /* estore_hot_partition_pct_thresholds */,
            1.5 /*adr multiplier*/, true /* enable plan reuse */,
            true /* enable wait for plan reuse */,
            partition_lock_mode::no_lock /* optimistic*/,
            partition_lock_mode::try_lock /* physical adjustment*/,
            partition_lock_mode::try_lock /* force*/,
            construct_periodic_site_selector_operations_configs( 1, 10 ) );

    std::unique_ptr<std::vector<client_conn_pool>> pool(
        new std::vector<client_conn_pool>() );
    std::vector<mock_sm_client_conn *> client_conns;
    for( uint32_t cid = 0; cid < configs.num_clients_ - 1; cid++ ) {
        std::unique_ptr<std::vector<sm_client_ptr>> loc_clients(
            new std::vector<sm_client_ptr>() );
        for( int site = 0; site < num_sites; site++ ) {
            mock_sm_client_conn *cli_conn = new mock_sm_client_conn();
            mock_sm_client_conn *cli_conn_copy = cli_conn;
            client_conns.push_back( cli_conn );

            sm_client_ptr client( std::move( cli_conn_copy ) );
            loc_clients->push_back( std::move( client ) );
        }

        pool->push_back( std::move( loc_clients ) );
    }

    std::unique_ptr<std::vector<sm_client_ptr>> clients(
        new std::vector<sm_client_ptr>() );
    std::vector<propagation_configuration> site_prop_configs;
    for( int site = 0; site < num_sites; site++ ) {
        mock_sm_client_conn *cli_conn = new mock_sm_client_conn();

        site_statistics_results stat_result;
        stat_result.status = exec_status_type::COMMAND_OK;

        propagation_configuration prop_config;
        prop_config.type = propagation_type::VECTOR;
        prop_config.partition = site;
        prop_config.offset = 0;
        std::vector<propagation_configuration> prop_configs = {prop_config};
        stat_result.prop_configs = prop_configs;
        std::vector<int64_t> prop_counts = {0};
        stat_result.prop_counts = prop_counts;

        EXPECT_CALL( *cli_conn, rpc_get_site_statistics( _ ) )
            .WillRepeatedly( testing::SetArgReferee<0>( stat_result ) );

        mock_sm_client_conn *cli_conn_copy = cli_conn;
        client_conns.push_back( cli_conn );

        sm_client_ptr client( std::move( cli_conn_copy ) );
        clients->push_back( std::move( client ) );
        site_prop_configs.push_back( prop_config );
    }
    pool->push_back( std::move( clients ) );

    std::shared_ptr<client_conn_pools> conn_pool(
        new client_conn_pools( std::move( pool ), configs.num_clients_,
                               configs.num_worker_threads_per_client_ ) );

    cost_model_prediction_holder model_prediction;
    transaction_preparer         preparer(
        conn_pool, data_loc_tab, cost_model2, site_partition_version_info,
        std::move( evaluator ), query_predictor, stat_enumerator_holder,
        tier_tracking, configs );

    // insert
    mk_s_eval->insert_only_destination_ = 1;
    mk_s_eval->insert_partition_type_ = partition_type::type::COLUMN;
    mk_s_eval->insert_storage_type_ = storage_tier_type::type::DISK;
    ::clientid                   cid = 0;
    std::vector<cell_key_ranges> ckrs;
    ckrs.push_back(
        create_cell_key_ranges( 0, 0, 19, 0, num_cols_per_part - 1 ) );
    auto p_0_9 =
        create_partition_column_identifier( 0, 0, 9, 0, num_cols_per_part - 1 );
    auto p_10_19 = create_partition_column_identifier( 0, 10, 19, 0,
                                                       num_cols_per_part - 1 );

    commit_result cr;
    cr.status = exec_status_type::COMMAND_OK;
    EXPECT_CALL(
        *( client_conns.at( 1 ) ),
        rpc_add_partitions( _, cid, _, ElementsAre( p_0_9, p_10_19 ),
                            mk_s_eval->insert_only_destination_,
                            ElementsAre( partition_type::type::COLUMN,
                                         partition_type::type::COLUMN ),
                            ElementsAre( storage_tier_type::type::DISK,
                                         storage_tier_type::type::DISK ),
                            _ ) )
        .WillOnce( testing::SetArgReferee<0>( cr ) );

    begin_result br;
    br.status = exec_status_type::COMMAND_OK;
    EXPECT_CALL(
        *( client_conns.at( 1 ) ),
        rpc_begin_transaction( _, cid, _, ElementsAre( p_0_9, p_10_19 ),
                               IsEmpty(), IsEmpty() ) )
        .WillOnce( testing::SetArgReferee<0>( br ) );

    int insert_destination_site =
        preparer.prepare_transaction( cid, ckrs, {}, {} );
    EXPECT_EQ( insert_destination_site, mk_s_eval->insert_only_destination_ );

    // do a read
    ckrs.clear();
    ckrs.emplace_back(
        create_cell_key_ranges( 0, 15, 19, 0, num_cols_per_part - 1 ) );
    br.status = exec_status_type::COMMAND_OK;
    EXPECT_CALL( *( client_conns.at( 1 ) ),
                 rpc_begin_transaction( _, cid, _, IsEmpty(),
                                        ElementsAre( p_10_19 ), IsEmpty() ) )
        .WillOnce( testing::SetArgReferee<0>( br ) );

    int no_change_destination_site =
        preparer.prepare_transaction( cid, {}, ckrs, {} );
    EXPECT_EQ( no_change_destination_site,
               mk_s_eval->insert_only_destination_ );

    // insert partition 20-29
    mk_s_eval->insert_only_destination_ = 0;
    mk_s_eval->insert_partition_type_ = partition_type::type::ROW;
    mk_s_eval->insert_storage_type_ = storage_tier_type::type::MEMORY;
    ckrs.clear();
    ckrs.emplace_back(
        create_cell_key_ranges( 0, 20, 29, 0, num_cols_per_part - 1 ) );
    auto p_20_29 = create_partition_column_identifier( 0, 20, 29, 0, 1 );

    cr.status = exec_status_type::COMMAND_OK;
    EXPECT_CALL( *( client_conns.at( 0 ) ),
                 rpc_add_partitions(
                     _, cid, _, ElementsAre( p_20_29 ),
                     mk_s_eval->insert_only_destination_,
                     ElementsAre( partition_type::type::ROW ),
                     ElementsAre( storage_tier_type::type::MEMORY ), _ ) )
        .WillOnce( testing::SetArgReferee<0>( cr ) );

    br.status = exec_status_type::COMMAND_OK;
    EXPECT_CALL( *( client_conns.at( 0 ) ),
                 rpc_begin_transaction( _, cid, _, ElementsAre( p_20_29 ),
                                        IsEmpty(), IsEmpty() ) )
        .WillOnce( testing::SetArgReferee<0>( br ) );

    insert_destination_site = preparer.prepare_transaction( cid, ckrs, {}, {} );
    EXPECT_EQ( insert_destination_site, mk_s_eval->insert_only_destination_ );

    // now do a scan on p_0_9 and p_20_22
    one_shot_scan_result  one_scan_ret;
    char *                one_scan_buff = NULL;

    std::vector<arg_code> one_scan_arg_codes = {};
    std::vector<void *>   one_scan_arg_ptrs = {};
    size_t                one_scan_serialize_len = serialize_for_sproc(
        one_scan_arg_codes, one_scan_arg_ptrs, &one_scan_buff );
    EXPECT_GE( one_scan_serialize_len, 4 );
    std::string one_scan_str_buff( one_scan_buff, one_scan_serialize_len );
    EXPECT_GE( one_scan_str_buff.size(), 4 );

    predicate_chain pred_chain;
    cell_predicate  c_pred;
    c_pred.table_id = 0;
    c_pred.col_id = 0;
    c_pred.type = data_type::type::STRING;
    c_pred.data = "foo";
    c_pred.predicate = predicate_type::type::LESS_THAN_OR_EQUAL;
    predicate_chain pred;
    pred.and_predicates.emplace_back( c_pred );

    ckrs.clear();
    ckrs.emplace_back( create_cell_key_ranges( 0, 0, 9, 0, 0 ) );
    ckrs.emplace_back( create_cell_key_ranges( 0, 21, 29, 0, 0 ) );
    scan_arguments scan_arg;
    scan_arg.label = 0;
    scan_arg.predicate = pred;
    scan_arg.read_ckrs = ckrs;
    std::vector<scan_arguments> scan_args = {scan_arg};

    // a subsequent locking call should also work
    one_shot_scan_result scan1_res;
    scan1_res.status = exec_status_type::COMMAND_OK;

    one_shot_scan_result scan2_res;
    scan2_res.status = exec_status_type::COMMAND_OK;

    result_cell c_0;
    result_cell c_1;
    result_cell c_2;

    c_0.col_id = 0;
    c_1.col_id = 1;
    c_2.col_id = 2;

    result_tuple r_0_0;
    result_tuple r_0_1;
    result_tuple r_0_3;
    result_tuple r_1_1_a;
    result_tuple r_1_1_b;

    r_0_0.table_id = 0;
    r_0_0.row_id = 0;
    r_0_0.cells = {c_0, c_2, c_1};

    r_0_1.table_id = 0;
    r_0_1.row_id = 1;
    r_0_1.cells = {c_2, c_1, c_0};

    r_0_3.table_id = 0;
    r_0_3.row_id = 3;
    r_0_3.cells = {c_1, c_0, c_2};

    r_1_1_a.table_id = 1;
    r_1_1_a.row_id = 1;
    r_1_1_a.cells = {c_1};

    r_1_1_b.table_id = 1;
    r_1_1_b.row_id = 1;
    r_1_1_b.cells = {c_0, c_2};

    // not the right mapping but w/e
    scan2_res.res_tuples[0] = {r_0_0, r_0_1, r_1_1_a};
    scan1_res.res_tuples[0] = {r_0_0, r_0_3, r_1_1_b};

    scan_arguments s1_scan_args = scan_arg;
    scan_arguments s2_scan_args = scan_arg;

    s1_scan_args.read_ckrs.clear();
    s2_scan_args.read_ckrs.clear();

    s2_scan_args.read_ckrs.emplace_back(
        create_cell_key_ranges( 0, 21, 29, 0, 0 ) );
    s1_scan_args.read_ckrs.emplace_back(
        create_cell_key_ranges( 0, 0, 9, 0, 0 ) );

    // MTODO check the scan args and the name, set the return vals
    EXPECT_CALL(
        *( client_conns.at( 1 ) ),
        rpc_one_shot_scan( _, cid, _, ElementsAre( p_0_9 ),
                           IsEmpty() /* inflight*/, Eq( "scan_sproc" ) /*name*/,
                           ElementsAre( s1_scan_args ), _ /* sproc args */
                           ) )
        .WillOnce( testing::SetArgReferee<0>( scan2_res ) );

    EXPECT_CALL(
        *( client_conns.at( 0 ) ),
        rpc_one_shot_scan( _, cid, _, ElementsAre( p_20_29 ),
                           IsEmpty() /* inflight*/, Eq( "scan_sproc" ) /*name*/,
                           ElementsAre( s2_scan_args ), _ /* sproc args */
                           ) )
        .WillOnce( testing::SetArgReferee<0>( scan1_res ) );

    auto scan_dests = preparer.execute_one_shot_scan(
        one_scan_ret, cid, {} /* cli state */, "scan_sproc", scan_args,
        one_scan_str_buff, true /* allow missing data */,
        false /* allow force change */ );
    EXPECT_EQ( 2, scan_dests.size() );
    bool s1_first = false;
    if ( scan_dests.at(0) == 0) {
        EXPECT_EQ( scan_dests.at( 0 ), 0 );
        EXPECT_EQ( scan_dests.at( 1 ), 1 );
        s1_first = true;
    } else {
        EXPECT_EQ( scan_dests.at( 0 ), 1 );
        EXPECT_EQ( scan_dests.at( 1 ), 0 );
    }

    // MTODO check the result of the join
    EXPECT_EQ( one_scan_ret.status, exec_status_type::COMMAND_OK );
    EXPECT_EQ( 1, one_scan_ret.res_tuples.size() );
    EXPECT_EQ( 1, one_scan_ret.res_tuples.count( 0 ) );
    // r_0_0, r_0_1, r_1_1, r_0_3
    EXPECT_EQ( 4, one_scan_ret.res_tuples.at( 0 ).size() );

    std::vector<int32_t> expected_tables = {0, 0, 1, 0};
    std::vector<int64_t> expected_rows = {0, 1, 1, 3};
    if ( s1_first ) {
        expected_rows = {0, 3, 1, 1};
        expected_tables = {0, 0, 1, 0};
    }
    std::vector<result_cell> expected_cells = {c_0, c_1, c_2};

    EXPECT_EQ( expected_tables.size(), expected_rows.size() );
    EXPECT_EQ( expected_tables.size(), one_scan_ret.res_tuples.at( 0 ).size() );

    for( uint32_t pos = 0; pos < expected_tables.size(); pos++ ) {
        const auto &res_tuple = one_scan_ret.res_tuples.at( 0 ).at( pos );
        EXPECT_EQ( expected_tables.at( pos ), res_tuple.table_id );
        EXPECT_EQ( expected_rows.at( pos ), res_tuple.row_id );
        EXPECT_EQ( expected_cells.size(), res_tuple.cells.size() );
        for( uint32_t cell_pos = 0; cell_pos < expected_cells.size();
             cell_pos++ ) {
            EXPECT_EQ( expected_cells.at( cell_pos ).col_id,
                       res_tuple.cells.at( cell_pos ).col_id );
        }
    }

    DVLOG( 40 ) << "FORCE CHANGE";
    mk_s_eval->force_change_ = true;
    // trigger a force change, this should change p_0_9 from disk to memory and
    // column to row (lowest cost in our example) at site 1
    commit_result change_res;
    change_res.status = exec_status_type::COMMAND_OK;

    EXPECT_CALL( *( client_conns.at( 1 ) ),
                 rpc_change_partition_type(
                     _, cid, ElementsAre( p_0_9 ),
                     ElementsAre( partition_type::type::ROW ),
                     ElementsAre( storage_tier_type::type::MEMORY ) ) )
        .WillOnce( testing::SetArgReferee<0>( change_res ) );

    EXPECT_CALL( *( client_conns.at( 1 ) ),
                 rpc_one_shot_scan(
                     _, cid, _, ElementsAre( p_0_9 ), IsEmpty() /* inflight */,
                     Eq( "scan_sproc" ) /*name */, ElementsAre( s1_scan_args ),
                     _ /* sproc_args */ ) )
        .WillOnce( testing::SetArgReferee<0>( scan2_res ) );

    EXPECT_CALL(
        *( client_conns.at( 0 ) ),
        rpc_one_shot_scan( _, cid, _, ElementsAre( p_20_29 ),
                           IsEmpty() /* inflight*/, Eq( "scan_sproc" ) /*name*/,
                           ElementsAre( s2_scan_args ), _ /* sproc args */
                           ) )
        .WillOnce( testing::SetArgReferee<0>( scan1_res ) );

    scan_dests = preparer.execute_one_shot_scan(
        one_scan_ret, cid, {} /* cli state */, "scan_sproc", scan_args,
        one_scan_str_buff, true /* allow missing data */,
        true /* allow force change */ );
    EXPECT_EQ( 2, scan_dests.size() );

    s1_first = false;
    if ( scan_dests.at(0) == 0) {
        EXPECT_EQ( scan_dests.at( 0 ), 0 );
        EXPECT_EQ( scan_dests.at( 1 ), 1 );
        s1_first = true;
    } else {
        EXPECT_EQ( scan_dests.at( 0 ), 1 );
        EXPECT_EQ( scan_dests.at( 1 ), 0 );
    }

    // MTODO check the result of the join
    EXPECT_EQ( one_scan_ret.status, exec_status_type::COMMAND_OK );
    EXPECT_EQ( 1, one_scan_ret.res_tuples.size() );
    EXPECT_EQ( 1, one_scan_ret.res_tuples.count( 0 ) );
    // r_0_0, r_0_1, r_1_1, r_0_3
    EXPECT_EQ( 4, one_scan_ret.res_tuples.at( 0 ).size() );

    expected_tables = {0, 0, 1, 0};
    expected_rows = {0, 1, 1, 3};
    if ( s1_first ) {
        expected_rows = {0, 3, 1, 1};
        expected_tables = {0, 0, 1, 0};
    }
    expected_cells = {c_0, c_1, c_2};

    EXPECT_EQ( expected_tables.size(), expected_rows.size() );
    EXPECT_EQ( expected_tables.size(), one_scan_ret.res_tuples.at( 0 ).size() );

    for( uint32_t pos = 0; pos < expected_tables.size(); pos++ ) {
        const auto &res_tuple = one_scan_ret.res_tuples.at( 0 ).at( pos );
        EXPECT_EQ( expected_tables.at( pos ), res_tuple.table_id );
        EXPECT_EQ( expected_rows.at( pos ), res_tuple.row_id );
        EXPECT_EQ( expected_cells.size(), res_tuple.cells.size() );
        for( uint32_t cell_pos = 0; cell_pos < expected_cells.size();
             cell_pos++ ) {
            EXPECT_EQ( expected_cells.at( cell_pos ).col_id,
                       res_tuple.cells.at( cell_pos ).col_id );
        }
    }
}
