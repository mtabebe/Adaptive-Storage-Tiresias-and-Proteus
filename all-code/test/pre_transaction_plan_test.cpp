#define GTEST_HAS_TR1_TUPLE 0

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <thread>

#include "../src/data-site/db/partition_metadata.h"
#include "../src/site-selector/pre_transaction_plan.h"

class pre_transaction_plan_test : public ::testing::Test {};

TEST_F( pre_transaction_plan_test, test ) {
    int master_site = 0;
    int replica_site = 1;

    pre_transaction_plan plan( nullptr );

    auto p_0_10 = create_partition_column_identifier( 0, 0, 10, 0, 1 );
    auto p_0_10_payload = std::make_shared<partition_payload>( p_0_10 );
    auto p_11_20 = create_partition_column_identifier( 0, 11, 20, 0, 1 );
    auto p_11_20_payload = std::make_shared<partition_payload>( p_11_20 );
    auto p_21_30 = create_partition_column_identifier( 0, 21, 30, 0, 1 );
    auto p_21_30_payload = std::make_shared<partition_payload>( p_21_30 );

    std::vector<partition_type::type> partition_types = {
        partition_type::type::ROW, partition_type::type::COLUMN};
    std::vector<storage_tier_type::type> storage_types = {
        storage_tier_type::type::DISK, storage_tier_type::type::MEMORY};
    std::vector<std::shared_ptr<partition_payload>> insert_partitions = {
        p_0_10_payload, p_11_20_payload, p_21_30_payload};

    partition_lock_mode lock_mode = partition_lock_mode::lock;

    for( auto payload : insert_partitions ) {
        payload->lock( lock_mode );
    }

    uint32_t update_destination_slot = 0;

    std::shared_ptr<pre_action> add_action = create_add_partition_pre_action(
        master_site, master_site, insert_partitions, partition_types,
        storage_types, update_destination_slot );
    plan.add_pre_action_work_item( add_action );

    cost_model_prediction_holder cost_model;
    std::shared_ptr<pre_action>  add_replica_action =
        create_add_replica_partition_pre_action(
            replica_site, master_site, {p_0_10}, {partition_type::type::ROW},
            {storage_tier_type::type::MEMORY}, {add_action}, 1.0, cost_model );
    plan.add_pre_action_work_item( add_replica_action );

    std::shared_ptr<pre_action> remaster_action =
        create_remaster_partition_pre_action(
            master_site, replica_site, {p_0_10},
            {add_action, add_replica_action}, 1.0, cost_model,
            {update_destination_slot} );
    plan.add_pre_action_work_item( remaster_action );

    plan.read_pids_ = {};
    plan.write_pids_ = {p_0_10, p_11_20};

    plan.build_work_queue();

    EXPECT_EQ( 3, plan.get_num_work_items() );

    plan.add_to_partition_mappings( insert_partitions );
    std::shared_ptr<pre_action> next_action =
        plan.get_next_pre_action_work_item();
    EXPECT_NE( next_action, nullptr );
    EXPECT_EQ( 0, next_action->id_ );
    EXPECT_EQ( pre_action_type::ADD_PARTITIONS, next_action->type_ );
    plan.unlock_payloads_if_able( 0, insert_partitions, lock_mode );
    p_21_30_payload->lock( lock_mode );
    p_21_30_payload->unlock( lock_mode );

    auto second_action = plan.get_next_pre_action_work_item();
    EXPECT_NE( second_action, nullptr );
    EXPECT_EQ( 1, second_action->id_ );
    EXPECT_EQ( pre_action_type::ADD_REPLICA_PARTITIONS, second_action->type_ );
    plan.unlock_payload_if_able( 1, p_0_10_payload, lock_mode );

    std::thread r = std::thread( [&second_action]() {
        action_outcome outcome = second_action->wait_to_satisfy_dependencies();
        EXPECT_EQ( outcome, action_outcome::OK );
    } );

    next_action->mark_complete( action_outcome::OK );
    r.join();

    next_action = plan.get_next_pre_action_work_item();
    EXPECT_NE( next_action, nullptr );
    EXPECT_EQ( 2, next_action->id_ );
    EXPECT_EQ( pre_action_type::REMASTER, next_action->type_ );
    plan.unlock_payload_if_able( 2, p_0_10_payload, lock_mode );

    next_action = plan.get_next_pre_action_work_item();
    EXPECT_EQ( next_action, nullptr );
    plan.unlock_remaining_payloads( lock_mode );

    // everthing should be unlocked;
    for( auto payload : insert_partitions ) {
        payload->lock( lock_mode );
    }
    unlock_payloads( insert_partitions, lock_mode );
}

TEST_F( pre_transaction_plan_test, test_failure ) {
    int master_site = 0;
    int replica_site = 1;

    pre_transaction_plan plan( nullptr );

    auto p_0_10 = create_partition_column_identifier( 0, 0, 10, 0, 1 );
    auto p_0_10_payload = std::make_shared<partition_payload>( p_0_10 );
    auto p_11_20 = create_partition_column_identifier( 0, 11, 20, 0, 1 );
    auto p_11_20_payload = std::make_shared<partition_payload>( p_11_20 );
    auto p_21_30 = create_partition_column_identifier( 0, 21, 30, 0, 1 );
    auto p_21_30_payload = std::make_shared<partition_payload>( p_21_30 );

    std::vector<std::shared_ptr<partition_payload>> insert_partitions = {
        p_0_10_payload, p_11_20_payload, p_21_30_payload};
    std::vector<partition_type::type> partition_types = {
        partition_type::type::ROW, partition_type::type::COLUMN,
        partition_type::type::SORTED_COLUMN,
        partition_type::type::MULTI_COLUMN,
        partition_type::type::SORTED_MULTI_COLUMN,
    };
    std::vector<storage_tier_type::type> storage_types = {
        storage_tier_type::type::MEMORY, storage_tier_type::type::MEMORY,
        storage_tier_type::type::DISK,   storage_tier_type::type::DISK,
        storage_tier_type::type::MEMORY,
    };

    partition_lock_mode lock_mode = partition_lock_mode::lock;

    for( auto payload : insert_partitions ) {
        payload->lock( lock_mode );
    }

    uint32_t                    update_destination_slot = 0;
    std::shared_ptr<pre_action> add_action = create_add_partition_pre_action(
        master_site, master_site, insert_partitions, partition_types,
        storage_types, update_destination_slot );
    plan.add_pre_action_work_item( add_action );

    cost_model_prediction_holder cost_model;
    std::shared_ptr<pre_action>  add_replica_action =
        create_add_replica_partition_pre_action(
            replica_site, master_site, {p_0_10}, {partition_type::type::ROW},
            {storage_tier_type::type::MEMORY}, {add_action}, 1.0, cost_model );
    plan.add_pre_action_work_item( add_replica_action );

    std::shared_ptr<pre_action> remaster_action =
        create_remaster_partition_pre_action(
            master_site, replica_site, {p_0_10},
            {add_action, add_replica_action}, 1.0, cost_model,
            {update_destination_slot} );
    plan.add_pre_action_work_item( remaster_action );

    plan.read_pids_ = {};
    plan.write_pids_ = {p_0_10, p_11_20};

    plan.build_work_queue();

    EXPECT_EQ( 3, plan.get_num_work_items() );

    plan.add_to_partition_mappings( insert_partitions );
    std::shared_ptr<pre_action> next_action =
        plan.get_next_pre_action_work_item();
    EXPECT_NE( next_action, nullptr );
    EXPECT_EQ( 0, next_action->id_ );
    EXPECT_EQ( pre_action_type::ADD_PARTITIONS, next_action->type_ );
    plan.unlock_payloads_if_able( 0, insert_partitions, lock_mode );
    EXPECT_FALSE( p_0_10_payload->try_lock() );
    EXPECT_FALSE( p_11_20_payload->try_lock() );
    EXPECT_TRUE( p_21_30_payload->try_lock() );  // it's not used anywhere

    auto second_action = plan.get_next_pre_action_work_item();
    EXPECT_NE( second_action, nullptr );
    EXPECT_EQ( 1, second_action->id_ );
    EXPECT_EQ( pre_action_type::ADD_REPLICA_PARTITIONS, second_action->type_ );

    std::thread r = std::thread( [&second_action]() {
        action_outcome outcome = second_action->wait_to_satisfy_dependencies();
        EXPECT_EQ( outcome, action_outcome::FAILED );
    } );

    next_action->mark_complete( action_outcome::FAILED );
    r.join();

    plan.unlock_payload_if_able( 1, p_0_10_payload, lock_mode );
    EXPECT_FALSE( p_0_10_payload->try_lock() );

    plan.mark_plan_as_failed();
    EXPECT_FALSE( p_0_10_payload->try_lock() );

    plan.unlock_payloads_because_of_failure( lock_mode );

    EXPECT_TRUE( p_0_10_payload->try_lock() );
    EXPECT_TRUE( p_11_20_payload->try_lock() );

    unlock_payloads( insert_partitions, lock_mode );
}
