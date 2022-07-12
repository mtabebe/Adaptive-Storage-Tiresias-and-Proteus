#define GTEST_HAS_TR1_TUPLE 0

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>

#include "../src/data-site/db/partition_metadata.h"
#include "../src/site-selector/partition_payload.h"

class partition_payload_test : public ::testing::Test {};

TEST_F( partition_payload_test, partition_location_information ) {

    std::shared_ptr<partition_location_information> pr =
        std::make_shared<partition_location_information>();
    EXPECT_EQ( 0, pr->version_ );
    EXPECT_EQ( k_unassigned_master, pr->master_location_ );
    EXPECT_EQ( 0, pr->replica_locations_.size() );

    std::shared_ptr<partition_location_information> pr1 =
        std::make_shared<partition_location_information>();
    pr1->set_from_existing_location_information_and_increment_version( pr );
    pr1->master_location_ = 1;
    pr1->partition_types_[pr1->master_location_] = partition_type::type::ROW;
    EXPECT_EQ( 1, pr1->version_ );
    EXPECT_EQ( 1, pr1->master_location_ );
    auto found_part_type = pr1->get_partition_type( 1 );
    EXPECT_TRUE( std::get<0>( found_part_type ) );
    EXPECT_EQ( partition_type::type::ROW, std::get<1>( found_part_type ) );
    EXPECT_EQ( 0, pr1->replica_locations_.size() );

    std::shared_ptr<partition_location_information> pr2 =
        std::make_shared<partition_location_information>();
    pr2->set_from_existing_location_information_and_increment_version( pr1 );
    pr2->replica_locations_.emplace( 0 );
    pr2->partition_types_.emplace( 0, partition_type::type::ROW );

    EXPECT_EQ( 2, pr2->version_ );
    EXPECT_EQ( 1, pr2->master_location_ );
    EXPECT_EQ( 1, pr2->replica_locations_.size() );
    EXPECT_EQ( 1, pr2->replica_locations_.count( 0 ) );
    EXPECT_EQ( partition_type::type::ROW, pr2->partition_types_.at( 0 ) );
    EXPECT_EQ( 0, pr2->replica_locations_.count( 1 ) );
}

TEST_F( partition_payload_test, partition_payload ) {
    uint32_t tid = 0;
    auto     pid_0_9 = create_partition_column_identifier( tid, 0, 9, 0, 1 );
    auto     payload_0_9 = std::make_shared<partition_payload>( pid_0_9 );

    auto location_information = payload_0_9->get_location_information();
    EXPECT_EQ( nullptr, location_information );

    auto pr1 = std::make_shared<partition_location_information>();
    pr1->master_location_ = 1;
    pr1->partition_types_[1] = partition_type::type::ROW;
    pr1->version_ = 1;

    bool swapped = payload_0_9->compare_and_swap_location_information(
        pr1, location_information );
    EXPECT_TRUE( swapped );

    swapped = payload_0_9->compare_and_swap_location_information(
        pr1, location_information );
    EXPECT_FALSE( swapped );

    bool locked = payload_0_9->lock( partition_lock_mode::lock );
    EXPECT_TRUE( locked );

    location_information = payload_0_9->atomic_get_location_information();
    EXPECT_NE( nullptr, location_information );
    EXPECT_EQ( 1, location_information->version_ );
    EXPECT_EQ( 1, location_information->master_location_ );
    EXPECT_EQ( partition_type::type::ROW,
               location_information->partition_types_.at( 1 ) );

    auto pr2 = std::make_shared<partition_location_information>();
    pr2->set_from_existing_location_information_and_increment_version(
        location_information );
    pr2->replica_locations_.emplace( 0);
    pr2->partition_types_.emplace( 0, partition_type::type::COLUMN );
    payload_0_9->set_location_information( pr2 );
    payload_0_9->unlock( partition_lock_mode::lock );

    locked = payload_0_9->lock( partition_lock_mode::no_lock );
    EXPECT_TRUE( locked );
    payload_0_9->unlock( partition_lock_mode::no_lock );
}

TEST_F( partition_payload_test, partition_payload_row_holder ) {
    auto pid_0_1 = create_partition_column_identifier( 0, 0, 9, 0, 1 );
    auto pid_0_5 = create_partition_column_identifier( 0, 0, 9, 0, 5 );
    auto pid_2_2 = create_partition_column_identifier( 0, 0, 9, 2, 2 );
    auto pid_3_5 = create_partition_column_identifier( 0, 0, 9, 3, 5 );
    auto pid_3_5_low = create_partition_column_identifier( 0, 0, 5, 3, 5 );
    auto pid_3_5_high = create_partition_column_identifier( 0, 6, 9, 3, 5 );
    auto pid_3_4_low = create_partition_column_identifier( 0, 0, 5, 3, 4 );
    auto pid_5_5_low = create_partition_column_identifier( 0, 0, 5, 5, 5 );

    auto pay_0_1 = std::make_shared<partition_payload>();
    pay_0_1->identifier_ = pid_0_1;
    auto pay_0_1_copy = std::make_shared<partition_payload>();
    pay_0_1_copy->identifier_ = pid_0_1;
    auto pay_0_5 = std::make_shared<partition_payload>();
    pay_0_5->identifier_ = pid_0_5;
    auto pay_2_2 = std::make_shared<partition_payload>();
    pay_2_2->identifier_ = pid_2_2;
    auto pay_3_5_low = std::make_shared<partition_payload>();
    pay_3_5_low->identifier_ = pid_3_5_low;
    auto pay_3_5_high = std::make_shared<partition_payload>();
    pay_3_5_high->identifier_ = pid_3_5_high;
    auto pay_3_4_low = std::make_shared<partition_payload>();
    pay_3_4_low->identifier_ = pid_3_4_low;
    auto pay_5_5_low = std::make_shared<partition_payload>();
    pay_5_5_low->identifier_ = pid_5_5_low;

    auto     holder = std::make_shared<partition_payload_row_holder>( 3 );
    cell_key ck;
    ck.table_id = 0;
    ck.col_id = 0;
    ck.row_id = 3;

    auto found = holder->get_payload( ck );
    EXPECT_EQ( found, nullptr );

    found = holder->get_or_create_payload( ck, pay_0_1 );
    EXPECT_NE( found, nullptr );
    EXPECT_EQ( pay_0_1.get(), found.get() );
    EXPECT_EQ( pid_0_1, found->identifier_ );

    ck.col_id = 1;
    found = holder->get_payload( ck );
    EXPECT_NE( found, nullptr );
    EXPECT_EQ( pay_0_1.get(), found.get() );
    EXPECT_EQ( pid_0_1, found->identifier_ );

    found = holder->get_or_create_payload( ck, pay_0_1_copy );
    EXPECT_NE( found, nullptr );
    EXPECT_EQ( pay_0_1.get(), found.get() );
    EXPECT_EQ( pid_0_1, found->identifier_ );

    ck.col_id = 2;
    found = holder->get_or_create_payload( ck, pay_2_2 );
    EXPECT_NE( found, nullptr );
    EXPECT_EQ( pay_2_2.get(), found.get() );
    EXPECT_EQ( pid_2_2, found->identifier_ );

    ck.col_id = 3;
    found = holder->get_payload( ck );
    EXPECT_EQ( found, nullptr );

    found = holder->get_or_create_payload( ck, pay_0_5 );
    EXPECT_EQ( found, nullptr );

    found = holder->get_or_create_payload_and_expand( ck, pay_0_5 );
    EXPECT_NE( found, nullptr );
    EXPECT_EQ( pay_0_5.get(), found.get() );
    EXPECT_EQ( pid_3_5, found->identifier_ );
    EXPECT_EQ( pid_3_5, pay_0_5->identifier_ );
    auto pay_3_5 = pay_0_5;

    bool ok = holder->remove_payload( pay_0_1 );
    EXPECT_TRUE( ok );

    ck.col_id = 0;
    found = holder->get_payload( ck );
    EXPECT_EQ( found, nullptr );

    ok = holder->split_payload( pay_3_5, pay_3_5_low, pay_3_5_high );
    EXPECT_TRUE( ok );

    ck.col_id = 3;
    found = holder->get_payload( ck );
    EXPECT_NE( found, nullptr );
    EXPECT_EQ( pay_3_5_low.get(), found.get() );
    EXPECT_EQ( pid_3_5_low, found->identifier_ );

    ok = holder->split_payload( pay_3_5_low, pay_3_4_low, pay_5_5_low );
    EXPECT_TRUE( ok );

    found = holder->get_payload( ck );
    EXPECT_NE( found, nullptr );
    EXPECT_EQ( pay_3_4_low.get(), found.get() );
    EXPECT_EQ( pid_3_4_low, found->identifier_ );

    ck.col_id = 5;
    found = holder->get_payload( ck );
    EXPECT_NE( found, nullptr );
    EXPECT_EQ( pay_5_5_low.get(), found.get() );
    EXPECT_EQ( pid_5_5_low, found->identifier_ );

    ok = holder->merge_payload( pay_3_4_low, pay_5_5_low, pay_3_5_low );
    EXPECT_TRUE( ok );

    for( int32_t col : {3, 4, 5} ) {
        ck.col_id = col;
        found = holder->get_payload( ck );
        EXPECT_NE( found, nullptr );
        EXPECT_EQ( pay_3_5_low.get(), found.get() );
        EXPECT_EQ( pid_3_5_low, found->identifier_ );
    }

    ok = holder->merge_payload( pay_3_5_low, pay_3_5_high, pay_3_5 );
    EXPECT_TRUE( ok );
    ck.col_id = 3;
    found = holder->get_payload( ck );
    EXPECT_NE( found, nullptr );
    EXPECT_EQ( pay_3_5.get(), found.get() );
    EXPECT_EQ( pid_3_5, found->identifier_ );
}
