#include "multi_query_optimization.h"

#include <glog/logging.h>

void build_multi_query_plan_entry(
    std::shared_ptr<multi_query_plan_entry>& mq_plan_entry,
    const partition_column_identifier_set&   write_partitions_set,
    const partition_column_identifier_set&   read_partitions_set ) {

    mq_plan_entry->set_pid_sets( write_partitions_set, read_partitions_set );
    auto mq_plan = std::make_shared<multi_query_plan>( -1 );
    mq_plan_entry->set_plan( mq_plan, multi_query_plan_type::WAITING );
}

void add_multi_query_plan_entry_to_partitions(
    std::shared_ptr<multi_query_plan_entry>&               mq_plan_entry,
    const std::vector<std::shared_ptr<partition_payload>>& partitions ) {
    for( const auto& part : partitions ) {
        part->multi_query_entry_->add_entry( mq_plan_entry );
    }
}
transaction_query_entry build_transaction_query_entry(
    const std::vector<std::shared_ptr<partition_payload>>& write_partitions,
    const std::vector<std::shared_ptr<partition_payload>>& read_partitions,
    const partition_column_identifier_set&                 write_partitions_set,
    const partition_column_identifier_set& read_partitions_set ) {
    transaction_query_entry query_entry;

    for( const auto& part : write_partitions ) {
        part->multi_query_entry_->update_transaction_query_entry(
            query_entry, write_partitions_set, read_partitions_set );
    }
    for( const auto& part : read_partitions ) {
        part->multi_query_entry_->update_transaction_query_entry(
            query_entry, write_partitions_set, read_partitions_set );
    }

    return query_entry;
}
