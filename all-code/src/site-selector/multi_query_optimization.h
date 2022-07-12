#pragma once

#include <vector>
#include <memory>

#include "../common/partition_funcs.h"
#include "multi_query_tracking.h"
#include "partition_payload.h"

void build_multi_query_plan_entry(
    std::shared_ptr<multi_query_plan_entry>& mq_plan_entry,
    const partition_column_identifier_set&   write_partitions_set,
    const partition_column_identifier_set&   read_partitions_set );

void add_multi_query_plan_entry_to_partitions(
    std::shared_ptr<multi_query_plan_entry>&               mq_plan_entry,
    const std::vector<std::shared_ptr<partition_payload>>& partitions );

transaction_query_entry build_transaction_query_entry(
    const std::vector<std::shared_ptr<partition_payload>>& write_partitions,
    const std::vector<std::shared_ptr<partition_payload>>& read_partitions,
    const partition_column_identifier_set&                 write_partitions_set,
    const partition_column_identifier_set& read_partitions_set );
