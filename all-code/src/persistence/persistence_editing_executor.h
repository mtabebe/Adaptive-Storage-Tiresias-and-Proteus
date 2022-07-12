#pragma once

#include <memory>

#include "../benchmark/workload_type.h"
#include "../common/constants.h"
#include "data_location_rewriter_interface.h"
#include "persistence_editor.h"
#include "rewriter_type.h"

void execute_rewriter(
    int num_sites, const std::vector<table_metadata>&       tables_metadata,
    const std::unordered_map<int, std::unordered_set<int>>& replica_locs,
    const partition_type::type&    master_type,
    const storage_tier_type::type& master_storage_type,
    const std::unordered_map<int, partition_type::type>&    replica_types,
    const std::unordered_map<int, storage_tier_type::type>& storage_types,
    const std::vector<std::vector<propagation_configuration>>& prop_configs,
    const persistence_configs&                                 per_configs,
    std::shared_ptr<update_destination_generator>              update_generator,
    std::shared_ptr<update_enqueuers> enqueuers, const std::string& in_ss,
    const std::string& in_sm, const std::string& in_part,
    const std::string& out_ss, const std::string& out_sm,
    const std::string& out_part, const workload_type w_type = k_workload_type,
    const rewriter_type re_type = k_rewriter_type,
    double prob_range_rewriter_threshold = k_prob_range_rewriter_threshold );

