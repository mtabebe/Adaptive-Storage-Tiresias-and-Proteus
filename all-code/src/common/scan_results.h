#pragma once

#include <unordered_map>
#include <vector>

#include "../gen-cpp/gen-cpp/proto_types.h"

void join_scan_results(
    one_shot_scan_result&                    _return,
    const std::vector<one_shot_scan_result>& per_site_results,
    const std::vector<scan_arguments>&       scan_args );

std::unordered_map<int, scan_arguments> map_scan_args(
    const std::vector<scan_arguments>& scan_args );

void get_scan_read_ckrs(
    const std::unordered_map<int, scan_arguments>& scan_args,
    std::vector<cell_key_ranges>& ckrs );
void get_scan_read_ckrs( const std::vector<scan_arguments>& scan_args,
                         std::vector<cell_key_ranges>&      ckrs );

