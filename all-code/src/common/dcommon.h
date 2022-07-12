#pragma once
#include "../site-selector/client_conn_pool.h"
#include "../site-selector/site_selector_handler.h"
#include "../gen-cpp/gen-cpp/site_manager.h"
#include <memory>
#include <vector>

bool create_site_managers_from_file(
    std::string file_name, bool bind_to_nic, std::string nic_name,
    std::unique_ptr<std::vector<client_conn_pool>> const& pool,
    std::unique_ptr<std::unordered_map<int, site_manager_location>> const&
        site_manager_locs,
    int num_clients, int num_threads );
