#pragma once


#include <memory>
#include <string>
#include <unordered_map>
#include <chrono>

#include "../gen-cpp/gen-cpp/SiteSelector.h"
#include "client_conn_pool.h"
#include "ss_types.h"
#include "transaction_preparer.h"
#include "../common/constants.h"

struct site_manager_location {
    std::string ip_;
    int         port_;
    site_manager_location( std::string ip, int port )
        : ip_( ip ), port_( port ) {}
};

struct decay_thread_timer {
    std::chrono::high_resolution_clock::time_point decay_timeout_;
    ::clientid id_;
    decay_thread_timer( std::chrono::high_resolution_clock::time_point decay_timeout, ::clientid id )
        : decay_timeout_( decay_timeout ), id_( id ) { }

    bool operator<( const decay_thread_timer &o ) const {
        return decay_timeout_ < o.decay_timeout_;
    }
};

class site_selector_handler : public SiteSelectorIf {
   public:
    site_selector_handler(
        std::unique_ptr<std::unordered_map<int, site_manager_location>>,
        std::unique_ptr<transaction_preparer> preparer,
        ss_mastering_type mastering_type = k_ss_mastering_type );
    ~site_selector_handler();

    void rpc_begin_transaction(
        std::vector<site_selector_begin_result>& _return, const ::clientid id,
        const ::snapshot_vector&          client_session_version_vector,
        const std::vector<::cell_key_ranges>& write_set,
        const std::vector<::cell_key_ranges>& read_set );

    void rpc_begin_fully_replicated_transaction(
        std::vector<site_selector_begin_result>& _return, const ::clientid id,
        const ::snapshot_vector&          client_session_version_vector,
        const std::vector<::cell_key_ranges>& fully_replicated_write_set );

    void rpc_one_shot_sproc(
        one_shot_sproc_result& _return, const ::clientid id,
        const ::snapshot_vector&          client_session_version_vector,
        const std::vector<::cell_key_ranges>& write_set,
        const std::vector<::cell_key_ranges>& read_set, const std::string& name,
        const std::string& sproc_args, bool allow_force_change,
        const previous_context_timer_information& previous_contexts );
    // doesn't handle creating partition
    void rpc_one_shot_scan(
        one_shot_scan_result& _return, const ::clientid id,
        const ::snapshot_vector&           client_session_version_vector,
        const std::vector<scan_arguments>& scan_args, const std::string& name,
        const std::string& sproc_args, bool allow_missing_data,
        bool                                      allow_force_change,
        const previous_context_timer_information& previous_contexts );

    void rpc_persist_state_to_files( persistence_result& _return,
                                     const ::clientid    id,
                                     const std::string&  out_ss_name,
                                     const std::string&  out_db_name,
                                     const std::string&  out_part_name );
    void rpc_restore_state_from_files( persistence_result& _return,
                                       const ::clientid    id,
                                       const std::string&  in_ss_name,
                                       const std::string&  in_db_name,
                                       const std::string&  in_part_name );

    void rpc_wait_for_stable_state_among_sites( persistence_result& _return,
                                                const ::clientid    id );

   private:
    void internal_rpc_begin_transaction(
        std::vector<site_selector_begin_result>& _return, const ::clientid id,
        const ::snapshot_vector&          client_session_version_vector,
        const std::vector<::cell_key_ranges>& write_set,
        const std::vector<::cell_key_ranges>& read_set,
        const std::vector<::cell_key_ranges>& full_replica_set );

    void begin_transaction_on_behalf_of_client(
        std::vector<site_selector_begin_result>& _return, const ::clientid id,
        const ::snapshot_vector&          client_session_version_vector,
        const std::vector<::cell_key_ranges>& write_set,
        const std::vector<::cell_key_ranges>& read_set,
        const std::vector<::cell_key_ranges>& full_replica_set );

    std::unique_ptr<std::unordered_map<int, site_manager_location>>
                                          site_manager_locs_;
    std::unique_ptr<transaction_preparer> preparer_;
    ss_mastering_type                     mastering_type_;
};

