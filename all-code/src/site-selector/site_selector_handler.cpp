#include "site_selector_handler.h"
#include "../gen-cpp/gen-cpp/dynamic_mastering_types.h"
#include "exceptions.h"
#include <glog/logging.h>
#include <glog/stl_logging.h>
#include <queue>

#include "../common/constants.h"
#include "../common/perf_tracking.h"

#define ss_catch_t_except                                          \
    catch( apache::thrift::TException & tx ) {                     \
        LOG( FATAL ) << "thrift exception for: " << id             \
                     << " is: " << tx.what();                      \
    }                                                              \
    catch( std::exception & e ) {                                  \
        LOG( FATAL ) << "error for :" << id << "is: " << e.what(); \
    }

site_selector_handler::site_selector_handler(
    std::unique_ptr<std::unordered_map<int, site_manager_location>>
                                          site_manager_locs,
    std::unique_ptr<transaction_preparer> preparer,
    ss_mastering_type                     mastering_type )
    : site_manager_locs_( std::move( site_manager_locs ) ),
      preparer_( std::move( preparer ) ),
      mastering_type_( mastering_type ) {}

site_selector_handler::~site_selector_handler() {}

void site_selector_handler::rpc_begin_transaction(
    std::vector<site_selector_begin_result>& _return, const ::clientid id,
    const ::snapshot_vector&              client_session_version_vector,
    const std::vector<::cell_key_ranges>& write_set,
    const std::vector<::cell_key_ranges>& read_set ) {

    std::vector<::cell_key_ranges> full_replica_set;
    internal_rpc_begin_transaction( _return, id, client_session_version_vector,
                                    write_set, read_set, full_replica_set );
}

void site_selector_handler::rpc_begin_fully_replicated_transaction(
    std::vector<site_selector_begin_result>& _return, const ::clientid id,
    const ::snapshot_vector&              client_session_version_vector,
    const std::vector<::cell_key_ranges>& fully_replicated_write_set ) {
    std::vector<::cell_key_ranges> read_set;
    std::vector<::cell_key_ranges> write_set;
    internal_rpc_begin_transaction( _return, id, client_session_version_vector,
                                    write_set, read_set,
                                    fully_replicated_write_set );
}

void site_selector_handler::internal_rpc_begin_transaction(
    std::vector<site_selector_begin_result>& _return, const ::clientid id,
    const ::snapshot_vector&              client_session_version_vector,
    const std::vector<::cell_key_ranges>& write_set,
    const std::vector<::cell_key_ranges>& read_set,
    const std::vector<::cell_key_ranges>& full_replica_set ) {

    DVLOG( k_rpc_handler_log_level )
        << "SS RPC: rpc_begin_transaction for client:" << id;

    start_timer( SS_RPC_BEGIN_TRANSACTION_TIMER_ID );

    try {
        site_selector_begin_result begin_result;

        begin_transaction_on_behalf_of_client(
            _return, id, client_session_version_vector, write_set, read_set,
            full_replica_set );
        DVLOG( 7 ) << "begin for client:" << id << ", okay!";
    } catch( site_selector_exception& e ) {
        LOG( WARNING ) << "Caught SS Exception: " << e.exception_code_;
        site_selector_begin_result begin_result;
        begin_result.status = e.exception_code_;
        _return.push_back( begin_result );
    } catch( apache::thrift::TException& tx ) {
        LOG( FATAL ) << "thrift exception: " << tx.what();
    } catch( std::exception& e ) {
        LOG( FATAL ) << "error for :" << id << "is: " << e.what();
    }

    DVLOG( k_rpc_handler_log_level )
        << "SS RETURN RPC: rpc_begin_transaction for client:" << id
        << ", return status:" << _return.at( 0 ).status;

    stop_timer( SS_RPC_BEGIN_TRANSACTION_TIMER_ID );
}

void site_selector_handler::begin_transaction_on_behalf_of_client(
    std::vector<site_selector_begin_result>& _return, const ::clientid id,
    const ::snapshot_vector&              client_session_version_vector,
    const std::vector<::cell_key_ranges>& write_set,
    const std::vector<::cell_key_ranges>& read_set,
    const std::vector<::cell_key_ranges>& full_replica_set ) {

    if( preparer_->is_single_site_transaction_system() ) {
        DCHECK_EQ( 0, full_replica_set.size() );
        int destination = preparer_->prepare_transaction(
            id, write_set, read_set, client_session_version_vector );
        if( destination < 0 ) {
            LOG( WARNING ) << "Unable to begin transaction: " << id
                           << ",error:" << destination;
        } else {
            DVLOG( 10 ) << "begin_transaction_on_behalf_of_client:" << id
                        << ", at destination:" << destination;

            site_selector_begin_result begin_result;

            site_manager_location loc = site_manager_locs_->at( destination );
            begin_result.site_id = destination;
            begin_result.ip = loc.ip_;
            begin_result.port = loc.port_;
            begin_result.status = exec_status_type::MASTER_CHANGE_OK;
            begin_result.items_at_site = write_set;

            _return.push_back( begin_result );
        }
    } else {
        std::unordered_map<int, std::vector<::cell_key_ranges>> destinations =
            preparer_->prepare_multi_site_transaction(
                id, write_set, read_set, full_replica_set,
                client_session_version_vector );
        if( destinations.size() > 0 ) {
            DVLOG( 10 ) << "begin_transaction_on_behalf_of_client:" << id
                        << ", at num destinations:" << destinations.size();
            for( const auto& dest : destinations ) {
                int destination = dest.first;
                DVLOG( 15 ) << "begin_transaction_on_behalf_of_client:" << id
                            << ", at destination:" << destination;

                site_selector_begin_result begin_result;

                site_manager_location loc =
                    site_manager_locs_->at( destination );
                begin_result.site_id = destination;
                begin_result.ip = loc.ip_;
                begin_result.port = loc.port_;
                begin_result.status = exec_status_type::MASTER_CHANGE_OK;
                begin_result.items_at_site = dest.second;

                _return.push_back( begin_result );
            }
        }
    }

    if( _return.size() == 0 ) {
        LOG( WARNING ) << "Unable to begin transaction: " << id;
        site_selector_begin_result begin_result;
        begin_result.status = exec_status_type::DATA_NOT_LOCATED_AT_ONE_SITE;
        _return.push_back( begin_result );
    }
}

void site_selector_handler::rpc_one_shot_sproc(
    one_shot_sproc_result& _return, const ::clientid id,
    const ::snapshot_vector&              client_session_version_vector,
    const std::vector<::cell_key_ranges>& write_set,
    const std::vector<::cell_key_ranges>& read_set, const std::string& name,
    const std::string& sproc_args, bool allow_force_change,
    const previous_context_timer_information& previous_contexts ) {

    DVLOG( k_rpc_handler_log_level )
        << "SS RPC: rpc_one_shot_sproc for client:" << id << ", name:" << name
        << ", allow_force_change:" << allow_force_change;

    start_timer( SS_RPC_ONE_SHOT_SPROC_TRANSACTION_TIMER_ID );

    try {
        DVLOG( 20 ) << "rpc_one_shot_sproc:" << name << ", by client:" << id;

        preparer_->add_previous_context_timer_information( id,
                                                           previous_contexts );

        int destination = -1;

        if( preparer_->is_single_site_transaction_system() ) {
            destination = preparer_->execute_one_shot_sproc(
                _return, id, client_session_version_vector, write_set, read_set,
                name, sproc_args, allow_force_change );
        } else {
            destination =
                preparer_->execute_one_shot_sproc_for_multi_site_system(
                    _return, id, client_session_version_vector, write_set,
                    read_set, name, sproc_args );
        }

        if( destination >= 0 ) {
            DVLOG( 7 ) << "rpc one shot sproc for client:" << id
                       << ", okay at:" << destination;
            site_manager_location loc = site_manager_locs_->at( destination );
            _return.site_of_exec_id = destination;
            _return.site_of_exec_ip = loc.ip_;
            _return.site_of_exec_port = loc.port_;
        } else {
            LOG( WARNING ) << "rpc_one_shot_sproc:" << name << ", by client"
                           << id << ", error:" << _return.status;
        }
    } catch( site_selector_exception& e ) {
        LOG( WARNING ) << "Caught SS Exception: " << e.exception_code_;
        _return.status = e.exception_code_;
    } catch( apache::thrift::TException& tx ) {
        LOG( FATAL ) << "thrift exception: " << tx.what();
    } catch( std::exception& e ) {
        LOG( FATAL ) << "error for :" << id << "is: " << e.what();
    }

    DVLOG( k_rpc_handler_log_level )
        << "SS RETURN RPC: rpc_one_shot_sproc for client:" << id
        << ", name:" << name << ", allow_force_change:" << allow_force_change
        << ", return status:" << _return.status;

    stop_timer( SS_RPC_ONE_SHOT_SPROC_TRANSACTION_TIMER_ID );
}

void site_selector_handler::rpc_one_shot_scan(
    one_shot_scan_result& _return, const ::clientid id,
    const ::snapshot_vector&           client_session_version_vector,
    const std::vector<scan_arguments>& scan_args, const std::string& name,
    const std::string& sproc_args, bool allow_missing_data,
    bool                                      allow_force_change,
    const previous_context_timer_information& previous_contexts ) {

    DVLOG( k_rpc_handler_log_level )
        << "SS RPC: rpc_one_shot_scan for client:" << id << ", name:" << name
        << ", allow_force_change:" << allow_force_change;

    start_timer( SS_RPC_ONE_SHOT_SCAN_TRANSACTION_TIMER_ID );

    try {
        DVLOG( 20 ) << "rpc_one_shot_scan:" << name << ", by client:" << id;

        preparer_->add_previous_context_timer_information( id,
                                                           previous_contexts );
        auto destinations = preparer_->execute_one_shot_scan(
            _return, id, client_session_version_vector, name, scan_args,
            sproc_args, allow_missing_data, allow_force_change );
        if( destinations.size() > 0 ) {
            DVLOG( 7 ) << "rpc one shot scan for client:" << id
                       << ", okay at:" << destinations;
            for( const auto destination : destinations ) {
                site_manager_location loc =
                    site_manager_locs_->at( destination );
                _return.site_of_exec_ids.push_back( destination );
                _return.site_of_exec_ips.push_back( loc.ip_ );
                _return.site_of_exec_ports.push_back( loc.port_ );
            }
        } else {
            LOG( WARNING ) << "rpc_one_shot_scan:" << name << ", by client"
                           << id << ", error:" << _return.status;
        }
    } catch( site_selector_exception& e ) {
        LOG( WARNING ) << "Caught SS Exception: " << e.exception_code_;
        _return.status = e.exception_code_;
    } catch( apache::thrift::TException& tx ) {
        LOG( FATAL ) << "thrift exception: " << tx.what();
    } catch( std::exception& e ) {
        LOG( FATAL ) << "error for :" << id << "is: " << e.what();
    }

    DVLOG( k_rpc_handler_log_level )
        << "SS RETURN RPC: rpc_one_shot_scan for client:" << id
        << ", name:" << name << ", allow_force_change:" << allow_force_change
        << ", return status:" << _return.status;

    stop_timer( SS_RPC_ONE_SHOT_SCAN_TRANSACTION_TIMER_ID );
}

void site_selector_handler::rpc_persist_state_to_files(
    persistence_result& _return, const ::clientid id,
    const std::string& out_ss_name, const std::string& out_db_name,
    const std::string& out_part_name ) {
    DVLOG( k_rpc_handler_log_level )
        << "SS RPC: rpc_persist_state_to_files for client:" << id
        << ", out_ss_name:" << out_ss_name << ", out_db_name:" << out_db_name
        << ", out_part_name:" << out_part_name;

    try {
        DVLOG( 5 ) << "Persisting state to file";
        preparer_->persist_state_to_files( id, out_ss_name, out_db_name,
                                           out_part_name );
        _return.status = exec_status_type::COMMAND_OK;
        DVLOG( 5 ) << "Persisting state to file okay!";
    } catch( apache::thrift::TException& tx ) {
        LOG( FATAL ) << "thrift exception: " << tx.what();
    } catch( std::exception& e ) {
        LOG( FATAL ) << "error for :" << id << "is: " << e.what();
    }
    DVLOG( k_rpc_handler_log_level )
        << "SS RETURN RPC: rpc_persist_state_to_files for client:" << id
        << ", out_ss_name:" << out_ss_name << ", out_db_name:" << out_db_name
        << ", out_part_name:" << out_part_name
        << ", return status:" << _return.status;
}
void site_selector_handler::rpc_restore_state_from_files(
    persistence_result& _return, const ::clientid id,
    const std::string& in_ss_name, const std::string& in_db_name,
    const std::string& in_part_name ) {
    DVLOG( k_rpc_handler_log_level )
        << "SS RPC: rpc_restore_state_from_files for client:" << id
        << ", in_ss_name:" << in_ss_name << ", in_db_name:" << in_db_name
        << ", in_part_name:" << in_part_name;

    try {
        DVLOG( 5 ) << "Restore state from file";

        preparer_->restore_state_from_files( id, in_ss_name, in_db_name,
                                             in_part_name );

        _return.status = exec_status_type::COMMAND_OK;
        DVLOG( 5 ) << "Restore state from file okay!";
    } catch( apache::thrift::TException& tx ) {
        LOG( FATAL ) << "thrift exception: " << tx.what();
    } catch( std::exception& e ) {
        LOG( FATAL ) << "error for :" << id << "is: " << e.what();
    }
    DVLOG( k_rpc_handler_log_level )
        << "SS RETURN RPC: rpc_restore_state_from_files for client:" << id
        << ", in_ss_name:" << in_ss_name << ", in_db_name:" << in_db_name
        << ", in_part_name:" << in_part_name
        << ", return status:" << _return.status;
}

void site_selector_handler::rpc_wait_for_stable_state_among_sites(
    persistence_result& _return, const ::clientid id ) {
    DVLOG( k_rpc_handler_log_level )
        << "SS RPC: rpc_wait_for_stable_state_among_sites for client:" << id;

    try {
        DVLOG( 5 ) << "Wait for stable state among sites";
        _return.status = exec_status_type::COMMAND_OK;
        DVLOG( 5 ) << "Wait for stable state among sites okay!";
    } catch( apache::thrift::TException& tx ) {
        LOG( FATAL ) << "thrift exception: " << tx.what();
    } catch( std::exception& e ) {
        LOG( FATAL ) << "error for :" << id << "is: " << e.what();
    }

    DVLOG( k_rpc_handler_log_level )
        << "SS RETURN RPC: rpc_wait_for_stable_state_among_sites for client:"
        << id << ", return status:" << _return.status;
}
