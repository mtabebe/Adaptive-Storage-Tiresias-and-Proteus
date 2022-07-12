#include "site_manager_handler.h"

#include <glog/logging.h>
#include <glog/stl_logging.h>

#include "../../common/bucket_funcs.h"
#include "../../common/constants.h"
#include "../../common/partition_funcs.h"
#include "../../common/perf_tracking.h"
#include "../../common/thread_utils.h"
#include "../../persistence/data_site_persistence_manager.h"

static const std::string k_insert_str = "insert";
static const std::string k_update_str = "update";
static const std::string k_delete_str = "delete";

#define sm_catch_t_except                                          \
    catch( apache::thrift::TException & tx ) {                     \
        LOG( FATAL ) << "thrift exception for: " << id             \
                     << " is: " << tx.what();                      \
    }                                                              \
    catch( std::exception & e ) {                                  \
        LOG( FATAL ) << "error for :" << id << "is: " << e.what(); \
    }

#define SFENCE_IF_2PC()                                                \
    if( k_ss_mastering_type == ss_mastering_type::TWO_PC or            \
        k_ss_mastering_type == ss_mastering_type::PARTITIONED_STORE or \
        k_ss_mastering_type == ss_mastering_type::E_STORE or           \
        k_ss_mastering_type == ss_mastering_type::CLAY or              \
        k_ss_mastering_type == ss_mastering_type::ADR )                \
    SFENCE()

#define LFENCE_IF_2PC()                                                \
    if( k_ss_mastering_type == ss_mastering_type::TWO_PC or            \
        k_ss_mastering_type == ss_mastering_type::PARTITIONED_STORE or \
        k_ss_mastering_type == ss_mastering_type::E_STORE or           \
        k_ss_mastering_type == ss_mastering_type::CLAY or              \
        k_ss_mastering_type == ss_mastering_type::ADR )                \
    LFENCE()

#define do_internal_site_manager_function( _method, _result, _id, _args... ) \
    try {                                                                    \
        start_timer( RPC_AT_DATA_SITE_SERVICE_TIME_TIMER_ID );               \
        _method( _result, _id, _args );                                      \
        stop_timer( RPC_AT_DATA_SITE_SERVICE_TIME_TIMER_ID );                \
        _result.timers = get_context_timers();                               \
        reset_context_timers();                                              \
    }                                                                        \
    sm_catch_t_except

#define do_internal_site_manager_function_no_args( _method, _result, _id ) \
    try {                                                                  \
        start_timer( RPC_AT_DATA_SITE_SERVICE_TIME_TIMER_ID );             \
        _method( _result, _id );                                           \
        stop_timer( RPC_AT_DATA_SITE_SERVICE_TIME_TIMER_ID );              \
        _result.timers = get_context_timers();                             \
        reset_context_timers();                                            \
    }                                                                      \
    sm_catch_t_except

static const std::string k_one_shot_sproc_timer_str = "rpc_one_shot_sproc";

site_manager_handler::site_manager_handler(
    std::unique_ptr<db> db, std::unique_ptr<sproc_lookup_table> sprocs,
    void *sproc_opaque )
    : db_( std::move( db ) ),
      sprocs_( std::move( sprocs ) ),
      cli_state_(),
      mach_stats_(),
      sproc_opaque_( sproc_opaque ) {}

site_manager_handler::~site_manager_handler() {
    destroy_sproc_opaque_pointer( sproc_opaque_ );
}

void site_manager_handler::init( uint32_t num_clients,
                                 uint32_t num_client_threads,
                                 void *   create_args ) {
    db *                  db_ptr = db_.get();
    std::vector<arg_code> create_table_args = {BIGINT_CODE, BIGINT_CODE};
    std::vector<void *>   create_table_vals = {(void *) db_ptr, create_args};

    function_identifier create_func_id( k_create_config_tables_sproc_name,
                                        create_table_args );

    std::vector<cell_key_ranges> empty_ckrs;

    function_skeleton create_function =
        sprocs_->lookup_function( create_func_id );
    create_function( nullptr, 0, empty_ckrs, empty_ckrs, create_table_args,
                     create_table_vals, sproc_opaque_ );

#if 0  // HDB-OUT
  db_->init_update_propagator( std::move( update_propagator ) );
  db_->init_update_applier( std::move( update_applier ) );

  db_->init_garbage_collector( db_->compute_gc_sleep_time() );
#endif
    cli_state_.init( num_clients * num_client_threads,
                     db_->get_site_location() );
}

void site_manager_handler::internal_rpc_begin_transaction(
    begin_result &_return, const clientid id,
    const ::snapshot_vector &client_session_version_vector,
    const std::vector<partition_column_identifier> &write_set,
    const std::vector<partition_column_identifier> &read_set,
    const std::vector<partition_column_identifier> &inflight_set,
    bool                                            allow_missing ) {
    LFENCE_IF_2PC();
    DVLOG( 10 ) << "Begin for client:" << id;
    DVLOG( 20 ) << "Begin " << id
                << " session:" << client_session_version_vector;
    DVLOG( 20 ) << "Begin " << id << " write_set:" << write_set
                << ", read_set:" << read_set;

    uint32_t        cid = id;
    snapshot_vector cvv( client_session_version_vector.begin(),
                         client_session_version_vector.end() );
    partition_column_identifier_set write_pids =
        convert_to_partition_column_identifier_set( write_set );
    partition_column_identifier_set read_pids =
        convert_to_partition_column_identifier_set( read_set );
    partition_column_identifier_set inflight_pids =
        convert_to_partition_column_identifier_set( inflight_set );

    _return.status = exec_status_type::COMMAND_OK;

    start_timer( RPC_BEGIN_TRANSACTION_TIMER_ID );

    partition_lookup_operation lookup_op = partition_lookup_operation::GET;
    if( allow_missing ) {
        lookup_op = partition_lookup_operation::GET_ALLOW_MISSING;
    }

    transaction_partition_holder *partition_holder =
        db_->get_partitions_with_begin( cid, cvv, write_pids, read_pids,
                                        inflight_pids, lookup_op );
    if( partition_holder == nullptr ) {
        LOG( WARNING ) << "Unable to get partitions with begin, for client:"
                       << cid;
        _return.status = exec_status_type::DATA_NOT_LOCATED_AT_ONE_SITE;
    } else {

        bool is_master_ok = partition_holder->does_site_master_write_set(
            db_->get_site_location() );

        cli_state_.set_partition_holder( cid, partition_holder );

        if( !is_master_ok ) {
            partition_holder->abort_transaction();

            _return.status = exec_status_type::ERROR_WITH_WRITE_SET;
            _return.errMsg =
                "Unable to begin write transaction, data is not mastered"
                " at site";
            LOG( WARNING ) << _return.errMsg << ", for client:" << cid;

            cli_state_.reset_partition_holder( cid );
        }
    }

    stop_timer( RPC_BEGIN_TRANSACTION_TIMER_ID );
    DVLOG( 10 ) << "Begin for client:" << id << " " << _return.status;
    SFENCE_IF_2PC();
}
void site_manager_handler::rpc_begin_transaction(
    begin_result &_return, const clientid id,
    const ::snapshot_vector &                client_session_version_vector,
    const std::vector<partition_column_identifier> &write_set,
    const std::vector<partition_column_identifier> &read_set,
    const std::vector<partition_column_identifier> &inflight_set ) {
    DVLOG( k_rpc_handler_log_level )
        << "SM RPC: rpc_begin_transaction for client:" << id;

    do_internal_site_manager_function( internal_rpc_begin_transaction, _return,
                                       id, client_session_version_vector,
                                       write_set, read_set, inflight_set,
                                       false /* allow missing */ );

    DVLOG( k_rpc_handler_log_level )
        << "SM RETURN RPC: rpc_begin_transaction for client:" << id
        << ", return status:" << _return.status;
}

void site_manager_handler::internal_rpc_prepare_transaction(
    commit_result &_return, const ::clientid id ) {
    DVLOG( 10 ) << "Prepare for client:" << id;
    // TODO not sure what we do here
    _return.status = exec_status_type::COMMAND_OK;
    DVLOG( 15 ) << "Prepare for client:" << id << " " << _return.status;
}
void site_manager_handler::rpc_prepare_transaction( commit_result &  _return,
                                                    const ::clientid id )  {
    do_internal_site_manager_function_no_args( internal_rpc_prepare_transaction,
                                               _return, id );
}

void site_manager_handler::internal_rpc_commit_transaction(
    commit_result &_return, const clientid id ) {

    //Grab client lock
    LFENCE_IF_2PC();

    DVLOG( 10 ) << "Commit for client:" << id;

    uint32_t cid = id;

    start_timer( RPC_COMMIT_TRANSACTION_TIMER_ID );
    transaction_partition_holder *holder =
        cli_state_.get_partition_holder( cid );
    snapshot_vector cvv = holder->commit_transaction();
    stop_timer( RPC_COMMIT_TRANSACTION_TIMER_ID );

    _return.status = exec_status_type::COMMAND_OK;
    _return.session_version_vector.clear();
    _return.session_version_vector = std::move( cvv );

    cli_state_.reset_partition_holder( cid );

    DVLOG( 10 ) << "Commit for client:" << id << " " << _return.status;
    SFENCE_IF_2PC();
}
void site_manager_handler::rpc_commit_transaction( commit_result &_return,
                                                   const clientid id ) {
    DVLOG( k_rpc_handler_log_level )
        << "SM RPC: rpc_commit_transaction for client:" << id;

    do_internal_site_manager_function_no_args( internal_rpc_commit_transaction,
                                               _return, id );

    DVLOG( k_rpc_handler_log_level )
        << "SM RETURN RPC: rpc_commit_transaction for client:" << id
        << ", return status:" << _return.status;
}

void site_manager_handler::internal_rpc_abort_transaction(
    abort_result &_return, const clientid id ) {
    LFENCE_IF_2PC();
    DVLOG( 10 ) << "Abort for client:" << id;

    uint32_t cid = id;

    start_timer( RPC_ABORT_TRANSACTION_TIMER_ID );
    transaction_partition_holder *holder =
        cli_state_.get_partition_holder( cid );
    if ( holder ) {
        holder->abort_transaction();
        cli_state_.reset_partition_holder( cid );
    }
    stop_timer( RPC_ABORT_TRANSACTION_TIMER_ID );

    _return.status = exec_status_type::COMMAND_OK;

    DVLOG( 10 ) << "Abort for client:" << id << " " << _return.status;
    SFENCE_IF_2PC();
}
void site_manager_handler::rpc_abort_transaction( abort_result & _return,
                                                  const clientid id ) {
    DVLOG( k_rpc_handler_log_level )
        << "SM RPC: rpc_abort_transaction for client:" << id;

    do_internal_site_manager_function_no_args( internal_rpc_abort_transaction,
                                               _return, id );

    DVLOG( k_rpc_handler_log_level )
        << "SM RETURN RPC: rpc_abort_transaction for client:" << id
        << ", return status:" << _return.status;
}

void site_manager_handler::rpc_stored_procedure_helper(
    sproc_result &_return, const ::clientid id, const std::string &name,
    const std::vector<cell_key_ranges> &write_ckrs,
    const std::vector<cell_key_ranges> &read_ckrs,
    const std::string &                 sproc_args ) {

    DVLOG( 20 ) << "Sproc:" << name << " " << id;

    uint32_t cid = id;

    bool ret_ok = true;

    if( sproc_args.size() < 4 ) {
        LOG( WARNING )
            << "Invalid parameters to rpc_stored_procedure, for client:" << id;
        _return.status = exec_status_type::INVALID_SERIALIZATION;
        return;
    }

    start_timer( RPC_SPROC_DESERIALIZE_TIMER_ID );
    char *buffer = (char *) sproc_args.c_str();

    // Read the length of stuff to deserialize
    int len = *(int *) buffer;
    len = ntohl( len );
    buffer += sizeof( int );

    DVLOG( 20 ) << "Sproc len:" << len;

    std::vector<arg_code> arg_codes;
    std::vector<void *>   arg_ptrs;

    ret_ok = deserialize( buffer, len, arg_codes, arg_ptrs );
    stop_timer( RPC_SPROC_DESERIALIZE_TIMER_ID );
    if( !ret_ok ) {
        LOG( WARNING ) << "Could not deserialize in "
                       << "rpc_stored_procedure, invalid "
                       << "serialization, for client:" << id;
        _return.status = exec_status_type::INVALID_SERIALIZATION;
        return;
    }

    function_identifier func_id( name, arg_codes );

    function_skeleton skel = sprocs_->lookup_function( func_id );
    if( skel == NULL ) {
        LOG( WARNING ) << "Asked to execute non-existing function " << name
                       << ", for client:" << id;

        _return.status = exec_status_type::FUNCTION_NOT_FOUND;
        free_args( arg_ptrs );
        return;
    }

    start_timer( RPC_SPROC_EXECUTE_TIMER_ID );
    _return = skel( cli_state_.get_partition_holder( cid ), cid, write_ckrs,
                    read_ckrs, arg_codes, arg_ptrs, sproc_opaque_ );
    stop_timer( RPC_SPROC_EXECUTE_TIMER_ID );

    DVLOG( 20 ) << "Executing sproc:" << name << ", result:" << _return.status;
    free_args( arg_ptrs );
}

void site_manager_handler::internal_rpc_stored_procedure(
    sproc_result &_return, const ::clientid id, const std::string &name,
    const std::vector<cell_key_ranges> &write_ckrs,
    const std::vector<cell_key_ranges> &read_ckrs,
    const std::string &                 sproc_args ) {
    LFENCE_IF_2PC();

    DVLOG( 10 ) << "Stored procedure:" << id;
    start_timer( RPC_STORED_PROCEDURE_TIMER_ID );

    rpc_stored_procedure_helper( _return, id, name, write_ckrs, read_ckrs,
                                 sproc_args );

    stop_timer( RPC_STORED_PROCEDURE_TIMER_ID );
    DVLOG( 10 ) << "Stored procedure:" << id << " " << _return.status;
    SFENCE_IF_2PC();
}
void site_manager_handler::rpc_stored_procedure(
    sproc_result &_return, const ::clientid id, const std::string &name,
    const std::vector<cell_key_ranges> &write_ckrs,
    const std::vector<cell_key_ranges> &read_ckrs,
    const std::string &                 sproc_args ) {

    DVLOG( k_rpc_handler_log_level )
        << "SM RPC: rpc_stored_procedure for client:" << id
        << ", name:" << name;

    do_internal_site_manager_function( internal_rpc_stored_procedure, _return,
                                       id, name, write_ckrs, read_ckrs,
                                       sproc_args );

    DVLOG( k_rpc_handler_log_level )
        << "SM RETURN RPC: rpc_stored_procedure for client:" << id
        << ", name:" << name << ", return status:" << _return.status;
}

void site_manager_handler::rpc_scan_helper(
    scan_result &_return, const ::clientid id, const std::string &name,
    const std::vector<scan_arguments> &scan_args,
    const std::string &                sproc_args ) {

    DVLOG( 20 ) << "Sproc:" << name << " " << id;

    uint32_t cid = id;

    bool ret_ok = true;

    if( sproc_args.size() < 4 ) {
        LOG( WARNING )
            << "Invalid parameters to rpc_stored_procedure, for client:" << id;
        _return.status = exec_status_type::INVALID_SERIALIZATION;
        return;
    }

    start_timer( RPC_SPROC_DESERIALIZE_TIMER_ID );
    char *buffer = (char *) sproc_args.c_str();

    // Read the length of stuff to deserialize
    int len = *(int *) buffer;
    len = ntohl( len );
    buffer += sizeof( int );

    DVLOG( 20 ) << "Sproc len:" << len;

    std::vector<arg_code> arg_codes;
    std::vector<void *>   arg_ptrs;

    ret_ok = deserialize( buffer, len, arg_codes, arg_ptrs );
    stop_timer( RPC_SPROC_DESERIALIZE_TIMER_ID );
    if( !ret_ok ) {
        LOG( WARNING ) << "Could not deserialize in "
                       << "rpc_stored_procedure, invalid "
                       << "serialization, for client:" << id;
        _return.status = exec_status_type::INVALID_SERIALIZATION;
        return;
    }

    function_identifier func_id( name, arg_codes );

    scan_function_skeleton skel = sprocs_->lookup_scan_function( func_id );
    if( skel == NULL ) {
        LOG( WARNING ) << "Asked to execute non-existing function " << name
                       << ", for client:" << id;

        _return.status = exec_status_type::FUNCTION_NOT_FOUND;
        free_args( arg_ptrs );
        return;
    }

    start_timer( RPC_SCAN_EXECUTE_TIMER_ID );
    skel( cli_state_.get_partition_holder( cid ), cid, scan_args, arg_codes,
          arg_ptrs, sproc_opaque_, _return );
    stop_timer( RPC_SCAN_EXECUTE_TIMER_ID );
    (void) cid;

    DVLOG( 20 ) << "Executing scan:" << name << ", result:" << _return.status;
    free_args( arg_ptrs );
}

void site_manager_handler::internal_rpc_scan(
    scan_result &_return, const ::clientid id, const std::string &name,
    const std::vector<scan_arguments> &scan_args,
    const std::string &                sproc_args ) {
    LFENCE_IF_2PC();

    DVLOG( 10 ) << "Scan:" << id;
    start_timer( RPC_SCAN_TIMER_ID );

    rpc_scan_helper( _return, id, name, scan_args, sproc_args );

    stop_timer( RPC_SCAN_TIMER_ID );
    DVLOG( 10 ) << "Scan:" << id << " " << _return.status;
    SFENCE_IF_2PC();
}
void site_manager_handler::rpc_scan(
    scan_result &_return, const ::clientid id, const std::string &name,
    const std::vector<scan_arguments> &scan_args,
    const std::string &                sproc_args ) {

    DVLOG( k_rpc_handler_log_level )
        << "SM RPC: rpc_scan for client:" << id
        << ", name:" << name;

    do_internal_site_manager_function( internal_rpc_scan, _return, id, name,
                                       scan_args, sproc_args );

    DVLOG( k_rpc_handler_log_level )
        << "SM RETURN RPC: rpc_scan for client:" << id << ", name:" << name
        << ", return status:" << _return.status;
}

template <class T_ret>
void site_manager_handler::remaster(
    T_ret &_return, const clientid id,
    const std::vector<partition_column_identifier> &pids, uint32_t master_location,
    const std::vector<propagation_configuration> &new_prop_configs,
    const ::snapshot_vector &session_version_vector, bool is_release,
    const partition_lookup_operation &lookup_operation ) {
    uint32_t              cid = id;
    snapshot_vector                cvv = session_version_vector;
    snapshot_vector                remaster_vv;

    auto remaster_ret = db_->remaster_partitions(
        cid, session_version_vector, pids, new_prop_configs, master_location,
        is_release, lookup_operation );

    bool okay = std::get<0>( remaster_ret );
    if( !okay ) {
        LOG( WARNING ) << "Unable to remaster, for client:" << id;
        _return.status = exec_status_type::MASTER_CHANGE_ERROR;
        return;
    }

    remaster_vv = std::get<1>( remaster_ret );

    _return.status = exec_status_type::MASTER_CHANGE_OK;
    _return.session_version_vector.clear();
    _return.session_version_vector = std::move( remaster_vv );
}

void site_manager_handler::internal_rpc_grant_mastership(
    grant_result &_grant_res, const clientid id,
    const std::vector<partition_column_identifier> &     pids,
    const std::vector<propagation_configuration> &new_prop_configs,
    const ::snapshot_vector &                     session_version_vector ) {
    DVLOG( 10 ) << "Grant mastership for client:" << id;

    uint32_t master_location = db_->get_site_location();
    start_timer( RPC_GRANT_MASTERSHIP_TIMER_ID );
    remaster<grant_result>( _grant_res, id, pids, master_location,
                            new_prop_configs, session_version_vector, false,
                            // replicas may not be there, because they might
                            // be a consequence of a split etc.
                            partition_lookup_operation::GET_OR_CREATE );
    stop_timer( RPC_GRANT_MASTERSHIP_TIMER_ID );

    DVLOG( 10 ) << "Grant mastership for client:" << id << " "
                << _grant_res.status;
}
void site_manager_handler::rpc_grant_mastership(
    grant_result &_grant_res, const clientid id,
    const std::vector<partition_column_identifier> &     pids,
    const std::vector<propagation_configuration> &new_prop_configs,
    const ::snapshot_vector &                     session_version_vector ) {

    DVLOG( k_rpc_handler_log_level )
        << "SM RPC: rpc_grant_mastership for client:" << id
        << ", pids:" << pids;

    do_internal_site_manager_function( internal_rpc_grant_mastership,
                                       _grant_res, id, pids, new_prop_configs,
                                       session_version_vector );
    DVLOG( k_rpc_handler_log_level )
        << "SM RETURN RPC: rpc_grant_mastership for client:" << id
        << ", pids:" << pids << ", return status:" << _grant_res.status;
}

void site_manager_handler::internal_rpc_release_mastership(
    release_result &_release_res, const clientid id,
    const std::vector<partition_column_identifier> &pids, const int destination,
    const std::vector<propagation_configuration> &new_prop_configs,
    const ::snapshot_vector &                     session_version_vector ) {
    DVLOG( 10 ) << "Release mastership for client:" << id;

    uint32_t master_location = destination;

    start_timer( RPC_RELEASE_MASTERSHIP_TIMER_ID );
    remaster<release_result>(
        _release_res, id, pids, master_location, new_prop_configs,
        session_version_vector, true,
        // these should be here because they are the master
        partition_lookup_operation::GET );
    stop_timer( RPC_RELEASE_MASTERSHIP_TIMER_ID );

    DVLOG( 10 ) << "Release mastership for client:" << id << " "
                << _release_res.status;
}
void site_manager_handler::rpc_release_mastership(
    release_result &_release_res, const clientid id,
    const std::vector<partition_column_identifier> &pids, const int destination,
    const std::vector<propagation_configuration> &new_prop_configs,
    const ::snapshot_vector &                     session_version_vector ) {

    DVLOG( k_rpc_handler_log_level )
        << "SM RPC: rpc_release_mastership for client:" << id
        << ", pids:" << pids << ", destination:" << destination;

    do_internal_site_manager_function(
        internal_rpc_release_mastership, _release_res, id, pids, destination,
        new_prop_configs, session_version_vector );

    DVLOG( k_rpc_handler_log_level )
        << "SM RETURN RPC: rpc_release_mastership for client:" << id
        << ", pids:" << pids << ", destination:" << destination
        << ", return status:" << _release_res.status;
}

void site_manager_handler::rpc_release_transfer_mastership(
    snapshot_partition_columns_results &_release_res, const clientid id,
    const std::vector<partition_column_identifier> &pids, const int destination,
    const ::snapshot_vector &session_version_vector ) {
    DVLOG( k_rpc_handler_log_level )
        << "SM RPC: rpc_release_transfer_mastership for client:" << id
        << ", pids:" << pids << ", destination:" << destination;

    do_internal_site_manager_function( internal_rpc_release_transfer_mastership,
                                       _release_res, id, pids, destination,
                                       session_version_vector );

    DVLOG( k_rpc_handler_log_level )
        << "SM RPC: rpc_release_transfer_mastership for client:" << id
        << ", pids:" << pids << ", destination:" << destination
        << ", return status:" << _release_res.status;
}
void site_manager_handler::internal_rpc_release_transfer_mastership(
    snapshot_partition_columns_results &_release_res, const clientid id,
    const std::vector<partition_column_identifier> &pids, const int destination,
    const ::snapshot_vector &session_version_vector ) {
    DVLOG( 10 ) << "Release transfer mastership for client:" << id;

    uint32_t master_location = destination;

    partition_column_identifier_set pid_set =
        convert_to_partition_column_identifier_set( pids );

    start_timer( RPC_RELEASE_MASTERSHIP_TIMER_ID );
    uint32_t cid = id;
    bool     okay = db_->snapshot_and_remove_partitions(
        cid, pid_set, master_location, _release_res.snapshots );
    if( !okay ) {
        LOG( WARNING ) << "Unable to snapshot and remove partition " << pids
                       << " not located at site, for client:" << id;
        _release_res.status = exec_status_type::DATA_NOT_LOCATED_AT_ONE_SITE;
        _release_res.snapshots.clear();
    } else {
        _release_res.status = exec_status_type::MASTER_CHANGE_OK;
    }

    stop_timer( RPC_RELEASE_MASTERSHIP_TIMER_ID );

    DVLOG( 10 ) << "Release transfer mastership for client:" << id << " "
                << _release_res.status;
}

void site_manager_handler::rpc_grant_transfer_mastership(
    grant_result &_grant_res, const clientid id,
    const std::vector<snapshot_partition_column_state> &snapshots,
    const std::vector<partition_type::type> &           p_types,
    const std::vector<storage_tier_type::type> &        s_types ) {

    DVLOG( k_rpc_handler_log_level )
        << "SM RPC: rpc_grant_transfer_mastership for client:" << id;

    do_internal_site_manager_function( internal_rpc_grant_transfer_mastership,
                                       _grant_res, id, snapshots, p_types,
                                       s_types );

    DVLOG( k_rpc_handler_log_level )
        << "SM RETURN RPC: rpc_grant_transfer_mastership for client:" << id
        << ", return status:" << _grant_res.status;
}
void site_manager_handler::internal_rpc_grant_transfer_mastership(
    grant_result &_grant_res, const clientid id,
    const std::vector<snapshot_partition_column_state> &snapshots,
    const std::vector<partition_type::type> &           p_types,
    const std::vector<storage_tier_type::type> &        s_types ) {
    DVLOG( 10 ) << "Grant transfer mastership for client:" << id;

    uint32_t master_location = db_->get_site_location();
    start_timer( RPC_GRANT_MASTERSHIP_TIMER_ID );
    uint32_t cid = id;
    auto     remaster_ret = db_->add_partitions_from_snapshots(
        cid, snapshots, p_types, s_types, master_location );

    bool okay = std::get<0>( remaster_ret );
    if( !okay ) {
        LOG( WARNING ) << "Unable to grant, for client:" << id;
        _grant_res.status = exec_status_type::MASTER_CHANGE_ERROR;
    } else {
        auto remaster_vv = std::get<1>( remaster_ret );

        _grant_res.status = exec_status_type::MASTER_CHANGE_OK;
        _grant_res.session_version_vector.clear();
        _grant_res.session_version_vector = std::move( remaster_vv );
    }

    stop_timer( RPC_GRANT_MASTERSHIP_TIMER_ID );

    DVLOG( 10 ) << "Grant transfer mastership for client:" << id << " "
                << _grant_res.status;
}

void site_manager_handler::rpc_get_site_statistics(
    site_statistics_results &_site_stats_res ) {
    DVLOG( k_rpc_handler_log_level ) << "SM RPC: rpc_get_site_statistics";

    try {

        DVLOG( 7 ) << "Getting site statistics";
        db_->get_update_partition_states(
            _site_stats_res.partition_col_versions );
        _site_stats_res.prop_configs = db_->get_propagation_configurations();
        _site_stats_res.prop_counts = db_->get_propagation_counts();
        _site_stats_res.mach_stats = mach_stats_.get_machine_statistics();
        _site_stats_res.col_widths = db_->get_column_widths();
        _site_stats_res.selectivities = db_->get_selectivities();
        _site_stats_res.storage_changes = db_->get_changes_to_storage_tiers();
        _site_stats_res.timers =
            get_aggregated_global_timers( k_relevant_model_timer_ids );
#if 0 // HDB-OUT

        client_version_vector svv = db_->get_site_version_vector();
        _site_stats_res.site_version_vector.insert(
            _site_stats_res.site_version_vector.begin(), svv.begin(),
            svv.end() );
#endif
        _site_stats_res.status = exec_status_type::COMMAND_OK;
    } catch( apache::thrift::TException &tx ) {
        LOG( FATAL ) << "thrift exception: " << tx.what();
    } catch( std::exception &e ) {
        LOG( FATAL ) << "error for get_site_statistics is: " << e.what();
    }

    DVLOG( k_rpc_handler_log_level )
        << "SM RETURN RPC: rpc_get_site_statistics"
        << ", return status:" << _site_stats_res.status;
}

void site_manager_handler::internal_rpc_select(
    query_result &_return, const ::clientid id,
    const std::vector<::cell_key> &read_keys ) {
    DVLOG( 10 ) << "rpc_select for client:" << id;

    uint32_t cid = id;

    start_timer( RPC_SELECT_TIMER_ID );
    std::vector<cell_identifier> cids( read_keys.begin(), read_keys.end() );

    _return.status = exec_status_type::TUPLES_OK;
    transaction_partition_holder *partition_holder =
        cli_state_.get_partition_holder( cid );

    for( const cell_identifier &c_id : cids ) {
        DVLOG( 25 ) << "rpc_select:" << c_id;
        auto res_tup =
            partition_holder->get_data( c_id, db_->get_cell_data_type( c_id ) );
        _return.tuples.push_back( res_tup );
    }

    stop_timer( RPC_SELECT_TIMER_ID );
    DVLOG( 10 ) << "rpc_select for client:" << id << " " << _return.status;
    SFENCE_IF_2PC();
}
void site_manager_handler::rpc_select(
    query_result &_return, const ::clientid id,
    const std::vector<::cell_key> &read_keys ) {

    DVLOG( k_rpc_handler_log_level ) << "SM RPC: rpc_select for client:" << id;

    do_internal_site_manager_function( internal_rpc_select, _return, id,
                                       read_keys );
    DVLOG( k_rpc_handler_log_level )
        << "SM RETURN RPC: rpc_select for client:" << id
        << ", return status:" << _return.status;
}

void site_manager_handler::internal_rpc_delete(
    query_result &_return, const ::clientid id,
    const std::vector<::cell_key> &delete_keys ) {

    LFENCE_IF_2PC();
    DVLOG( 15 ) << "rpc_delete for client:" << id;
    start_timer( RPC_DELETE_TIMER_ID );

    _return.status = exec_status_type::COMMAND_OK;

    uint32_t                      cid = id;
    transaction_partition_holder *partition_holder =
        cli_state_.get_partition_holder( cid );

    std::vector<cell_identifier> cids( delete_keys.begin(), delete_keys.end() );
    for( const auto& c_id : cids) {
        bool delete_ok = partition_holder->remove_data( c_id );

        if( !delete_ok ) {
            _return.errMsg = "Unable to delete cell";
            LOG( WARNING ) << _return.errMsg << ":" << c_id << ", client:" << id;
            break;
        }
    }
    SFENCE_IF_2PC();
    stop_timer( RPC_DELETE_TIMER_ID );

    DVLOG( 15 ) << "rpc_delete for client:" << id << " " << _return.status;
}

void site_manager_handler::rpc_delete(
    query_result &_return, const ::clientid id,
    const std::vector<::cell_key> &delete_keys ) {
    DVLOG( k_rpc_handler_log_level ) << "SM RPC: rpc_delete for client:" << id;

    do_internal_site_manager_function( internal_rpc_delete, _return, id,
                                       delete_keys );
    DVLOG( k_rpc_handler_log_level )
        << "SM RETURN RPC: rpc_delete for client:" << id
        << ", return status:" << _return.status;
}

void site_manager_handler::internal_rpc_update(
    query_result &_return, const ::clientid id,
    const std::vector<::cell_key> & write_keys,
    const std::vector<std::string> &write_vals ) {

    LFENCE_IF_2PC();
    DVLOG( 15 ) << "rpc_update for client:" << id;
    start_timer( RPC_UPDATE_TIMER_ID );

    _return.status = exec_status_type::COMMAND_OK;

    uint32_t                      cid = id;
    transaction_partition_holder *partition_holder =
        cli_state_.get_partition_holder( cid );

    std::vector<cell_identifier> cids( write_keys.begin(), write_keys.end() );
    DCHECK_EQ( write_keys.size(), write_vals.size() );
    DCHECK_EQ( cids.size(), write_vals.size() );
    for( uint32_t pos = 0; pos < cids.size(); pos++ ) {
        const auto &c_id = cids.at( pos );
        bool        update_ok = partition_holder->update_data(
            c_id, write_vals.at( pos ), db_->get_cell_data_type( c_id ) );

        if( !update_ok ) {
            _return.errMsg = "Unable to delete cell";
            LOG( WARNING ) << _return.errMsg << ":" << cid << ", client:" << id;
            break;
        }
    }
    SFENCE_IF_2PC();
    stop_timer( RPC_UPDATE_TIMER_ID );

    DVLOG( 15 ) << "rpc_update for client:" << id << " " << _return.status;
}
void site_manager_handler::rpc_update(
    query_result &_return, const ::clientid id,
    const std::vector<::cell_key> &write_keys,
    const std::vector<std::string> &  write_vals ) {
    DVLOG( k_rpc_handler_log_level ) << "SM RPC: rpc_update for client:" << id;

    do_internal_site_manager_function( internal_rpc_update, _return, id,
                                       write_keys, write_vals );

    DVLOG( k_rpc_handler_log_level )
        << "SM RETURN RPC: rpc_update for client:" << id
        << ", return status:" << _return.status;
}

void site_manager_handler::internal_rpc_insert(
    query_result &_return, const ::clientid id,
    const std::vector<::cell_key> & write_keys,
    const std::vector<std::string> &write_vals ) {

    LFENCE_IF_2PC();
    DVLOG( 15 ) << "rpc_insert for client:" << id;
    start_timer( RPC_INSERT_TIMER_ID );

    _return.status = exec_status_type::COMMAND_OK;

    uint32_t cid = id;
    transaction_partition_holder *partition_holder =
        cli_state_.get_partition_holder( cid );

    std::vector<cell_identifier> cids( write_keys.begin(), write_keys.end() );
    DCHECK_EQ( write_keys.size(), write_vals.size() );
    DCHECK_EQ( cids.size(), write_vals.size() );
    for( uint32_t pos = 0; pos < cids.size(); pos++ ) {
        const auto &c_id = cids.at( pos );
        bool        insert_ok = partition_holder->insert_data(
            c_id, write_vals.at( pos ), db_->get_cell_data_type( c_id ) );

        if( !insert_ok ) {
            _return.errMsg = "Unable to insert cell";
            LOG( WARNING ) << _return.errMsg << ":" << c_id << ", client:" << id;
            break;
        }
    }
    SFENCE_IF_2PC();
    stop_timer( RPC_INSERT_TIMER_ID );

    DVLOG( 15 ) << "rpc_insert for client:" << id << " " << _return.status;
}
void site_manager_handler::rpc_insert(
    query_result &_return, const ::clientid id,
    const std::vector<::cell_key> &write_keys,
    const std::vector<std::string> &  write_vals ) {
    DVLOG( k_rpc_handler_log_level ) << "SM RPC: rpc_insert for client:" << id;

    do_internal_site_manager_function( internal_rpc_insert, _return, id,
                                       write_keys, write_vals );

    DVLOG( k_rpc_handler_log_level )
        << "SM RETURN RPC: rpc_insert for client:" << id
        << ", return status:" << _return.status;
}

void site_manager_handler::internal_rpc_one_shot_sproc(
    one_shot_sproc_result &_return, const ::clientid id,
    const ::snapshot_vector &client_session_version_vector,
    const std::vector<::partition_column_identifier> &write_set,
    const std::vector<::partition_column_identifier> &read_set,
    const std::vector<::partition_column_identifier> &inflight_set,
    const std::string &name, const std::vector<cell_key_ranges> &write_ckrs,
    const std::vector<cell_key_ranges> &read_ckrs,
    const std::string &                 sproc_args ) {

    abort_result  abort_res;  // we need to abort ourselves here
    begin_result  begin_res;
    commit_result commit_res;
    sproc_result  sproc_res;

    DVLOG( 10 ) << "One shot sproc for client:" << id << ", " << name;
    start_timer( RPC_ONE_SHOT_SPROC_TIMER_ID );

    DVLOG( 20 ) << "One shot sproc for client:" << id << ", " << name
                << ", write_set:" << write_set << ", read_set:" << read_set
                << ", inflight_set:" << inflight_set
                << ", write_ckrs:" << write_ckrs << ", read_ckrs:" << read_ckrs;

    internal_rpc_begin_transaction(
        begin_res, id, client_session_version_vector, write_set, read_set,
        inflight_set, false /* allow missing */ );
    if( begin_res.status != exec_status_type::COMMAND_OK ) {
        _return.status = begin_res.status;
        DVLOG( 5 ) << "Unable to begin one shot sproc " << name << " :"
                   << begin_res.status << ", " << begin_res.errMsg;

        if( _return.status != exec_status_type::DATA_NOT_LOCATED_AT_ONE_SITE ) {
            // if the data isn't there than we can't abort, so only abort in
            // this case
            internal_rpc_abort_transaction( abort_res, id );
        }

        stop_timer( RPC_ONE_SHOT_SPROC_TIMER_ID );
        return;
    }

    internal_rpc_stored_procedure( sproc_res, id, name, write_ckrs, read_ckrs,
                                   sproc_args );
    if( sproc_res.status != exec_status_type::COMMAND_OK ) {
        _return.status = sproc_res.status;
        DVLOG( 5 ) << "Unable to execute one shot sproc " << name << " :"
                   << sproc_res.status;

        internal_rpc_abort_transaction( abort_res, id );

        stop_timer( RPC_ONE_SHOT_SPROC_TIMER_ID );
        return;
    }

    _return.return_code = sproc_res.return_code;
    _return.res_args = sproc_res.res_args;

    internal_rpc_commit_transaction( commit_res, id );
    if( commit_res.status != exec_status_type::COMMAND_OK ) {
        _return.status = commit_res.status;
        DVLOG( 5 ) << "Unable to commit one shot sproc:" << commit_res.status
                   << ", " << commit_res.errMsg;

        rpc_abort_transaction( abort_res, id );

        stop_timer( RPC_ONE_SHOT_SPROC_TIMER_ID );
        return;
    }

    _return.status = commit_res.status;
    _return.session_version_vector =
        std::move( commit_res.session_version_vector );
    _return.write_set = write_set;
    _return.read_set = read_set;

    stop_timer( RPC_ONE_SHOT_SPROC_TIMER_ID );
    DVLOG( 10 ) << "One shot sproc for client:" << id << ", " << name
                << " okay!";
}
void site_manager_handler::rpc_one_shot_sproc(
    one_shot_sproc_result &_return, const ::clientid id,
    const ::snapshot_vector &client_session_version_vector,
    const std::vector<::partition_column_identifier> &write_set,
    const std::vector<::partition_column_identifier> &read_set,
    const std::vector<::partition_column_identifier> &inflight_set,
    const std::string &name, const std::vector<cell_key_ranges> &write_ckrs,
    const std::vector<cell_key_ranges> &read_ckrs,
    const std::string &                 sproc_args ) {

    DVLOG( k_rpc_handler_log_level )
        << "SM RPC: rpc_one_shot_sproc for client:" << id << ", name:" << name;

    do_internal_site_manager_function( internal_rpc_one_shot_sproc, _return, id,
                                       client_session_version_vector, write_set,
                                       read_set, inflight_set, name, write_ckrs,
                                       read_ckrs, sproc_args );

    DVLOG( k_rpc_handler_log_level )
        << "SM RETURN RPC: rpc_one_shot_sproc for client:" << id
        << ", name:" << name << ", return status:" << _return.status;
}

void site_manager_handler::internal_rpc_one_shot_scan(
    one_shot_scan_result &_return, const ::clientid id,
    const ::snapshot_vector &client_session_version_vector,
    const std::vector<::partition_column_identifier> &read_set,
    const std::vector<::partition_column_identifier> &inflight_set,
    const std::string &name, const std::vector<scan_arguments> &scan_args,
    const std::string &sproc_args ) {

    abort_result  abort_res;  // we need to abort ourselves here
    begin_result  begin_res;
    commit_result commit_res;
    scan_result   scan_res;

    DVLOG( 10 ) << "One shot scan for client:" << id << ", " << name;
    start_timer( RPC_ONE_SHOT_SCAN_TIMER_ID );

    std::vector<::partition_column_identifier> write_set;

    DVLOG( 20 ) << "One shot scan for client:" << id << ", " << name
                << ", read_set:" << read_set
                << ", inflight_set:" << inflight_set;

    internal_rpc_begin_transaction(
        begin_res, id, client_session_version_vector, write_set, read_set,
        inflight_set, true /* allowing missing */ );
    if( begin_res.status != exec_status_type::COMMAND_OK ) {
        _return.status = begin_res.status;
        DVLOG( 5 ) << "Unable to begin one shot scan " << name << " :"
                   << begin_res.status << ", " << begin_res.errMsg;

        if( _return.status != exec_status_type::DATA_NOT_LOCATED_AT_ONE_SITE ) {
            // if the data isn't there than we can't abort, so only abort in
            // this case
            internal_rpc_abort_transaction( abort_res, id );
        }

        stop_timer( RPC_ONE_SHOT_SCAN_TIMER_ID );
        return;
    }

    internal_rpc_scan( scan_res, id, name, scan_args, sproc_args );
    if( scan_res.status != exec_status_type::COMMAND_OK ) {
        _return.status = scan_res.status;
        DVLOG( 5 ) << "Unable to execute one shot sproc " << name << " :"
                   << scan_res.status;

        internal_rpc_abort_transaction( abort_res, id );

        stop_timer( RPC_ONE_SHOT_SCAN_TIMER_ID );
        return;
    }

    _return.return_code = scan_res.return_code;
    _return.res_tuples = scan_res.res_tuples;

    internal_rpc_commit_transaction( commit_res, id );
    if( commit_res.status != exec_status_type::COMMAND_OK ) {
        _return.status = commit_res.status;
        DVLOG( 5 ) << "Unable to commit one shot scan:" << commit_res.status
                   << ", " << commit_res.errMsg;

        rpc_abort_transaction( abort_res, id );

        stop_timer( RPC_ONE_SHOT_SCAN_TIMER_ID );
        return;
    }

    _return.status = commit_res.status;
    _return.session_version_vector =
        std::move( commit_res.session_version_vector );
    _return.write_set = write_set;
    _return.read_set = read_set;

    stop_timer( RPC_ONE_SHOT_SCAN_TIMER_ID );
    DVLOG( 10 ) << "One shot scan for client:" << id << ", " << name
                << " okay!";
}
void site_manager_handler::rpc_one_shot_scan(
    one_shot_scan_result &_return, const ::clientid id,
    const ::snapshot_vector &client_session_version_vector,
    const std::vector<::partition_column_identifier> &read_set,
    const std::vector<::partition_column_identifier> &inflight_set,
    const std::string &name, const std::vector<scan_arguments> &scan_args,
    const std::string &sproc_args ) {

    DVLOG( k_rpc_handler_log_level )
        << "SM RPC: rpc_one_shot_scan for client:" << id << ", name:" << name;

    do_internal_site_manager_function(
        internal_rpc_one_shot_scan, _return, id, client_session_version_vector,
        read_set, inflight_set, name, scan_args, sproc_args );

    DVLOG( k_rpc_handler_log_level )
        << "SM RETURN RPC: rpc_one_shot_scan for client:" << id
        << ", name:" << name << ", return status:" << _return.status;
}

void site_manager_handler::internal_rpc_add_partitions(
    commit_result &_return, const ::clientid id,
    const ::snapshot_vector &client_session_version_vector,
    const std::vector<::partition_column_identifier> &pids_to_add,
    const int32_t                                     master_location,
    const std::vector<partition_type::type> &         p_types,
    const std::vector<storage_tier_type::type> &      s_types,
    const ::propagation_configuration &               prop_config ) {
    DVLOG( 10 ) << "rpc_add_partitions client:" << id;

    DCHECK_EQ( master_location, db_->get_site_location() );
    DCHECK_EQ( p_types.size(), pids_to_add.size() );
    DCHECK_EQ( s_types.size(), pids_to_add.size() );

    start_timer( RPC_ADD_PARTITIONS_TIMER_ID );

    uint32_t cid = id;

    for( uint32_t pos = 0; pos < pids_to_add.size(); pos++ ) {
        const auto &pid = pids_to_add.at( pos );
        const auto &p_type = p_types.at( pos );
        const auto &s_type = s_types.at( pos );

        DVLOG( 15 ) << "add partition client:" << id << ", partition:" << pid
                    << ", master_location:" << master_location
                    << ", partition type:" << p_type
                    << ", storage type:" << s_type;
        db_->add_partition( cid, pid, master_location, p_type, s_type,
                            prop_config );
    }

    _return.status = exec_status_type::COMMAND_OK;
    _return.session_version_vector = client_session_version_vector;

    stop_timer( RPC_ADD_PARTITIONS_TIMER_ID );

    DVLOG( 10 ) << "rpc_add_partitions client:" << id << " okay!";
}
void site_manager_handler::rpc_add_partitions(
    commit_result &_return, const ::clientid id,
    const ::snapshot_vector &client_session_version_vector,
    const std::vector<::partition_column_identifier> &pids_to_add,
    const int32_t                                     master_location,
    const std::vector<partition_type::type> &         p_types,
    const std::vector<storage_tier_type::type> &      s_types,
    const ::propagation_configuration &               prop_config ) {

    DVLOG( k_rpc_handler_log_level )
        << "SM RPC: rpc_add_partitions for client:" << id
        << ", pids_to_add:" << pids_to_add;

    do_internal_site_manager_function(
        internal_rpc_add_partitions, _return, id, client_session_version_vector,
        pids_to_add, master_location, p_types, s_types, prop_config );

    DVLOG( k_rpc_handler_log_level )
        << "SM RETURN RPC: rpc_add_partitions for client:" << id
        << ", pids_to_add:" << pids_to_add
        << ", return status:" << _return.status;
}

void site_manager_handler::internal_rpc_add_replica_partitions(
    commit_result &_return, const ::clientid id,
    const std::vector<snapshot_partition_column_state> &snapshots,
    const std::vector<partition_type::type> &           p_types,
    const std::vector<storage_tier_type::type> &        s_types ) {

    DVLOG( 10 ) << "rpc_add_replica_partitions client:" << id;

    start_timer( RPC_ADD_REPLICA_PARTITIONS_TIMER_ID );


    uint32_t cid = id;
    db_->add_replica_partitions( cid, snapshots, p_types, s_types );

    _return.status = exec_status_type::COMMAND_OK;
    // _return.session_version_vector = client_session_version_vector;

    stop_timer( RPC_ADD_REPLICA_PARTITIONS_TIMER_ID );

    DVLOG( 10 ) << "rpc_add_replica_partitions client:" << id << " okay!";
}
void site_manager_handler::rpc_add_replica_partitions(
    commit_result &_return, const ::clientid id,
    const std::vector<snapshot_partition_column_state> &snapshots,
    const std::vector<partition_type::type> &           p_types,
    const std::vector<storage_tier_type::type> &        s_types ) {
    DVLOG( k_rpc_handler_log_level )
        << "SM RPC: rpc_add_replica_partitions for client:" << id;

    do_internal_site_manager_function( internal_rpc_add_replica_partitions,
                                       _return, id, snapshots, p_types,
                                       s_types );
    DVLOG( k_rpc_handler_log_level )
        << "SM RETURN RPC: rpc_add_replica_partitions for client:" << id
        << ", return status:" << _return.status;
}

void site_manager_handler::internal_rpc_snapshot_partitions(
    snapshot_partition_columns_results &_return, const ::clientid id,
    const std::vector<::partition_column_identifier> &pids ) {
    DVLOG( 10 ) << "rpc_snapshot_partitions client:" << id;

    start_timer( RPC_SNAPSHOT_PARTITIONS_TIMER_ID );

    uint32_t cid = id;
    _return.status = exec_status_type::COMMAND_OK;

    for( const auto &pid : pids ) {
        DVLOG( 15 ) << "rpc snapshot partition client:" << id
                    << ", partition:" << pid;
        auto ret = db_->snapshot_partition( cid, pid );
        bool okay = std::get<0>( ret );
        if( !okay ) {
            LOG( WARNING ) << "Unable to snapshot, as partition " << pid
                           << " not located at site, for client:" << id;
            _return.status = exec_status_type::DATA_NOT_LOCATED_AT_ONE_SITE;
            _return.snapshots.clear();
            break;
        }
        auto snap_state = std::get<1>( ret );
        _return.snapshots.push_back( snap_state );
    }

    stop_timer( RPC_SNAPSHOT_PARTITIONS_TIMER_ID );
    (void) cid;

    DVLOG( 10 ) << "rpc_snapshot_partitions client:" << id << " okay!";
}
void site_manager_handler::rpc_snapshot_partitions(
    snapshot_partition_columns_results &_return, const ::clientid id,
    const std::vector<::partition_column_identifier> &pids ) {
    DVLOG( k_rpc_handler_log_level )
        << "SM RPC: rpc_snapshot_partitions for client:" << id
        << ", pids:" << pids;

    do_internal_site_manager_function( internal_rpc_snapshot_partitions,
                                       _return, id, pids );

    DVLOG( k_rpc_handler_log_level )
        << "SM RETURN RPC: rpc_snapshot_partitions for client:" << id
        << ", pids:" << pids << ", return status:" << _return.status;
}

void site_manager_handler::internal_rpc_remove_partitions(
    commit_result &_return, const ::clientid id,
    const ::snapshot_vector &                  client_session_version_vector,
    const std::vector<::partition_column_identifier> &pids_to_remove ) {
    DVLOG( 10 ) << "rpc_remove_partitions client:" << id;

    start_timer( RPC_REMOVE_PARTITIONS_TIMER_ID );

    uint32_t cid = id;

    for( const auto &pid : pids_to_remove ) {
        DVLOG( 15 ) << "remove partition client:" << id
                    << ", partition:" << pid;
        db_->remove_partition( cid, pid );
    }

    _return.status = exec_status_type::COMMAND_OK;
    _return.session_version_vector = client_session_version_vector;

    stop_timer( RPC_REMOVE_PARTITIONS_TIMER_ID );

    DVLOG( 10 ) << "rpc_remove_partitions client:" << id << " okay!";
}
void site_manager_handler::rpc_remove_partitions(
    commit_result &_return, const ::clientid id,
    const ::snapshot_vector &                  client_session_version_vector,
    const std::vector<::partition_column_identifier> &pids_to_remove ) {

    DVLOG( k_rpc_handler_log_level )
        << "SM RPC: rpc_remove_partitions for client:" << id
        << ", pids_to_remove:" << pids_to_remove;

    do_internal_site_manager_function( internal_rpc_remove_partitions, _return,
                                       id, client_session_version_vector,
                                       pids_to_remove );

    DVLOG( k_rpc_handler_log_level )
        << "SM RETURN RPC: rpc_remove_partitions for client:" << id
        << ", pids_to_remove:" << pids_to_remove
        << ", return status:" << _return.status;
}

void site_manager_handler::internal_rpc_merge_partition(
    commit_result &_return, const ::clientid id,
    const ::snapshot_vector &            client_session_version_vector,
    const ::partition_column_identifier &lower_partition,
    const ::partition_column_identifier &higher_partition,
    const partition_type::type &         merge_type,
    const storage_tier_type::type &      merge_storage_type,
    const ::propagation_configuration &  prop_config ) {
    DVLOG( 10 ) << "rpc_merge_partition client:" << id;

    start_timer( RPC_MERGE_PARTITION_TIMER_ID );

    uint32_t cid = id;

    auto merged_result = db_->merge_partition(
        cid, client_session_version_vector, lower_partition, higher_partition,
        merge_type, merge_storage_type, prop_config );

    if( !std::get<0>( merged_result ) ) {
        DLOG( WARNING ) << "Unable to rpc_merge_partition client:" << id;
        _return.status = exec_status_type::DATA_NOT_LOCATED_AT_ONE_SITE;
    } else {
        _return.status = exec_status_type::COMMAND_OK;
        _return.session_version_vector = client_session_version_vector;
    }

    stop_timer( RPC_MERGE_PARTITION_TIMER_ID );

    DVLOG( 10 ) << "rpc_merge_partition client:" << id << " okay!";
}
void site_manager_handler::rpc_merge_partition(
    commit_result &_return, const ::clientid id,
    const ::snapshot_vector &            client_session_version_vector,
    const ::partition_column_identifier &lower_partition,
    const ::partition_column_identifier &higher_partition,
    const partition_type::type           merge_type,
    const storage_tier_type::type        merge_storage_type,
    const ::propagation_configuration &  prop_config ) {
    DVLOG( k_rpc_handler_log_level )
        << "SM RPC: rpc_merge_partition for client:" << id
        << ", lower_partition:" << lower_partition
        << ", higher_partition:" << higher_partition;

    do_internal_site_manager_function(
        internal_rpc_merge_partition, _return, id,
        client_session_version_vector, lower_partition, higher_partition,
        merge_type, merge_storage_type, prop_config );

    DVLOG( k_rpc_handler_log_level )
        << "SM RETURN RPC: rpc_merge_partition for client:" << id
        << ", lower_partition:" << lower_partition
        << ", higher_partition:" << higher_partition
        << ", merge_type:" << merge_type
        << ", merge_storage_type:" << merge_storage_type
        << ", return status:" << _return.status;
}

void site_manager_handler::internal_rpc_split_partition(
    commit_result &_return, const ::clientid id,
    const ::snapshot_vector &            client_session_version_vector,
    const ::partition_column_identifier &partition,
    const ::cell_key &split_point, const partition_type::type &low_type,
    const partition_type::type &                    high_type,
    const storage_tier_type::type &                 low_storage_type,
    const storage_tier_type::type &                 high_storage_type,
    const std::vector<::propagation_configuration> &prop_configs ) {
    DVLOG( 10 ) << "rpc_split_partition client:" << id;
    start_timer( RPC_SPLIT_PARTITION_TIMER_ID );

    uint32_t cid = id;

    auto split_result = db_->split_partition(
        cid, client_session_version_vector, partition, split_point.row_id,
        split_point.col_id, low_type, high_type, low_storage_type,
        high_storage_type, prop_configs );

    if( !std::get<0>( split_result ) ) {
        DLOG( WARNING ) << "Unable to rpc_split_partition client:" << id;
        _return.status = exec_status_type::DATA_NOT_LOCATED_AT_ONE_SITE;
    } else {
        _return.status = exec_status_type::COMMAND_OK;
        _return.session_version_vector = client_session_version_vector;
    }

    stop_timer( RPC_SPLIT_PARTITION_TIMER_ID );
    DVLOG( 10 ) << "rpc_split_partition client:" << id << " okay!";
}
void site_manager_handler::rpc_split_partition(
    commit_result &_return, const ::clientid id,
    const ::snapshot_vector &            client_session_version_vector,
    const ::partition_column_identifier &partition,
    const ::cell_key &split_point, const partition_type::type low_type,
    const partition_type::type                      high_type,
    const storage_tier_type::type                   low_storage_type,
    const storage_tier_type::type                   high_storage_type,
    const std::vector<::propagation_configuration> &prop_configs ) {
    DVLOG( k_rpc_handler_log_level )
        << "SM RPC: rpc_split_partition for client:" << id
        << ", partition:" << partition << ", split_point:" << split_point;

    do_internal_site_manager_function(
        internal_rpc_split_partition, _return, id,
        client_session_version_vector, partition, split_point, low_type,
        high_type, low_storage_type, high_storage_type, prop_configs );

    DVLOG( k_rpc_handler_log_level )
        << "SM RETURN RPC: rpc_split_partition for client:" << id
        << ", partition:" << partition << ", split_point:" << split_point
        << ", return status:" << _return.status;
}

void site_manager_handler::internal_rpc_change_partition_output_destination(
    commit_result &_return, const ::clientid id,
    const ::snapshot_vector &                  client_session_version_vector,
    const std::vector<::partition_column_identifier> &pids,
    const ::propagation_configuration &        new_prop_config ) {
    DVLOG( 10 ) << "rpc_change_partition_output_destination client:" << id;
    start_timer( RPC_CHANGE_PARTITION_OUTPUT_DESTINATION_TIMER_ID );

    uint32_t cid = id;

    auto change_result = db_->change_partition_output_destination(
        cid, client_session_version_vector, pids, new_prop_config );

    if( !std::get<0>( change_result ) ) {
        LOG( WARNING )
            << "Unable to rpc_change_partition_output_destination client:"
            << id;
        _return.status = exec_status_type::DATA_NOT_LOCATED_AT_ONE_SITE;
    } else {
        _return.status = exec_status_type::COMMAND_OK;
        _return.session_version_vector = client_session_version_vector;
    }

    stop_timer( RPC_CHANGE_PARTITION_OUTPUT_DESTINATION_TIMER_ID );
    DVLOG( 10 ) << "rpc_change_partition_output_destination client:" << id
                << " okay!";
}
void site_manager_handler::rpc_change_partition_output_destination(
    commit_result &_return, const ::clientid id,
    const ::snapshot_vector &                  client_session_version_vector,
    const std::vector<::partition_column_identifier> &pids,
    const ::propagation_configuration &        new_prop_config ) {
    DVLOG( k_rpc_handler_log_level )
        << "SM RPC: rpc_change_partition_output_destination for client:" << id
        << ", pid:" << pids << ", new_prop_config:" << new_prop_config;

    do_internal_site_manager_function(
        internal_rpc_change_partition_output_destination, _return, id,
        client_session_version_vector, pids, new_prop_config );

    DVLOG( k_rpc_handler_log_level )
        << "SM RETURN RPC: rpc_change_partition_output_destination for client:"
        << id << ", pid:" << pids << ", new_prop_config:" << new_prop_config
        << ", return status:" << _return.status;
}

void site_manager_handler::internal_rpc_change_partition_type(
    commit_result &_return, const ::clientid id,
    const std::vector<::partition_column_identifier> &pids,
    const std::vector<partition_type::type> &         part_types,
    const std::vector<storage_tier_type::type> &      storage_types ) {
    DVLOG( 10 ) << "rpc_change_partition_type client:" << id;
    start_timer( RPC_CHANGE_PARTITION_CHANGE_TYPE_TIMER_ID );

    uint32_t cid = id;

    auto change_result =
        db_->change_partition_types( cid, pids, part_types, storage_types );

    if( !std::get<0>( change_result ) ) {
        LOG( WARNING ) << "Unable to rpc_change_partition_type client:" << id;
        _return.status = exec_status_type::DATA_NOT_LOCATED_AT_ONE_SITE;
    } else {
        _return.status = exec_status_type::COMMAND_OK;
        _return.session_version_vector = std::get<1>( change_result );
    }

    stop_timer( RPC_CHANGE_PARTITION_CHANGE_TYPE_TIMER_ID );
    DVLOG( 10 ) << "rpc_change_partition_type client:" << id << " okay!";
}
void site_manager_handler::rpc_change_partition_type(
    commit_result &_return, const ::clientid id,
    const std::vector<::partition_column_identifier> &pids,
    const std::vector<partition_type::type> &         part_types,
    const std::vector<storage_tier_type::type> &      storage_types ) {
    DVLOG( k_rpc_handler_log_level )
        << "SM RPC: rpc_change_partition_type for client:" << id
        << ", pid:" << pids << ", partition_type:" << part_types
        << ", storage_type:" << storage_types;

    do_internal_site_manager_function( internal_rpc_change_partition_type,
                                       _return, id, pids, part_types,
                                       storage_types );

    DVLOG( k_rpc_handler_log_level )
        << "SM RETURN RPC: rpc_change_partition_type for client:" << id
        << ", pid:" << pids << ", partition_types:" << part_types
        << ", storage_types:" << storage_types
        << ", return status:" << _return.status;
}


void site_manager_handler::rpc_persist_db_to_file(
    persistence_result &res, const clientid id, const std::string &out_name,
    const std::string &out_part_name,
    const std::vector<std::vector<propagation_configuration>>
        &site_propagation_configs ) {
    DVLOG( k_rpc_handler_log_level )
        << "SM RPC: rpc_persist_db_to_file for client:" << id
        << ", out_name:" << out_name << ", out_part_name:" << out_part_name;

    try {
        DVLOG( 5 ) << "Persisting database to file:" << out_name;

        data_site_persistence_manager manager(
            out_name, out_part_name, db_->get_tables(),
            db_->get_update_destination_generator(), site_propagation_configs,
            construct_persistence_configs(), db_->get_site_location() );
        manager.persist();

        res.status = exec_status_type::COMMAND_OK;

        DVLOG( 5 ) << "Persisting database to file:" << out_name << " okay!";
    }
    sm_catch_t_except;

    DVLOG( k_rpc_handler_log_level )
        << "SM RETURN RPC: rpc_persist_db_to_file for client:" << id
        << ", out_name:" << out_name << ", out_part_name:" << out_part_name
        << ", return status:" << res.status;
}

void site_manager_handler::rpc_restore_db_from_file(
    persistence_result &res, const clientid id, const std::string &in_name,
    const std::string &in_part_name,
    const std::vector<std::vector<propagation_configuration>>
        &site_propagation_configs ) {
    DVLOG( k_rpc_handler_log_level )
        << "SM RPC: rpc_restore_db_from_file for client:" << id
        << ", in_name:" << in_name << ", in_part_name:" << in_part_name;

    try {
        DVLOG( 5 ) << "Restoring database from file:" << in_name;
        data_site_persistence_manager manager(
            in_name, in_part_name, db_->get_tables(),
            db_->get_update_destination_generator(), site_propagation_configs,
            construct_persistence_configs(), db_->get_site_location() );
        manager.load();

        res.status = exec_status_type::COMMAND_OK;
        DVLOG( 5 ) << "Restoring database from file:" << in_name << " okay!";
    }
    sm_catch_t_except;

    DVLOG( k_rpc_handler_log_level )
        << "SM RETURN RPC: rpc_restore_db_from_file for client:" << id
        << ", in_name:" << in_name << ", in_part_name:" << in_part_name
        << ", return status:" << res.status;
}

