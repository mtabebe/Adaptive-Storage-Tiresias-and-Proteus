#include <thrift/TToString.h>
#include <thrift/concurrency/PlatformThreadFactory.h>
#include <thrift/concurrency/ThreadManager.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TSimpleServer.h>
#include <thrift/server/TThreadPoolServer.h>
#include <thrift/server/TThreadedServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportUtils.h>

#include <arpa/inet.h>
#include <cerrno>
#include <fstream>
#include <iostream>
#include <signal.h>
#include <net/if.h>
#include <netinet/tcp.h>
#include <sstream>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <sys/types.h>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "../src/benchmark/benchmark_executor.h"
#include "../src/common/dcommon.h"
#include "../src/common/perf_tracking.h"
#include "../src/gen-cpp/gen-cpp/SiteSelector.h"
#include "../src/gen-cpp/gen-cpp/site_manager.h"
#include "../src/site-selector/client_conn_pool.h"
#include "../src/site-selector/site_selector_executor.h"
#include "../src/site-selector/site_selector_handler.h"

using namespace std;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::server;

DEFINE_string( sm_config_file, "~/.site_manager_list.cfg",
               "site selector configuration file" );
DEFINE_string( ss_address, "0.0.0.0", "address to bind to" );
DEFINE_int32( ss_port, 9090, "site selector port to listen on" );
DEFINE_bool( bind_to_NIC, true,
             "Should the site selector bind to a specific NIC interface" );
DEFINE_string( NIC_name, "enp4s0",
               "The NIC interface the site selector should bind to" );

std::shared_ptr<site_selector_handler> construct_site_selector_handler() {
    std::unique_ptr<std::unordered_map<int, site_manager_location>>
        site_manager_locs(
            new std::unordered_map<int, site_manager_location>() );
    std::unique_ptr<std::vector<client_conn_pool>> clients(
        new std::vector<client_conn_pool>() );
    auto trans_preparer_configs = construct_transaction_preparer_configs();

    int num_threads = std::max(
        trans_preparer_configs.num_worker_threads_per_client_,
        trans_preparer_configs.periodic_site_selector_operations_configs_
            .num_poller_threads_ );
    int  num_clients = trans_preparer_configs.num_clients_;
    bool ok = create_site_managers_from_file(
        FLAGS_sm_config_file, FLAGS_bind_to_NIC, FLAGS_NIC_name, clients,
        site_manager_locs, num_clients, num_threads );
    if( !ok ) {
        LOG( ERROR ) << "Could not read config file " << FLAGS_sm_config_file;
        return nullptr;
    }

    std::shared_ptr<client_conn_pools> conn_pool( new client_conn_pools(
        std::move( clients ), num_clients, num_threads ) );

    LOG( INFO ) << "Established connections to site managers!";

    auto cost_model =
        std::make_shared<cost_modeller2>( construct_cost_modeller_configs() );

    std::shared_ptr<partition_data_location_table> data_loc_tab =
        construct_partition_data_location_table(
            construct_partition_data_location_table_configs(
                site_manager_locs->size() ) );

    auto site_partition_version_info =
        std::make_shared<sites_partition_version_information>( cost_model );

    auto query_predictor = std::make_shared<query_arrival_predictor>(
        construct_query_arrival_predictor_configs() );

    add_tables_to_query_arrival_predictor( query_predictor, data_loc_tab );

    auto enumerator_holder = std::make_shared<stat_tracked_enumerator_holder>(
        construct_stat_tracked_enumerator_configs() );
    auto tier_tracking = std::make_shared<partition_tier_tracking>(
        data_loc_tab, cost_model, query_predictor );

    std::unique_ptr<abstract_site_evaluator> evaluator(
        construct_site_evaluator( construct_heuristic_site_evaluator_configs(
                                      site_manager_locs->size() ),
                                  cost_model, data_loc_tab,
                                  site_partition_version_info, query_predictor,
                                  enumerator_holder, tier_tracking ) );

    // Need table keys and estimated sizes
    std::unique_ptr<transaction_preparer> preparer( new transaction_preparer(
        conn_pool, data_loc_tab, cost_model, site_partition_version_info,
        std::move( evaluator ), query_predictor, enumerator_holder,
        tier_tracking, trans_preparer_configs ) );
    std::shared_ptr<site_selector_handler> handler( new site_selector_handler(
        std::move( site_manager_locs ), std::move( preparer ) ) );
    return handler;
}

void run_site_selector_server() {

    std::shared_ptr<site_selector_handler> handler =
        construct_site_selector_handler();

    VLOG( 0 ) << "Starting the site selector on " << FLAGS_ss_address
              << ", port:" << FLAGS_ss_port;

    std::shared_ptr<TProcessor> processor(
        new SiteSelectorProcessor( handler ) );
    std::shared_ptr<TServerTransport> serverTransport(
        new TServerSocket( FLAGS_ss_address, FLAGS_ss_port ) );
    std::shared_ptr<TTransportFactory> transportFactory(
        new TBufferedTransportFactory() );
    std::shared_ptr<TProtocolFactory> protocolFactory(
        new TBinaryProtocolFactory() );
    TThreadedServer server( processor, serverTransport, transportFactory,
                            protocolFactory );

    if( k_enable_timer_dumps ) {
        struct sigaction action;
        memset( &action, 0, sizeof( struct sigaction ) );
        action.sa_handler = dump_counters;
        sigaction( SIGTERM, &action, NULL );
        sigaction( SIGSEGV, &action, NULL );

        // Normally we wouldn't want to catch this, but this only happens on
        // shutdown and the only reason for something to throw is that a thread
        // is not
        // being joined. In these cases, we still want to dump the counters to
        // see if
        // something has gone horribly wrong...
        sigaction( SIGABRT, &action, NULL );
    }

    VLOG( 0 ) << "Starting the server...";
    google::FlushLogFiles( google::INFO );
    server.serve();
    VLOG( 0 ) << "Done.";
}

int main( int argc, char** argv ) {

    gflags::ParseCommandLineFlags( &argc, &argv, true );
    google::InitGoogleLogging( argv[0] );
    google::InstallFailureSignalHandler();

    init_global_state();

    run_site_selector_server();
    return 0;
}
