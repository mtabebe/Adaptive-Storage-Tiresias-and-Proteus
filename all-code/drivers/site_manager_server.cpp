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

#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <signal.h>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "../src/benchmark/benchmark_executor.h"
#include "../src/common/constants.h"
#include "../src/common/perf_tracking.h"
#include "../src/data-site/site-manager/site_manager_handler.h"
#include "../src/data-site/stored-procedures/stored_procedures_executor.h"
#include "../src/data-site/update-propagation/kafka_update_destination.h"
#include "../src/data-site/update-propagation/kafka_update_source.h"
#include "../src/data-site/update-propagation/vector_update_destination.h"
#include "../src/data-site/update-propagation/vector_update_source.h"
#include "../src/gen-cpp/gen-cpp/site_manager.h"

using namespace std;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::server;

DEFINE_int32( sm_port, 9091, "site manager port to listen on" );
DEFINE_string( sm_address, "0.0.0.0", "address to bind to" );
DEFINE_string( sm_producer_config_file, "~/.site_manager_producer_list.cfg",
               "site mananger producer configuration file" );
DEFINE_string( kafka_brokers, "localhost:9092", "Kafka broker" );

std::vector<kafka_topic_connection_configs> get_kafka_configuration_from_file(
    const std::string& file_name ) {
    std::vector<kafka_topic_connection_configs> conn_configs;

    std::ifstream infile( file_name );

    // line format:
    // offset(int64), partition (int), topic (str)
    std::string partition_str;
    int32_t     partition;
    std::string offset_str;
    int64_t     offset;
    std::string topic_str;
    for( ;; ) {
        bool ok = (bool) std::getline( infile, offset_str, ',' );
        DVLOG( 40 ) << "Line offset_str:" << offset_str;
        if( !ok ) {
            break;
        }
        offset = std::stoll( offset_str );
        ok = (bool) std::getline( infile, partition_str, ',' );
        DVLOG( 40 ) << "Line partition_str:" << partition_str;
        if( !ok ) {
            break;
        }
        partition = std::stoi( partition_str );
        ok = (bool) std::getline( infile, topic_str, '\n' );
        DVLOG( 40 ) << "Line topic_str:" << topic_str;
        DCHECK( ok );

        conn_configs.push_back( construct_kafka_topic_connection_configs(
            topic_str, partition, offset ) );
    }

    return conn_configs;
}

std::shared_ptr<update_destination_generator> make_vector_update_generator() {
    uint32_t num_dests = k_num_update_destinations;
    auto     update_gen = std::make_shared<update_destination_generator>(
        construct_update_destination_generator_configs( num_dests ) );
    for( uint32_t dest = 0; dest < num_dests; dest++ ) {
        update_gen->add_update_destination(
            std::make_shared<vector_update_destination>( dest ) );
    }
    return update_gen;
}

std::shared_ptr<update_destination_generator> make_kafka_update_generator(
    const std::string& broker, const std::string& config_file ) {
    std::vector<kafka_topic_connection_configs> topic_conns =
        get_kafka_configuration_from_file( config_file );
    kafka_producer_configs producer_configs =
        construct_kafka_producer_configs( construct_kafka_common_configs() );
    kafka_connection_configs conn_config =
        construct_kafka_connection_configs( broker );

    DCHECK_LE( k_num_update_destinations, topic_conns.size() );

    auto update_gen = std::make_shared<update_destination_generator>(
        construct_update_destination_generator_configs(
            k_num_update_destinations ) );

    for( uint32_t pos = 0; pos < k_num_update_destinations; pos++ ) {
        update_gen->add_update_destination(
            std::make_shared<kafka_update_destination>(
                conn_config, topic_conns.at( pos ), producer_configs ) );
    }

    return update_gen;
}

std::shared_ptr<update_enqueuers> make_vector_enqueuers() {

    auto enqueuers = make_update_enqueuers();

    auto enqueuer_configs = construct_update_enqueuer_configs();

    for( uint32_t id = 0; id < k_num_update_sources; id++ ) {
        auto enqueuer = std::make_shared<update_enqueuer>();
        enqueuer->set_update_consumption_source(
            std::make_shared<vector_update_source>(), enqueuer_configs );
        enqueuers->add_enqueuer( enqueuer );
    }

    return enqueuers;
}

std::shared_ptr<update_enqueuers> make_kafka_enqueuers(
    const std::string& broker ) {
    kafka_connection_configs conn_config =
        construct_kafka_connection_configs( broker );
    kafka_consumer_configs consumer_config =
        construct_kafka_consumer_configs( construct_kafka_common_configs() );

    auto enqueuers = make_update_enqueuers();

    auto enqueuer_configs = construct_update_enqueuer_configs();

    for( uint32_t id = 0; id < k_num_update_sources; id++ ) {
        auto enqueuer = std::make_shared<update_enqueuer>();

        enqueuer->set_update_consumption_source(
            std::make_shared<kafka_update_source>( conn_config,
                                                   consumer_config ),
            enqueuer_configs );
        enqueuers->add_enqueuer( enqueuer );
    }


    return enqueuers;
}

std::shared_ptr<update_enqueuers> construct_update_enqueuers() {
    std::shared_ptr<update_enqueuers> enqueuers = nullptr;

    switch( k_update_source_type ) {
        case NO_OP_SOURCE:
            enqueuers = make_update_enqueuers();
            break;
        case VECTOR_SOURCE:
            enqueuers = make_vector_enqueuers();
            break;
        case KAFKA_SOURCE:
            enqueuers = make_kafka_enqueuers( FLAGS_kafka_brokers );
            break;
        case UNKNOWN_UPDATE_SOURCE:
            LOG( FATAL ) << "Unknown update source";
    }

    return enqueuers;
}

std::shared_ptr<update_destination_generator> construct_update_generator() {
    std::shared_ptr<update_destination_generator> update_gen = nullptr;
    switch( k_update_destination_type ) {
        case NO_OP_DESTINATION:
            update_gen = make_no_op_update_destination_generator();
            break;
        case VECTOR_DESTINATION:
            update_gen = make_vector_update_generator();
            break;
        case KAFKA_DESTINATION:
            update_gen = make_kafka_update_generator(
                FLAGS_kafka_brokers, FLAGS_sm_producer_config_file );
            break;
        case UNKNOWN_UPDATE_DESTINATION:
            LOG( FATAL ) << "Unknown update destination type!";
    }

    return update_gen;
}

std::shared_ptr<site_manager_handler> construct_site_manager_handler(
    std::shared_ptr<update_destination_generator> update_gen,
    std::shared_ptr<update_enqueuers>             update_enqueuers ) {
    std::vector<table_partition_information> table_partition_info =
        get_benchmark_data_sizes();

    int32_t num_clients = k_bench_num_clients;
    int32_t num_threads =
        std::max( (int32_t) k_num_site_selector_worker_threads_per_client,
                  (int32_t) k_ss_num_poller_threads );

    std::unique_ptr<db> database = construct_database(
        update_gen, update_enqueuers, num_clients, num_threads );
    std::unique_ptr<sproc_lookup_table>       sproc_table =
        construct_sproc_lookup_table();
    void* opaque = construct_sproc_opaque_pointer();
    // gets destroyed by the handler
    std::shared_ptr<site_manager_handler> handler( new site_manager_handler(
        std::move( database ), std::move( sproc_table ), opaque ) );
    handler->init( num_clients, num_threads );
    return handler;
}

void start_site_manager() {
    std::shared_ptr<site_manager_handler> handler =
        construct_site_manager_handler( construct_update_generator(),
                                        construct_update_enqueuers() );

    VLOG( 0 ) << "Starting the site site manager on " << FLAGS_sm_address
              << ", port:" << FLAGS_sm_port;

    std::shared_ptr<TProcessor> processor(
        new site_managerProcessor( handler ) );
    std::shared_ptr<TServerTransport> serverTransport(
        new TServerSocket( FLAGS_sm_address, FLAGS_sm_port ) );
    std::shared_ptr<TTransportFactory> transportFactory(
        new TBufferedTransportFactory() );
    std::shared_ptr<TProtocolFactory> protocolFactory(
        new TBinaryProtocolFactory() );
    TThreadedServer server( processor, serverTransport, transportFactory,
                            protocolFactory );

    // Set up signal handler to dump counters on SIGTERM
    if( k_enable_timer_dumps ) {
        struct sigaction action;
        memset( &action, 0, sizeof( struct sigaction ) );
        action.sa_handler = dump_counters;
        sigaction( SIGTERM, &action, NULL );
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

    start_site_manager();
    return 0;
}
