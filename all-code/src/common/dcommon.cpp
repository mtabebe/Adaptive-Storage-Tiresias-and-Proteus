#include "dcommon.h"

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

using namespace std;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::server;

TSocket* create_bound_socket( const site_manager_location& loc,
                              const std::string&           nic_name ) {
    // TCP/IP socket
    int sock;
    sock = socket( AF_INET, SOCK_STREAM, 0 );
    if( sock == -1 ) {
        LOG( FATAL ) << "Cannot create socket: " << errno;
    }

    // Bind our socket on the 10 Gbit interface
    struct ifreq ifr;
    memset( &ifr, 0, sizeof( ifr ) );
    strcpy( ifr.ifr_name, nic_name.c_str() );
    if( setsockopt( sock, SOL_SOCKET, SO_BINDTODEVICE, (void*) &ifr,
                    sizeof( ifr ) ) < 0 ) {
        LOG( FATAL ) << "Cannot bind to device " << nic_name << " :" << errno;
    }

    // Set up address
    struct sockaddr_in saddr;
    saddr.sin_addr.s_addr = inet_addr( loc.ip_.c_str() );
    saddr.sin_family = AF_INET;
    saddr.sin_port = htons( loc.port_ );

    int one = 1;
    int rc = setsockopt( sock, SOL_TCP, TCP_NODELAY, &one, sizeof( one ) );
    if( rc != 0 ) {
        LOG( FATAL )
            << "Could not set TCP_NODELAY on SiteSelector socket... errno: "
            << errno;
    }

    // Connect
    if( connect( sock, (struct sockaddr*) &saddr, sizeof( saddr ) ) < 0 ) {
        LOG( FATAL ) << "Could not connect to site manager: " << errno;
    }

    return new TSocket( sock );
}



TSocket* create_socket_to_site_manager( const site_manager_location& loc,
                                        bool               bind_to_NIC,
                                        const std::string& nic_name ) {
    if( bind_to_NIC ) {
        return create_bound_socket( loc, nic_name );
    }
    return new TSocket( loc.ip_.c_str(), loc.port_ );
}

bool create_site_managers_from_file(
    string file_name, bool bind_to_nic, string nic_name,
    unique_ptr<std::vector<client_conn_pool>> const& pool,
    unique_ptr<std::unordered_map<int, site_manager_location>> const&
        site_manager_locs,
    int num_clients, int num_threads ) {
    ifstream infile( file_name );
    string   line;
    int           num_sites = 0;
    for( int i = 0;; i++ ) {
        bool ok = (bool) std::getline( infile, line, ':' );
        if( !ok ) {
            break;
        }
        DVLOG( 5 ) << "line:" << line;
        string ip( std::move( line ) );

        std::getline( infile, line, '\n' );
        int                   port = std::stoi( line );
        site_manager_location loc( ip, port );
        DVLOG( 5 ) << "Got location:" << i << ", as:" << ip << ", " << port;
        site_manager_locs->insert( {i, loc} );
        num_sites = i;
    }
    infile.close();
    DCHECK_EQ( num_sites, k_num_sites - 1 );

    for( int tid = 0; tid <= num_threads; tid++ ) {
        for( int client = 0; client <= num_clients; client++ ) {
            std::unique_ptr<std::vector<sm_client_ptr>> clients(
                new std::vector<sm_client_ptr>() );

            int translated_id = ( tid * num_clients ) + client;

            for( int i = 0; i <= num_sites; i++ ) {
                site_manager_location loc = site_manager_locs->at( i );
                TSocket*              sock =
                    create_socket_to_site_manager( loc, bind_to_nic, nic_name );

                // Feed our bound socket in
                std::shared_ptr<TTransport> cliSock( sock );
                std::shared_ptr<TTransport> cliTrans(
                    new TBufferedTransport( cliSock ) );
                std::shared_ptr<TProtocol> cliProto(
                    new TBinaryProtocol( cliTrans ) );
                cliTrans->open();

                DVLOG( 10 ) << "adding connection:" << i << " at " << loc.ip_
                            << "," << loc.port_ << " for client:" << client
                            << ", thread_id:" << tid
                            << ", translated clientid:" << translated_id;
                clients->push_back( sm_client_ptr(
                    new site_managerConcurrentClient( cliProto ) ) );
            }

            pool->push_back( std::move( clients ) );
        }
    }
    return true;
}
