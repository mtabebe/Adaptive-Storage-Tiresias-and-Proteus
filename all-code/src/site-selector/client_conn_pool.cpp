#include "client_conn_pool.h"

#include <glog/logging.h>

client_conn_pool::client_conn_pool(
    std::unique_ptr<std::vector<sm_client_ptr>> clients )
    : clients_( std::move( clients ) ) {}

const sm_client_ptr &client_conn_pool::getClient( const int site ) const {
    DVLOG( 20 ) << "Get client site:" << site;
    DVLOG( 20 ) << "Clients_ size:" << clients_->size();
    DCHECK_LT( site, (signed) clients_->size() );
    return clients_->at( site );
}

client_conn_pools::client_conn_pools(
    std::unique_ptr<std::vector<client_conn_pool>> clients, int num_clients,
    int num_threads )
    : clients_( std::move( clients ) ),
      num_clients_( num_clients ),
      num_threads_( num_threads ) {
    DCHECK_LE( num_clients_ * num_threads_, clients_->size() );
}

const sm_client_ptr &client_conn_pools::getClient( const int site,
                                                   const int client_id,
                                                   const int thread_id ) const {
    return getClient( site, client_id, thread_id,
                      translate_client_id( client_id, thread_id ) );
}

const sm_client_ptr &client_conn_pools::getClient(
    const int site, const int client_id, const int thread_id,
    const int translated_id ) const {
    DVLOG( 20 ) << "Get client site:" << site << ", client_id:" << client_id
                << ", thread_id:" << thread_id << ", translated_id";
    DVLOG( 20 ) << "Clients_ size:" << clients_->size()
                << ", num_threads_:" << num_threads_;
    DCHECK_EQ( translate_client_id( client_id, thread_id ), translated_id );
    DCHECK_LT( translated_id, (signed) clients_->size() );
    return clients_->at( translated_id ).getClient( site );
}

int client_conn_pools::translate_client_id( const int client_id,
                                            const int thread_id ) const {
    DCHECK_LT( client_id, num_clients_ );
    DCHECK_LT( thread_id, num_threads_ );
    int translated_id = ( thread_id * num_clients_ ) + client_id;
    DVLOG( 20 ) << "Translated client:" << client_id
                << ", thread_id:" << thread_id << ", to:" << translated_id;
    return translated_id;
}

size_t client_conn_pools::get_size() const {
    if( clients_->size() == 0 ) {
        return 0;
    }

    return clients_->at( 0 ).get_size();
}
