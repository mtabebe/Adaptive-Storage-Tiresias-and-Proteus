#pragma once

#include <memory>
#include <vector>

#include "../gen-cpp/gen-cpp/site_manager.h"

typedef std::unique_ptr<site_managerIf> sm_client_ptr;

class client_conn_pool {

   public:
    client_conn_pool( std::unique_ptr<std::vector<sm_client_ptr>> clients );
    const sm_client_ptr &getClient( const int site ) const;
    inline size_t get_size() const { return clients_->size(); }

   private:
    std::unique_ptr<std::vector<sm_client_ptr>> clients_;
};

class client_conn_pools {
   public:
    client_conn_pools( std::unique_ptr<std::vector<client_conn_pool>> clients,
                       int num_clients, int num_threads );

    const sm_client_ptr &getClient( const int site, const int client_id,
                                    const int thread_id ) const;
    const sm_client_ptr &getClient( const int site, const int client_id,
                                    const int thread_id,
                                    const int translated_id ) const;

    int translate_client_id( const int client_id, const int thread_id ) const;

    size_t get_size() const;

   private:
    std::unique_ptr<std::vector<client_conn_pool>> clients_;

    int num_clients_;
    int num_threads_;
};
