#pragma once

#include "../../data-site/db/db.h"
#include "../../data-site/single-site-db/db_abstraction.h"
#include "../../data-site/single-site-db/db_abstraction_types.h"
#include "../../templates/twitter_benchmark_types.h"
#include "../benchmark_interface.h"
#include "../workload_operation_selector.h"
#include "twitter_benchmark_worker.h"
#include "twitter_configs.h"
#include "twitter_db_operators.h"
#include "twitter_loader.h"

class twitter_benchmark : public benchmark_interface {
   public:
    twitter_benchmark( const twitter_configs&      configs,
                         const db_abstraction_configs& abstraction_configs );
    ~twitter_benchmark();

    void init();

    void                 create_database();
    void                 load_database();
    void                 run_workload();
    benchmark_statistics get_statistics();


   private:
    void load_users( uint32_t client_id, uint32_t user_start, uint32_t user_end,
                     const std::vector<std::vector<uint32_t>>& follow_data,
                     const std::vector<std::vector<uint32_t>>& follower_data,
                     const std::vector<uint32_t>&              tweet_data,
                     const std::vector<uint32_t>&              follow_ids,
                     const std::vector<uint32_t>&              follower_ids );

    // helpers for run benchmark worker
    ALWAYS_INLINE
    std::vector<twitter_benchmark_worker_templ_do_commit*> create_workers();
    ALWAYS_INLINE void                                       start_workers(
        std::vector<twitter_benchmark_worker_templ_do_commit*>& workers )
        const;
    ALWAYS_INLINE void stop_workers(
        std::vector<twitter_benchmark_worker_templ_do_commit*>& workers )
        const;
    ALWAYS_INLINE void gather_workers(
        std::vector<twitter_benchmark_worker_templ_do_commit*>& workers );

    db_abstraction*        db_;
    twitter_configs        configs_;
    db_abstraction_configs abstraction_configs_;

    zipf_distribution_cdf*      tweet_z_cdf_;
    zipf_distribution_cdf*      follows_z_cdf_;
    zipf_distribution_cdf*      followee_z_cdf_;
    workload_operation_selector op_selector_;

    benchmark_statistics statistics_;
};

#include "twitter_benchmark-inl.h"
