#pragma once

#define twitter_loader_types template <bool do_begin_commit>
#define twitter_loader_templ twitter_loader<do_begin_commit>
#define twitter_loader_templ_do_commit twitter_loader<true>
#define twitter_loader_templ_no_commit twitter_loader<false>

#define twitter_benchmark_worker_types template <bool do_begin_commit>
#define twitter_benchmark_worker_templ \
    twitter_benchmark_worker<do_begin_commit>

#define twitter_benchmark_worker_templ_do_commit \
    twitter_benchmark_worker<true>
#define twitter_benchmark_worker_templ_no_commit \
    twitter_benchmark_worker<false>

#define db_instance_types lock_t, update_propagator_t, update_applier_a
