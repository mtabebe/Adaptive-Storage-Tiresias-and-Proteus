#pragma once

#define ycsb_benchmark_worker_types template <bool do_begin_commit>
#define ycsb_benchmark_worker_templ ycsb_benchmark_worker<do_begin_commit>

#define ycsb_benchmark_worker_templ_do_commit ycsb_benchmark_worker<true>
#define ycsb_benchmark_worker_templ_no_commit ycsb_benchmark_worker<false>

