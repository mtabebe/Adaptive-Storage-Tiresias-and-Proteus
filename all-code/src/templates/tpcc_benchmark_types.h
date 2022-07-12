#pragma once

#define tpcc_loader_types template <bool do_begin_commit>
#define tpcc_loader_templ tpcc_loader<do_begin_commit>
#define tpcc_loader_templ_do_commit tpcc_loader<true>
#define tpcc_loader_templ_no_commit tpcc_loader<false>

#define tpcc_benchmark_worker_types template <bool do_begin_commit>
#define tpcc_benchmark_worker_templ tpcc_benchmark_worker<do_begin_commit>

#define tpcc_benchmark_worker_templ_do_commit tpcc_benchmark_worker<true>
#define tpcc_benchmark_worker_templ_no_commit tpcc_benchmark_worker<false>

