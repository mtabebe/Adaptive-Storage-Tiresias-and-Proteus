#pragma once

#define tpch_loader_types template <bool do_begin_commit>
#define tpch_loader_templ tpch_loader<do_begin_commit>
#define tpch_loader_templ_do_commit tpch_loader<true>
#define tpch_loader_templ_no_commit tpch_loader<false>

#define tpch_benchmark_worker_types template <bool do_begin_commit>
#define tpch_benchmark_worker_templ tpch_benchmark_worker<do_begin_commit>

#define tpch_benchmark_worker_templ_do_commit tpch_benchmark_worker<true>
#define tpch_benchmark_worker_templ_no_commit tpch_benchmark_worker<false>

