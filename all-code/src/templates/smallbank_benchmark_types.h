#pragma once

#define smallbank_loader_types template <bool do_begin_commit>
#define smallbank_loader_templ smallbank_loader<do_begin_commit>
#define smallbank_loader_templ_do_commit smallbank_loader<true>
#define smallbank_loader_templ_no_commit smallbank_loader<false>

#define smallbank_benchmark_worker_types template <bool do_begin_commit>
#define smallbank_benchmark_worker_templ \
    smallbank_benchmark_worker<do_begin_commit>

#define smallbank_benchmark_worker_templ_do_commit \
    smallbank_benchmark_worker<true>
#define smallbank_benchmark_worker_templ_no_commit \
    smallbank_benchmark_worker<false>

