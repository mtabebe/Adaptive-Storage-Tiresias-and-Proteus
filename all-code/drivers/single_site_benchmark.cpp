#include <gflags/gflags.h>
#include <glog/logging.h>

#include "../src/common/constants.h"
#include "../src/common/perf_tracking.h"

#include "../src/benchmark/benchmark_executor.h"

void run_single_site_benchmark() {
    VLOG( 0 ) << "Running single site benchmark";

    run_benchmark();

    VLOG( 0 ) << "Done running single site benchmark";
}

int main( int argc, char **argv ) {
    google::InitGoogleLogging( argv[0] );
    google::ParseCommandLineFlags( &argc, &argv, true );
    google::InstallFailureSignalHandler();

    init_global_state();

    run_single_site_benchmark();

    dump_counters( 0 );

    return 0;
}
