#include "benchmark_interface.h"

#include <thread>

void sleep_until_end_of_benchmark(
    std::chrono::high_resolution_clock::time_point end_time ) {
    DVLOG( 5 ) << "Sleeping until end of benchmark";
    std::chrono::high_resolution_clock::time_point cur =
        std::chrono::high_resolution_clock::now();

    while( cur < end_time ) {
        std::this_thread::sleep_until( end_time );
        cur = std::chrono::high_resolution_clock::now();
    }
}
