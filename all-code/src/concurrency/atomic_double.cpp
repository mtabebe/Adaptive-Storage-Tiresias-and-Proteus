#include "atomic_double.h"

#include <glog/logging.h>

void atomic_fetch_add( std::vector<std::atomic<double>>* vec, uint32_t pos,
                       double arg ) {
    DCHECK_LT( pos, vec->size() );
    std::atomic<double>* a = vec->data() + pos;
    atomic_fetch_add( a, arg );
}
void atomic_fetch_add( std::atomic<double>* a, double arg ) {
    double expected = a->load();
    while( !a->compare_exchange_weak( expected, expected + arg ) ) {
    }
}
