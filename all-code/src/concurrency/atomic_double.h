
#pragma once

#include <atomic>
#include <vector>

void atomic_fetch_add( std::vector<std::atomic<double>>* vec, uint32_t pos,
                       double arg ) ;
void atomic_fetch_add( std::atomic<double>* a, double arg );
