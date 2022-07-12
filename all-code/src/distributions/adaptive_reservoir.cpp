#include "adaptive_reservoir.h"

#include <glog/logging.h>

adaptive_reservoir::adaptive_reservoir( size_t reservoir_size, float orig_mult,
                                        float weight_incr, float decay_rate )
    : rand_dist_( nullptr /* do not use zipf */ ),
      reservoir_size_( reservoir_size ),
      current_weight_( orig_mult * reservoir_size ),
      weight_incr_( weight_incr ),
      decay_rate_( decay_rate ) {}
bool adaptive_reservoir::should_sample() {
    double draw = rand_dist_.get_uniform_double_in_range( 0, 1.0 );
    double target_prob = reservoir_size_ / current_weight_;
    float val = current_weight_.load( std::memory_order_acquire );
    while( !current_weight_.compare_exchange_weak( val, val + weight_incr_ ) );

    return draw <= target_prob;
}
void adaptive_reservoir::decay() {
    float read_val = current_weight_.load( std::memory_order_acquire );
    while( !current_weight_.compare_exchange_weak( read_val, read_val * decay_rate_ ) );
    LOG( INFO ) << "Decaying reservoir: new weight: "
                << current_weight_.load( std::memory_order_acquire );
}
float adaptive_reservoir::get_current_weight() const { return current_weight_; }
