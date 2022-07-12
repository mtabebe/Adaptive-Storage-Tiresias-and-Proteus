#pragma once

#include <atomic>

#include "../distributions/distributions.h"

class adaptive_reservoir {
   public:
    adaptive_reservoir( size_t reservoir_size, float orig_mult,
                        float weight_incr, float decay_rate );
    bool should_sample();
    void decay();

    float get_current_weight() const;

    distributions rand_dist_;

   private:
    size_t             reservoir_size_;
    std::atomic<float> current_weight_;
    float              weight_incr_;
    float              decay_rate_;
};

