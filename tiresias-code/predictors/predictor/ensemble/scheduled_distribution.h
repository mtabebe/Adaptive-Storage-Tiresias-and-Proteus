#pragma once

#include <iostream>
#include <memory>

typedef uint64_t epoch_time;
typedef uint64_t query_count;

class scheduled_distribution {
   protected:
    epoch_time base_time_, period_time_, time_window_;

   public:
    scheduled_distribution( epoch_time base_time, epoch_time period_time,
                            epoch_time time_window );
    scheduled_distribution( std::istream &in );

    virtual ~scheduled_distribution();
    virtual query_count get_estimated_query_count( epoch_time time ) = 0;
    static std::shared_ptr<scheduled_distribution> get_distribution(
        std::istream &in );
};
