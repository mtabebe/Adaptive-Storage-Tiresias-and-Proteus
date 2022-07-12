#pragma once

#include "scheduled_distribution.h"
#include <iostream>

class gaussian_scheduled_distribution : public scheduled_distribution {
    double y_max_, y_min_, mean_, std_deviation_;

    double distribution_function( double x );

   public:
    gaussian_scheduled_distribution( epoch_time base_time,
                                     epoch_time period_time,
                                     epoch_time time_window, double y_max,
                                     double y_min, double mean,
                                     double std_deviation );
    /**
     * Expected format:
     *
     * ```
     * gaussian
     * $base_time $period_time $time_window
     * $y_max $y_min $mean $std_deviation
     * ```
     * NOTE: If $period_time is 0, then we consider it a one-time event
     */
    gaussian_scheduled_distribution( std::istream &in );

    query_count get_estimated_query_count( epoch_time time );
};
