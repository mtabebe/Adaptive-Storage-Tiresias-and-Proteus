#include "gaussian_scheduled_distribution.h"
#include <glog/logging.h>
#include <math.h>
#include <sstream>

using namespace std;

double gaussian_scheduled_distribution::distribution_function( double x ) {
    return (
        exp( -pow( ( x - mean_ ), 2 ) / ( 2 * pow( std_deviation_, 2 ) ) ) /
        ( std_deviation_ * ( pow( 2 * 3.1415, 0.5 ) ) ) );
}

gaussian_scheduled_distribution::gaussian_scheduled_distribution(
    epoch_time base_time, epoch_time period_time, epoch_time time_window,
    double y_max, double y_min, double mean, double std_deviation )
    : scheduled_distribution( base_time, period_time, time_window ),
      y_max_( y_max ),
      y_min_( y_min ),
      mean_( mean ),
      std_deviation_( std_deviation ) {
    DCHECK_GT( std_deviation_, 0 );
}

gaussian_scheduled_distribution::gaussian_scheduled_distribution( istream &in )
    : scheduled_distribution( in ) {
    string line;

    getline( in, line );
    stringstream ss( line );
    ss >> y_max_;
    ss >> y_min_;
    ss >> mean_;
    ss >> std_deviation_;

    DCHECK_GT( std_deviation_, 0 );

    if( in.fail() ) {
        throw std::runtime_error(
            "Error: Unable to reached EOF while reading distribution info for "
            "gaussian distribution!" );
    }
}

query_count gaussian_scheduled_distribution::get_estimated_query_count(
    epoch_time time ) {

    if( time < base_time_ ) {
        return 0;
    }

    time -= base_time_;

    if( period_time_ != 0 ) {
        time %= period_time_;
    }

    /**
     * Determine if the query is inside the desired search time window.
     * For normal distributions specifically, we split it in half broken
     * around one period_time.
     *
     * [123456789|BASE|12345|12345|123*5|12345....]
     *           |    |-----|         ^
     *           ^base ^period        x
     *
     * Ex: consider a time_window_ of 2. We'd first normalize the result so that
     * it is a number shifted to the left by base_time_, and then we remove all
     * periods by running modulos on it.
     *
     * Therefore, x will move far left by 9 first. Then, we modulo the position
     * by 5.
     * Lastly, we observe where it falls inside that cycle, to determine if we
     * should
     * consider the query at all.
     *
     * [12345|12345|12345...]
     *  ^   ^ ^   ^ ^   ^
     *
     * We only want the positions with ^ to be captured. Hence, we would check
     * if it is closer to the beginning of a time_period_ or left, and then
     * check if it is close enough to the start of a period.
     */

    if( period_time_ != 0 ) {

        if( time <= ( period_time_ / 2 ) && time > ( time_window_ / 2 ) ) {
            return 0;
        }

        if( time > ( period_time_ / 2 ) &&
            time < ( period_time_ - ( time_window_ / 2 ) ) ) {
            return 0;
        }
    } else {
        if( time + base_time_ < base_time_ - ( time_window_ / 2 ) ||
            time + base_time_ > ( base_time_ + ( time_window_ / 2 ) ) ) {
            return 0;
        }
    }

    /**
     * We want to reshape the graph to fit our y_max and y_min
     * characteristics. First, identify the gap that we want to
     * have between y_max and y_min, that will be the desired peak
     * of the graph; after we simply add y_min to the result.
     *
     * Moreover, we want to wrap around to the nearest period_time_
     * for periodic queries.
     */

    if( period_time_ != 0 && time > period_time_ / 2 ) {
        time = period_time_ - time;
    }

    double scaling_value = ( y_max_ - y_min_ ) / distribution_function( 0 );
    return scaling_value * distribution_function( time ) + y_min_;
}
