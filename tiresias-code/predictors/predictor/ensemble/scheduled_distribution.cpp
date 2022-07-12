#include "scheduled_distribution.h"
#include "gaussian_scheduled_distribution.h"
#include <sstream>

using namespace std;

scheduled_distribution::scheduled_distribution( epoch_time base_time,
                                                epoch_time period_time,
                                                epoch_time time_window )
    : base_time_( base_time ),
      period_time_( period_time ),
      time_window_( time_window ) {}

scheduled_distribution::scheduled_distribution( istream &in ) {
    string input;
    // First line should be the type of distribution
    getline( in, input );

    // Second line contains the base_time, period_time, time_window
    getline( in, input );

    stringstream ss( input );
    ss >> base_time_;
    ss >> period_time_;
    ss >> time_window_;

    if( in.fail() ) {
        throw std::runtime_error(
            "Error: Reached EOF while reading distribution info!" );
    }
}

scheduled_distribution::~scheduled_distribution() {}

std::shared_ptr<scheduled_distribution>
    scheduled_distribution::get_distribution( istream &in ) {
    string distribution_name;
    getline( in, distribution_name );

    in.seekg( 0, ios::beg );
    if( distribution_name == "gaussian" ) {
        return std::make_shared<gaussian_scheduled_distribution>( in );
    }
    throw std::runtime_error( "Error: Unknown distribution!" );
}
