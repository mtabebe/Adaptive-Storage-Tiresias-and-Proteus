#include "early_query_predictor.h"
#include <chrono>
#include <sstream>

using namespace std;

early_query_predictor::~early_query_predictor() {}

void early_query_predictor::train() {
    epoch_time current_time =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch() )
            .count();
    train( current_time );
}

query_count early_query_predictor::get_max_query_count( query_id q_id ) {
    if( max_query_count_.find( q_id ) == max_query_count_.end() ) {
        return DEFAULT_MAX_QUERY_COUNT_;
    }
    return max_query_count_[q_id];
}

void early_query_predictor::populate_max_query_count( std::istream &is ) {
    while( true ) {
        string input;
        getline( is, input );
        if( is.fail() ) {
            break;
        }

        stringstream ss{input};
        query_id     q_id;
        query_count  max_size;

        ss >> q_id >> max_size;
        max_query_count_[q_id] = max_size;
    }
}
