#pragma once

#include <chrono>
#include <string>

using Clock = std::chrono::high_resolution_clock;
using TimePoint = std::chrono::time_point<Clock>;

enum query_arrival_predictor_type { SIMPLE_QUERY_PREDICTOR };

std::string query_arrival_predictor_type_string(
    const query_arrival_predictor_type& qap_type );
query_arrival_predictor_type string_to_query_arrival_predictor_type(
    const std::string& s );


