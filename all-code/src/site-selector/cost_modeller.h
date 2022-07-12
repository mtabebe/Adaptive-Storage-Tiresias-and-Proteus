#pragma once

#include <chrono>

#include "../common/perf_tracking.h"
#include "../common/predictor/predictor.h"
#include "../common/predictor2.h"
#include "../distributions/distributions.h"

#include "cost_modeller_types.h"

class cost_model_prediction_holder {
   public:
    cost_model_prediction_holder();
    cost_model_prediction_holder(
        double                                prediction,
        const std::vector<predictor3_result>& component_predictions,
        const std::unordered_map<int, std::unordered_map<int, int32_t>>&
            component_types_to_prediction_positions );

    double get_prediction() const;
    void add_real_timers( const std::vector<context_timer>& timers );

    void add_prediction_linearly(
        const cost_model_component_type& model_type,
        const std::tuple<bool, predictor3_result>& pred_res,
        double prediction_multiplier = 1 );
    void add_prediction_max(
        const cost_model_component_type& model_type,
        const std::tuple<bool, predictor3_result>& pred_res );

    void add_prediction_linearly(
        const cost_model_component_type& model_type,
        const std::tuple<bool, predictor2_result>& pred_res,
        double prediction_multiplier = 1 );
    void add_prediction_max(
        const cost_model_component_type& model_type,
        const std::tuple<bool, predictor2_result>& pred_res );

    double                        prediction_;
    std::vector<predictor3_result> component_predictions_;
    std::unordered_map<int, std::unordered_map<int, int32_t>>
                               component_types_to_prediction_positions_;
    std::vector<context_timer> timers_;
};

#include "cost_modeller2.h"

std::vector<context_timer> merge_context_timers(
    const std::vector<context_timer>& left,
    const std::vector<context_timer>& right );
