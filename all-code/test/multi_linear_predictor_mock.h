#pragma once
#include <vector>
#include <gmock/gmock.h>

#include "../src/common/constants.h"
#include "../src/common/predictor/multi_linear_predictor.h"
#include "../src/common/predictor/predictor.h"

class multi_linear_predictor_mock : public multi_linear_predictor {
   protected:
    MOCK_METHOD1(
        add_observations_to_model,
        void( const std::vector<observation>& obs ));

   public:
    MOCK_METHOD3( predict,
                          double( const std::vector<double>& w,
                             double b, const std::vector<double>& i) );
    MOCK_CONST_METHOD1( make_prediction,
                        double( const std::vector<double>& w ) );

    MOCK_METHOD2( add_observation,
                  void( const std::vector<double>&, const double& ) );

    multi_linear_predictor_mock( const std::vector<double>& init_weight,
                                 double init_bias, double regularization,
                                 double bias_regularization,
                                 double learning_rate,
                                 const std::vector<double>& max_input,
                                 double min_prediction_range,
                                 double max_prediction_range, bool is_static )
        : multi_linear_predictor( init_weight, init_bias, regularization,
                                  bias_regularization, learning_rate, max_input,
                                  min_prediction_range, max_prediction_range,
                                  is_static ) {}
};
