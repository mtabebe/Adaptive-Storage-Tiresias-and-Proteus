#pragma once

#include <atomic>
#include <iostream>
#include <memory>
#include <vector>
#include <valarray>

#include <folly/concurrency/AtomicSharedPtr.h>

#include "partition_funcs.h"
#include "predictor/krls_predictor2.h"
#include "predictor/mlp_predictor2.h"
#include "predictor/multi_linear_predictor.h"
#include "predictor/predictor_util.h"
#include "predictor/single_linear_predictor.h"

// based on
// https://ml-cheatsheet.readthedocs.io/en/latest/linear_regression.html
// normalization based on
// http://web.mit.edu/zoya/www/linearRegression.pdf
// http://edithlaw.ca/teaching/cs480/w19/lectures/6-linear-models.pdf

class predictor2_result {
   public:
    predictor2_result( double input, double weight, double bias );
    predictor2_result();

    void add_actual_output( double actual );

    void make_prediction();

    void rescale_prediction_to_range( double min_bound, double max_bound );

    double get_prediction() const;
    double get_actual_output() const;

    double get_input() const;
    double get_weight() const;
    double get_bias() const;
    double get_error() const;

    friend std::ostream& operator<<( std::ostream&            os,
                                     const predictor2_result& res );

   private:
    double rescale_to_bound( double pred, double bound );

    double input_;

    double weight_;
    double bias_;

    double predicted_output_;
    double actual_output_;
    double error_;
};

enum predictor_type {
    SINGLE_LINEAR_PREDICTOR,           // single_linear_predictor
    MULTI_LINEAR_PREDICTOR,            // multi_linear_predictor
    KRLS_RADIAL_PREDICTOR,             // krls_radial_basis_predictor
    MULTI_LAYER_PERCEPTRON_PREDICTOR,  // mlp_predictor2
};

std::string predictor_type_to_string(const predictor_type& p);
predictor_type string_to_predictor_type( const std::string& s );

std::valarray<double> valarray_from_vector( const std::vector<double>& vec );

class predictor3_result {
  public:
   predictor3_result( const std::vector<double>& input, double prediction,
                      const partition_type::type& part_type );
   predictor3_result();

   void add_actual_output( double actual );

   void rescale_prediction_to_range( double min_bound, double max_bound );

   double get_prediction() const;
   double get_actual_output() const;
   double get_error() const;

   partition_type::type get_partition_type() const;

   std::vector<double> get_input() const;

   friend std::ostream& operator<<( std::ostream&            os,
                                    const predictor3_result& res );

  private:
    double rescale_to_bound( double pred, double bound );

    std::vector<double> input_;

    double predicted_output_;
    double actual_output_;
    double error_;

    partition_type::type partition_type_;
};

class predictor3 {
   public:
    predictor3( const predictor_type&       type,
                const partition_type::type& part_type,
                const std::vector<double>& init_weights, double init_bias,
                double learning_rate, double regularization,
                double bias_regularization, double momentum,
                uint32_t max_internal_model_size, double kernel_gamma,
                long layer_1_nodes, long layer_2_nodes,
                const std::vector<double>& max_inputs, double min_pred_range,
                double max_pred_range, bool is_static );
    ~predictor3();

    predictor3_result make_prediction_result(
        const std::vector<double>& input ) const;
    double make_prediction( const std::vector<double>& input ) const;

    void update_weights();

    void add_observation( const predictor3_result& pred_res );

    std::vector<double> get_max_inputs() const;

    predictor_type      get_predictor_type() const;
    void*               get_predictor_pointer() const;

    friend std::ostream& operator<<( std::ostream& os, const predictor3& p );

   private:
    predictor_type       predictor_type_;
    partition_type::type part_type_;
    std::vector<double>  max_inputs_;

    void* internal_predictor_;
};

struct deriv_holder {
    float weight_deriv_;
    float bias_deriv_;
};

class internal_predictor2 {
   public:
    internal_predictor2( double weight, double bias );

    std::tuple<uint64_t, float, float>
        get_num_observations_weight_deriv_and_bias_deriv();

    void add_error( const predictor2_result& res, double max_input );

    double weight_;
    double bias_;

   private:
    // this should be incremented and loaded before changing the weights.
    // That
    // way the
    std::atomic<uint64_t> num_observed_;
    // weight, bias
    std::atomic<deriv_holder> derivs_;
};

class predictor2 {
   public:
    predictor2( double init_weight, double init_bias, double learning_rate,
                double regularization, double bias_regularization,
                double max_input, bool is_static );
    ~predictor2();

    predictor2_result make_prediction( double input ) const;
    void add_result( const predictor2_result& res );

    double get_max_input() const;

    void update_weights();

    friend std::ostream& operator<<( std::ostream& os, const predictor2& p );

   private:
    bool   is_static_;
    double learning_rate_;
    double regularization_;
    double bias_regularization_;
    double max_input_;

    folly::atomic_shared_ptr<internal_predictor2> internal_predictor2_;
};

class internal_multi_predictor2 {
   public:
    internal_multi_predictor2( const std::vector<double>& weights,
                               double                     bias );

    double make_prediction( const std::vector<double>& input ) const;

    void update_local_weight_derivs_and_bias_derivs(
        const std::vector<double>& input, double prediction, double actual,
        std::vector<double>& weight_derivs, double& bias_deriv,
        const std::vector<double>& max_inputs ) const;

    std::vector<double> weights_;
    double              bias_;
};

class multi_predictor2 {
   public:
    multi_predictor2( const std::vector<double>& init_weights, double init_bias,
                      double learning_rate, double regularization,
                      double                     bias_regularization,
                      const std::vector<double>& max_inputs, bool is_static );
    ~multi_predictor2();

    double make_prediction( const std::vector<double>& input ) const;

    std::vector<double> get_max_inputs() const;

    std::shared_ptr<internal_multi_predictor2> get_internal_model() const;
    void update_model( std::shared_ptr<internal_multi_predictor2> old_model,
                       uint64_t                                   num_observed,
                       const std::vector<double>&                 weight_derivs,
                       double                                     bias_deriv );

    friend std::ostream& operator<<( std::ostream&           os,
                                     const multi_predictor2& p );

   private:
    bool                is_static_;
    double              learning_rate_;
    double              regularization_;
    double              bias_regularization_;
    std::vector<double> max_inputs_;

    folly::atomic_shared_ptr<internal_multi_predictor2> internal_predictor2_;
};

