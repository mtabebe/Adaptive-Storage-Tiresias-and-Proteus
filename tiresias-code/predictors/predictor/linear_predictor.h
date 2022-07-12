#pragma once

#include "predictor.h"
#include <atomic>
#include <folly/concurrency/AtomicSharedPtr.h>

// based on
// https://ml-cheatsheet.readthedocs.io/en/latest/linear_regression.html
// normalization based on
// http://web.mit.edu/zoya/www/linearRegression.pdf
// http://edithlaw.ca/teaching/cs480/w19/lectures/6-linear-models.pdf

template <typename input_t>
class linear_predictor : public predictor<input_t, double> {
   public:
    linear_predictor( const input_t& init_weight, double init_bias,
                      double regularization, double bias_regularization,
                      double learning_rate, const input_t& max_input,
                      double min_prediction_range, double max_prediction_range,
                      bool is_static );
    ~linear_predictor();

    uint64_t model_version() const;
    input_t  weight() const;
    double   bias() const;
    double   regularization() const;
    double   bias_regularization() const;

    double make_prediction( const input_t& input ) const override;

   protected:
    virtual double predict( const input_t& w, double b,
                            const input_t& i ) const = 0;

    virtual void write( std::ostream& os ) const override;

    struct internal_model {
        input_t  weight_;
        double   bias_;
        uint64_t model_version_;

        internal_model( const input_t& weight, double bias,
                        uint64_t model_version );
    };

    folly::atomic_shared_ptr<internal_model> internal_model_;

    const double regularization_;
    const double bias_regularization_;
};

#include "linear_predictor.tcc"
