#pragma once

#include "vector_util.h"
#include <iostream>
#include <memory>
#include <mutex>
#include <vector>

template <typename input_t, typename result_t>
class predictor {
   public:
    bool    is_static() const;
    double  learning_rate() const;
    input_t max_input() const;

    virtual void add_observation( const input_t& input, const result_t& res );
    void set_static( bool is_static );

    virtual uint64_t model_version() const = 0;
    virtual result_t make_prediction( const input_t& input ) const = 0;
    void update_model();

    template <typename I, typename R>
    friend std::ostream& operator<<( std::ostream& os,
                                     const predictor<I, R>& p );

   protected:
    predictor( double learning_rate, const input_t& max_input,
               result_t min_prediction_range, result_t max_prediction_range,
               bool is_static );
    virtual ~predictor();

    virtual void write( std::ostream& os ) const;

    struct observation {
        input_t  input_;
        result_t result_;
        observation( const input_t& input, const result_t& result );
    };

    virtual void add_observations_to_model(
        const std::vector<observation>& obs ) = 0;

    bool    is_static_;
    double  learning_rate_;
    input_t max_input_;

    result_t min_prediction_range_;
    result_t max_prediction_range_;

    std::vector<observation> observations_;
    std::mutex               observations_mutex_;

   private:
};

#include "predictor.tcc"
