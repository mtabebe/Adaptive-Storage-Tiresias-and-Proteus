#pragma once

#include "krls_predictor.h"

#include <glog/logging.h>

template <long N, typename kernel_type>
krls_predictor<N, kernel_type>::internal_model::internal_model(
    uint64_t model_version, const dlib::krls<kernel_type>& krls )
    : model_version_( model_version ), krls_( krls ) {}

template <long N, typename kernel_type>
krls_predictor<N, kernel_type>::internal_model::internal_model(
    uint64_t model_version, dlib::krls<kernel_type>&& krls )
    : model_version_( model_version ), krls_( std::move( krls ) ) {}

template <long N, typename kernel_type>
krls_predictor<N, kernel_type>::krls_predictor(
    double learning_rate, dvector<N> max_input, bool is_static,
    const kernel_type& kernel, uint32_t max_internal_model_size,
    double min_prediction_range, double max_prediction_range )
    : predictor<dvector<N>, double>( learning_rate, max_input,
                                     min_prediction_range, max_prediction_range,
                                     is_static ),
      internal_model_( nullptr ) {

    dlib::krls<kernel_type> new_krls( kernel, learning_rate );
    internal_model_.store( std::make_shared<internal_model>( 0, new_krls ) );
}

template <long N, typename kernel_type>
krls_predictor<N, kernel_type>::~krls_predictor() {}

template <long N, typename kernel_type>
uint64_t       krls_predictor<N, kernel_type>::model_version() const {
    return internal_model_.load()->model_version_;
}

template <long N, typename kernel_type>
double         krls_predictor<N, kernel_type>::make_prediction(
    const dvector<N>& input ) const {
    const dlib::krls<kernel_type>& krls = internal_model_.load()->krls_;
    return krls( input );
}

template <long N, typename kernel_type>
void           krls_predictor<N, kernel_type>::add_observations_to_model(
    const std::vector<typename predictor<dvector<N>, double>::observation>&
        obs ) {

    const std::shared_ptr<internal_model>& old_model = internal_model_.load();

    dlib::krls<kernel_type> new_krls = old_model->krls_;

    uint64_t num_observed = 0;
    for( const typename predictor<dvector<N>, double>::observation& o : obs ) {
        new_krls.train( o.input_, o.result_ );
        num_observed += 1;
    }

    if( num_observed == 0 ) {
        DVLOG( 40 ) << "update_model: no observations";
        return;
    }

    uint64_t new_model_version = old_model->model_version_ + 1;

    internal_model_.store( std::make_shared<internal_model>(
        new_model_version, std::move( new_krls ) ) );

    DVLOG( 40 ) << "update_model: " << num_observed << " observations added";

    return;
}

template <long N, typename kernel_type>
void krls_predictor<N, kernel_type>::write( std::ostream& os ) const {
    std::shared_ptr<internal_model> model = internal_model_.load();
    os << "[ model_version_:" << model->model_version_ << ", parent:";
    predictor<dvector<N>, double>::write( os );
    os << " ]";
}
