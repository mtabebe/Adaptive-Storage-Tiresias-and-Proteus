#pragma once

#include <glog/logging.h>

#include "vector_util.h"

template <typename kernel_type>
krls_predictor2<kernel_type>::internal_model::internal_model(
    uint64_t model_version, const dlib::krls<kernel_type>& krls )
    : model_version_( model_version ), krls_( krls ) {}

template <typename kernel_type>
krls_predictor2<kernel_type>::internal_model::internal_model(
    uint64_t model_version, dlib::krls<kernel_type>&& krls )
    : model_version_( model_version ), krls_( std::move( krls ) ) {}

template <typename kernel_type>
krls_predictor2<kernel_type>::krls_predictor2(
    double learning_rate, const std::vector<double>& max_input, bool is_static,
    const kernel_type& kernel, uint32_t max_internal_model_size,
    double min_prediction_range, double max_prediction_range )
    : predictor<std::vector<double>, double>( learning_rate, max_input,
                                              min_prediction_range,
                                              max_prediction_range, is_static ),
      internal_model_( nullptr ) {

    dlib::krls<kernel_type> new_krls( kernel, learning_rate );
    internal_model_.store( std::make_shared<internal_model>( 0, new_krls ) );
}

template <typename kernel_type>
krls_predictor2<kernel_type>::~krls_predictor2() {}

template <typename kernel_type>
uint64_t krls_predictor2<kernel_type>::model_version() const {
    return internal_model_.load()->model_version_;
}

template <typename kernel_type>
double krls_predictor2<kernel_type>::make_prediction(
    const std::vector<double>& input ) const {
    const dlib::krls<kernel_type>& krls = internal_model_.load()->krls_;
    double                         ret = krls( dvector_from_vector( input ) );
    return ret;
}

template <typename kernel_type>
void krls_predictor2<kernel_type>::add_observations_to_model(
    const std::vector<
        typename predictor<std::vector<double>, double>::observation>& obs ) {

    const std::shared_ptr<internal_model>& old_model = internal_model_.load();

    dlib::krls<kernel_type> new_krls = old_model->krls_;

    uint64_t num_observed = 0;
    for( const typename predictor<std::vector<double>, double>::observation& o :
         obs ) {
        new_krls.train( dvector_from_vector( o.input_ ), o.result_ );
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

template <typename kernel_type>
void krls_predictor2<kernel_type>::write( std::ostream& os ) const {
    std::shared_ptr<internal_model> model = internal_model_.load();
    os << "[ model_version_:" << model->model_version_ << ", parent:";
    predictor<std::vector<double>, double>::write( os );
    os << " ]";
}
