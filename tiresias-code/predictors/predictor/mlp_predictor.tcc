#pragma once

#include <glog/logging.h>

template <long N>
mlp_predictor<N>::mlp_predictor( bool is_static, dvector<N> max_input,
                                 long layer_1_nodes, long layer_2_nodes,
                                 double learning_rate, double momentum,
                                 double min_prediction_range,
                                 double max_prediction_range )
    : predictor<dvector<N>, double>( learning_rate, max_input,
                                     min_prediction_range, max_prediction_range,
                                     is_static ),
      model_version_( 0 ),
      net_( nullptr ),
      net_bk_( nullptr ) {

    net_.store( std::make_shared<dlib::mlp::kernel_1a_c>(
        N, layer_1_nodes, layer_2_nodes, 1, learning_rate, momentum ) );

    net_bk_.store( std::make_shared<dlib::mlp::kernel_1a_c>(
        N, layer_1_nodes, layer_2_nodes, 1, learning_rate, momentum ) );
}

template <long N>
mlp_predictor<N>::~mlp_predictor() {}

template <long N>
uint64_t       mlp_predictor<N>::model_version() const {
    return model_version_;
}

template <long N>
double mlp_predictor<N>::make_prediction( const dvector<N>& input ) const {
    const dlib::mlp::kernel_1a_c& net = *( net_.load() );
    return net( input )( 0 );
}

template <long N>
void           mlp_predictor<N>::add_observations_to_model(
    const std::vector<typename predictor<dvector<N>, double>::observation>&
        obs ) {

    std::shared_ptr<dlib::mlp::kernel_1a_c> net_new = net_bk_.load();

    uint64_t num_observed = 0;
    for( const typename predictor<dvector<N>, double>::observation& o : obs ) {
        net_new->train( o.input_, o.result_ );
        num_observed += 1;
    }

    if( num_observed == 0 ) {
        DVLOG( 40 ) << "update_model: no observations";
        return;
    }

    model_version_ += 1;
    DVLOG( 40 ) << "update_model: new model_version: " << model_version_;

    std::shared_ptr<dlib::mlp::kernel_1a_c> net_old = net_.exchange( net_new );

    for( const typename predictor<dvector<N>, double>::observation& o : obs ) {
        net_old->train( o.input_, o.result_ );
    }
    net_bk_.store( net_old );

    DVLOG( 40 ) << "update_model: " << num_observed << " observations added";

    return;
}

template <long N>
void mlp_predictor<N>::write( std::ostream& os ) const {
    os << "[ model_version_:" << model_version_ << ", parent:";
    predictor<dvector<N>, double>::write( os );
    os << " ]";
}
