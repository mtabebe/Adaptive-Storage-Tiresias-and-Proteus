#include "mlp_predictor2.h"

#include <glog/logging.h>

#include "vector_util.h"

mlp_predictor2::mlp_predictor2( bool                       is_static,
                                const std::vector<double>& max_input,
                                long layer_1_nodes, long layer_2_nodes,
                                double learning_rate, double momentum,
                                double min_prediction_range,
                                double max_prediction_range )
    : predictor<std::vector<double>, double>( learning_rate, max_input,
                                              min_prediction_range,
                                              max_prediction_range, is_static ),
      model_version_( 0 ),
      net_( nullptr ),
      net_bk_( nullptr ) {

    net_.store( std::make_shared<dlib::mlp::kernel_1a_c>(
        max_input.size(), layer_1_nodes, layer_2_nodes, 1, learning_rate,
        momentum ) );

    net_bk_.store( std::make_shared<dlib::mlp::kernel_1a_c>(
        max_input.size(), layer_1_nodes, layer_2_nodes, 1, learning_rate,
        momentum ) );
}

mlp_predictor2::~mlp_predictor2() {}

uint64_t       mlp_predictor2::model_version() const {
    return model_version_;
}

double mlp_predictor2::make_prediction(
    const std::vector<double>& input ) const {
    const dlib::mlp::kernel_1a_c& net = *( net_.load() );
    return net( dvector_from_vector( input ) )( 0 );
}

void mlp_predictor2::add_observations_to_model(
    const std::vector<
        typename predictor<std::vector<double>, double>::observation>& obs ) {

    std::shared_ptr<dlib::mlp::kernel_1a_c> net_new = net_bk_.load();

    uint64_t num_observed = 0;
    for( const typename predictor<std::vector<double>, double>::observation& o :
         obs ) {
        net_new->train( dvector_from_vector( o.input_ ), o.result_ );
        num_observed += 1;
    }

    if( num_observed == 0 ) {
        DVLOG( 40 ) << "update_model: no observations";
        return;
    }

    model_version_ += 1;
    DVLOG( 40 ) << "update_model: new model_version: " << model_version_;

    std::shared_ptr<dlib::mlp::kernel_1a_c> net_old = net_.exchange( net_new );

    for( const typename predictor<std::vector<double>, double>::observation& o : obs ) {
        net_old->train( dvector_from_vector( o.input_ ), o.result_ );
    }
    net_bk_.store( net_old );

    DVLOG( 40 ) << "update_model: " << num_observed << " observations added";

    return;
}

void mlp_predictor2::write( std::ostream& os ) const {
    os << "[ model_version_:" << model_version_ << ", parent:";
    predictor<std::vector<double>, double>::write( os );
    os << " ]";
}
