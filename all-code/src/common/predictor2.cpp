#include "predictor2.h"

#include <cmath>

#include <glog/logging.h>

std::string predictor_type_to_string(const predictor_type& p) {
    switch( p ) {
        case predictor_type::SINGLE_LINEAR_PREDICTOR: {
            return "SINGLE_LINEAR_PREDICTOR";
            break;
        }
        case predictor_type::MULTI_LINEAR_PREDICTOR: {
            return "MULTI_LINEAR_PREDICTOR";
        }
        case predictor_type::KRLS_RADIAL_PREDICTOR: {
            return "KRLS_RADIAL_PREDICTOR";
        }
        case predictor_type::MULTI_LAYER_PERCEPTRON_PREDICTOR: {
            return "MULTI_LAYER_PERCEPTRON_PREDICTOR";
        }
    }
    return "NOT_FOUND";
}
predictor_type string_to_predictor_type( const std::string& s ) {
    std::string lower = s;
    std::transform( lower.begin(), lower.end(), lower.begin(), ::tolower );

    if( lower.compare( "single_linear_predictor" ) == 0 ) {
        return predictor_type::SINGLE_LINEAR_PREDICTOR;
    } else if( lower.compare( "multi_linear_predictor" ) == 0 ) {
        return predictor_type::MULTI_LINEAR_PREDICTOR;
    } else if( lower.compare( "krls_radial_predictor" ) == 0 ) {
        return predictor_type::KRLS_RADIAL_PREDICTOR;
    } else if( lower.compare( "multi_layer_perceptron_predictor" ) == 0 ) {
        return predictor_type::MULTI_LAYER_PERCEPTRON_PREDICTOR;
    } else {
        LOG( WARNING ) << "String to predictor type:" << s << ", not found!";
    }

    return predictor_type::SINGLE_LINEAR_PREDICTOR;
}

std::valarray<double> valarray_from_vector( const std::vector<double>& vec ) {
    std::valarray<double> vals( vec.data(), vec.size() );
    return vals;
}

predictor3_result::predictor3_result( const std::vector<double>&  input,
                                      double                      prediction,
                                      const partition_type::type& part_type )
    : input_( input ),
      predicted_output_( prediction ),
      actual_output_( 0 ),
      error_( 0 ),
      partition_type_( part_type ) {}
predictor3_result::predictor3_result()
    : input_(),
      predicted_output_( 0 ),
      actual_output_( 0 ),
      error_( 0 ),
      partition_type_( partition_type::type::ROW ) {}

void predictor3_result::add_actual_output( double actual ) {
  actual_output_ = actual;
  error_ = actual_output_ - predicted_output_;
}

void predictor3_result::rescale_prediction_to_range( double min_bound,
                                                     double max_bound ) {
    predicted_output_ =
        scale_prediction_to_range( predicted_output_, min_bound, max_bound );
}

double predictor3_result::get_prediction() const { return predicted_output_; }
double predictor3_result::get_actual_output() const { return actual_output_; }
double predictor3_result::get_error() const { return error_; }

partition_type::type predictor3_result::get_partition_type() const {
    return partition_type_;
}
std::vector<double> predictor3_result::get_input() const { return input_; }

std::ostream& operator<<( std::ostream& os, const predictor3_result& res ) {
    os << "[ input_: (";
    for( double i : res.get_input() ) {
        os << i << ", ";
    }
    os << ")"
       << ", predicted_output_:" << res.get_prediction()
       << ", actual_output_:" << res.get_actual_output()
       << ", partition_type_:" << res.get_partition_type()
       << ", error_:" << res.get_error() << " ]";

    return os;
}

predictor3::predictor3(
    const predictor_type& type, const partition_type::type& part_type,
    const std::vector<double>& init_weights, double init_bias,
    double learning_rate, double regularization, double bias_regularization,
    double momentum, uint32_t max_internal_model_size, double kernel_gamma,
    long layer_1_nodes, long layer_2_nodes,
    const std::vector<double>& max_inputs, double min_pred_range,
    double max_pred_range, bool is_static )
    : predictor_type_( type ),
      part_type_( part_type ),
      max_inputs_( max_inputs ),
      internal_predictor_( nullptr ) {
    DCHECK_EQ( init_weights.size(), max_inputs.size() );

    switch( predictor_type_ ) {
        case predictor_type::SINGLE_LINEAR_PREDICTOR: {

            internal_predictor_ = (void*) new single_linear_predictor(
                init_weights.at( 0 ), init_bias, regularization,
                bias_regularization, learning_rate, max_inputs.at( 0 ),
                min_pred_range, max_pred_range, is_static );
            break;
        }
        case predictor_type::MULTI_LINEAR_PREDICTOR: {
            internal_predictor_ = (void*) new multi_linear_predictor(
                init_weights, init_bias, regularization, bias_regularization,
                learning_rate, max_inputs, min_pred_range, max_pred_range,
                is_static );
            break;
        }
        case predictor_type::KRLS_RADIAL_PREDICTOR: {
            dlib::radial_basis_kernel<d_var_vector> kernel( kernel_gamma );
            internal_predictor_ = (void*) new krls_radial_basis_predictor(
                learning_rate, max_inputs, is_static, kernel,
                max_internal_model_size, min_pred_range, max_pred_range );
            break;
        }
        case predictor_type::MULTI_LAYER_PERCEPTRON_PREDICTOR: {
            internal_predictor_ = (void*) new mlp_predictor2(
                is_static, max_inputs, layer_1_nodes, layer_2_nodes,
                learning_rate, momentum, min_pred_range, max_pred_range );
            break;
        }
    }
    DVLOG( 20 ) << "Initialized predictor:" << *this;
}
predictor3::~predictor3() {
  switch( predictor_type_ ) {
      case predictor_type::SINGLE_LINEAR_PREDICTOR: {
          single_linear_predictor* lpred =
              (single_linear_predictor*) internal_predictor_;
          delete lpred;
          break;
      }
      case predictor_type::MULTI_LINEAR_PREDICTOR: {
          multi_linear_predictor* lpred =
              (multi_linear_predictor*) internal_predictor_;
          delete lpred;
          break;
      }
      case predictor_type::KRLS_RADIAL_PREDICTOR: {
          krls_radial_basis_predictor* lpred =
              (krls_radial_basis_predictor*) internal_predictor_;
          delete lpred;
          break;
      }
      case predictor_type::MULTI_LAYER_PERCEPTRON_PREDICTOR: {
          mlp_predictor2* lpred = (mlp_predictor2*) internal_predictor_;
          delete lpred;
          break;
      }
  }
  internal_predictor_ = nullptr;
}

predictor3_result predictor3::make_prediction_result(
    const std::vector<double>& input ) const {
    predictor3_result res( input, make_prediction( input ), part_type_ );
    return res;
}
double predictor3::make_prediction( const std::vector<double>& input ) const {
    double pred = 0;
    DCHECK_EQ( input.size(), max_inputs_.size() );

    DCHECK( internal_predictor_ );
    switch( predictor_type_ ) {
        case predictor_type::SINGLE_LINEAR_PREDICTOR: {
            single_linear_predictor* lpred =
                (single_linear_predictor*) internal_predictor_;
            DVLOG(20) << "SLP make prediction";
            pred = lpred->make_prediction( input.at( 0 ) );
            break;
        }
        case predictor_type::MULTI_LINEAR_PREDICTOR: {
            multi_linear_predictor* lpred =
                (multi_linear_predictor*) internal_predictor_;
            DVLOG(20) << "MLP make prediction";
            pred = lpred->make_prediction( input );
            break;
        }
        case predictor_type::KRLS_RADIAL_PREDICTOR: {
            krls_radial_basis_predictor* lpred =
                (krls_radial_basis_predictor*) internal_predictor_;
            DVLOG(20) << "KRLS make prediction";
            pred = lpred->make_prediction( input );
            break;
        }
        case predictor_type::MULTI_LAYER_PERCEPTRON_PREDICTOR: {
            mlp_predictor2* lpred = (mlp_predictor2*) internal_predictor_;
            DVLOG(20) << "KRLS make prediction";
            pred = lpred->make_prediction( input );
            break;
        }
    }

    return pred;
}

void predictor3::add_observation( const predictor3_result& pred_res ) {
    DCHECK( internal_predictor_ );
    switch( predictor_type_ ) {
        case predictor_type::SINGLE_LINEAR_PREDICTOR: {
            single_linear_predictor* lpred =
                (single_linear_predictor*) internal_predictor_;
            DCHECK_EQ( 1, pred_res.get_input().size());
            lpred->add_observation( pred_res.get_input().at( 0 ),
                                    pred_res.get_actual_output() );
            break;
        }
        case predictor_type::MULTI_LINEAR_PREDICTOR: {
            multi_linear_predictor* lpred =
                (multi_linear_predictor*) internal_predictor_;
            lpred->add_observation( pred_res.get_input(),
                                           pred_res.get_actual_output() );
            break;
        }
        case predictor_type::KRLS_RADIAL_PREDICTOR: {
            krls_radial_basis_predictor* lpred =
                (krls_radial_basis_predictor*) internal_predictor_;
            lpred->add_observation( pred_res.get_input(),
                                           pred_res.get_actual_output() );
            break;
        }
        case predictor_type::MULTI_LAYER_PERCEPTRON_PREDICTOR: {
            mlp_predictor2* lpred = (mlp_predictor2*) internal_predictor_;
            lpred->add_observation( pred_res.get_input(),
                                           pred_res.get_actual_output() );
            break;
        }
    }
}

predictor_type predictor3::get_predictor_type() const {
    return predictor_type_;
}
void* predictor3::get_predictor_pointer() const { return internal_predictor_; }

void predictor3::update_weights() {
    DCHECK( internal_predictor_ );
    switch( predictor_type_ ) {
        case predictor_type::SINGLE_LINEAR_PREDICTOR: {
            single_linear_predictor* lpred =
                (single_linear_predictor*) internal_predictor_;
            lpred->update_model();
            break;
        }
        case predictor_type::MULTI_LINEAR_PREDICTOR: {
            multi_linear_predictor* lpred =
                (multi_linear_predictor*) internal_predictor_;
            lpred->update_model();

            break;
        }
        case predictor_type::KRLS_RADIAL_PREDICTOR: {
            krls_radial_basis_predictor* lpred =
                (krls_radial_basis_predictor*) internal_predictor_;
            lpred->update_model();

            break;
        }
        case predictor_type::MULTI_LAYER_PERCEPTRON_PREDICTOR: {
            mlp_predictor2* lpred = (mlp_predictor2*) internal_predictor_;
            lpred->update_model();

            break;
        }
    }
}

std::vector<double> predictor3::get_max_inputs() const {
  return max_inputs_;
}

std::ostream& operator<<( std::ostream& os, const predictor3& p ) {
    os << "[ type:" << predictor_type_to_string( p.get_predictor_type() )
       << ", predictor:";
    switch( p.get_predictor_type() ) {
        case predictor_type::SINGLE_LINEAR_PREDICTOR: {
            single_linear_predictor* lpred =
                (single_linear_predictor*) p.get_predictor_pointer();
            os << *lpred;
            break;
        }
        case predictor_type::MULTI_LINEAR_PREDICTOR: {
            multi_linear_predictor* lpred =
                (multi_linear_predictor*) p.get_predictor_pointer();
            os << *lpred;
            break;
        }
        case predictor_type::KRLS_RADIAL_PREDICTOR: {
            krls_radial_basis_predictor* lpred =
                (krls_radial_basis_predictor*) p.get_predictor_pointer();
            os << *lpred;
            break;
        }
        case predictor_type::MULTI_LAYER_PERCEPTRON_PREDICTOR: {
            mlp_predictor2* lpred = (mlp_predictor2*) p.get_predictor_pointer();
            os << *lpred;
            break;
        }
    }
    os << " ]";
    return os;
}

predictor2::predictor2( double init_weight, double init_bias,
                        double learning_rate, double regularization,
                        double bias_regularization, double max_input,
                        bool is_static )
    : is_static_( is_static ),
      learning_rate_( learning_rate ),
      regularization_( regularization ),
      bias_regularization_( bias_regularization ),
      max_input_( max_input ),
      internal_predictor2_( nullptr ) {
    if( !std::atomic<deriv_holder>{}.is_lock_free() ) {
        LOG( FATAL ) << "deriv_holder should be lock free";
    }
    internal_predictor2_.store(
        std::make_shared<internal_predictor2>( init_weight, init_bias ) );
}
predictor2::~predictor2() {}

predictor2_result predictor2::make_prediction( double input ) const {
    auto              internal = internal_predictor2_.load();
    predictor2_result res( input, internal->weight_, internal->bias_ );

    return res;
}

void predictor2::add_result( const predictor2_result& res ) {
    double err = res.get_error();

    DVLOG( 40 ) << "Prediction:" << res << ", Err:" << err;

    if( is_static_ ) {
        return;
    }
    auto internal = internal_predictor2_.load();
    internal->add_error( res, max_input_ );
}

double predictor2::get_max_input() const { return max_input_; }

void predictor2::update_weights() {
    if( is_static_ ) {
        return;
    }
    auto old_internal = internal_predictor2_.load();

    double old_weight = old_internal->weight_;
    double old_bias = old_internal->bias_;

    auto observations_and_derivs =
        old_internal->get_num_observations_weight_deriv_and_bias_deriv();

    uint64_t num_observed = std::get<0>( observations_and_derivs );
    float    weight_deriv = std::get<1>( observations_and_derivs );
    float    bias_deriv = std::get<2>( observations_and_derivs );

    DVLOG( 40 ) << "Num observed:" << num_observed
                << ", weight_deriv:" << weight_deriv
                << ", bias_deriv:" << bias_deriv
                << ", old_weight:" << old_weight << ", old_bias:" << old_bias;

    weight_deriv = weight_deriv + ( regularization_ * old_weight );
    bias_deriv = bias_deriv + ( bias_regularization_ * old_bias );

    DVLOG( 40 ) << "After regularization, weight_deriv:" << weight_deriv
                << ", bias_deriv:" << bias_deriv;

    if( num_observed == 0 ) {
        return;
    }

    double new_weight =
        old_weight - ( ( weight_deriv / num_observed ) * learning_rate_ );
    double new_bias =
        old_bias - ( ( bias_deriv / num_observed ) * learning_rate_ );

    auto new_internal =
        std::make_shared<internal_predictor2>( new_weight, new_bias );

    DVLOG( 40 ) << "New internal weights:" << new_weight
                << ", bias:" << new_bias;

    internal_predictor2_.store( new_internal );
}

internal_predictor2::internal_predictor2( double weight, double bias )
    : weight_( weight ), bias_( bias ), num_observed_( 0 ), derivs_( {0, 0} ) {}

std::tuple<uint64_t, float, float>
    internal_predictor2::get_num_observations_weight_deriv_and_bias_deriv() {

    deriv_holder ori_deriv = derivs_.load();
    uint64_t     observation_count = num_observed_.load();

    return std::make_tuple<>( observation_count, ori_deriv.weight_deriv_,
                              ori_deriv.bias_deriv_ );
}

void internal_predictor2::add_error( const predictor2_result& res,
                                     double                   max_input ) {
    if( not( ( weight_ == res.get_weight() ) and
             ( bias_ == res.get_bias() ) ) ) {
        // doesn't match
        return;
    }
    double err = res.get_error();

    float delta_w = ( -2 * std::min( res.get_input(), max_input ) ) * err;
    // double delta_w = -2 * err;
    float delta_b = -2 * err;

    // increment num observations first, so it will overcount
    num_observed_.fetch_add( 1 );

    deriv_holder ori_deriv = derivs_.load();
    deriv_holder new_deriv = ori_deriv;

    new_deriv.weight_deriv_ += delta_w;
    new_deriv.bias_deriv_ += delta_b;

    bool exchanged = derivs_.compare_exchange_strong( ori_deriv, new_deriv );

    if( !exchanged ) {
        num_observed_.fetch_sub( 1 );
        DVLOG( 40 ) << "Unable to install new internal deriv CAS failed";
    } else {
        DVLOG( 50 ) << "Installed new internal deriv: weight_deriv: "
                    << new_deriv.weight_deriv_
                    << ", bias_deriv:" << new_deriv.bias_deriv_;
    }
}

multi_predictor2::multi_predictor2( const std::vector<double>& init_weights,
                                    double init_bias, double learning_rate,
                                    double regularization,
                                    double bias_regularization,
                                    const std::vector<double>& max_inputs,
                                    bool                       is_static )
    : is_static_( is_static ),
      learning_rate_( learning_rate ),
      regularization_( regularization ),
      bias_regularization_( bias_regularization ),
      max_inputs_( max_inputs ),
      internal_predictor2_( std::make_shared<internal_multi_predictor2>(
          init_weights, init_bias ) ) {}

multi_predictor2::~multi_predictor2() {}

double multi_predictor2::make_prediction(
    const std::vector<double>& input ) const {
    return internal_predictor2_.load()->make_prediction( input );
}

std::vector<double> multi_predictor2::get_max_inputs() const {
    return max_inputs_;
}

std::shared_ptr<internal_multi_predictor2>
    multi_predictor2::get_internal_model() const {
    return internal_predictor2_.load();
}

void multi_predictor2::update_model(
    std::shared_ptr<internal_multi_predictor2> old_model, uint64_t num_observed,
    const std::vector<double>& weight_derivs, double bias_deriv ) {
    if( old_model.get() != internal_predictor2_.load().get() ) {
        return;
    }
    if( is_static_ or ( num_observed == 0 ) ) {
        return;
    }

    std::vector<double> new_weights = old_model->weights_;
    double              new_bias = old_model->bias_;

    DCHECK_EQ( weight_derivs.size(), new_weights.size() );

    for( uint32_t i = 0; i < new_weights.size(); i++ ) {
        double old_weight = new_weights.at( i );

        double weight_deriv =
            weight_derivs.at( i ) + ( regularization_ * old_weight );

        double new_weight =
            old_weight - ( ( weight_deriv / num_observed ) * learning_rate_ );

        DVLOG( 40 ) << "Multi predictor2, weight_deriv[" << i << "]"
                    << ", after regularization weight_deriv:" << weight_deriv
                    << ", weight_deriv:" << weight_derivs.at( i )
                    << ", old_weight:" << old_weight
                    << ", new_weight:" << new_weight;

        new_weights.at( i ) = new_weight;
    }

    double old_bias = new_bias;

    DVLOG( 40 ) << "Num observed:" << num_observed
                << ", bias_deriv:" << bias_deriv << ", old_bias:" << old_bias;

    bias_deriv = bias_deriv + ( bias_regularization_ * old_bias );
    new_bias = old_bias - ( ( bias_deriv / num_observed ) * learning_rate_ );

    DVLOG( 40 ) << "After regularization, bias_deriv:" << bias_deriv;

    auto new_internal =
        std::make_shared<internal_multi_predictor2>( new_weights, new_bias );

    internal_predictor2_.store( new_internal );
}

internal_multi_predictor2::internal_multi_predictor2(
    const std::vector<double>& weights, double bias )
    : weights_( weights ), bias_( bias ) {}

double internal_multi_predictor2::make_prediction(
    const std::vector<double>& input ) const {
    DCHECK_EQ( input.size(), weights_.size() );

    double pred = bias_;
    for( uint32_t i = 0; i < input.size(); i++ ) {
        pred += ( input.at( i ) * weights_.at( i ) );
    }

    return pred;
}

void internal_multi_predictor2::update_local_weight_derivs_and_bias_derivs(
    const std::vector<double>& input, double prediction, double actual,
    std::vector<double>& weight_derivs, double& bias_deriv,
    const std::vector<double>& max_inputs ) const {
    double err = actual - prediction;
    DCHECK_EQ( input.size(), weights_.size() );
    DCHECK_EQ( input.size(), weight_derivs.size() );
    DCHECK_EQ( input.size(), max_inputs.size() );

    double delta_b = -2 * err;
    bias_deriv = bias_deriv + delta_b;

    for( uint32_t i = 0; i < input.size(); i++ ) {
        double delta_w =
            ( -2 * std::min( input.at( i ), max_inputs.at( i ) ) ) * err;
        weight_derivs.at( i ) += delta_w;
    }
}

predictor2_result::predictor2_result()
    : input_( 0 ),
      weight_( 0 ),
      bias_( 0 ),
      predicted_output_( 0 ),
      actual_output_( 0 ),
      error_( 0 ) {}
predictor2_result::predictor2_result( double input, double weight, double bias )
    : input_( input ),
      weight_( weight ),
      bias_( bias ),
      predicted_output_( 0 ),
      actual_output_( 0 ),
      error_( 0 ) {
    make_prediction();
}

void predictor2_result::add_actual_output( double actual ) {
    actual_output_ = actual;
    error_ = actual_output_ - predicted_output_;
}

void predictor2_result::make_prediction() {
    predicted_output_ = ( input_ * weight_ ) + bias_;
}

void predictor2_result::rescale_prediction_to_range( double min_bound,
                                                     double max_bound ) {
    predicted_output_ =
        scale_prediction_to_range( predicted_output_, min_bound, max_bound );
}

double predictor2_result::get_input() const { return input_; }
double predictor2_result::get_weight() const { return weight_; }
double predictor2_result::get_bias() const { return bias_; }
double predictor2_result::get_prediction() const { return predicted_output_; }
double predictor2_result::get_actual_output() const { return actual_output_; }
double predictor2_result::get_error() const { return error_; }

std::ostream& operator<<( std::ostream& os, const predictor2_result& res ) {
    os << "[ input_:" << res.input_ << ", weight_:" << res.weight_
       << ", bias_:" << res.bias_
       << ", predicted_output_:" << res.predicted_output_
       << ", actual_output_:" << res.actual_output_ << ", error_:" << res.error_
       << " ]";

    return os;
}
std::ostream& operator<<( std::ostream& os, const predictor2& p ) {
    auto pred_result = p.make_prediction( 0 );
    os << "[ is_static_:" << p.is_static_
       << ", learning_rate_:" << p.learning_rate_
       << ", weight_:" << pred_result.get_weight()
       << ", bias_:" << pred_result.get_bias()
       << ", max_input_:" << p.max_input_ << " ]";

    return os;
}
std::ostream& operator<<( std::ostream& os, const multi_predictor2& p ) {

    os << "[ is_static_:" << p.is_static_ << ", weight_: ( ";
    auto internal = p.internal_predictor2_.load();
    for( const auto& w : internal->weights_ ) {
        os << w << ", ";
    }

    os << " ), bias_:" << internal->bias_ << ", max_inputs_: ( ";
    for( const auto& m : p.max_inputs_ ) {
        os << m << ", ";
    }

    os << " ), learning_rate_:" << p.learning_rate_ << " ]";

    return os;
}


