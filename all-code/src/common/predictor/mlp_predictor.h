#pragma once

#include "predictor.h"
#include <dlib/mlp.h>
#include <folly/concurrency/AtomicSharedPtr.h>

// One- or Two-Layer perceptron network that is trained using the back
//   propagation algorithm.

// The training algorithm also incorporates the momentum method. That is, each
//   round of back propagation training also adds a fraction of the previous
//   update. This fraction is controlled by the momentum term set in the
//   constructor.

template <long N>
class mlp_predictor : public predictor<dvector<N>, double> {
   public:
    /**
     * nodes_in_input_layer = N
     * nodes_in_first_hidden_layer = layer_1_nodes
     * nodes_in_second_hidden_layer = layer_2_nodes // if want one layer set
     * this to 0
     * nodes_in_output_layer = 1 // to have a single regression
     * alpha = learning_rate // default to k_cost_model_learning_rate
     * momentum = momentum // reasonable default is 0.8
     */
    mlp_predictor( bool is_static, dvector<N> max_input, long layer_1_nodes,
                   long layer_2_nodes, double learning_rate, double momentum,
                   double min_prediction_range, double max_prediction_range );
    ~mlp_predictor();

    uint64_t model_version() const;
    double make_prediction( const dvector<N>& input ) const;

   protected:
    void add_observations_to_model(
        const std::vector<typename predictor<dvector<N>, double>::observation>&
            obs ) override;

    void write( std::ostream& os ) const;

   private:
    uint64_t                                         model_version_;
    folly::atomic_shared_ptr<dlib::mlp::kernel_1a_c> net_;
    folly::atomic_shared_ptr<dlib::mlp::kernel_1a_c> net_bk_;
};

#include "mlp_predictor.tcc"
