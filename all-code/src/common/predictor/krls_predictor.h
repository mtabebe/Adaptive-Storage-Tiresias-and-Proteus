#pragma once

#include "../constants.h"
#include "predictor.h"
#include <dlib/svm.h>
#include <folly/concurrency/AtomicSharedPtr.h>

// Kernel Recursive Least Squares: an online kernel based regression algorithm
// Based on the paper The Kernel Recursive Least Squares Algorithm by Yaakov
// Engel.

template <long N, typename kernel_type = dlib::radial_basis_kernel<dvector<N>>>
class krls_predictor : public predictor<dvector<N>, double> {
   public:
    krls_predictor( double learning_rate, dvector<N> max_input, bool is_static,
                    const kernel_type& kernel,
                    uint32_t           max_internal_model_size /* =
                        k_predictor_max_internal_model_size */,
                    double min_prediction_range, double max_prediction_range );
    ~krls_predictor();

    uint64_t model_version() const;
    double make_prediction( const dvector<N>& input ) const;

   protected:
    void add_observations_to_model(
        const std::vector<typename predictor<dvector<N>, double>::observation>&
            obs ) override;

    void write( std::ostream& os ) const;

   private:
    struct internal_model {
        uint64_t                model_version_;
        dlib::krls<kernel_type> krls_;

        internal_model( uint64_t                       model_version,
                        const dlib::krls<kernel_type>& krls );

        internal_model( uint64_t                  model_version,
                        dlib::krls<kernel_type>&& krls );
    };

    folly::atomic_shared_ptr<internal_model> internal_model_;
};

#include "krls_predictor.tcc"
