#pragma once

#include "predictor.h"
#include <dlib/svm.h>
#include <folly/concurrency/AtomicSharedPtr.h>

// Kernel Recursive Least Squares: an online kernel based regression algorithm
// Based on the paper The Kernel Recursive Least Squares Algorithm by Yaakov
// Engel.

template <typename kernel_type = dlib::radial_basis_kernel<d_var_vector>>
class krls_predictor2 : public predictor<std::vector<double>, double> {
   public:
    krls_predictor2( double learning_rate, const std::vector<double>& max_input,
                     bool is_static, const kernel_type& kernel,
                     uint32_t max_internal_model_size /* =
                        k_predictor_max_internal_model_size */,
                     double min_prediction_range, double max_prediction_range );
    ~krls_predictor2();

    uint64_t model_version() const;
    double make_prediction( const std::vector<double>& input ) const;

   protected:
    void add_observations_to_model(
        const std::vector<
            typename predictor<std::vector<double>, double>::observation>& obs )
        override;

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

using krls_radial_basis_predictor =
    krls_predictor2<dlib::radial_basis_kernel<d_var_vector>>;

#include "krls_predictor2.tcc"
