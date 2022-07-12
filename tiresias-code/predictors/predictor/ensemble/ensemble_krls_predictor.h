#pragma once
#include <dlib/svm.h>

#include "../krls_predictor.h"
#include "ensemble_early_query_predictor_configs.h"
#include "ensemble_predictor.h"

typedef uint64_t query_id;
typedef uint64_t query_count;

class ensemble_krls_predictor : public ensemble_predictor {
    dvector<1> max_input_{(double) ( max_query_count_ )};

    dlib::radial_basis_kernel<dvector<1>> kernel_{configs_.krls_gamma_};
    std::unique_ptr<krls_predictor<1>>    raw_krls_predictor_;
    folly::Synchronized<std::unique_ptr<krls_predictor<1>>> krls_predictor_;

   public:
    ~ensemble_krls_predictor() {}

    ensemble_krls_predictor(
        folly::Synchronized<folly::EvictingCacheMap<
            epoch_time,
            std::shared_ptr<std::unordered_map<query_id, query_count>>>>&
                                                     query_observations,
        const ensemble_early_query_predictor_configs configs,
        query_count                                  max_query_count );

    ensemble_krls_predictor(
        folly::Synchronized<folly::EvictingCacheMap<
            epoch_time,
            std::shared_ptr<std::unordered_map<query_id, query_count>>>>&
                    query_observations,
        query_count max_query_count );

    query_count get_estimated_query_count( query_id q_id, epoch_time time );
    void train( query_id q_id, epoch_time current_time );
    void add_observation( query_id observed_query_id, epoch_time time );
};

