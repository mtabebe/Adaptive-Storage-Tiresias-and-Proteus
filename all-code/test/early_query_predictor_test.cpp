#define GTEST_HAS_TR1_TUPLE 0

#include <chrono>
#include <cmath>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <stdlib.h>

#include "../src/common/constants.h"
#include "../src/common/predictor/early_query_predictor.h"
#include "../src/common/predictor/ensemble/ensemble_early_query_predictor.h"
#include "../src/common/predictor/ensemble/ensemble_early_query_predictor_configs.h"
#include "../src/common/predictor/spar_early_query_predictor.h"

class early_query_predictor_test : public ::testing::Test {};


query_count eqp_sinx_linear_function( epoch_time time, epoch_time base_time ) {
    epoch_time deltaTime = ( time - base_time );

    double scaledDelta = ((double) deltaTime ) / ((double) 100*1000);
    double scaledTime = ((double) time ) / ((double) 100*1000);
    int linearComp = 200 * scaledDelta;
    double innerSin  =scaledTime * dlib::pi;
    double sinVal = sin( innerSin );
    int sinComp = abs(  500 * sinVal );
    int ret = linearComp + sinComp;
    query_count qRet = ret;
    if ( ret < 0 ) {
      qRet = 0;
    }

    DLOG( WARNING ) << "EQP_SL: time:" << time << ", base:" << base_time
                    << ", ret:" << qRet << ", scaledDelta:" << scaledDelta
                    << ", scaledTime:" << scaledTime
                    << ", linearComp:" << linearComp << ", sinComp:" << sinComp
                    << ", innerSin:" << innerSin << ", sinVal:" << sinVal;

    return qRet;
}

query_count eqp_apply_noise( int query_count ) {
    srand( time( nullptr ) );
    int noise = ( rand() % 10 ) - 5;
    return query_count - noise < 0 ? 0 : query_count - noise;
}

void sinx_linx_test( std::shared_ptr<early_query_predictor>& query_predictor,
                     const std::string&                      label ) {
    LOG( INFO ) << "sinx_linx_test: label:" << label;
    int queryId = 0;


    epoch_time current_time =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch() )
            .count();
    epoch_time base_time = current_time - 100 * 1000 * 9;
    DLOG( WARNING ) << "current time:" << current_time
                << ", base_time:" << base_time;

    if( k_skip_slow_tests ) {
        DLOG( WARNING ) << "Skipping test";
        return;
    }
    for( epoch_time x = base_time; x <= current_time; x += 10 ) {
        query_count count = eqp_sinx_linear_function( x, base_time );
        query_count noiseCount = eqp_apply_noise( count );
        DLOG( WARNING ) << "Time:" << x << ", count:" << count
                        << ", noiseCount:" << noiseCount;
        if ( noiseCount > 0 ) {
            query_predictor->add_observation( queryId, x, noiseCount );
        }
    }

    query_predictor->train( current_time );
    int numPred = 0;
    double sumErr = 0;

    for( epoch_time x = current_time; x <= current_time + ( 100 * 10 ); x += 100 ) {
        double count = eqp_sinx_linear_function( x, base_time ) * 10;
        double estimated = query_predictor->get_estimated_query_count(
            queryId, x, current_time );
        double err = ( count - estimated );
        double loc_rmse = sqrt( ( err * err ) );
        numPred += 1;
        sumErr += loc_rmse;
        DLOG( WARNING ) << "Time:" << x << ", actual:" << count
                        << ", predicted:" << estimated << ", err:" << err;
    }
    double rmse = sumErr / numPred;
    DLOG( WARNING ) << "Predictor:" << label << ", rmse:" << rmse;

#if 0
#endif
}

TEST_F( early_query_predictor_test, spar_sinx_linx ) {
    static constexpr long variate_count =
        SPAR_PERIODIC_QUANTITY + SPAR_TEMPORAL_QUANTITY;

    spar_predictor_configs spar_configs = construct_spar_predictor_configs(
        1000,                    /* width */
        100,                     /* slot */
        2000,                    /* training interval */
        10,                      /* temporal granularity */
        20,                      /* periodic granularity */
        10,                      /* arrival window */
        25000,                   /* normalization parameter */
        0.07,                    /* gamma */
        0.1 /* learning rate */  // .01 0.0000001
        );
    std::shared_ptr<early_query_predictor> spar_eqp =
        std::make_shared<spar_early_query_predictor>(
            spar_configs, std::make_shared<spar_predictor<variate_count>>() );

    sinx_linx_test( spar_eqp, "SPAR" );
}

TEST_F( early_query_predictor_test, ensemble_sinx_linx ) {
    ensemble_early_query_predictor_configs ensemble_configs =
        construct_ensemble_early_query_predictor_configs(
            1000, /* slot */
            10,   /* search interval */
            75,   /* linear training interval */
            50,   /* rnn training interval */
            4,    /* scheduled training interval */
            0.05, /* rnn learning rate */
            0.1,  /* krls learning rate */
            1.5,  /* outlier threshold */
            1.0,  /* krls gamma*/
            75,   /* rnn epoch count*/
            12,   /* rnn sequence count*/
            5     /* rnn layer count*/
        );

    std::shared_ptr<early_query_predictor> ensemble_eqp =
        std::make_shared<ensemble_early_query_predictor>( ensemble_configs );

    sinx_linx_test( ensemble_eqp, "ENSEMBLE" );
}

