#include "crack_predictor.h"

CrackPredictionArgs::CrackPredictionArgs(
    bool doPrediction, double perQueryTime, bool clearCracker,
    int numJumpsForward, int numPredictiveQueries, int maxPredictionWindow,
    bool doTargettedRandomCrack, int crackSize, int dataBucketSize,
    int numPredictorLoaderQueries, int trainFrequency,
    double predictionThreshold, std::shared_ptr<early_query_predictor> eqp )
    : perQueryTime_( perQueryTime ),
      doPrediction_( doPrediction ),
      numJumpsForward_( numJumpsForward ),
      numPredictiveQueries_( numPredictiveQueries ),
      maxPredictionWindow_( maxPredictionWindow ),
      doTargettedRandomCrack_( doTargettedRandomCrack ),
      crackSize_( crackSize ),
      dataBucketSize_( dataBucketSize ),
      numPredictorLoaderQueries_( numPredictorLoaderQueries ),
      clearCracker_( clearCracker ),
      trainFrequency_( trainFrequency ),
      predictionThreshold_( predictionThreshold ),
      futureW_(),
      trueW_( nullptr ),
      predA_( 0 ),
      predB_( 0 ),
      eqp_( eqp ),
      dist_( nullptr ) {}

void CrackPredictionArgs::init() {
    VLOG( 5 ) << "Init crack predictions: numJumps:" << numJumpsForward_;
    advance_queries();
}

void CrackPredictionArgs::advance_queries() {
    if( trueW_ == nullptr ) {
        return;
    }
    while( futureW_.I < trueW_->I + numJumpsForward_ ) {
        futureW_.query( predA_, predB_ );
        VLOG( 5 ) << "Jump:" << futureW_.I << ", query [" << predA_ << ", "
                  << predB_ << "], trueW:" << trueW_->I;
    }
}

int CrackPredictionArgs::bucketize( int a ) {
    int divided = a / dataBucketSize_;
    int mult = divided * dataBucketSize_;

    return mult;
}

void CrackPredictionArgs::record_query( int a, int b ) {
    int bucketA = bucketize( a );
    int bucketB = bucketize( b );
    VLOG( 0 ) << "Record query at time:" << trueW_->I << " ( " << a << ", " << b
              << " ), buckets ( " << bucketA << ", " << bucketB << " )";
    advance_queries();

    if( eqp_ ) {
        eqp_->add_observation( bucketA, trueW_->I );

        if ( trueW_->I % trainFrequency_ == 0 ) {
            eqp_->train( trueW_->I );
        }
    }
}
std::vector<std::tuple<int, int>> CrackPredictionArgs::get_predictive_query() {

    std::vector<std::tuple<int, int>> ret;
    if( futureW_.I > trueW_->I + maxPredictionWindow_ ) {
        return ret;
    }
    futureW_.query( predA_, predB_ );

    int na = bucketize( predA_ );
    int nb = bucketize(  predB_ );

    if( nb == na ) {
      nb = nb + dataBucketSize_;
    }
    if( eqp_ ) {
        auto predQueries = eqp_->get_predicted_queries(
            trueW_->I + 1, trueW_->I + maxPredictionWindow_,
            predictionThreshold_ );
        for( const auto& pred : predQueries ) {
            VLOG( 0 ) << "EQP Predict query at time:" << trueW_->I
                      << ", predicted access time up to:" << futureW_.I << " ( "
                      << pred << ", " << pred + dataBucketSize_ << " )";

            ret.emplace_back( std::make_tuple( pred, pred + dataBucketSize_ ) );
        }
    } else {
      // flip a coin prob of acceptance
      double flip = dist_.get_uniform_double();
      if( flip >= predictionThreshold_ ) {
          ret.emplace_back( std::make_tuple<>( na, nb ) );
      }
    }

    VLOG( 0 ) << "Predict query at time:" << trueW_->I
              << ", predicted access time:" << futureW_.I << " ( " << predA_
              << ", " << predB_ << " ), buckets ( " << na << ", " << nb << " )"
              << ", prediction size: " << ret.size();

    return ret;
}
