#pragma once

#include "workload.h"

#include <glog/logging.h>
#include <memory>

#include "../common/predictor/early_query_predictor.h"
#include "../distributions/distributions.h"

class CrackPredictionArgs {
  public:
   CrackPredictionArgs( bool doPrediction, double perQueryTime,
                        bool clearCracker, int numJumpsForward,
                        int numPredictiveQueries, int maxPredictionWindow,
                        bool doTargettedRandomCrack, int crackSize,
                        int dataBucketSize, int numPredictorLoaderQueries,
                        int trainFrequency, double predictionThreshold,
                        std::shared_ptr<early_query_predictor> eqp );

   void init();

   int bucketize( int a );

   void record_query( int a, int b );
   std::vector<std::tuple<int, int>> get_predictive_query();

   void advance_queries();

   double perQueryTime_;
   bool   doPrediction_;
   int    numJumpsForward_;
   int    numPredictiveQueries_;
   int    maxPredictionWindow_;
   bool   doTargettedRandomCrack_;
   int    crackSize_;
   int    dataBucketSize_;
   int    numPredictorLoaderQueries_;

   bool clearCracker_;

   int trainFrequency_;
   double predictionThreshold_;

   Workload futureW_;
   Workload* trueW_;

   int predA_;
   int predB_;

   std::shared_ptr<early_query_predictor> eqp_;

   distributions dist_;
};
