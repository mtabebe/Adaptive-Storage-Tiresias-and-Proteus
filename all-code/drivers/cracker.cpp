#include <gflags/gflags.h>
#include <glog/logging.h>

#include "../src/common/constants.h"
#include "../src/common/perf_tracking.h"

#include "../src/crack/tester.h"

#include "../src/common/predictor/ensemble/ensemble_early_query_predictor.h"
#include "../src/common/predictor/simple_early_query_predictor.h"
#include "../src/common/predictor/spar_early_query_predictor.h"

DEFINE_string( scrack_data_dir, "../scrack/data/100000000.data",
               "The directory of data for the workload." );

DEFINE_string( scrack_query_workload, "Periodic",
               "The scrack query workload." );
DEFINE_string( scrack_update_workload, "NOUP", "The scrack update workload." );
DEFINE_string( scrack_query_ret, "view", "The scrack query return value." );

DEFINE_int32( scrack_num_queries, 21470, "The number of queries to run." );

DEFINE_int32( scrack_update_freq, FLAGS_scrack_num_queries + 1,
              "After how many queries do updates execute" );
DEFINE_int32( scrack_update_amount, 0, "How many updates get executed" );

DEFINE_int32( scrack_time_limit, 30, "The time limit of the measurement." );
DEFINE_double( scrack_selectivity, 0.01, "The selectivity of scan queries." );
DEFINE_double( scrack_per_query_time, 750000000,
               "The time allowed per query in ns" );
DEFINE_bool( scrack_do_prediction, false, "Should scrack make predictions" );
DEFINE_int32( scrack_num_jumps_forward, 5,
              "How many steps forward should scrack predict" );
DEFINE_int32( scrack_num_predictive_queries, 2,
              "How many predictive queries should execute at a time" );
DEFINE_int32(
    scrack_max_prediction_window,
    FLAGS_scrack_num_jumps_forward* FLAGS_scrack_num_predictive_queries,
    "How far in the future should predictive queries be executed." );
DEFINE_int32(
    scrack_crack_size, 512,
    "How big should a run be to be worthwhile cracking predictively" );
DEFINE_int32( scrack_data_bucket_size, 10000000,
              "The size of a bucket to aggregate data for predictions" );
DEFINE_bool(
    scrack_predictive_random_crack, true,
    "Should predictive cracking be done using a stochastic algorithm" );

DEFINE_int32( scrack_num_predictor_loader_queries, 2147000,
              "The number of queries to run only to feed to the predictor" );

DEFINE_int32(
      scrack_train_frequency, 214, "The frequency with which to train the predictor" );
DEFINE_double( scrack_prediction_threshold, 0.5,
              "The threshold for a prediction to be acted on" );

DEFINE_string(
    scrack_eqp_type, "None",
    "The type of EQP to be used (SPAR, Ensemble, Simple) to predict accesses" );

DEFINE_bool( scrack_clear_cracker, false,
             "Should the cracker index be cleared periodically" );

std::shared_ptr<early_query_predictor> make_crack_eqp(
    const std::string& eqpLabel ) {
    std::shared_ptr<early_query_predictor> ret = nullptr;

    if( eqpLabel.compare( "SPAR" ) == 0 ) {
      auto spar_configs =
construct_spar_predictor_configs();
      ret = std::make_shared<spar_early_query_predictor>(
          spar_configs,
          std::make_shared<
              spar_predictor<spar_early_query_predictor::variate_count>>(
              spar_configs ) );
    } else if( eqpLabel.compare( "Ensemble" ) == 0 ) {
        ret = std::make_shared<ensemble_early_query_predictor>(
            construct_ensemble_early_query_predictor_configs() );
    } else if( eqpLabel.compare( "Simple" ) == 0 ) {
        ret= std::make_shared<simple_early_query_predictor>();
    }

    return ret;
}

void run_cracker() {
  std::string data = FLAGS_scrack_data_dir;
  int numQueries = FLAGS_scrack_num_queries;
  std::string queryWorkload = FLAGS_scrack_query_workload;
  double selectivity= FLAGS_scrack_selectivity;
  std::string updateWorkload = FLAGS_scrack_update_workload;
  int timeLimit = FLAGS_scrack_time_limit;
  std::string queryRet = FLAGS_scrack_query_ret;

  int updateFreq = FLAGS_scrack_update_freq;
  int updateAmount = FLAGS_scrack_update_amount;

  std::string eqpLabel = FLAGS_scrack_eqp_type;

  CrackPredictionArgs args(
      FLAGS_scrack_do_prediction, FLAGS_scrack_per_query_time,
      FLAGS_scrack_clear_cracker,
      FLAGS_scrack_num_jumps_forward, FLAGS_scrack_num_predictive_queries,
      FLAGS_scrack_max_prediction_window, FLAGS_scrack_predictive_random_crack,
      FLAGS_scrack_crack_size, FLAGS_scrack_data_bucket_size,
      FLAGS_scrack_num_predictor_loader_queries, FLAGS_scrack_train_frequency,
      FLAGS_scrack_prediction_threshold, make_crack_eqp( eqpLabel ) );

  LOG( WARNING ) << "Run Cracker";
  LOG( WARNING ) << "Cracker benchmark args";
  LOG( WARNING ) << "CRACK_ARGS:" << " scrack_data:" << data;
  LOG( WARNING ) << "CRACK_ARGS:" << " numQueries:" << numQueries;
  LOG( WARNING ) << "CRACK_ARGS:" << " queryWorkload:" << queryWorkload;
  LOG( WARNING ) << "CRACK_ARGS:" << " selectivity:" << selectivity;
  LOG( WARNING ) << "CRACK_ARGS:" << " updateWorkload:" << updateWorkload;
  LOG( WARNING ) << "CRACK_ARGS:" << " clearPredictor:" << args.clearCracker_;

  LOG( WARNING ) << "CRACK_ARGS:" << " updateFrequency:" << updateFreq;
  LOG( WARNING ) << "CRACK_ARGS:" << " updateAmount:" << updateAmount;
  LOG( WARNING ) << "CRACK_ARGS:" << " updateWorkload:" << updateWorkload;

  LOG( WARNING ) << "CRACK_ARGS:" << " timeLimit:" << timeLimit;
  LOG( WARNING ) << "CRACK_ARGS:" << " queryRet:" << queryRet;
  LOG( WARNING ) << "CRACK_ARGS:" << " perQueryTime:" << args.perQueryTime_;
  LOG( WARNING ) << "CRACK_ARGS:" << " doPrediction:" << args.doPrediction_;
  LOG( WARNING ) << "CRACK_ARGS:" << " numJumpsForward:" << args.numJumpsForward_;
  LOG( WARNING ) << "CRACK_ARGS:"
              << " numPredictiveQueries:" << args.numPredictiveQueries_;
  LOG( WARNING ) << "CRACK_ARGS:"
    << " maxPredictionWindow:" << args.maxPredictionWindow_;
  LOG( WARNING ) << "CRACK_ARGS:"
    << " doTargettedRandomCrack:" << args.doTargettedRandomCrack_;
  LOG( WARNING ) << "CRACK_ARGS:" << " crackSize:" << args.crackSize_;
  LOG( WARNING ) << "CRACK_ARGS:" << " dataBucketSize:" << args.dataBucketSize_;

  LOG( WARNING ) << "CRACK_ARGS:" << " numPredictorLoaderQueries:"
              << args.numPredictorLoaderQueries_;
  LOG( WARNING ) << "CRACK_ARGS:"
              << " EQPLabel:" << eqpLabel;
  LOG( WARNING ) << "CRACK_ARGS:" << " trainFrequency:" << args.trainFrequency_;
  LOG( WARNING ) << "CRACK_ARGS:"
              << " predictionThreshold:" << args.predictionThreshold_;

  LOG( WARNING ) << "Running...";

  crack_main( data, numQueries, queryWorkload, updateWorkload, selectivity,
              updateFreq, updateAmount, timeLimit, queryRet, args );

  LOG( WARNING ) << "Done Cracker";
}

int main( int argc, char** argv ) {

    gflags::ParseCommandLineFlags( &argc, &argv, true );
    google::InitGoogleLogging( argv[0] );
    google::InstallFailureSignalHandler();

    init_global_state();

    run_cracker();

    dump_counters( 0 );

    return 0;
}
