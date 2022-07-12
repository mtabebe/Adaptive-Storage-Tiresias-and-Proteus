#include "crack.h"      // require implementations of "query"

#include "../common/perf_tracking.h"

extern int n_cracks;
Random rr;

void clear_cracker() {
    VLOG( 0 ) << "Clearing cracker";
    ci.clear();
//    std::random_shuffle( &arr[0], &arr[N] );
}

int add_crack_wrapper( ci_type &ci, int &N, value_type v, int p ) {
    int ret = 0;

    start_timer( CRACK_ADD_CRACK_TIMER_ID );
    ret = add_crack( ci, N, v, p );
    stop_timer( CRACK_ADD_CRACK_TIMER_ID );

    return ret;
}
int merge_ripple_wrapper( ci_type &ci, value_type *arr, int &N,
                          std::multiset<int> &pins, std::multiset<int> &pdel,
                          value_type a, value_type b ) {
    int ret = 0;

    start_timer( CRACK_MERGE_RIPPLE_TIMER_ID );
    ret = merge_ripple( ci, arr, N, pins, pdel, a, b );
    stop_timer( CRACK_MERGE_RIPPLE_TIMER_ID );

    return ret;
}

void split_ab_wrapper(value_type *arr, int L, int R, value_type a, value_type b, int &i1, int &i2){
    start_timer( CRACK_SPLIT_AB_TIMER_ID );
    split_ab( arr, L, R, a, b, i1, i2 );
    stop_timer( CRACK_SPLIT_AB_TIMER_ID );
}

int view_query(int a, int b){
  start_timer( CRACK_VIEW_QUERY_TIMER_ID );

    VLOG( 5 ) << "view_query:" << a << ", " << b;
#ifdef RANDOM_CRACK_PER_QUERY
    for (int i=0; i<RANDOM_CRACK_PER_QUERY; i++)
      naive_random_crack();
  #endif

  #ifdef RANDOM_CRACK_EVERY_NTH_QUERY
    static int nth = 0;
    if (++nth % RANDOM_CRACK_EVERY_NTH_QUERY == 0)
      naive_random_crack();
  #endif

  int cnt = crack(a,b);
  n_cracks += ci.size();

  stop_timer( CRACK_VIEW_QUERY_TIMER_ID );

  VLOG( 5 ) << "view_query:" << a << ", " << b << ", ret:" << cnt;

  return cnt;
}

int count_query( int a, int b ) {
    start_timer( CRACK_COUNT_QUERY_TIMER_ID );

    VLOG( 5 ) << "count_query:" << a << ", " << b;
    view_query( a, b );

    int     L = 0, cnt = 0;
    ci_iter it1, it2;
    if( ci.count( a ) ) {
        it1 = ci.lower_bound( a );
        L = it1->second.pos;
    } else {
        int L1, R1;
        it1 = find_piece( ci, N, a, L1, R1 );
        for( int i = L1; i < R1; i++ )
            if( arr[i] >= a && arr[i] < b ) cnt++;
        L = R1;
        it1++;
    }
    assert( it1 != ci.end() );
    it1++;

    if( ci.count( b ) ) {
        it2 = ci.lower_bound( b );
  } else {
    int L2, R2;
    it2 = find_piece( ci, N, b, L2, R2 );
    for (int i=L2; i<R2; i++)
      if (arr[i] >= a && arr[i] < b) cnt++;
    assert( it1 != it2 );
    it2--;
  }

  while( true ) {
      cnt += it1->second.prev_pos() - L;
      L = it1->second.pos;
      if( it1 == it2 ) break;
      it1++;
  }
  stop_timer( CRACK_COUNT_QUERY_TIMER_ID );

  return cnt;
}

void init( int *a, int n, int cap ) {
    ci.clear();
    msize = 0;
    N = n;
    marr = new int[cap];
    arr = new int[cap];                          // for updates expansion
    for( int i = 0; i < N; i++ ) arr[i] = a[i];  // copy all
}

void insert( int v ) {
    start_timer( CRACK_INSERT_TIMER_ID );

    VLOG( 5 ) << "insert:" << v << ",";
    if( pdel.count( v ) ) {
        pdel.erase( pdel.lower_bound( v ) );  // don't insert if exists in pdel
    } else {
        pins.insert( v );
    }
    stop_timer( CRACK_INSERT_TIMER_ID );
}

void remove(int v){
    start_timer( CRACK_REMOVE_TIMER_ID );

    VLOG( 5 ) << "remove:" << v << ",";
  if (pins.count(v)){
    pins.erase(pins.lower_bound(v));    // don't delete if exists in pins
  } else {
    pdel.insert(v);
  }

  stop_timer( CRACK_REMOVE_TIMER_ID );
}
int crack( value_type a, value_type b ) {
    start_timer( CRACK_CRACK_TIMER_ID );

    VLOG( 5 ) << "crack: [" << a << ", " << b << "]";
    merge_ripple_wrapper( ci, arr, N, pins, pdel, a,
                          b );  // merge qualified updates

    tree_t.start();
    int L1, R1, i1;
    find_piece( ci, N, a, L1, R1 );
    int L2, R2, i2;
    find_piece( ci, N, b, L2, R2 );
    tree_t.stop();

    n_touched += R1 - L1;  // examine the left tuple

    crack_t.start();
    if( L1 == L2 ) {  // a and b is on the same piece
        assert( R1 == R2 );
        split_ab_wrapper( arr, L1, R1, a, b, i1, i2 );  // 3-split in one scan
    } else {                   // a and b is on different piece
        n_touched += R2 - L2;  // examine the right piece
        i1 = partition( arr, a, L1, R1 );
        i2 = partition( arr, b, L2, R2 );
    }
    crack_t.stop();

    tree_t.start();
    add_crack_wrapper( ci, N, a, i1 );
    add_crack_wrapper( ci, N, b, i2 );
    tree_t.stop();

    stop_timer( CRACK_CRACK_TIMER_ID );

    return i2 - i1;  // return number of qualified tuples
}

// do random crack on the target piece, then materialize
int random_crack_and_materialize(value_type a, value_type b){
  merge_ripple_wrapper(ci, arr, N, pins, pdel, a, b);  // merge qualified updates
  int L1,R1,i1; find_piece(ci, N, a, L1,R1);
  int L2,R2,i2; find_piece(ci, N, b, L2,R2);
  assert(L1 != L2 || R1 == R2);
  n_touched += R1 - L1;
  if (L1 < R1){
    value_type X1 = arr[L1 + rand()%(R1-L1)];
    add_crack_wrapper(ci, N, X1, partition(arr,X1,L1,R1));
  }
  if (L1 != L2){
    n_touched += R2 - L2;
    if (L2 < R2){
      value_type X2 = arr[L2 + rand()%(R2-L2)];
      add_crack_wrapper(ci, N, X2, partition(arr,X2,L2,R2));
    }
  }
  msize = 0;
  materialize_it(ci, arr, N, a, b, i1, i2, marr, msize);
  return msize + std::max(0, i2 - i1);
}

void naive_random_crack(){
  value_type x = arr[rr.nextInt(N)];
  int L,R; find_piece(ci, N, x,L,R);
  n_touched += R-L;
  add_crack_wrapper(ci, N, x, partition(arr, x,L,R));
}

void record_query_internal( int a, int b, CrackPredictionArgs &args ) {
    if( !args.doPrediction_ ) {
        return;
    }
    args.record_query( a, b );
}

int ddr_find( value_type v, CrackPredictionArgs &args ) {
    start_timer( CRACK_DDR_FIND_TIMER_ID );
    int L, R;
    find_piece( ci, N, v, L, R );
    n_touched += R - L;
    int ret = targeted_random_crack(
        ci, v, arr, N, L, R, args.numPredictiveQueries_ / 2, args.crackSize_ );
    stop_timer( CRACK_DDR_FIND_TIMER_ID );
    return ret;
}

int do_single_predictive_crack( CrackPredictionArgs &args ) {
    std::vector<std::tuple<int, int>> preds = args.get_predictive_query();
    if( preds.empty() ) {
        return 0;
    }
    for( const auto &pred : preds ) {
        int a = std::get<0>( pred );
        int b = std::get<1>( pred );

        VLOG( 5 ) << "Predicted query [" << a << ", " << b << "]";

        if( args.doTargettedRandomCrack_ ) {
            ddr_find( a, args );
            ddr_find( b, args );
        } else {
            crack( a, b );
        }
        VLOG( 5 ) << "Done predictive query crack [" << a << ", " << b << "]";
    }

    return preds.size();
}

void do_predictive_cracking_internal( double               queryLat,
                                      CrackPredictionArgs &args ) {
    if( !args.doPrediction_ ) {
        return;
    }
    int numPreds = 0;
    while( ( queryLat < args.perQueryTime_ ) and
           ( numPreds < args.numPredictiveQueries_ ) ) {
        double locLatency;

        start_timer( CRACK_ONE_PREDICTION_TIMER_ID );

        if( args.doPrediction_ ) {
            int obsPreds = do_single_predictive_crack( args );
            if ( obsPreds == 0 ) {
                obsPreds = args.numPredictiveQueries_;
            }
            numPreds += obsPreds;

        } else {
            double sleepTime = args.perQueryTime_ - queryLat;
            VLOG( 10 ) << "Query executed for queryLat:"
                       << ", sleepTime:" << sleepTime;

            std::chrono::duration<int, std::nano> sleepDuration(
                (int) sleepTime );

            std::this_thread::sleep_for( sleepDuration );

            VLOG( 10 ) << "Query executed for queryLat:"
                       << ", sleepTime:" << sleepTime << ", done sleep";
        }

        stop_and_store_timer( CRACK_ONE_PREDICTION_TIMER_ID, locLatency );

        queryLat += locLatency;
    }
}

void record_query( int a, int b, CrackPredictionArgs &args ) {
    start_timer( CRACK_RECORD_QUERY_TIMER_ID );
    record_query_internal( a, b, args );
    stop_timer( CRACK_RECORD_QUERY_TIMER_ID );
}
void do_predictive_cracking( double queryLat, CrackPredictionArgs &args ) {
    start_timer( CRACK_PREDICTIVE_CRACKING_TIMER_ID );
    do_predictive_cracking_internal( queryLat, args );
    stop_timer( CRACK_PREDICTIVE_CRACKING_TIMER_ID );
}

