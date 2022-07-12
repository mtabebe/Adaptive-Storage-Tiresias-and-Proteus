#include "tester.h"

#include "../common/perf_tracking.h"

int N1, N2, N3, N4, N5, N6;

struct Timer mri_t, mrd_t, crack_t, tree_t, t5;
int QQQ,n_cracks,n_trash,n_rippled,n_touched;



double timing() {
    static struct timeval t1, t2;
	gettimeofday(&t2,NULL);
	double ret = t2.tv_sec - t1.tv_sec + (t2.tv_usec - t1.tv_usec) * 1e-6;
	t1 = t2;
	return ret;
}

int crack_main( const std::string &argDataFile, int argNumQueries,
                const std::string &argQueryWorkload,
                const std::string argUpdateWorkload, double argSelectivity,
                int argUpdateFreq, int argUpdateAmount, int argTimelimit,
                const std::string &  argQueryRet,
                CrackPredictionArgs &argPredictions ) {

    /*
      if (argc < 7){
          fprintf(stderr,"usage: ./a.out [input-file] [num of queries]
      [selectivity] [query workload] [update workload] [time limit]
      [output-file]\n"); exit(1);
      }
      */

	int MAXN = 600000000;
	int *arr = new int[MAXN], TLE, Q, K1, K2, ROLLV=1010000000;
	if (!arr){ fprintf(stderr,"Cannot allocate %d integers\n", MAXN); exit(1); }
    FILE *in = fopen( argDataFile.c_str(), "rb" );
    if( !in ) {
        fprintf( stderr, "Error opening file %s\n", argDataFile.c_str() );
        exit( 1 );
    }
    Q = argNumQueries;
    int total = 0;
	int* arrpos = arr;
	while (int sumtot = fread(arrpos, sizeof(int), 1000, in)) {
		total += sumtot;
		arrpos += sumtot;
	}
	int N = total;
	int cap = N*2+100;
    if( ferror( in ) ) {
        perror( "Error while reading file" );
        fprintf( stderr, "Error reading %s!\n", argDataFile.c_str() );
        exit( 1 );
    }
    if (!feof(in)){ fprintf(stderr,"Allocated array not enough! (read = %d)\n",N); exit(1); }
	fclose(in);
	int mx = *std::max_element(arr, arr+N), Qret = 0;
    fprintf( stderr,
             "%15s %5s S=%-8.0lf N=%-9d(%10d) I=", argQueryWorkload.c_str(),
             argUpdateWorkload.c_str(), argSelectivity * mx, N, mx );

    Workload W( mx, (char *) argQueryWorkload.c_str(),
                (int) ( argSelectivity * mx ) );
    argPredictions.futureW_ = Workload( mx, (char *) argQueryWorkload.c_str(),
                                        (int) ( argSelectivity * mx ) );
    argPredictions.trueW_ = &W;

    argPredictions.init();

    char *   updatew = (char *) argUpdateWorkload.c_str();
    char *   returning = (char *) argQueryRet.c_str();
    if (strcmp(updatew,"ROLL") == 0){
		for (int i=0; i<N; i++)
			if (arr[i] <= ROLLV) arr[i] += ROLLV;
	} else if (strcmp(updatew,"APPEND") == 0){
		K1 = 1000, K2 = -1000;
		assert(N >= 500000000);
		N = 10000000;
	}

	timing();
	init(arr, N, cap);
	double total_t = timing();
	fprintf(stderr,"%.3lf ",total_t);

    if( strcmp( updatew, "NOUP" ) == 0 )
        K1 = Q + 1, K2 = 0;
    else if( strcmp( updatew, "LFHV" ) == 0 )
        K1 = 1000, K2 = 1000;
    else if( strcmp( updatew, "HFLV" ) == 0 )
        K1 = 10, K2 = 10;
    else if( strcmp( updatew, "ROLL" ) == 0 )
        K1 = 10, K2 = -10, std::sort( arr, arr + N );
    else if( strcmp( updatew, "TRASH" ) == 0 )
        K1 = 10, K2 = -10000;
    else if( strcmp( updatew, "DELETE" ) == 0 )
        K1 = 1000, K2 = -100000;
    else if( strcmp( updatew, "APPEND" ) == 0 ) {
    } else if( strcmp( updatew, "CTRL" ) == 0 ) {
      K1 = argUpdateFreq;
      K2 = argUpdateAmount;

    } else {
        fprintf( stderr, "Update workload \"%s\" is not found!\n", updatew );
        exit( 1 );
    }

    if (strcmp(returning,"view") == 0) Qret = 0;
	else if (strcmp(returning,"count") == 0) Qret = 1;
	fprintf(stderr, "%c", Qret?'C':'V');

    TLE = argTimelimit;


	Random r(160284);
	GzWriter result_size_f("res/result_size.gz");
	GzWriter examined_f("res/examined.gz");
	GzWriter n_cracks_f("res/n_cracks.gz");
	GzWriter n_touched_f("res/n_touched.gz");
	GzWriter n_trash_f("res/n_trash.gz");
	GzWriter n_rippled_f("res/n_rippled.gz");
	GzWriter update_t_f("res/update_t.gz");
	GzWriter mri_t_f("res/mri_t.gz");
	GzWriter mrd_t_f("res/mrd_t.gz");
	GzWriter crack_t_f("res/crack_t.gz");
	GzWriter tree_t_f("res/tree_t.gz");
	GzWriter search_t_f("res/search_t.gz");
	GzWriter total_t_f("res/total_t.gz");

    int a = 0;
    int b = 0;
    for( int i = 0; i < argPredictions.numPredictorLoaderQueries_; i++ ) {
        bool ok = W.query( a, b );  // get query endpoints based on the workload
        if( !ok ) {
            Q = i;
            break;
        }

        VLOG( 5 ) << "Loader W query: [ " << a << ", " << b << " ]";
        record_query( a, b, argPredictions );
    }

    int res = 0;
    int ROLL = N - 1;
    for( int i = argPredictions.numPredictorLoaderQueries_;
         i < argPredictions.numPredictorLoaderQueries_ + Q; i++ ) {
        VLOG( 5 ) << "iter: " << i << ", < " << Q << ", total_t:" << total_t;
        QQQ = i;
        if( total_t > TLE ) {
            fprintf( stderr, "X" );
            Q = i;
            break;
        }
        if( !( i & ( i + 1 ) ) ) fprintf( stderr, "." );

        double update_t = ( i == 0 ) ? total_t : 0, search_t = 0;
        mri_t.clear();
        mrd_t.clear();
        crack_t.clear();
        tree_t.clear();

        bool ok = W.query( a, b );  // get query endpoints based on the workload
        if( !ok ) {
            Q = i;
            break;
        }

        VLOG( 5 ) << "W query: [ " << a << ", " << b << " ]";

        // do updates every K1 iteration
        if( ( i + 1 ) % K1 == 0 ) {
            if( K2 == -10000 ) {  // TRASH update workload
                // only insert once in the middle domain: 10000
                for( int j = 0; i == K1 - 1 && j < 1000000; j++ ) {
                    int val = abs( r.nextInt() );
                    assert( val >= 0 && val <= 2147483647 );
                    timing();
                    insert( val );
					update_t += timing();
				}
				fprintf(stderr, "H");
				K1 *= 10000;
				// K1 = Q+1;
			} else if (K2 == -1000){
				if (N < 580000000){
					// insert 10M tuples
					timing();
					for (int j=0; j<10000000; j++){
						insert(arr[N++]);
					}
					update_t += timing();
					fprintf(stderr, "A");
				}
			} else if (K2 == -100000){
				for (int j=0; j<1000; j++){
					int idx = r.nextInt(N);
					timing();
						remove(arr[idx]);
					update_t += timing();
					arr[idx] = arr[--N];
				}
			} else if (K2 < 0){
				// if ((i+1)%1000000 == 0) fprintf(stderr, "arr[%d] = %d, next = %d\n", ROLL,arr[ROLL],ROLLV);
				for (int j=0; j>K2; j--){
					timing();
						remove(arr[ROLL]);
						insert(ROLLV);
					update_t += timing();

					arr[ROLL--] = ROLLV--;
					if (ROLL<0) ROLL = N-1;
					assert(ROLLV >= 0);
				}
			} else if (K2 > 0){
				// each update consists of K2 replacements
				// fprintf(stderr, "x");
				for (int j=0; j<K2; j++){
					int idx = r.nextInt(N), val = abs(r.nextInt());
					timing();
						remove(arr[idx]);
						insert(val);
					update_t += timing();
					arr[idx] = val;
				}
			}
			// fprintf(stderr, "y");
            VLOG( 0 ) << "ArgPredictions clearCracker:"
                      << argPredictions.clearCracker_;
            if( argPredictions.clearCracker_ ) {
                clear_cracker();
            }
        }
		if (K2 == -10){
			a = ROLLV;
			b = ROLLV + 10000;
		}

		n_cracks = n_trash = n_rippled = n_touched = 0;

        record_query( a, b, argPredictions );

        double queryLat;

        start_timer( CRACK_QUERY_TIMER_ID );

        if( Qret == 0 ) {
            timing();
            res = view_query( a, b );
            search_t = timing();
        } else if( Qret == 1 ) {
            timing();
            res = count_query( a, b );
            search_t = timing();
        } else {
            assert( 0 );
        }

        stop_and_store_timer( CRACK_QUERY_TIMER_ID, queryLat );

        do_predictive_cracking( queryLat, argPredictions );

        if (i==0) fprintf(stderr,"F=%.3lf ",search_t);

		total_t += update_t + search_t;

		result_size_f.printf("%d\n", res);
		examined_f.printf("%d\n", n_trash+n_rippled+n_touched);
		n_cracks_f.printf("%d\n", n_cracks);
		n_touched_f.printf("%d\n", n_touched);
		n_trash_f.printf("%d\n", n_trash);
		n_rippled_f.printf("%d\n", n_rippled);
		mri_t_f.printf("%.6lf\n", mri_t.elapsed());
		mrd_t_f.printf("%.6lf\n", mrd_t.elapsed());
		crack_t_f.printf("%.6lf\n", crack_t.elapsed());
		tree_t_f.printf("%.6lf\n", tree_t.elapsed());
		search_t_f.printf("%.6lf\n", search_t);
		update_t_f.printf("%.6lf\n", update_t);
		total_t_f.printf("%.6lf\n", update_t + search_t);
    }
    fprintf(stderr," T=%9.6lf Q=%d\n",total_t,Q);
	// fprintf(stderr,"N1 = %8d, T1 = %9.6lf, rat = %.6lf\n",N1,t1,N1/t1);
	// fprintf(stderr,"N2 = %8d, T2 = %9.6lf, rat = %.6lf\n",N2,t2,N2/t2);
	// fprintf(stderr,"N3 = %8d, T3 = %9.6lf, rat = %.6lf\n",N3,t3,N3/t3);
	// fprintf(stderr,"N4 = %8d, T4 = %9.6lf, rat = %.6lf\n",N4,t4,N4/t4);
	// fprintf(stderr,"N5 = %8d, T5 = %9.6lf, rat = %.6lf\n",N5,t5,N5/t5);
	// fprintf(stderr,"N6 = %8d, T6 = %9.6lf, rat = %.6lf\n",N6,t6,N6/t6);
	// fprintf(stderr,"Total = %9.6lf\n",t1+t2+t3+t4+t5+t6);

	FILE *QF = fopen("res/res_q","w"); fprintf(QF,"%d\n",Q); fclose(QF);
    return 0;
}
