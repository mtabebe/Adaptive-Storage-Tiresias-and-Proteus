#pragma once

#include <stdio.h>
#include <stdarg.h>
#include <assert.h>
#include <sys/time.h>
#include "workload.h"
#include "crack_predictor.h"
#include <zlib.h>
#include <string>

#include <glog/logging.h>



void init(int *a, int n, int cap);
void insert(int v);
void remove(int v);
int view_query(int a, int b);	// returns a view
int count_query(int a, int b);

void clear_cracker();

void record_query( int a, int b, CrackPredictionArgs &args );
void do_predictive_cracking( double queryLat, CrackPredictionArgs &args );

struct Timer {
	struct timeval tv0, tv1;
	double total;
	void start(){ gettimeofday(&tv0,0); }
	void stop(){ gettimeofday(&tv1,0); total += tv1.tv_sec - tv0.tv_sec + (tv1.tv_usec - tv0.tv_usec)*1e-6; }
	void clear(){ total = 0; }
	double elapsed(){ return total; }
};

extern int N1, N2, N3, N4, N5, N6;
extern struct Timer mri_t, mrd_t, crack_t, tree_t, t5;
extern int QQQ,n_cracks,n_trash,n_rippled,n_touched;


double timing();

class GzWriter {
   public:
    gzFile f;
    std::string fileName_;
    GzWriter( const char *const fn ) : fileName_( fn ) {
        f = gzopen(fn,"wb");
		if (!f){
			fprintf(stderr, "gzopen of '%s' failed:.\n", fn);
			exit(EXIT_FAILURE);
		}
    }
    ~GzWriter(){ gzclose(f); }
	void printf(const char *fmt, ...){
		static char s[10000];
		va_list argptr;
		va_start(argptr,fmt);
		int ns = vsprintf(s, fmt, argptr);
		va_end(argptr);

		assert(ns > 0);
		int w = gzwrite(f,s,ns);
        VLOG( 10 ) << "GZ - " << fileName_ << " - " << std::string( s, ns );
        if (w == 0){
			int err_no = 0;
			fprintf(stderr, "Error during compression: %s", gzerror(f, &err_no));
			gzclose(f);
			exit(err_no);
		}
	}
};

// ./a.out input-data num-of-queries query-workload update-workload time-limit
// int main(int argc, char *argv[]){
// int crack_main( int argc, char *argv[] ) {

int crack_main( const std::string &argDataFile, int argNumQueries,
                const std::string &argQueryWorkload,
                const std::string argUpdateWorkload, double argSelectivity,
                int argUpdateFreq, int argUpdateAmount, int argTimelimit,
                const std::string &  argQueryRet,
                CrackPredictionArgs &argPredictions );
