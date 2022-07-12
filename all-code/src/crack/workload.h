#pragma once

#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <algorithm>
#include "random.h"

#include <glog/logging.h>

// using namespace std;

class Workload {
   public:
    int    N;     // the number of elements in arr
    int    W;     // the selected workload to be generated
    int    S;     // the selectivity (unused for some workloads)
    int    I;     // the I'th query (internal use only)
    int    a, b;  // the last query range [a,b]

   private:
    Random r;     // Pseudo Random Generator

    // based on the predefined queries from file
    bool skyserver_w() {
        static FILE *in = NULL;
        if( I == 0 ) {
            in = fopen( "data/skyserver.queries", "r" );
            if( !in )
                fprintf( stderr, "Fail loading file data/skyserver.queries\n" );
        }
        if( !in ) return false;
        double x, y;
        if( fscanf( in, "%lf %lf", &x, &y ) == EOF ) {
            if( in ) {
                fclose( in );
                in = NULL;
            }
            return false;
        }
        a = int( y * 1000000 );
        b = a + S;
        return true;
  }

  // a and b is selected uniformly at random and a < b
  bool random_w(){
    if (S == 0){ // random selectivity
      do {
        a = r.nextInt(N);
        b = r.nextInt(N);
        if (a > b) std::swap(a, b);
      } while (a==b);
    } else {
      a = r.nextInt(N - S + 1);
      b = std::max(a + 1, a + S);
    }
    return true;
  }

  // a will be incremented by 10 every subsequent query
  // the range may overlap with the next queries
  bool seq_over_w(){
    a = 10 + I * 20;
    if (a + 5 > N) return false;
    if (S == 0){
      b = a + r.nextInt(N-a) + 1;
    } else {
      b = a + S;
    }
    return b <= N;
  }

  // the opposit direction from the seq_over_w
  bool seq_inv_w(){
    if (!seq_over_w()) return false;
    a = N - a;
    b = N - b;
    std::swap(a,b);
    return true;
  }

  // half the time is seq_over half the time is random_w
  bool seq_rand_w(){
    if (I&1){
      return seq_over_w();
    } else {
      return random_w();
    }
  }

  // sequential with no overlap with the subsequent query ranges
  bool seq_no_over_w(){
    static int prevB; if (!I) prevB = 0;
    a = prevB + 10;
    if (a + 5 > N) return false;
    if (S == 0){
      b = a + r.nextInt(N-a) + 1;
    } else {
      b = a + S;
    }
    prevB = b;
    return b <= N;
  }

  // sequential alternate at the beginning and end
  bool seq_alt_w(){
    if (I&1){
      return seq_over_w();
    } else{
      return seq_inv_w();
    }
  }

  // pick 1000 integers and produce range queries with endpoints
  // using the 1000 picked integers
  bool cons_rand_w(){
    static int R[1000];
    if (!I) for (int i=0; i<1000; i++) R[i] = r.nextInt(N);
    do {
      a = R[r.nextInt(1000)];
      b = R[r.nextInt(1000)];
    } while (a == b);
    if (a > b) std::swap(a,b);
    return true;
  }

  // start at the [middle - 100500, middle + 100500),
  // then zoom in by making the query range smaller by 100 on each query
  bool zoom_in_w(){
    static int L; if (!I) L = N/3;
    static int R; if (!I) R = 2*N/3;
    if (L >= R || L<0 || R>N) return false;
    a = L; L += 100;  // make the range smaller
    b = R; R -= 100;
    return true;
  }

  // start at the [middle - 500, middle + 500),
  // then zoom out by making the query range larger by 100 each query
  bool zoom_out_w(){
    static int L; if (!I) L = N/2 - 500;
    static int R; if (!I) R = N/2 + 500;
    if (L<1 || R>N) return false;
    a = L; L -= 100;  // make the range bigger
    b = R; R += 100;
    return true;
  }

  // after zooming in on one region, move to next unexplored region to the right
  bool seq_zoom_in(){
    static int L; if (!I) L = 1;
    static int G = 100000;
    static int R; if (!I) R = G;
    if (L >= R) L += G, R = L + G;
    if (R > N) return false;
    a = L; L += 100;
    b = R; R -= 100;
    return true;
  }

  // after zooming out on one ragion, move to the next unexplored region on the right
  bool seq_zoom_out(){
    static int G = 100000;
    static int L; if (!I) L = G/2+1000;
    static int R; if (!I) R = L + 10;
    if (R > L+G) L = R+G/2+1000, R = L+10;
    if (R > N) return false;
    a = L; L -= 100;
    b = R; R += 100;
    return true;
  }

  //where 80 percent of the queries falls within 20 percent of the value range and
  //20 percent of the queries falls within the 80 percent of the value range
  bool skew_w(){
    if (I >= 10000) return false;
    if (I < 8000){
      do {
        a = r.nextInt(N/5);
        b = r.nextInt(N/5);
      } while (a == b);
    } else {
      do {
        a = N/5 + r.nextInt((N*4)/5);
        b = N/5 + r.nextInt((N*4)/5);
      } while (a == b);
    }
    if (a > b) std::swap(a,b);
    return true;
  }

  // start at the [middle - 500, middle + 500),
  // then zoom out by making the query range larger by 100 each query
  bool zoom_out_alt_w(){
    static int L; if (!I) L = N/2 - 500;
    static int R; if (!I) R = N/2 + 500;
    if (L<1 || R>N) return false;
    if (I&1){
      a = L;
      L -= 100;  // make the range bigger
      b = a + 10;
    } else {
      b = R;
      R += 100;
      a = b - 10;
    }
    return true;
  }

  // start at the [middle - 500, middle + 500),
  // then zoom out by making the query range larger by 100 each query
  bool skew_zoom_out_alt_w(){
    static int L; if (!I) L = N - 355000;
    static int R; if (!I) R = N - 350000;
    if (L<1 || R>N) return false;
    if (I&1){
      b = R;
      R += 20;
      a = b - 10;
    } else {
      a = L;
      L -= 20;  // make the range bigger
      b = a + 10;
    }
    return true;
  }

  bool periodic_w(){
    static long long jump = 1000001;
    a = (I * jump) % N;
    b = a + 10;
    VLOG_EVERY_N( 5, 10 ) << "periodic_w:" << I << ", jump:" << jump
                          << ", N:" << N << ", a:" << a << ", b:" << b;

    return true;
  }

  bool mixed_w(){
    static int work = 0;
    static int base = 0;
    if (I%1000 == 0){
      work = r.nextInt(15) + 1;
      base = r.nextInt(20);
    }
    int tW = W; W = work;
    int tI = I; I %= 1000;
    int tN = N; N /= 20;
    int ta, tb;
    bool ok = query(ta,tb);
    W = tW;
    I = tI;
    if (!ok){
      N = tN;
      work = r.nextInt(15) + 1;
      return mixed_w();
    }
    a = ta + base * N;
    b = tb + base * N;
    N = tN;
    return true;
  }

public :
 Workload() {
     N = 0;
     S = 0;
     r = Random( 29284 );
     W = 0;
     I = 0;
     a = b = 0;
 }
 Workload( int nElem, char *workload, int selectivity )
     : N( nElem ), S( selectivity ) {
     r = Random( 29284 );

     const char *names[17] = {
         // workload names
         "SkyServer",       // 0
         "Random",          // 1
         "SeqOver",         // 2
         "SeqInv",          // 3
         "SeqRand",         // 4
         "SeqNoOver",       // 5
         "SeqAlt",          // 6
         "ConsRandom",      // 7
         "ZoomIn",          // 8
         "ZoomOut",         // 9
         "SeqZoomIn",       // 10
         "SeqZoomOut",      // 11
         "Skew",            // 12
         "ZoomOutAlt",      // 13
         "SkewZoomOutAlt",  // 14
         "Periodic",        // 15
         "Mixed"            // 16
     };

     for( I = W = 0; W < 17 && strcmp( workload, names[W] ); W++ )
         ;

     if( W == 17 ) {
         fprintf( stderr, "Workload \"%s\" is not found!\n", workload );
         exit( 1 );
     }
     LOG( INFO ) << "Workload:" << names[W] << ", N:" << N << ", S:" << S;
  }

  bool query(int &na, int &nb){
    switch (W){
      case 0 : if (!skyserver_w()) return false; break;
      case 1 : if (!random_w()) return false; break;
      case 2 : if (!seq_over_w()) return false; break;
      case 3 : if (!seq_inv_w()) return false; break;
      case 4 : if (!seq_rand_w()) return false; break;
      case 5 : if (!seq_no_over_w()) return false; break;
      case 6 : if (!seq_alt_w()) return false; break;
      case 7 : if (!cons_rand_w()) return false; break;
      case 8 : if (!zoom_in_w()) return false; break;
      case 9 : if (!zoom_out_w()) return false; break;
      case 10 : if (!seq_zoom_in()) return false; break;
      case 11 : if (!seq_zoom_out()) return false; break;
      case 12 : if (!skew_w()) return false; break;
      case 13 : if (!zoom_out_alt_w()) return false; break;
      case 14 : if (!skew_zoom_out_alt_w()) return false; break;
      case 15 : if (!periodic_w()) return false; break;
      case 16 : if (!mixed_w()) return false; break;
      default : assert(0);
    }
    na = a; nb = b; I++;
    return true;
  }
};
