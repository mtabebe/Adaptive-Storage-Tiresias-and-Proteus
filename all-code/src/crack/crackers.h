#pragma once

#include <map>
#include <set>
#include <vector>

#include "tester.h"

// using namespace std;

struct CIndex {
  int pos;      // the cracker index position
  int holes;    // the number of holes in front
  bool sorted;  // is this piece sorted
  int prev_pos() const { return pos - holes; }
};

typedef int value_type;                   // the data element value type (can be int or long long)
typedef std::map<value_type, CIndex> ci_type;  // cracker[cracker_value] = (cracker_index, cracker_holes)
typedef ci_type::iterator ci_iter;        // the iterator type for the cracker

int     partition( value_type *arr, value_type v, int L, int R );
ci_iter find_piece( ci_type &ci, int N, value_type v, int &L, int &R );
int     add_crack( ci_type &ci, int &N, value_type v, int p );
void    print_crackers( ci_type &ci );
void    print_all( ci_type &ci, value_type *arr, int &N,
                   std::multiset<int> &pending_insert,
                   std::multiset<int> &pending_delete, int line );
void    check( ci_type &ci, value_type *arr, int &N,
               std::multiset<int> &pending_insert,
               std::multiset<int> &pending_delete );
void split_ab( value_type *arr, int L, int R, value_type a, value_type b,
               int &i1, int &i2 );

void symmetric_crack3( value_type *b, int L, int R, value_type lo,
                       value_type hi, int &ll, int &hh );
void split_abc( value_type *arr, int L, int R, value_type a, value_type b,
                value_type c, int &i1, int &i2, int &i3 );

void materialize_piece( value_type *arr, value_type a, value_type b, int L,
                        int R, value_type *marr, int &msize );
void materialize_it( ci_type &ci, value_type *arr, int &N, value_type a,
                     value_type b, int &i1, int &i2, value_type *marr,
                     int &msize );

#include "hash.h"

void get_pendings( ci_type &ci, value_type a, value_type b, ci_iter it1,
                   ci_iter it2, std::multiset<value_type> &pend,
                   std::vector<value_type> &res );

bool piece_is_empty(ci_type &ci, ci_iter &it2);

void do_merge_ripple_insert( ci_type &ci, value_type *arr, int &N,
                             std::multiset<int> &pending_insert,
                             std::multiset<int> &pending_delete, value_type a,
                             value_type b, value_type &new_hi );

void do_merge_ripple_delete( ci_type &ci, value_type *arr, int &N,
                             std::multiset<int> &pending_insert,
                             std::multiset<int> &pending_delete, value_type a,
                             value_type b );

// this is to make all qualifying tuples in range [a,b) are inside the main array
int merge_ripple( ci_type &ci, value_type *arr, int &N, std::multiset<int> &pins,
                  std::multiset<int> &pdel, value_type a, value_type b );

int targeted_random_crack( ci_type &ci, value_type v, value_type *arr, int &N,
                           int L, int R, int ncracks, int crack_at );

int targeted_random_crack3( ci_type &ci, value_type v, value_type *arr, int &N,
                            int L, int R, int crack_at );
