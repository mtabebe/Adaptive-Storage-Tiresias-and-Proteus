#pragma once

#include "tester.h"       // require implementations of init,insert,remove,query
#include "crackers.h"

std::multiset<int> pins, pdel;          // pending updates (insert / delete)
int *arr, N;              // the dataset array
int *marr, msize;         // for materialization
ci_type ci;               // the cracker index

void init( int *a, int n, int cap );

void insert(int v);

void remove(int v);

int crack(value_type a, value_type b);

// do random crack on the target piece, then materialize
int random_crack_and_materialize(value_type a, value_type b);
