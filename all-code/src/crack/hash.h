
#ifndef _FH_HASH_
#define _FH_HASH_

#include <string.h>
#include <assert.h>

class Hash {
  int *key,*val,N,S;

  bool empty(int k, bool skip_holes){
    return skip_holes? (key[k]==-1) : (key[k]<0);
  }

  // adopted from http://en.wikipedia.org/wiki/Jenkins_hash_function
  int lookup(int k, bool skip_holes){
    unsigned int h1 = k;
    h1 += (h1 << 3);
    h1 ^= (h1 >> 11);
    h1 += (h1 << 15);
    h1 %= S;
    if (key[h1] == k || empty(h1,skip_holes)) return h1;
    unsigned int h2 = k;
    h2 += (h2 << 10);
    h2 ^= (h2 >> 6);
    h2 += (h2 << 14);
    if (!(h2&1)) h2++;
    h2 %= S;
    for (int i=0; i<S; i++){
      int h3 = (h1 + i*h2) % S;
      if (key[h3] == k || empty(h3,skip_holes)) return h3;
    }
    fprintf(stderr,"k=%d, %d, N=%d,S=%d, %d %d\n",k,skip_holes,N,S,h1,h2);
    assert(false);
    return 0;
  }

public :
  Hash(int n):N(n){
    S = 1;
    while (S < n) S <<= 1;
    S <<= 1;
    key = new int[S];
    val = new int[S];
    clear();
  }

  void print(){
    fprintf(stderr,"N = %d, S = %d\n",N,S);
    for (int i=0; i<S; i++) if (!empty(i,0))
      fprintf(stderr,"%d -> %d %d\n",i,key[i],val[i]);
  }

  void clear(){
    memset(key,-1,sizeof(int)*S);
  }

  ~Hash(){
    delete[] key;
    delete[] val;
  }

  void set(int k, int v){
    int id = lookup(k,0);
//    assert(key[id] < 0);
    key[id] = k;
    val[id] = v;
  }

  void erase(int k){
    int id = lookup(k,1);
    assert(key[id]>=0);
    key[id] = -2;
  }

  int get(int k){
    int id = lookup(k,1);
    return (key[id]>=0)? val[id] : -1;
  }
};

#endif
