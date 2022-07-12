#pragma once

#include <unordered_map>
#include <vector>

#include "lock_interface.h"
#include "mutex_lock.h"

template <typename K, typename V, typename Hash = std::hash<K>,
          typename L = mutex_lock>
class concurrent_map {
   public:
    concurrent_map();

    std::pair<bool, V> insert( const K& key, const V& value,
                               bool overwrite = false );
    std::pair<bool, V> find( const K& key );
    std::pair<bool, V> erase( const K& key );

   private:
    L guard;
    std::unordered_map<K, V, Hash> map;

    std::pair<bool, V> locked_find( const K& key );
};

#include "concurrent_map.tcc"
