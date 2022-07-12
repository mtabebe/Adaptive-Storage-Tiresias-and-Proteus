#pragma once

#define ConcurrentMapTypes \
    template <typename K, typename V, typename Hash, typename L>
#define ConcurrentMapKV concurrent_map<K, V, Hash, L>

ConcurrentMapTypes ConcurrentMapKV::concurrent_map() : guard(), map() {}

ConcurrentMapTypes std::pair<bool, V> ConcurrentMapKV::insert(
    const K &key, const V &value, bool overwrite ) {
    guard.lock();
    std::pair<bool, V> previous = locked_find( key );
    if( !previous.first or overwrite ) {
        map[key] = value;
    }
    guard.unlock();

    return previous;
}

ConcurrentMapTypes std::pair<bool, V> ConcurrentMapKV::find( const K &key ) {
    guard.lock();
    std::pair<bool, V> found = locked_find( key );
    guard.unlock();
    return found;
}

ConcurrentMapTypes std::pair<bool, V> ConcurrentMapKV::erase( const K &key ) {
    guard.lock();
    std::pair<bool, V> previous = locked_find( key );

    map.erase( key );
    guard.unlock();
    return previous;
}

ConcurrentMapTypes std::pair<bool, V> ConcurrentMapKV::locked_find(
    const K &key ) {

    std::pair<bool, V> found;
    found.first = false;

    auto search = map.find( key );
    if( search != map.end() ) {
        found.first = true;
        found.second = search->second;
    }

    return found;
}
