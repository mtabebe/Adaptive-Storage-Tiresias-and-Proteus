#pragma once

#include <glog/logging.h>

inline uint64_t make_user_profile_key( const twitter_user_profile& t ) {
    return (uint64_t) t.u_id;
}

inline uint64_t combine_keys( uint64_t high, uint64_t low ) {
    uint64_t id = ( ( high ) << 32 ) | low;
    return id;
}

inline uint64_t make_followers_key( const twitter_followers& t ) {
    return combine_keys( t.u_id, t.f_id );
}
inline uint64_t make_follows_key( const twitter_follows& t ) {
    return combine_keys( t.u_id, t.f_id );
}
inline uint64_t make_tweets_key( const twitter_tweets& t ) {
    return combine_keys( t.u_id, t.tweet_id );
}

inline uint32_t get_user_from_key( int32_t table_id, uint64_t key ) {
    uint64_t u_id = key;
    if( table_id != k_twitter_user_profiles_table_id ) {
        u_id = key >> 32;
    }
    return (uint32_t) u_id;
}

inline uint32_t get_lower_from_key( uint64_t key ) {
  uint64_t u_id = key;
  u_id  = (u_id << 32 )  >> 32;
  return (uint32_t) u_id;
}

inline uint32_t get_follows_id_from_key( int32_t table_id, uint64_t key ) {
    if( ( table_id != k_twitter_followers_table_id ) and
        ( table_id != k_twitter_follows_table_id ) ) {
        DLOG( WARNING )
            << "Getting follows_id from key of non followers or follows table:"
            << table_id;
    }
    return get_lower_from_key( key );
}
inline uint32_t get_tweet_from_key( int32_t table_id, uint64_t key ) {
  if ( table_id != k_twitter_tweets_table_id) {
      DLOG( WARNING ) << "Getting tweet from key of non tweets table:"
                      << table_id;
  }
  return get_lower_from_key( key );
}
