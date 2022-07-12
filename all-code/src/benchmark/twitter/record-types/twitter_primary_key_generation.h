#pragma once

#include "../../../common/hw.h"
#include "../../primary_key_generation.h"
#include "../twitter_table_ids.h"
#include "twitter_record_types.h"

ALWAYS_INLINE uint64_t make_user_profile_key( const twitter_user_profile& t );
ALWAYS_INLINE uint64_t make_followers_key( const twitter_followers& t );
ALWAYS_INLINE uint64_t make_follows_key( const twitter_follows& t );
ALWAYS_INLINE uint64_t make_tweets_key( const twitter_tweets& t );

ALWAYS_INLINE uint32_t get_user_from_key( int32_t table_id, uint64_t key );
ALWAYS_INLINE uint32_t get_follows_id_from_key( int32_t  table_id,
                                                uint64_t key );
ALWAYS_INLINE uint32_t get_tweet_from_key( int32_t table_id, uint64_t key );

ALWAYS_INLINE uint64_t combine_keys( uint64_t high, uint64_t low );

#include "twitter_primary_key_generation-inl.h"

