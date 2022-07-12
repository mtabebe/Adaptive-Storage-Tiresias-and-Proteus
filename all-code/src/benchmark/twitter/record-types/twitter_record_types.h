#pragma once

#include "../../../common/cell_data_type.h"

// Including all these things became hella annoying. So I made a file to include
// other files, so you can just include this file in all your other files

struct twitter_followers {
	public:

		uint32_t u_id;
		uint32_t f_id;
		uint32_t follower_u_id;
};
struct twitter_followers_cols {
	public:

		 static constexpr uint32_t u_id = 0;
		 static constexpr uint32_t f_id = 1;
		 static constexpr uint32_t follower_u_id = 2;
};

struct twitter_follows {
	public:

		uint32_t u_id;
		uint32_t f_id;
		uint32_t following_u_id;
};
struct twitter_follows_cols {
	public:

		 static constexpr uint32_t u_id = 0;
		 static constexpr uint32_t f_id = 1;
		 static constexpr uint32_t following_u_id = 2;
};

struct twitter_tweets {
	public:
     static const uint32_t MIN_TWEET_LEN = 1;
     static const uint32_t MAX_TWEET_LEN = 140;

     uint32_t    u_id;
     uint32_t    tweet_id;
     std::string text;
     uint64_t    createdate;
};
struct twitter_tweets_cols {
	public:

		 static constexpr uint32_t u_id = 0;
		 static constexpr uint32_t tweet_id = 1;
		 static constexpr uint32_t text = 2;
		 static constexpr uint32_t createdate = 3;
};

struct twitter_user_profile {
	public:
     static const uint32_t MIN_NAME_LEN = 3;
     static const uint32_t MAX_NAME_LEN = 20;

     uint32_t    u_id;
     std::string name;
     uint32_t    num_followers;
     uint32_t    num_following;
     uint32_t    num_tweets;
     uint32_t    next_tweet_id;
};
struct twitter_user_profile_cols {
	public:

		 static constexpr uint32_t u_id = 0;
		 static constexpr uint32_t name = 1;
		 static constexpr uint32_t num_followers = 2;
		 static constexpr uint32_t num_following = 3;
		 static constexpr uint32_t num_tweets = 4;
		 static constexpr uint32_t next_tweet_id = 5;
};

static constexpr uint32_t k_twitter_followers_num_columns = 3;
static const std::vector<cell_data_type> k_twitter_followers_col_types = {
    cell_data_type::UINT64 /* u_id */, cell_data_type::UINT64 /* f_id */,
    cell_data_type::UINT64 /* follower_u_id */,
};

static constexpr uint32_t                k_twitter_follows_num_columns = 3;
static const std::vector<cell_data_type> k_twitter_follows_col_types = {
    cell_data_type::UINT64 /* u_id */, cell_data_type::UINT64 /* f_id */,
    cell_data_type::UINT64 /* following_u_id */,
};

static constexpr uint32_t                k_twitter_tweets_num_columns = 4;
static const std::vector<cell_data_type> k_twitter_tweets_col_types = {
    cell_data_type::UINT64 /* u_id */, cell_data_type::UINT64 /* tweet_id */,
    cell_data_type::STRING /* text */, cell_data_type::UINT64 /* createdate */,
};

static constexpr uint32_t k_twitter_user_profile_num_columns = 6;
static const std::vector<cell_data_type> k_twitter_user_profile_col_types = {
    cell_data_type::UINT64 /* u_id */,
    cell_data_type::STRING /* name */,
    cell_data_type::UINT64 /* num_followers */,
    cell_data_type::UINT64 /* num_following */,
    cell_data_type::UINT64 /* num_tweets */,
    cell_data_type::UINT64 /* next_tweet_id */,
};
