#include "twitter_db_operators.h"

#include <glog/logging.h>

#include "../../common/string_conversion.h"
#include "../db_operators_macros.h"

void twitter_create_tables( db_abstraction* db, const twitter_configs& configs ) {
    DVLOG( 10 ) << "Creating twitter tables";
    auto table_infos =
        create_twitter_table_metadata( configs, db->get_site_location() );
    for( auto m : table_infos ) {
        db->create_table( m );
    }
    DVLOG( 10 ) << "Creating twitter tables okay!";
}

void twitter_create_tables( db* database, const twitter_configs& configs ) {
    DVLOG( 10 ) << "Creating twitter tables";

    auto table_infos =
        create_twitter_table_metadata( configs, database->get_site_location() );
    for( auto m : table_infos ) {
        database->get_tables()->create_table( m );
    }
    DVLOG( 10 ) << "Creating twitter tables okay!";
}

void insert_twitter_followers( benchmark_db_operators*  db_ops,
                               const twitter_followers* var, uint64_t row_id,
                               const cell_key_ranges& ckr ) {
    DCHECK( db_ops );
	DCHECK( var );
	auto cid = create_cell_identifier( ckr.table_id, ckr.col_id_start, row_id );

	for( int32_t col = ckr.col_id_start; col <= ckr.col_id_end; col++) {
		cid.col_id_ = col;
		switch( col ) {
			case twitter_followers_cols::u_id: {
				DO_DB_OP( db_ops, insert_uint64, uint64_t, var, u_id, cid );
				break;
			}
			case twitter_followers_cols::f_id: {
				DO_DB_OP( db_ops, insert_uint64, uint64_t, var, f_id, cid );
				break;
			}
			case twitter_followers_cols::follower_u_id: {
				DO_DB_OP( db_ops, insert_uint64, uint64_t, var, follower_u_id, cid );
				break;
			}
		}
	}
}

void update_twitter_followers(
	benchmark_db_operators* db_ops, const twitter_followers* var,
	uint64_t row_id, const cell_key_ranges& ckr, bool do_propagate ) {
	DCHECK( db_ops );
	DCHECK( var );
	auto cid = create_cell_identifier( ckr.table_id, ckr.col_id_start, row_id );

	for( int32_t col = ckr.col_id_start; col <= ckr.col_id_end; col++) {
		cid.col_id_ = col;
		switch( col ) {
			case twitter_followers_cols::u_id: {
				DO_DB_OP( db_ops, write_uint64, uint64_t, var, u_id, cid );
				break;
			}
			case twitter_followers_cols::f_id: {
				DO_DB_OP( db_ops, write_uint64, uint64_t, var, f_id, cid );
				break;
			}
			case twitter_followers_cols::follower_u_id: {
				DO_DB_OP( db_ops, write_uint64, uint64_t, var, follower_u_id, cid );
				break;
			}
		}
	}
}

bool lookup_twitter_followers(
	benchmark_db_operators* db_ops, twitter_followers* var,
	uint64_t row_id, const cell_key_ranges& ckr,
	bool is_latest, bool is_nullable) {
	DCHECK( db_ops );
	DCHECK( var );
	auto cid = create_cell_identifier( ckr.table_id, ckr.col_id_start, row_id );
	bool ret = true;

	for( int32_t col = ckr.col_id_start; col <= ckr.col_id_end; col++) {
		bool loc_ret = true;
		cid.col_id_ = col;
		switch( col ) {
			case twitter_followers_cols::u_id: {
				DO_UINT64_READ_OP( loc_ret, db_ops, uint32_t, var, u_id, cid, is_latest, is_nullable );
				break;
			}
			case twitter_followers_cols::f_id: {
				DO_UINT64_READ_OP( loc_ret, db_ops, uint32_t, var, f_id, cid, is_latest, is_nullable );
				break;
			}
			case twitter_followers_cols::follower_u_id: {
				DO_UINT64_READ_OP( loc_ret, db_ops, uint32_t, var, follower_u_id, cid, is_latest, is_nullable );
				break;
			}
		}
		ret = ret and loc_ret;
	}
	return ret;
}

std::unordered_set<uint32_t> read_from_scan_twitter_followers(
	const result_tuple& res, twitter_followers* var) {
	std::unordered_set<uint32_t> ret;

	for( const auto& cell : res.cells ) {
		bool loc_ret = false;
		switch( cell.col_id ) {
			case twitter_followers_cols::u_id: {
				DO_SCAN_OP( loc_ret, string_to_uint64, uint32_t, cell, var, u_id );
			if ( loc_ret ) {
				 ret.emplace( twitter_followers_cols::u_id );
			}
				break;
			}
			case twitter_followers_cols::f_id: {
				DO_SCAN_OP( loc_ret, string_to_uint64, uint32_t, cell, var, f_id );
			if ( loc_ret ) {
				 ret.emplace( twitter_followers_cols::f_id );
			}
				break;
			}
			case twitter_followers_cols::follower_u_id: {
				DO_SCAN_OP( loc_ret, string_to_uint64, uint32_t, cell, var, follower_u_id );
			if ( loc_ret ) {
				 ret.emplace( twitter_followers_cols::follower_u_id );
			}
				break;
			}
		}
	}
	return ret;
}

void insert_twitter_follows(
	benchmark_db_operators* db_ops, const twitter_follows* var,
	uint64_t row_id, const cell_key_ranges& ckr ) {
	DCHECK( db_ops );
	DCHECK( var );
	auto cid = create_cell_identifier( ckr.table_id, ckr.col_id_start, row_id );

	for( int32_t col = ckr.col_id_start; col <= ckr.col_id_end; col++) {
		cid.col_id_ = col;
		switch( col ) {
			case twitter_follows_cols::u_id: {
				DO_DB_OP( db_ops, insert_uint64, uint64_t, var, u_id, cid );
				break;
			}
			case twitter_follows_cols::f_id: {
				DO_DB_OP( db_ops, insert_uint64, uint64_t, var, f_id, cid );
				break;
			}
			case twitter_follows_cols::following_u_id: {
				DO_DB_OP( db_ops, insert_uint64, uint64_t, var, following_u_id, cid );
				break;
			}
		}
	}
}

void update_twitter_follows(
	benchmark_db_operators* db_ops, const twitter_follows* var,
	uint64_t row_id, const cell_key_ranges& ckr, bool do_propagate ) {
	DCHECK( db_ops );
	DCHECK( var );
	auto cid = create_cell_identifier( ckr.table_id, ckr.col_id_start, row_id );

	for( int32_t col = ckr.col_id_start; col <= ckr.col_id_end; col++) {
		cid.col_id_ = col;
		switch( col ) {
			case twitter_follows_cols::u_id: {
				DO_DB_OP( db_ops, write_uint64, uint64_t, var, u_id, cid );
				break;
			}
			case twitter_follows_cols::f_id: {
				DO_DB_OP( db_ops, write_uint64, uint64_t, var, f_id, cid );
				break;
			}
			case twitter_follows_cols::following_u_id: {
				DO_DB_OP( db_ops, write_uint64, uint64_t, var, following_u_id, cid );
				break;
			}
		}
	}
}

bool lookup_twitter_follows(
	benchmark_db_operators* db_ops, twitter_follows* var,
	uint64_t row_id, const cell_key_ranges& ckr,
	bool is_latest, bool is_nullable) {
	DCHECK( db_ops );
	DCHECK( var );
	auto cid = create_cell_identifier( ckr.table_id, ckr.col_id_start, row_id );
	bool ret = true;

	for( int32_t col = ckr.col_id_start; col <= ckr.col_id_end; col++) {
		bool loc_ret = true;
		cid.col_id_ = col;
		switch( col ) {
			case twitter_follows_cols::u_id: {
				DO_UINT64_READ_OP( loc_ret, db_ops, uint32_t, var, u_id, cid, is_latest, is_nullable );
				break;
			}
			case twitter_follows_cols::f_id: {
				DO_UINT64_READ_OP( loc_ret, db_ops, uint32_t, var, f_id, cid, is_latest, is_nullable );
				break;
			}
			case twitter_follows_cols::following_u_id: {
				DO_UINT64_READ_OP( loc_ret, db_ops, uint32_t, var, following_u_id, cid, is_latest, is_nullable );
				break;
			}
		}
		ret = ret and loc_ret;
	}
	return ret;
}

std::unordered_set<uint32_t> read_from_scan_twitter_follows(
	const result_tuple& res, twitter_follows* var) {
	std::unordered_set<uint32_t> ret;

	for( const auto& cell : res.cells ) {
		bool loc_ret = false;
		switch( cell.col_id ) {
			case twitter_follows_cols::u_id: {
				DO_SCAN_OP( loc_ret, string_to_uint64, uint32_t, cell, var, u_id );
			if ( loc_ret ) {
				 ret.emplace( twitter_follows_cols::u_id );
			}
				break;
			}
			case twitter_follows_cols::f_id: {
				DO_SCAN_OP( loc_ret, string_to_uint64, uint32_t, cell, var, f_id );
			if ( loc_ret ) {
				 ret.emplace( twitter_follows_cols::f_id );
			}
				break;
			}
			case twitter_follows_cols::following_u_id: {
				DO_SCAN_OP( loc_ret, string_to_uint64, uint32_t, cell, var, following_u_id );
			if ( loc_ret ) {
				 ret.emplace( twitter_follows_cols::following_u_id );
			}
				break;
			}
		}
	}
	return ret;
}

void insert_twitter_tweets(
	benchmark_db_operators* db_ops, const twitter_tweets* var,
	uint64_t row_id, const cell_key_ranges& ckr ) {
	DCHECK( db_ops );
	DCHECK( var );
	auto cid = create_cell_identifier( ckr.table_id, ckr.col_id_start, row_id );

	for( int32_t col = ckr.col_id_start; col <= ckr.col_id_end; col++) {
		cid.col_id_ = col;
		switch( col ) {
			case twitter_tweets_cols::u_id: {
				DO_DB_OP( db_ops, insert_uint64, uint64_t, var, u_id, cid );
				break;
			}
			case twitter_tweets_cols::tweet_id: {
				DO_DB_OP( db_ops, insert_uint64, uint64_t, var, tweet_id, cid );
				break;
			}
			case twitter_tweets_cols::text: {
				DO_DB_OP( db_ops, insert_string, std::string, var, text, cid );
				break;
			}
			case twitter_tweets_cols::createdate: {
				DO_DB_OP( db_ops, insert_uint64, uint64_t, var, createdate, cid );
				break;
			}
		}
	}
}

void update_twitter_tweets(
	benchmark_db_operators* db_ops, const twitter_tweets* var,
	uint64_t row_id, const cell_key_ranges& ckr, bool do_propagate ) {
	DCHECK( db_ops );
	DCHECK( var );
	auto cid = create_cell_identifier( ckr.table_id, ckr.col_id_start, row_id );

	for( int32_t col = ckr.col_id_start; col <= ckr.col_id_end; col++) {
		cid.col_id_ = col;
		switch( col ) {
			case twitter_tweets_cols::u_id: {
				DO_DB_OP( db_ops, write_uint64, uint64_t, var, u_id, cid );
				break;
			}
			case twitter_tweets_cols::tweet_id: {
				DO_DB_OP( db_ops, write_uint64, uint64_t, var, tweet_id, cid );
				break;
			}
			case twitter_tweets_cols::text: {
				DO_DB_OP( db_ops, write_string, std::string, var, text, cid );
				break;
			}
			case twitter_tweets_cols::createdate: {
				DO_DB_OP( db_ops, write_uint64, uint64_t, var, createdate, cid );
				break;
			}
		}
	}
}

bool lookup_twitter_tweets(
	benchmark_db_operators* db_ops, twitter_tweets* var,
	uint64_t row_id, const cell_key_ranges& ckr,
	bool is_latest, bool is_nullable) {
	DCHECK( db_ops );
	DCHECK( var );
	auto cid = create_cell_identifier( ckr.table_id, ckr.col_id_start, row_id );
	bool ret = true;

	for( int32_t col = ckr.col_id_start; col <= ckr.col_id_end; col++) {
		bool loc_ret = true;
		cid.col_id_ = col;
		switch( col ) {
			case twitter_tweets_cols::u_id: {
				DO_UINT64_READ_OP( loc_ret, db_ops, uint32_t, var, u_id, cid, is_latest, is_nullable );
				break;
			}
			case twitter_tweets_cols::tweet_id: {
				DO_UINT64_READ_OP( loc_ret, db_ops, uint32_t, var, tweet_id, cid, is_latest, is_nullable );
				break;
			}
			case twitter_tweets_cols::text: {
				DO_STRING_READ_OP( loc_ret, db_ops, std::string, var, text, cid, is_latest, is_nullable );
				break;
			}
			case twitter_tweets_cols::createdate: {
				DO_UINT64_READ_OP( loc_ret, db_ops, uint64_t, var, createdate, cid, is_latest, is_nullable );
				break;
			}
		}
		ret = ret and loc_ret;
	}
	return ret;
}

std::unordered_set<uint32_t> read_from_scan_twitter_tweets(
	const result_tuple& res, twitter_tweets* var) {
	std::unordered_set<uint32_t> ret;

	for( const auto& cell : res.cells ) {
		bool loc_ret = false;
		switch( cell.col_id ) {
			case twitter_tweets_cols::u_id: {
				DO_SCAN_OP( loc_ret, string_to_uint64, uint32_t, cell, var, u_id );
			if ( loc_ret ) {
				 ret.emplace( twitter_tweets_cols::u_id );
			}
				break;
			}
			case twitter_tweets_cols::tweet_id: {
				DO_SCAN_OP( loc_ret, string_to_uint64, uint32_t, cell, var, tweet_id );
			if ( loc_ret ) {
				 ret.emplace( twitter_tweets_cols::tweet_id );
			}
				break;
			}
			case twitter_tweets_cols::text: {
				DO_SCAN_OP( loc_ret, string_to_string, std::string, cell, var, text );
			if ( loc_ret ) {
				 ret.emplace( twitter_tweets_cols::text );
			}
				break;
			}
			case twitter_tweets_cols::createdate: {
				DO_SCAN_OP( loc_ret, string_to_uint64, uint64_t, cell, var, createdate );
			if ( loc_ret ) {
				 ret.emplace( twitter_tweets_cols::createdate );
			}
				break;
			}
		}
	}
	return ret;
}

void insert_twitter_user_profile(
	benchmark_db_operators* db_ops, const twitter_user_profile* var,
	uint64_t row_id, const cell_key_ranges& ckr ) {
	DCHECK( db_ops );
	DCHECK( var );
	auto cid = create_cell_identifier( ckr.table_id, ckr.col_id_start, row_id );

	for( int32_t col = ckr.col_id_start; col <= ckr.col_id_end; col++) {
		cid.col_id_ = col;
		switch( col ) {
			case twitter_user_profile_cols::u_id: {
				DO_DB_OP( db_ops, insert_uint64, uint64_t, var, u_id, cid );
				break;
			}
			case twitter_user_profile_cols::name: {
				DO_DB_OP( db_ops, insert_string, std::string, var, name, cid );
				break;
			}
			case twitter_user_profile_cols::num_followers: {
				DO_DB_OP( db_ops, insert_uint64, uint64_t, var, num_followers, cid );
				break;
			}
			case twitter_user_profile_cols::num_following: {
				DO_DB_OP( db_ops, insert_uint64, uint64_t, var, num_following, cid );
				break;
			}
			case twitter_user_profile_cols::num_tweets: {
				DO_DB_OP( db_ops, insert_uint64, uint64_t, var, num_tweets, cid );
				break;
			}
			case twitter_user_profile_cols::next_tweet_id: {
				DO_DB_OP( db_ops, insert_uint64, uint64_t, var, next_tweet_id, cid );
				break;
			}
		}
	}
}

void update_twitter_user_profile(
	benchmark_db_operators* db_ops, const twitter_user_profile* var,
	uint64_t row_id, const cell_key_ranges& ckr, bool do_propagate ) {
	DCHECK( db_ops );
	DCHECK( var );
	auto cid = create_cell_identifier( ckr.table_id, ckr.col_id_start, row_id );

	for( int32_t col = ckr.col_id_start; col <= ckr.col_id_end; col++) {
		cid.col_id_ = col;
		switch( col ) {
			case twitter_user_profile_cols::u_id: {
				DO_DB_OP( db_ops, write_uint64, uint64_t, var, u_id, cid );
				break;
			}
			case twitter_user_profile_cols::name: {
				DO_DB_OP( db_ops, write_string, std::string, var, name, cid );
				break;
			}
			case twitter_user_profile_cols::num_followers: {
				DO_DB_OP( db_ops, write_uint64, uint64_t, var, num_followers, cid );
				break;
			}
			case twitter_user_profile_cols::num_following: {
				DO_DB_OP( db_ops, write_uint64, uint64_t, var, num_following, cid );
				break;
			}
			case twitter_user_profile_cols::num_tweets: {
				DO_DB_OP( db_ops, write_uint64, uint64_t, var, num_tweets, cid );
				break;
			}
			case twitter_user_profile_cols::next_tweet_id: {
				DO_DB_OP( db_ops, write_uint64, uint64_t, var, next_tweet_id, cid );
				break;
			}
		}
	}
}

bool lookup_twitter_user_profile(
	benchmark_db_operators* db_ops, twitter_user_profile* var,
	uint64_t row_id, const cell_key_ranges& ckr,
	bool is_latest, bool is_nullable) {
	DCHECK( db_ops );
	DCHECK( var );
	auto cid = create_cell_identifier( ckr.table_id, ckr.col_id_start, row_id );
	bool ret = true;

	for( int32_t col = ckr.col_id_start; col <= ckr.col_id_end; col++) {
		bool loc_ret = true;
		cid.col_id_ = col;
		switch( col ) {
			case twitter_user_profile_cols::u_id: {
				DO_UINT64_READ_OP( loc_ret, db_ops, uint32_t, var, u_id, cid, is_latest, is_nullable );
				break;
			}
			case twitter_user_profile_cols::name: {
				DO_STRING_READ_OP( loc_ret, db_ops, std::string, var, name, cid, is_latest, is_nullable );
				break;
			}
			case twitter_user_profile_cols::num_followers: {
				DO_UINT64_READ_OP( loc_ret, db_ops, uint32_t, var, num_followers, cid, is_latest, is_nullable );
				break;
			}
			case twitter_user_profile_cols::num_following: {
				DO_UINT64_READ_OP( loc_ret, db_ops, uint32_t, var, num_following, cid, is_latest, is_nullable );
				break;
			}
			case twitter_user_profile_cols::num_tweets: {
				DO_UINT64_READ_OP( loc_ret, db_ops, uint32_t, var, num_tweets, cid, is_latest, is_nullable );
				break;
			}
			case twitter_user_profile_cols::next_tweet_id: {
				DO_UINT64_READ_OP( loc_ret, db_ops, uint32_t, var, next_tweet_id, cid, is_latest, is_nullable );
				break;
			}
		}
		ret = ret and loc_ret;
	}
	return ret;
}

std::unordered_set<uint32_t> read_from_scan_twitter_user_profile(
	const result_tuple& res, twitter_user_profile* var) {
	std::unordered_set<uint32_t> ret;

	for( const auto& cell : res.cells ) {
		bool loc_ret = false;
		switch( cell.col_id ) {
			case twitter_user_profile_cols::u_id: {
				DO_SCAN_OP( loc_ret, string_to_uint64, uint32_t, cell, var, u_id );
			if ( loc_ret ) {
				 ret.emplace( twitter_user_profile_cols::u_id );
			}
				break;
			}
			case twitter_user_profile_cols::name: {
				DO_SCAN_OP( loc_ret, string_to_string, std::string, cell, var, name );
			if ( loc_ret ) {
				 ret.emplace( twitter_user_profile_cols::name );
			}
				break;
			}
			case twitter_user_profile_cols::num_followers: {
				DO_SCAN_OP( loc_ret, string_to_uint64, uint32_t, cell, var, num_followers );
			if ( loc_ret ) {
				 ret.emplace( twitter_user_profile_cols::num_followers );
			}
				break;
			}
			case twitter_user_profile_cols::num_following: {
				DO_SCAN_OP( loc_ret, string_to_uint64, uint32_t, cell, var, num_following );
			if ( loc_ret ) {
				 ret.emplace( twitter_user_profile_cols::num_following );
			}
				break;
			}
			case twitter_user_profile_cols::num_tweets: {
				DO_SCAN_OP( loc_ret, string_to_uint64, uint32_t, cell, var, num_tweets );
			if ( loc_ret ) {
				 ret.emplace( twitter_user_profile_cols::num_tweets );
			}
				break;
			}
			case twitter_user_profile_cols::next_tweet_id: {
				DO_SCAN_OP( loc_ret, string_to_uint64, uint32_t, cell, var, next_tweet_id );
			if ( loc_ret ) {
				 ret.emplace( twitter_user_profile_cols::next_tweet_id );
			}
				break;
			}
		}
	}
	return ret;
}

