#pragma once

#include <unordered_set>

#include "../../common/hw.h"
#include "../../data-site/db/db.h"
#include "../../data-site/single-site-db/db_abstraction.h"
#include "../../data-site/single-site-db/db_abstraction_types.h"
#include "../benchmark_db_operators.h"
#include "record-types/twitter_primary_key_generation.h"
#include "record-types/twitter_record_types.h"
#include "twitter_table_ids.h"
#include "twitter_table_sizes.h"
#include "twitter_workload_generator.h"

void twitter_create_tables( db_abstraction* db, const twitter_configs& configs );
void twitter_create_tables( db* database, const twitter_configs& configs );

void insert_twitter_followers(
	benchmark_db_operators* db_ops, const twitter_followers* var,
	uint64_t row_id, const cell_key_ranges& ckr );

void update_twitter_followers(
	benchmark_db_operators* db_ops, const twitter_followers* var,
	uint64_t row_id, const cell_key_ranges& ckr, bool do_propagate );

bool lookup_twitter_followers(
	benchmark_db_operators* db_ops, twitter_followers* var,
	uint64_t row_id, const cell_key_ranges& ckr,
	bool is_lates, bool is_nullable );
std::unordered_set<uint32_t> read_from_scan_twitter_followers(
	const result_tuple& res, twitter_followers* var);

void insert_twitter_follows(
	benchmark_db_operators* db_ops, const twitter_follows* var,
	uint64_t row_id, const cell_key_ranges& ckr );

void update_twitter_follows(
	benchmark_db_operators* db_ops, const twitter_follows* var,
	uint64_t row_id, const cell_key_ranges& ckr, bool do_propagate );

bool lookup_twitter_follows(
	benchmark_db_operators* db_ops, twitter_follows* var,
	uint64_t row_id, const cell_key_ranges& ckr,
	bool is_lates, bool is_nullable );
std::unordered_set<uint32_t> read_from_scan_twitter_follows(
	const result_tuple& res, twitter_follows* var);

void insert_twitter_tweets(
	benchmark_db_operators* db_ops, const twitter_tweets* var,
	uint64_t row_id, const cell_key_ranges& ckr );

void update_twitter_tweets(
	benchmark_db_operators* db_ops, const twitter_tweets* var,
	uint64_t row_id, const cell_key_ranges& ckr, bool do_propagate );

bool lookup_twitter_tweets(
	benchmark_db_operators* db_ops, twitter_tweets* var,
	uint64_t row_id, const cell_key_ranges& ckr,
	bool is_lates, bool is_nullable );
std::unordered_set<uint32_t> read_from_scan_twitter_tweets(
	const result_tuple& res, twitter_tweets* var);

void insert_twitter_user_profile(
	benchmark_db_operators* db_ops, const twitter_user_profile* var,
	uint64_t row_id, const cell_key_ranges& ckr );

void update_twitter_user_profile(
	benchmark_db_operators* db_ops, const twitter_user_profile* var,
	uint64_t row_id, const cell_key_ranges& ckr, bool do_propagate );

bool lookup_twitter_user_profile(
	benchmark_db_operators* db_ops, twitter_user_profile* var,
	uint64_t row_id, const cell_key_ranges& ckr,
	bool is_lates, bool is_nullable );
std::unordered_set<uint32_t> read_from_scan_twitter_user_profile(
	const result_tuple& res, twitter_user_profile* var);

