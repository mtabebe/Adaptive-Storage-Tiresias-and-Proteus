#include "twitter_prep_stmts.h"

#include <glog/logging.h>
#include <glog/stl_logging.h>

#include "../sproc_helpers.h"

sproc_result twitter_create_database(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges> &write_ckrs,
    const std::vector<cell_key_ranges> &read_ckrs, std::vector<arg_code> &codes,
    std::vector<void *> &values, void *sproc_opaque ) {
    DCHECK( sproc_opaque );
    twitter_sproc_helper_holder *sproc_helper =
        (twitter_sproc_helper_holder *) sproc_opaque;

    twitter_configs configs;

    DCHECK_EQ( codes.size(), values.size() );
    DCHECK_GE( codes.size(), 1 );
    if( codes.size() == 1 or values.at( 1 ) == nullptr ) {
        configs = construct_twitter_configs();
    } else {
        configs = *(twitter_configs *) values.at( 1 );
    }
    db *database = (db *) values.at( 0 );

    DVLOG( 5 ) << "Creating database for twitter_benchmark from configs:"
               << configs;

    twitter_create_tables( database, configs );
    sproc_helper->init( database );

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    return res;
}

sproc_result twitter_insert_entire_user(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges> &write_ckrs,
    const std::vector<cell_key_ranges> &read_ckrs, std::vector<arg_code> &codes,
    std::vector<void *> &values, void *sproc_opaque ) {
    DCHECK( sproc_opaque );
    twitter_sproc_helper_holder *sproc_helper =
        (twitter_sproc_helper_holder *) sproc_opaque;

    check_args( k_twitter_insert_entire_user_arg_codes, codes, values );

    twitter_loader_templ_no_commit *loader =
        sproc_helper->get_loader_and_set_holder( id, partition_holder );

    uint32_t  u_id = *( (uint32_t *) values.at( 0 ) );
    uint32_t  num_tweets = *( (uint32_t *) values.at( 1 ) );
    uint32_t *following_ids = (uint32_t *) values.at( 2 );
    uint32_t *followers_ids = (uint32_t *) values.at( 3 );

    uint32_t num_following = codes.at( 2 ).array_length;
    uint32_t num_followers = codes.at( 3 ).array_length;

    for( const auto &ckr : write_ckrs ) {
        if( ckr.table_id == k_twitter_user_profiles_table_id ) {
            loader->insert_user( u_id, num_followers, num_following, num_tweets,
                                 ckr );
        } else if( ckr.table_id == k_twitter_tweets_table_id ) {
            loader->do_load_user_tweets( u_id, ckr );
        } else if( ckr.table_id == k_twitter_followers_table_id ) {
            loader->do_load_user_followers( u_id, followers_ids, num_followers,
                                            ckr );
        } else if( ckr.table_id == k_twitter_follows_table_id ) {
            loader->do_load_user_follows( u_id, following_ids, num_following,
                                          ckr );
        }
    }

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    return res;
}

sproc_result twitter_insert_user(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges> &write_ckrs,
    const std::vector<cell_key_ranges> &read_ckrs, std::vector<arg_code> &codes,
    std::vector<void *> &values, void *sproc_opaque ) {
    DCHECK( sproc_opaque );
    twitter_sproc_helper_holder *sproc_helper =
        (twitter_sproc_helper_holder *) sproc_opaque;

    check_args( k_twitter_insert_user_arg_codes, codes, values );

    twitter_loader_templ_no_commit *loader =
        sproc_helper->get_loader_and_set_holder( id, partition_holder );

    uint32_t u_id = *( (uint32_t *) values.at( 0 ) );
    uint32_t num_followers = *( (uint32_t *) values.at( 1 ) );
    uint32_t num_following = *( (uint32_t *) values.at( 2 ) );
    uint32_t num_tweets = *( (uint32_t *) values.at( 3 ) );

    for ( const auto& ckr : write_ckrs ) {
        loader->insert_user( u_id, num_followers, num_following, num_tweets,
                             ckr );
    }

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    return res;
}
sproc_result twitter_insert_user_follows(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges> &write_ckrs,
    const std::vector<cell_key_ranges> &read_ckrs,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque ) {
    DCHECK( sproc_opaque );
    twitter_sproc_helper_holder *sproc_helper =
        (twitter_sproc_helper_holder *) sproc_opaque;

    check_args( k_twitter_insert_user_follows_arg_codes, codes, values );

    twitter_loader_templ_no_commit *loader =
        sproc_helper->get_loader_and_set_holder( id, partition_holder );

    uint32_t  u_id = *( (uint32_t *) values.at( 0 ) );
    uint32_t *follows_uids = ( (uint32_t *) values.at( 1 ) );

    uint32_t num_follows = codes.at( 1 ).array_length;

    for ( const auto& ckr : write_ckrs ) {
        loader->do_load_user_follows( u_id, follows_uids, num_follows, ckr );
    }

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    return res;
}

sproc_result twitter_insert_user_followers(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges> &write_ckrs,
    const std::vector<cell_key_ranges> &read_ckrs,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque ) {
    DCHECK( sproc_opaque );
    twitter_sproc_helper_holder *sproc_helper =
        (twitter_sproc_helper_holder *) sproc_opaque;

    check_args( k_twitter_insert_user_followers_arg_codes, codes, values );

    twitter_loader_templ_no_commit *loader =
        sproc_helper->get_loader_and_set_holder( id, partition_holder );

    uint32_t  u_id = *( (uint32_t *) values.at( 0 ) );
    uint32_t *follower_uids = ( (uint32_t *) values.at( 1 ) );

    uint32_t num_followers = codes.at( 1 ).array_length;

    for ( const auto& ckr: write_ckrs ) {
        loader->do_load_user_followers( u_id, follower_uids, num_followers,
                                        ckr );
    }

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    return res;
}

sproc_result twitter_insert_user_tweets(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges> &write_ckrs,
    const std::vector<cell_key_ranges> &read_ckrs,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque ) {
    DCHECK( sproc_opaque );
    twitter_sproc_helper_holder *sproc_helper =
        (twitter_sproc_helper_holder *) sproc_opaque;

    check_args( k_twitter_insert_user_tweets_arg_codes, codes, values );

    twitter_loader_templ_no_commit *loader =
        sproc_helper->get_loader_and_set_holder( id, partition_holder );

    uint32_t u_id = *( (uint32_t *) values.at( 0 ) );

    for( const auto &ckr : write_ckrs ) {
        loader->do_load_user_tweets( u_id, ckr );
    }

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    return res;
}

void twitter_get_tweet( transaction_partition_holder *     partition_holder,
                        const clientid                     id,
                        const std::vector<scan_arguments> &scan_args,
                        std::vector<arg_code> &            codes,
                        std::vector<void *> &values, void *sproc_opaque,
                        scan_result &ret_val ) {
    DCHECK( sproc_opaque );
    twitter_sproc_helper_holder *sproc_helper =
        (twitter_sproc_helper_holder *) sproc_opaque;

    check_args( k_twitter_get_tweet_arg_codes, codes, values );

    twitter_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_worker_and_set_holder( id, partition_holder );

    worker->get_tweets_by_scan( map_scan_args( scan_args ), ret_val );

    ret_val.status = exec_status_type::COMMAND_OK;
}

void twitter_get_tweets_from_following(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<scan_arguments> &scan_args, std::vector<arg_code> &codes,
    std::vector<void *> &values, void *sproc_opaque, scan_result &ret_val ) {
    DCHECK( sproc_opaque );
    twitter_sproc_helper_holder *sproc_helper =
        (twitter_sproc_helper_holder *) sproc_opaque;

    check_args( k_twitter_get_tweets_from_following_arg_codes, codes, values );

    twitter_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_worker_and_set_holder( id, partition_holder );

    worker->get_tweets_from_following_by_scan( map_scan_args( scan_args ),
                                               ret_val );

    ret_val.status = exec_status_type::COMMAND_OK;
}

void twitter_get_tweets_from_followers(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<scan_arguments> &scan_args, std::vector<arg_code> &codes,
    std::vector<void *> &values, void *sproc_opaque, scan_result &ret_val ) {
    DCHECK( sproc_opaque );
    twitter_sproc_helper_holder *sproc_helper =
        (twitter_sproc_helper_holder *) sproc_opaque;

    check_args( k_twitter_get_tweets_from_followers_arg_codes, codes, values );

    twitter_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_worker_and_set_holder( id, partition_holder );

    worker->get_tweets_from_followers_by_scan( map_scan_args( scan_args ),
                                               ret_val );

    ret_val.status = exec_status_type::COMMAND_OK;
}

void twitter_get_followers( transaction_partition_holder *     partition_holder,
                            const clientid                     id,
                            const std::vector<scan_arguments> &scan_args,
                            std::vector<arg_code> &            codes,
                            std::vector<void *> &values, void *sproc_opaque,
                            scan_result &ret_val ) {
    DCHECK( sproc_opaque );
    twitter_sproc_helper_holder *sproc_helper =
        (twitter_sproc_helper_holder *) sproc_opaque;

    check_args( k_twitter_get_followers_arg_codes, codes, values );

    twitter_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_worker_and_set_holder( id, partition_holder );

    worker->get_followers_by_scan( map_scan_args( scan_args ), ret_val );

    ret_val.status = exec_status_type::COMMAND_OK;
}

sproc_result twitter_fetch_and_set_next_tweet_id(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges> &write_ckrs,
    const std::vector<cell_key_ranges> &read_ckrs, std::vector<arg_code> &codes,
    std::vector<void *> &values, void *sproc_opaque ) {
    DCHECK( sproc_opaque );
    twitter_sproc_helper_holder *sproc_helper =
        (twitter_sproc_helper_holder *) sproc_opaque;

    check_args( k_twitter_fetch_and_set_next_tweet_id_arg_codes, codes,
                values );

    twitter_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_worker_and_set_holder( id, partition_holder );

    std::vector<int32_t> tweet_ids;
    for ( const auto& ckr : write_ckrs ) {
        worker->fetch_and_set_next_tweet_id( ckr, tweet_ids );
    }

    sproc_result res;
    std::vector<void *>   ret_ptrs = {tweet_ids.data()};
    std::vector<arg_code> return_arg_codes = {INTEGER_ARRAY_CODE};
    return_arg_codes.at( 0 ).array_length = tweet_ids.size();

    serialize_sproc_result( res, ret_ptrs, return_arg_codes );

    res.status = exec_status_type::COMMAND_OK;

    return res;
}

sproc_result twitter_insert_tweet(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges> &write_ckrs,
    const std::vector<cell_key_ranges> &read_ckrs,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque ) {
    DCHECK( sproc_opaque );
    twitter_sproc_helper_holder *sproc_helper =
        (twitter_sproc_helper_holder *) sproc_opaque;

    check_args( k_twitter_insert_tweet_arg_codes, codes, values );

    twitter_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_worker_and_set_holder( id, partition_holder );

    for (const auto& ckr : write_ckrs) {
        worker->insert_tweet( ckr );
    }

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    return res;
}

sproc_result twitter_update_follower_and_follows(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges> &write_ckrs,
    const std::vector<cell_key_ranges> &read_ckrs, std::vector<arg_code> &codes,
    std::vector<void *> &values, void *sproc_opaque ) {
    DCHECK( sproc_opaque );
    twitter_sproc_helper_holder *sproc_helper =
        (twitter_sproc_helper_holder *) sproc_opaque;

    check_args( k_twitter_update_follower_and_follows_arg_codes, codes,
                values );

    twitter_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_worker_and_set_holder( id, partition_holder );

    int32_t  u_id = *( (int32_t *) values.at( 0 ) );
    int32_t  follower_u_id = *( (int32_t *) values.at( 1 ) );
    int32_t  follower_id = *( (int32_t *) values.at( 2 ) );
    int32_t  follows_id = *( (int32_t *) values.at( 3 ) );

    for( const auto &ckr : write_ckrs ) {
        if( ckr.table_id == k_twitter_followers_table_id ) {
            worker->do_update_follower( u_id, follower_u_id, follower_id, ckr );
        } else if( ckr.table_id == k_twitter_follows_table_id ) {
            worker->do_update_follows( u_id, follower_u_id, follows_id, ckr );
        }
    }

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    return res;

}

sproc_result twitter_get_follower_counts(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges> &write_ckrs,
    const std::vector<cell_key_ranges> &read_ckrs, std::vector<arg_code> &codes,
    std::vector<void *> &values, void *sproc_opaque ) {
    DCHECK( sproc_opaque );
    twitter_sproc_helper_holder *sproc_helper =
        (twitter_sproc_helper_holder *) sproc_opaque;

    check_args( k_twitter_get_follower_counts_arg_codes, codes, values );

    twitter_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_worker_and_set_holder( id, partition_holder );

    std::vector<int32_t> num_followers;
    std::vector<int32_t> num_follows;

    for( const auto &ckr : read_ckrs ) {
        if( ( ckr.col_id_start <=
              (int32_t) twitter_user_profile_cols::num_followers ) and
            ( ckr.col_id_end >=
              (int32_t) twitter_user_profile_cols::num_followers ) ) {
            worker->do_get_num_followers( ckr, num_followers );
        }
        if( ( ckr.col_id_start <=
              (int32_t) twitter_user_profile_cols::num_following ) and
            ( ckr.col_id_end >=
              (int32_t) twitter_user_profile_cols::num_following ) ) {
            worker->do_get_num_following( ckr, num_follows );
        }
    }

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    std::vector<void *>   ret_ptrs = { num_followers.data(),
                                     num_follows.data() };
    std::vector<arg_code> return_arg_codes = {INTEGER_ARRAY_CODE, INTEGER_ARRAY_CODE};
    return_arg_codes.at( 0 ).array_length = num_followers.size();
    return_arg_codes.at( 1 ).array_length = num_follows.size();

    serialize_sproc_result( res, ret_ptrs, return_arg_codes );


    return res;
}
