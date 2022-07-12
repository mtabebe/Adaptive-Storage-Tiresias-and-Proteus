#pragma once

#include <folly/Hash.h>
#include <iostream>

#include "../../common/constants.h"
#include "../../common/hw.h"

class kafka_connection_configs {
   public:
    // connection information
    std::string broker_;
};
class kafka_topic_connection_configs {
   public:
    std::string topic_;
    int32_t     partition_;
    int64_t     start_offset_;

};

class kafka_common_configs {
   public:
    // our configs
    uint64_t poll_count_;
    uint64_t poll_sleep_ms_;
    int32_t  poll_timeout_;

    // rdkafka configs see kafka_helpers for use
    std::string socket_blocking_max_ms_;
};

class kafka_consumer_configs {
   public:
    kafka_common_configs common_;


    int32_t consume_timeout_ms_;
    int32_t seek_timeout_ms_;

    std::string fetch_max_ms_;
    std::string consume_max_messages_;
};

class kafka_producer_configs {
   public:
    kafka_common_configs common_;

    int32_t flush_wait_ms_;

    std::string produce_max_messages_;
    std::string queue_max_ms_;
    std::string required_acks_;
    std::string send_max_retries_;
    std::string max_buffered_messages_;
    std::string max_buffered_messages_size_;
};

class update_enqueuer_configs {
  public:
   uint32_t sleep_time_between_update_enqueue_iterations_;
   uint32_t num_updates_before_apply_self_;
   uint32_t num_updates_to_apply_self_;
};

class update_destination_generator_configs {
   public:
    uint32_t num_update_destinations_;
};

update_destination_generator_configs
    construct_update_destination_generator_configs(
        uint32_t num_update_destinations = k_num_update_destinations );

update_enqueuer_configs construct_update_enqueuer_configs(
    uint32_t sleep_time_between_update_enqueue_iterations =
        k_sleep_time_between_update_enqueue_iterations,
    uint32_t num_updates_before_apply_self = k_num_updates_before_apply_self,
    uint32_t num_updates_to_apply_self = k_num_updates_to_apply_self );

kafka_connection_configs construct_kafka_connection_configs(
    const std::string& broker );
kafka_topic_connection_configs construct_kafka_topic_connection_configs(
    const std::string& topic, int32_t partition, int64_t start_offset = 0 );

kafka_common_configs construct_kafka_common_configs(
    uint64_t           poll_count = k_kafka_poll_count,
    uint64_t           poll_sleep_ms = k_kafka_poll_sleep_ms,
    int32_t            poll_timeout = k_kafka_poll_timeout,
    const std::string& socket_blocking_max_ms =
        k_kafka_socket_blocking_max_ms );

kafka_consumer_configs construct_kafka_consumer_configs(
    const kafka_common_configs& common,
    int32_t                     consume_timeout_ms = k_kafka_consume_timeout_ms,
    int32_t                     seek_timeout_ms = k_kafka_seek_timeout_ms,
    /* rd kafka configs*/
    const std::string& fetch_max_ms = k_kafka_fetch_max_ms,
    const std::string& consume_max_messages = k_kafka_consume_max_messages );

kafka_producer_configs construct_kafka_producer_configs(
    const kafka_common_configs& common,
    int32_t                     flush_wait_ms = k_kafka_flush_wait_ms,
    const std::string& produce_max_messages = k_kafka_produce_max_messages,
    const std::string& queue_max_ms = k_kafka_queue_max_ms,
    const std::string& required_acks = k_kafka_required_acks,
    const std::string& send_max_retries = k_kafka_send_max_retries,
    const std::string& max_buffered_messages = k_kafka_max_buffered_messages,
    const std::string& max_buffered_messages_size =
        k_kafka_max_buffered_messages_size );

std::ostream& operator<<( std::ostream&                   os,
                          const kafka_connection_configs& conn_configs );
std::ostream& operator<<( std::ostream&                         os,
                          const kafka_topic_connection_configs& topic_configs );
std::ostream& operator<<( std::ostream&               os,
                          const kafka_common_configs& configs );
std::ostream& operator<<( std::ostream&                 os,
                          const kafka_consumer_configs& configs );
std::ostream& operator<<( std::ostream&                 os,
                          const kafka_producer_configs& configs );

std::ostream& operator<<( std::ostream&                  os,
                          const update_enqueuer_configs& configs );
std::ostream& operator<<( std::ostream&                               os,
                          const update_destination_generator_configs& configs );

bool operator==( const kafka_topic_connection_configs& a,
                 const kafka_topic_connection_configs& b );

struct propagation_configuration_equal_functor {
    bool operator()( const ::propagation_configuration& pi1,
                     const ::propagation_configuration& pi2 ) const {
        if( pi1.type != pi2.type ) {
            return false;
        }
        switch( pi1.type ) {
            case propagation_type::NO_OP:
                return true;
            case propagation_type::VECTOR:
                return ( ( pi1.partition == pi2.partition ) and
                         ( pi1.offset == pi2.offset ) );
            case propagation_type::KAFKA:
                return ( ( pi1.partition == pi2.partition ) and
                         ( pi1.offset == pi2.offset ) and
                         ( pi1.topic == pi2.topic ) );
        }
        return false;
    }
};

struct propagation_configuration_hasher {
    std::size_t operator()( const ::propagation_configuration& config ) const {
        std::size_t seed = 0;
        switch( config.type ) {
            case propagation_type::NO_OP:
              break;
            case propagation_type::VECTOR:
                seed = folly::hash::hash_combine( seed, config.partition );
                seed = folly::hash::hash_combine( seed, config.offset );
                break;
            case propagation_type::KAFKA:
                seed = folly::hash::hash_combine( seed, config.topic );
                seed = folly::hash::hash_combine( seed, config.partition );
                seed = folly::hash::hash_combine( seed, config.offset );
                break;
        }
        return seed;
    }
};

struct propagation_configuration_ignore_offset_equal_functor {
    bool operator()( const ::propagation_configuration& pi1,
                     const ::propagation_configuration& pi2 ) const {
        if( pi1.type != pi2.type ) {
            return false;
        }
        switch( pi1.type ) {
            case propagation_type::NO_OP:
                return true;
            case propagation_type::VECTOR:
                return ( pi1.partition == pi2.partition );
            case propagation_type::KAFKA:
                return ( ( pi1.partition == pi2.partition ) and
                         ( pi1.topic == pi2.topic ) );
        }
        return false;
    }
};

struct propagation_configuration_ignore_offset_hasher {
    std::size_t operator()( const ::propagation_configuration& config ) const {
        std::size_t seed = 0;
        switch( config.type ) {
            case propagation_type::NO_OP:
              break;
            case propagation_type::VECTOR:
                seed = folly::hash::hash_combine( seed, config.partition );
                break;
            case propagation_type::KAFKA:
                seed = folly::hash::hash_combine( seed, config.topic );
                seed = folly::hash::hash_combine( seed, config.partition );
                break;
        }
        return seed;
    }
};


