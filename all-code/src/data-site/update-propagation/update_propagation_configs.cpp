#include "update_propagation_configs.h"

#include <glog/logging.h>

kafka_connection_configs construct_kafka_connection_configs(
    const std::string& broker ) {
    kafka_connection_configs conn_configs;

    conn_configs.broker_ = broker;
    DVLOG( 1 ) << "Created: " << conn_configs;

    return conn_configs;
}
kafka_topic_connection_configs construct_kafka_topic_connection_configs(
    const std::string& topic, int32_t partition, int64_t start_offset ) {
    kafka_topic_connection_configs conn_configs;

    conn_configs.topic_ = topic;
    conn_configs.partition_ = partition;
    conn_configs.start_offset_ = start_offset;

    DVLOG( 1 ) << "Created: " << conn_configs;

    return conn_configs;
}

kafka_common_configs construct_kafka_common_configs(
    uint64_t poll_count, uint64_t poll_sleep_ms, int32_t poll_timeout,
    const std::string& socket_blocking_max_ms ) {
    kafka_common_configs configs;

    configs.poll_count_ = poll_count;
    configs.poll_sleep_ms_ = poll_sleep_ms;
    configs.poll_timeout_ = poll_timeout;
    configs.socket_blocking_max_ms_ = socket_blocking_max_ms;

    DVLOG( 1 ) << "Created: " << configs;

    return configs;
}

kafka_consumer_configs construct_kafka_consumer_configs(
    const kafka_common_configs& common, int32_t consume_timeout_ms,
    int32_t                     seek_timeout_ms,
    /* rd kafka configs*/
    const std::string& fetch_max_ms, const std::string& consume_max_messages ) {
    kafka_consumer_configs configs;

    configs.common_ = common;

    configs.consume_timeout_ms_ = consume_timeout_ms;
    configs.seek_timeout_ms_ = seek_timeout_ms;
    configs.fetch_max_ms_ = fetch_max_ms;
    configs.consume_max_messages_ = consume_max_messages;

    DVLOG( 1 ) << "Created:" << configs;

    return configs;
}

kafka_producer_configs construct_kafka_producer_configs(
    const kafka_common_configs& common, int32_t flush_wait_ms,
    const std::string& produce_max_messages, const std::string& queue_max_ms,
    const std::string& required_acks, const std::string& send_max_retries,
    const std::string& max_buffered_messages,
    const std::string& max_buffered_messages_size ) {

    kafka_producer_configs configs;

    configs.common_ = common;

    configs.flush_wait_ms_ = flush_wait_ms;
    configs.produce_max_messages_ = produce_max_messages;
    configs.queue_max_ms_ = queue_max_ms;
    configs.required_acks_ = required_acks;
    configs.send_max_retries_ = send_max_retries;
    configs.max_buffered_messages_ = max_buffered_messages;
    configs.max_buffered_messages_size_ = max_buffered_messages_size;

    DVLOG( 1 ) << "Created: " << configs;

    return configs;
}

update_enqueuer_configs construct_update_enqueuer_configs(
    uint32_t sleep_time_between_update_enqueue_iterations,
    uint32_t num_updates_before_apply_self,
    uint32_t num_updates_to_apply_self ) {
    update_enqueuer_configs configs;

    configs.sleep_time_between_update_enqueue_iterations_ =
        sleep_time_between_update_enqueue_iterations;
    configs.num_updates_before_apply_self_ = num_updates_before_apply_self;
    configs.num_updates_to_apply_self_ = num_updates_to_apply_self;

    return configs;
}

update_destination_generator_configs
    construct_update_destination_generator_configs(
        uint32_t num_update_destinations ) {
    update_destination_generator_configs configs;
    configs.num_update_destinations_ = num_update_destinations;
    return configs;
}

std::ostream& operator<<( std::ostream&                   os,
                          const kafka_connection_configs& configs ) {
    os << "Kafka Connection Config: ["
       << " broker:" << configs.broker_ << "]";
    return os;
}
std::ostream& operator<<( std::ostream&                         os,
                          const kafka_topic_connection_configs& configs ) {
    os << "Kafka Topic Connection Config: ["
       << " topic:" << configs.topic_ << ", partition:" << configs.partition_
       << ", start_offset:" << configs.start_offset_ << "]";
    return os;
}

std::ostream& operator<<( std::ostream&               os,
                          const kafka_common_configs& configs ) {
    os << "Kafka Common Config: ["
       << " poll_count:" << configs.poll_count_
       << " poll_sleep_ms:" << configs.poll_sleep_ms_
       << ", poll_timeout:" << configs.poll_timeout_
       << ", socket_blocking_max_ms" << configs.socket_blocking_max_ms_ << "]";
    return os;
}

std::ostream& operator<<( std::ostream&                 os,
                          const kafka_consumer_configs& configs ) {
    os << "Kafka Consumer Config: [" << configs.common_
       << ", consume_timeout_ms:" << configs.consume_timeout_ms_
       << ", seek_timeout_ms:" << configs.seek_timeout_ms_
       << ", fetch_max_ms:" << configs.fetch_max_ms_
       << ", consume_max_messages:" << configs.consume_max_messages_ << "]";
    return os;
}
std::ostream& operator<<( std::ostream&                 os,
                          const kafka_producer_configs& configs ) {
    os << "Kafka Producer Config: [ " << configs.common_
       << ", flush_wait_ms:" << configs.flush_wait_ms_
       << ", produce_max_messages:" << configs.produce_max_messages_
       << ", queue_max_ms:" << configs.queue_max_ms_
       << ", required_acks:" << configs.required_acks_
       << ", max buffered messages:" << configs.max_buffered_messages_
       << ", max buffered messages size:" << configs.max_buffered_messages_size_
       << "]";
    return os;
}
std::ostream& operator<<( std::ostream&                  os,
                          const update_enqueuer_configs& configs ) {
    os << "Update Enqueuer Config: [ "
          "sleep_time_between_update_enqueue_iterations:"
       << configs.sleep_time_between_update_enqueue_iterations_
       << ", num_updates_before_apply_self:"
       << configs.num_updates_before_apply_self_
       << ", num_updates_before_apply_self:"
       << configs.num_updates_to_apply_self_ << "]";
    return os;
}
std::ostream& operator<<(
    std::ostream& os, const update_destination_generator_configs& configs ) {
    os << "Update Destination Generator Configs: [ "
          "num_update_destinations:"
       << configs.num_update_destinations_ << "]";
    return os;

}

bool operator==( const kafka_topic_connection_configs& a,
                 const kafka_topic_connection_configs& b ) {
    return ( ( a.topic_.compare( b.topic_ ) == 0 ) and
             ( a.partition_ == b.partition_ ) and
             ( a.start_offset_ == b.start_offset_ ) );
}
