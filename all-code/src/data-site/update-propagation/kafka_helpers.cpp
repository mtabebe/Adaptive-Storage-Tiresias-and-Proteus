#include "kafka_helpers.h"

#include <glog/logging.h>

// https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md

rd_kafka_conf_t* create_kafka_conf() {
    rd_kafka_conf_t* conf = rd_kafka_conf_new();
    DCHECK( conf );

    return conf;
}

rd_kafka_topic_conf_t* create_kafka_topic_conf() {
    rd_kafka_topic_conf_t* topic_conf = rd_kafka_topic_conf_new();
    DCHECK( topic_conf );

    return topic_conf;
}

void add_both_configs( rd_kafka_conf_t* conf, rd_kafka_topic_conf_t* topic_conf,
                       const kafka_connection_configs& conn_config,
                       const kafka_common_configs&     common_config ) {
    // servers
    add_kafka_config( conf, "bootstrap.servers", conn_config.broker_ );
    add_kafka_config( conf, "socket.blocking.max.ms",
                      common_config.socket_blocking_max_ms_ );
}

void add_producer_configs( rd_kafka_conf_t*                conf,
                           rd_kafka_topic_conf_t*          topic_conf,
                           const kafka_connection_configs& conn_config,
                           const kafka_producer_configs&   config ) {
    add_both_configs( conf, topic_conf, conn_config, config.common_ );

    add_kafka_config( conf, "batch.num.messages",
                      config.produce_max_messages_ );
    add_kafka_config( conf, "queue.buffering.max.ms", config.queue_max_ms_ );
    add_kafka_topic_config( topic_conf, "request.required.acks",
                            config.required_acks_ );
    add_kafka_config( conf, "message.send.max.retries",
                      config.send_max_retries_ );
    add_kafka_config( conf, "queue.buffering.max.messages",
                      config.max_buffered_messages_ );
    add_kafka_config( conf, "queue.buffering.max.kbytes",
                      config.max_buffered_messages_size_ );
}

void add_consumer_configs( rd_kafka_conf_t*                conf,
                           rd_kafka_topic_conf_t*          topic_conf,
                           const kafka_connection_configs& conn_config,
                           const kafka_consumer_configs&   config ) {
    add_both_configs( conf, topic_conf, conn_config, config.common_ );

    add_kafka_config( conf, "fetch.error.backoff.ms", config.fetch_max_ms_ );
    add_kafka_config( conf, "fetch.wait.max.ms", config.fetch_max_ms_ );
    add_kafka_topic_config( topic_conf, "consume.callback.max.messages",
                            config.consume_max_messages_ );
}

void add_kafka_config( rd_kafka_conf_t* conf, const std::string& arg,
                       const std::string& value ) {
    char                errstr[512];
    rd_kafka_conf_res_t err;

    err = rd_kafka_conf_set( conf, arg.c_str(), value.c_str(), errstr,
                             sizeof( errstr ) );
    if( err != RD_KAFKA_CONF_OK ) {
        LOG( WARNING ) << "Unable to set " << arg << ", value:" << value
                       << ", err:" << errstr;
    }
}

void add_kafka_topic_config( rd_kafka_topic_conf_t* topic_conf,
                             const std::string&     arg,
                             const std::string&     value ) {
    char                errstr[512];
    rd_kafka_conf_res_t err;

    err = rd_kafka_topic_conf_set( topic_conf, arg.c_str(), value.c_str(),
                                   errstr, sizeof( errstr ) );
    if( err != RD_KAFKA_CONF_OK ) {
        LOG( WARNING ) << "Unable to set " << arg << ", value:" << value
                       << ", err:" << errstr;
    }
}
