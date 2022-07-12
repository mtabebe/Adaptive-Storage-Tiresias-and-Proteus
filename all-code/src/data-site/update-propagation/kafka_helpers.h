#pragma once

#include <librdkafka/rdkafka.h>

#include "update_propagation_configs.h"

rd_kafka_conf_t*       create_kafka_conf();
rd_kafka_topic_conf_t* create_kafka_topic_conf();

void add_producer_configs( rd_kafka_conf_t*                conf,
                           rd_kafka_topic_conf_t*          topic_conf,
                           const kafka_connection_configs& conn_config,
                           const kafka_producer_configs&   config );
void add_consumer_configs( rd_kafka_conf_t*                conf,
                           rd_kafka_topic_conf_t*          topic_conf,
                           const kafka_connection_configs& conn_config,
                           const kafka_consumer_configs&   config );

void add_kafka_config( rd_kafka_conf_t* conf, const std::string& arg,
                       const std::string& value );
void add_kafka_topic_config( rd_kafka_topic_conf_t* topic_conf,
                             const std::string& arg, const std::string& value );
