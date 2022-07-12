#pragma once

#include <atomic>
#include <librdkafka/rdkafka.h>
#include <thread>

#include "../../concurrency/semaphore.h"
#include "kafka_helpers.h"
#include "update_destination_interface.h"

class kafka_update_destination : public update_destination_interface {
   public:
    kafka_update_destination(
        const kafka_connection_configs&       conn_config,
        const kafka_topic_connection_configs& topic_config,
        const kafka_producer_configs&         producer_config );
    ~kafka_update_destination();

    void send_update( serialized_update&&         update,
                      const partition_column_identifier& pid );
    propagation_configuration get_propagation_configuration();
    uint64_t                  get_num_updates();

   private:
    static void message_delivery_callback( rd_kafka_t*               rk,
                                           const rd_kafka_message_t* rkmessage,
                                           void* no_op_opaque_ptr );
    void init();
    void shutdown();

    kafka_connection_configs       conn_config_;
    kafka_topic_connection_configs topic_config_;
    kafka_producer_configs         producer_config_;

    std::atomic<int64_t>  last_produced_offset_;
    std::atomic<uint64_t> num_updates_;

    rd_kafka_t*       rk_;
    rd_kafka_topic_t* topic_;
};

