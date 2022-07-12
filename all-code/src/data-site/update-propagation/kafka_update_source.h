#pragma once

#include <librdkafka/rdkafka.h>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

#include "../../common/partition_funcs.h"
#include "../../concurrency/semaphore.h"
#include "kafka_helpers.h"
#include "update_source_interface.h"

class kafka_consumer_topic {
   public:
    kafka_consumer_topic( const kafka_topic_connection_configs& configs );
    ~kafka_consumer_topic();

    void lock();
    void unlock();
    void notify();
    void wait();

    kafka_topic_connection_configs config_;

    rd_kafka_t*       rk_;
    rd_kafka_topic_t* topic_;

    std::unordered_map<partition_column_identifier, int64_t,
                       partition_column_identifier_key_hasher,
                       partition_column_identifier_equal_functor>
        partitions_;

    uint64_t              num_ops_;
    std::atomic<int64_t>  current_offset_;
    std::atomic<int64_t>  max_observed_offset_;
    void*                 enqueuer_opaque_ptr_;

    bool mark_started_;
    bool mark_stopped_;

   private:
    std::mutex              lock_;
    std::condition_variable cv_;
};

class kafka_update_source : public update_source_interface {
   public:
    kafka_update_source( const kafka_connection_configs& conn_config,
                         const kafka_consumer_configs&   consumer_config );
    ~kafka_update_source();

    void start();
    void stop();

    void enqueue_updates( void* enqueuer_opaque_ptr );

    bool add_source( const propagation_configuration& config,
                     const partition_column_identifier& pid, bool do_seek,
                     const std::string& cause );
    bool add_sources( const propagation_configuration&         config,
                      const std::vector<partition_column_identifier>& pids,
                      bool do_seek, const std::string& cause );

    void add_source_delay_start( propagation_configuration&  config,
                                 const partition_column_identifier& pid );
    std::tuple<bool, int64_t> remove_source(
        const propagation_configuration& config,
        const partition_column_identifier& pid, const std::string& cause );

    void start_consuming_topic(
        std::shared_ptr<kafka_consumer_topic> consumer_topic,
        int64_t                               start_offset );
    void stop_consuming_topic(
        std::shared_ptr<kafka_consumer_topic> consumer_topic );

    void build_subscription_offsets(
        std::unordered_map<
            propagation_configuration, int64_t /*offset*/,
            propagation_configuration_ignore_offset_hasher,
            propagation_configuration_ignore_offset_equal_functor>& offsets );

    void set_subscription_info(
        update_enqueuer_subscription_information* partition_subscription_info );

   private:
    void enqueue_updates_for_topic(
        std::shared_ptr<kafka_consumer_topic> consumer_topic,
        void*                                 enqueuer_opaque_ptr );

    std::shared_ptr<kafka_consumer_topic> remove_pid_from_topic(
        const propagation_configuration& config,
        const partition_column_identifier&      pid );
    // topic, is_new, seek_is_necessary
    // is_new if actually created the topic
    // seek_is_necessary if one of the partitions that we added hasn't had a
    // subscription before
    std::tuple<std::shared_ptr<kafka_consumer_topic>, bool, bool>
        create_or_get_topic( const propagation_configuration&         config,
                             const std::vector<partition_column_identifier>& pids );
    void add_topic( const propagation_configuration&      config,
                    std::shared_ptr<kafka_consumer_topic> topic );

    static void message_consume_callback( rd_kafka_message_t* rkmessage,
                                          void* consumer_topic_opaque_ptr );

    void seek_topic( std::shared_ptr<kafka_consumer_topic> topic,
                     int64_t                               offset );

    kafka_connection_configs                  conn_config_;
    kafka_consumer_configs                    consumer_config_;
    update_enqueuer_subscription_information* sub_info_;

    std::mutex                         mutex_;
    std::unordered_map<propagation_configuration,
                       std::shared_ptr<kafka_consumer_topic>,
                       propagation_configuration_ignore_offset_hasher,
                       propagation_configuration_ignore_offset_equal_functor>
        consumer_topics_;
};
