#include "kafka_update_source.h"

#include <glog/logging.h>
#include <glog/stl_logging.h>

#include "../../common/perf_tracking.h"
#include "kafka_helpers.h"

// a source of updates from kafka
// this multiplexes multiple kafka partitions

kafka_consumer_topic::kafka_consumer_topic(
    const kafka_topic_connection_configs& configs )
    : config_( configs ),
      rk_( nullptr ),
      topic_( nullptr ),
      partitions_(),
      num_ops_( 0 ),
      current_offset_( 0 ),
      max_observed_offset_( 0 ),
      enqueuer_opaque_ptr_( nullptr ),
      mark_started_( false ),
      mark_stopped_( false ),
      lock_(),
      cv_() {}
kafka_consumer_topic::~kafka_consumer_topic() {
    rk_ = nullptr;
    topic_ = nullptr;
}

void kafka_consumer_topic::lock() {
    DVLOG( 40 ) << "Locking:" << config_;
    lock_.lock();
    DVLOG( 40 ) << "Locked:" << config_;
}
void kafka_consumer_topic::unlock() {
    DVLOG( 40 ) << "Unlocking:" << config_;
    lock_.unlock();
}
void kafka_consumer_topic::notify() {
    DVLOG( 40 ) << "Notifying:" << config_;
    cv_.notify_all();
}
void kafka_consumer_topic::wait() {
  // condition variables don't let me pass in an true mutex, so you have to
  // assume that you hold the lock
  std::unique_lock<std::mutex> lk( lock_, std::adopt_lock );
  DVLOG( 40 ) << "Waiting:" << config_;
  cv_.wait( lk );
  DVLOG( 40 ) << "Done Waiting:" << config_;
}

kafka_update_source::kafka_update_source(
    const kafka_connection_configs& conn_config,
    const kafka_consumer_configs&   consumer_config )
    : conn_config_( conn_config ),
      consumer_config_( consumer_config ),
      sub_info_( nullptr ),
      mutex_(),
      consumer_topics_() {}
kafka_update_source::~kafka_update_source() { stop(); /*sub info is not ours*/ }

// iterate over every topic and subscribe
void kafka_update_source::enqueue_updates( void* enqueuer_opaque_ptr ) {
    std::vector<std::shared_ptr<kafka_consumer_topic>> topics_to_consume_;
    {
        DVLOG( 40 ) << "Locking update enqueuer to copy consumer_topics:"
                    << this;
        std::lock_guard<std::mutex> guard( mutex_ );
        for( auto& topic : consumer_topics_ ) {
            std::shared_ptr<kafka_consumer_topic> consumer_topic = topic.second;
            topics_to_consume_.push_back( consumer_topic );
        }
        DVLOG( 40 ) << "Unlocking update enqueuer to copy consumer_topics:"
                    << this;
    }
    for( auto consumer_topic : topics_to_consume_ ) {
        enqueue_updates_for_topic( consumer_topic, enqueuer_opaque_ptr );
    }
}

void kafka_update_source::enqueue_updates_for_topic(
    std::shared_ptr<kafka_consumer_topic> consumer_topic,
    void*                                 enqueuer_opaque_ptr ) {

    start_timer( KAFKA_ENQUEUE_UPDATES_FOR_TOPIC_TIMER_ID );

    DCHECK( consumer_topic );
    // change the ptr so we can track the offset
    consumer_topic->enqueuer_opaque_ptr_ = enqueuer_opaque_ptr;

    consumer_topic->lock();
    while( !consumer_topic->mark_started_ and !consumer_topic->mark_stopped_ ) {
        consumer_topic->wait();
    }
    DVLOG( 40 ) << "Consume message topic:" << consumer_topic->config_
                << ", mark_started_:" << consumer_topic->mark_started_
                << ", mark stopped_:" << consumer_topic->mark_stopped_;
    if( !consumer_topic->mark_started_ ) {
        // not started so don't waste time trying to get updates
        consumer_topic->unlock();
        stop_timer( KAFKA_ENQUEUE_UPDATES_FOR_TOPIC_TIMER_ID );
        return;
    } else if( consumer_topic->mark_stopped_ ) {
        // it is stopped so, stop consuming and get out
        stop_consuming_topic( consumer_topic );
        consumer_topic->unlock();
        stop_timer( KAFKA_ENQUEUE_UPDATES_FOR_TOPIC_TIMER_ID );
        return;
    }
    consumer_topic->unlock();

    DVLOG( 40 ) << "Calling consume on:" << consumer_topic->config_
                << ", source:" << this
                << ", current_offset:" << consumer_topic->current_offset_
                << ", max_observed_offset:"
                << consumer_topic->max_observed_offset_;

    begin_enqueue_batch( enqueuer_opaque_ptr );

    int32_t num_messages = rd_kafka_consume_callback(
        consumer_topic->topic_, consumer_topic->config_.partition_,
        consumer_config_.consume_timeout_ms_,
        &kafka_update_source::message_consume_callback,
        (void*) consumer_topic.get() );
    consumer_topic->num_ops_ = consumer_topic->num_ops_ + 1;
    if( num_messages < 0 ) {
        DVLOG( 20 ) << "No message found for consumption";
        // old code used to try and do stashed update application here, I don't
        // think we need to do this because, readers will apply stashed updates
    }
    if( consumer_topic->num_ops_ == consumer_config_.common_.poll_count_ ) {
        consumer_topic->num_ops_ = 0;
        DVLOG( 40 ) << "consumer rd_kafka_poll:" << consumer_topic->config_;
        DCHECK( consumer_topic->rk_ );
        rd_kafka_poll( consumer_topic->rk_,
                       consumer_config_.common_.poll_timeout_ );
    }

    end_enqueue_batch( enqueuer_opaque_ptr );

    DVLOG( 40 ) << "Done Calling consume on:" << consumer_topic->config_
                << ", source:" << this
                << ", current offset:" << consumer_topic->current_offset_
                << ", max_observed_offset:"
                << consumer_topic->max_observed_offset_;

    stop_timer( KAFKA_ENQUEUE_UPDATES_FOR_TOPIC_TIMER_ID );
}

std::tuple<std::shared_ptr<kafka_consumer_topic>, bool, bool>
    kafka_update_source::create_or_get_topic(
        const propagation_configuration&         config,
        const std::vector<partition_column_identifier>& pids ) {

    auto found = consumer_topics_.find( config );
    std::shared_ptr<kafka_consumer_topic> topic = nullptr;
    bool                                  is_new_topic = false;

    if( found == consumer_topics_.end() ) {
        topic = std::make_shared<kafka_consumer_topic>(
            construct_kafka_topic_connection_configs(
                config.topic, config.partition, config.offset ) );
        consumer_topics_[config] = topic;
        is_new_topic = true;
        DVLOG( 7 ) << "Adding topic:" << config << ", to source:" << this;
    } else {
        topic = found->second;
    }
    DCHECK( topic );

    topic->lock();

    bool seek_is_necessary = false;

    for( const auto& pid : pids ) {
        // insert don't overwrite
        auto ret = topic->partitions_.emplace( pid, config.offset );
        if( ret.second ) {
            // seek may be necessary
            seek_is_necessary = true;
        }
    }

    return std::make_tuple( topic, is_new_topic, seek_is_necessary );
}

std::shared_ptr<kafka_consumer_topic>
    kafka_update_source::remove_pid_from_topic(
        const propagation_configuration& config,
        const partition_column_identifier&      pid ) {

    auto found = consumer_topics_.find( config );
    if( found == consumer_topics_.end() ) {
        return nullptr;
    }

    std::shared_ptr<kafka_consumer_topic> topic = found->second;

    topic->lock();

    int removed = topic->partitions_.erase( pid );

    if( removed == 0 ) {
        topic->unlock();
        return nullptr;
    }

    if( topic->partitions_.size() == 0 ) {
        DVLOG( 7 ) << "Removing topic:" << config << ", from source:" << this;
        consumer_topics_.erase( config );
    }

    return topic;
}

void kafka_update_source::add_topic(
    const propagation_configuration&      config,
    std::shared_ptr<kafka_consumer_topic> topic ) {
    consumer_topics_[config] = topic;
}

// add a new topic and seek back if necessary
bool kafka_update_source::add_source( const propagation_configuration& config,
                                      const partition_column_identifier&      pid,
                                      bool do_seek, const std::string& cause ) {
    return add_sources( config, {pid}, do_seek, cause );
}

bool kafka_update_source::add_sources(
    const propagation_configuration&         config,
    const std::vector<partition_column_identifier>& pids, bool do_seek,
    const std::string& cause ) {
    std::shared_ptr<kafka_consumer_topic> consumer_topic = nullptr;
	bool is_new_topic = false;
    bool seek_is_necessary = false;

    DVLOG( k_update_propagation_log_level )
        << "add_sources:" << config << ", pids:" << pids
        << ", do_seek:" << do_seek << ", cause:" << cause;
    {
        DVLOG( 40 ) << "Locking update enqueuer to create or get topic:"
                    << this;
        std::lock_guard<std::mutex> guard( mutex_ );
        std::tuple<std::shared_ptr<kafka_consumer_topic>, bool, bool> topic_creation =
            create_or_get_topic( config, pids );
        consumer_topic = std::get<0>( topic_creation );
        is_new_topic = std::get<1>( topic_creation );
        seek_is_necessary = std::get<2>( topic_creation );

        DVLOG( 40 ) << "UnLocking update enqueuer to create or get topic:"
                    << this;
    }
    if( is_new_topic == 1 ) {
        start_consuming_topic( consumer_topic, config.offset );
    }
    int64_t current_offset = consumer_topic->current_offset_;
    bool    trad_seek_necessary = ( ( config.offset < current_offset ) and
                                 ( seek_is_necessary and do_seek ) );
    bool listen_seek_necessary = false;
    int64_t sub_offset = 0;
    if( trad_seek_necessary ) {
        DCHECK( sub_info_ );
        sub_offset =
            sub_info_->get_max_offset_of_partitions_for_config( pids, config );
        listen_seek_necessary = ( sub_offset >= config.offset );
        DVLOG( k_update_propagation_log_level )
            << "Add source, traditional seek is necessary:"
            << trad_seek_necessary
            << ", listen_seek_necessary:" << listen_seek_necessary
            << ", sub offset:" << sub_offset
            << ", current offset:" << current_offset
            << ", config offset:" << config.offset << ", pids:" << pids
            << ", cause:" << cause;
    }

    if( trad_seek_necessary and listen_seek_necessary ) {
        DVLOG( k_significant_update_propagation_log_level )
            << "Add source, Seeking topic:" << consumer_topic->config_
            << ", to offset:" << config.offset
            << ", current offset:" << current_offset
            << ", max_observed_offset_:" << consumer_topic->max_observed_offset_
            << ", sub offset:" << sub_offset << ", pids:" << pids
            << ", cause:" << cause;

        seek_topic( consumer_topic, config.offset );
    } else {
        DVLOG( k_update_propagation_log_level )
            << "Add source, not seeking topic:" << consumer_topic->config_
            << ", to offset:" << config.offset
            << ", current offset:" << current_offset
            << ", max_observed_offset_:" << consumer_topic->max_observed_offset_
            << ", pids:" << pids << ", seek_is_necessary:" << seek_is_necessary
            << ", trad_seek_necessary:" << trad_seek_necessary
            << ", listen_seek_necessary:" << listen_seek_necessary
            << ", sub offset:" << sub_offset << ", do_seek:" << do_seek;
    }
    consumer_topic->unlock();

    return is_new_topic;
}

void kafka_update_source::seek_topic(
    std::shared_ptr<kafka_consumer_topic> topic, int64_t offset ) {
    start_timer( KAFKA_SEEK_TOPIC_TIMER_ID );

    DCHECK_GE( offset, 0 );

    uint32_t timeout = consumer_config_.seek_timeout_ms_;

    for( ;; ) {
        rd_kafka_resp_err_t err_code = rd_kafka_seek(
            topic->topic_, topic->config_.partition_, offset, timeout );
        DVLOG( k_significant_update_propagation_log_level )
            << "Seeking topic:" << topic->config_ << ", to offset:" << offset
            << ", current offset:" << topic->current_offset_
            << ", max_observed_offset_:" << topic->max_observed_offset_;
        if( err_code != RD_KAFKA_RESP_ERR_NO_ERROR ) {
            LOG( ERROR ) << "Unable to seek topic:" << topic->config_
                         << ", to offset:" << offset
                         << ", current offset:" << topic->current_offset_
                         << ", max_observed_offset_:"
                         << topic->max_observed_offset_
                         << ", timeout:" << timeout
                         << ", error:" << rd_kafka_err2str( err_code );
            // multiply the timeout by a number greater than 3 ( current timeout
            // didn't work, and we already spent that amount of time, so we need
            // to go back at least 3 times)
            timeout = timeout * 10;
        } else {
            break;
        }
    }

    topic->current_offset_ = offset;

    stop_timer( KAFKA_SEEK_TOPIC_TIMER_ID );
}

std::tuple<bool, int64_t> kafka_update_source::remove_source(
    const propagation_configuration& config, const partition_column_identifier& pid,
    const std::string& cause ) {
    std::shared_ptr<kafka_consumer_topic> consumer_topic = nullptr;

    DVLOG( k_update_propagation_log_level ) << "remove_source:" << config
                                            << ", pids:" << pid
                                            << ", cause:" << cause;

    bool removed_source = false;
    int64_t last_offset = 0;

    {
        DVLOG( 40 ) << "Locking update enqueuer to remove source:" << this;
        std::lock_guard<std::mutex>         guard( mutex_ );
        consumer_topic = remove_pid_from_topic( config, pid );
        DVLOG( 40 ) << "UnLocking update enqueuer to remove source:" << this;
    }
    if( consumer_topic ) {
        if( consumer_topic->partitions_.size() == 0 ) {
            DVLOG( k_significant_update_propagation_log_level )
                << "Stopping topic:" << consumer_topic->config_
                << ", last consumed_offset:" << consumer_topic->current_offset_
                << ", max_observed_offset_:"
                << consumer_topic->max_observed_offset_;
            consumer_topic->mark_stopped_ = true;
            consumer_topic->notify();
            removed_source = true;
            last_offset = consumer_topic->current_offset_;
        }
        consumer_topic->unlock();
    }

    return std::make_tuple<>( removed_source, last_offset );
}

void kafka_update_source::add_source_delay_start(
    propagation_configuration& config, const partition_column_identifier& pid ) {
    {
        DVLOG( 40 ) << "Locking update enqueuer to add source:" << this;
        std::lock_guard<std::mutex>         guard( mutex_ );

        auto consumer_topic_tuple = create_or_get_topic( config, {pid} );
        auto consumer_topic = std::get<0>( consumer_topic_tuple );
        consumer_topic->unlock();
        DVLOG( 40 ) << "UnLocking update enqueuer to add source:" << this;
    }
}

// the kafka consume callback
void kafka_update_source::message_consume_callback(
    rd_kafka_message_t* rkmessage, void* consumer_topic_opaque_ptr ) {
    DCHECK( consumer_topic_opaque_ptr );

    if( !rkmessage ) {
        DVLOG( 20 ) << "No message found";
        return;
    } else if( rkmessage->err ) {
        if( rkmessage->err == RD_KAFKA_RESP_ERR__PARTITION_EOF ) {
            DVLOG( 30 ) << "Got kafka message EOF";
        } else {
            LOG( ERROR ) << "Got kafka message consume error:"
                         << rd_kafka_message_errstr( rkmessage );
        }
        return;
    }

    DVLOG( 40 ) << "Got message";

    kafka_consumer_topic* topic =
        (kafka_consumer_topic*) consumer_topic_opaque_ptr;
    int64_t cur_offset = topic->current_offset_;
    if( rkmessage->offset > cur_offset ) {
        // we may have already seeked back
        topic->current_offset_.compare_exchange_strong(
            cur_offset /*test*/, rkmessage->offset /*new val if match*/ );
    }
    int64_t max_offset = topic->max_observed_offset_;
    if( rkmessage->offset > max_offset ) {
        // we may have already seeked back
        topic->max_observed_offset_.compare_exchange_strong(
            max_offset /*test*/, rkmessage->offset /*new val if match*/ );
    }

    serialized_update update;
    update.length_ = rkmessage->len;
    update.buffer_ = (char*) rkmessage->payload;

    if( update.length_ < (int64_t) get_serialized_update_min_length() ) {
        LOG( ERROR ) << "Got kafka message of length:" << update.length_
                     << ", is too small";
        return;
    }
    DCHECK_EQ( update.length_, get_serialized_update_len( update ) );

    uint64_t message_id = get_serialized_update_message_id( update );
    DVLOG( 30 ) << "Got kafka message:" << message_id;

    DVLOG( 10 ) << "Got kafka message on partition:"
                << topic->config_.partition_
                << ", message offset:" << rkmessage->offset
                << ", message id:" << message_id;

    enqueue_update( update, topic->enqueuer_opaque_ptr_ );
    DVLOG( 30 ) << "Done applying kafka message:" << message_id;
}

void kafka_update_source::start() {
    {

        DVLOG( 40 ) << "Locking update enqueuer to start:" << this;
        std::lock_guard<std::mutex> guard( mutex_ );
        for( auto& topic : consumer_topics_ ) {
            std::shared_ptr<kafka_consumer_topic> consumer_topic = topic.second;
            consumer_topic->lock();
            start_consuming_topic( consumer_topic,
                                   consumer_topic->config_.start_offset_ );
            consumer_topic->unlock();
        }
        DVLOG( 40 ) << "UnLocking update enqueuer to start:" << this;
    }
}

void kafka_update_source::start_consuming_topic(
    std::shared_ptr<kafka_consumer_topic> consumer_topic,
    int64_t                               start_offset ) {
    if( consumer_topic->mark_started_ ) {
        return;
    }

    if( consumer_topic->rk_ != nullptr ) {
        return;
    }
    DVLOG( k_significant_update_propagation_log_level )
        << "Starting kafka topic reader:" << conn_config_
        << ", config:" << consumer_topic->config_
        << ", at offset:" << start_offset;

    char errstr[512];

    rd_kafka_conf_t* conf = create_kafka_conf();
    DCHECK( conf );
    rd_kafka_topic_conf_t* topic_conf = create_kafka_topic_conf();
    DCHECK( topic_conf );

    add_consumer_configs( conf, topic_conf, conn_config_, consumer_config_ );

    consumer_topic->rk_ =
        rd_kafka_new( RD_KAFKA_CONSUMER, conf, errstr, sizeof( errstr ) );
    if( !consumer_topic->rk_ ) {
        LOG( ERROR ) << "Unable to create new consumer, config:" << conn_config_
                     << consumer_config_ << ", error:" << errstr;
        return;
    }

    consumer_topic->topic_ = rd_kafka_topic_new(
        consumer_topic->rk_, consumer_topic->config_.topic_.c_str(),
        topic_conf );

    consumer_topic->current_offset_ = start_offset;
    consumer_topic->max_observed_offset_ = start_offset;
    DVLOG( 20 ) << "Starting topic consumer at:" << start_offset;
    rd_kafka_consume_start( consumer_topic->topic_,
                            consumer_topic->config_.partition_, start_offset );

    consumer_topic->mark_started_ = true;
    consumer_topic->notify();
}

void kafka_update_source::stop_consuming_topic(
    std::shared_ptr<kafka_consumer_topic> consumer_topic ) {
    DCHECK( consumer_topic->mark_stopped_ );

    if( consumer_topic->rk_ != nullptr ) {
        return;
    }

    DVLOG( 10 ) << "Shutting down kafka topic reader:" << conn_config_
                << ", config:" << consumer_topic->config_;
    if( consumer_topic->topic_ != nullptr ) {
        DVLOG( 30 ) << "Stopping consumer";
        rd_kafka_consume_stop( consumer_topic->topic_,
                               consumer_topic->config_.partition_ );
        rd_kafka_topic_destroy( consumer_topic->topic_ );
        consumer_topic->topic_ = nullptr;
    }
    if( consumer_topic->rk_ != nullptr ) {
        rd_kafka_destroy( consumer_topic->rk_ );
        consumer_topic->rk_ = nullptr;
    }
}

void kafka_update_source::build_subscription_offsets(
    std::unordered_map<propagation_configuration, int64_t /*offset*/,
                       propagation_configuration_ignore_offset_hasher,
                       propagation_configuration_ignore_offset_equal_functor>&
        offsets ) {
    {
        std::lock_guard<std::mutex> guard( mutex_ );
        for( const auto& topic : consumer_topics_ ) {
            const auto&                           config = topic.first;
            std::shared_ptr<kafka_consumer_topic> consumer_topic = topic.second;
            offsets[config] = consumer_topic->current_offset_;
        }
    }
}

void kafka_update_source::set_subscription_info(
    update_enqueuer_subscription_information* partition_subscription_info ) {
    sub_info_ = partition_subscription_info;
}

void kafka_update_source::stop() {
    {
        DVLOG( 40 ) << "Locking update enqueuer to stop:" << this;
        std::lock_guard<std::mutex> guard( mutex_ );
        for( auto& topic : consumer_topics_ ) {
            std::shared_ptr<kafka_consumer_topic> consumer_topic = topic.second;
            consumer_topic->lock();
            consumer_topic->mark_stopped_ = true;
            stop_consuming_topic( consumer_topic );
            consumer_topic->unlock();
        }
        consumer_topics_.clear();
        DVLOG( 40 ) << "UnLocking update enqueuer to stop:" << this;
    }
}

