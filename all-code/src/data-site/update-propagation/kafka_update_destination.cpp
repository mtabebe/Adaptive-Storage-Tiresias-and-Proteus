#include "kafka_update_destination.h"

#include "../../common/constants.h"
#include "../../common/perf_tracking.h"
#include <chrono>
#include <signal.h>

kafka_update_destination::kafka_update_destination(
    const kafka_connection_configs&       conn_config,
    const kafka_topic_connection_configs& topic_config,
    const kafka_producer_configs&         producer_config )
    : conn_config_( conn_config ),
      topic_config_( topic_config ),
      producer_config_( producer_config ),
      last_produced_offset_( 0 ),
      num_updates_( 0 ),
      rk_( nullptr ),
      topic_( nullptr ) {
    init();
}

kafka_update_destination::~kafka_update_destination() { shutdown(); }

// get the last confirmed offset
propagation_configuration
    kafka_update_destination::get_propagation_configuration() {

    rd_kafka_flush( rk_, producer_config_.flush_wait_ms_ );
    rd_kafka_poll( rk_, producer_config_.common_.poll_timeout_ );

    propagation_configuration config;
    config.type = propagation_type::KAFKA;

    config.topic = topic_config_.topic_;
    config.partition = topic_config_.partition_;
    config.offset = last_produced_offset_.load();

    DCHECK_GE( config.offset, 0 );

    return config;
}

uint64_t kafka_update_destination::get_num_updates() {

    return num_updates_.load();
}

// write an update out
void kafka_update_destination::send_update(
    serialized_update&& update, const partition_column_identifier& pid ) {
    start_timer( KAFKA_SEND_UPDATE_TIMER_ID );

    DCHECK( topic_ );

    DCHECK_GE( update.length_, get_serialized_update_min_length() );
    uint64_t message_id = get_serialized_update_message_id( update );

    DVLOG( 10 ) << "Writing out" << pid << ", message:" << message_id
                << " to:" << topic_config_;

    for( ;; ) {
        // we don't copy the buffer, kafka should free it
        int err;
        err = rd_kafka_produce( topic_, topic_config_.partition_,
                                RD_KAFKA_MSG_F_FREE, (void*) update.buffer_,
                                update.length_, NULL /*key*/, 0 /*key len*/,
                                (void*) this );
        if( err == 0 ) {
            break;
        }

        rd_kafka_resp_err_t err_code = rd_kafka_last_error();
        if( err_code != RD_KAFKA_RESP_ERR__QUEUE_FULL ) {
            LOG( ERROR ) << "Unable to write, config:" << conn_config_
                         << ", topic config:" << topic_config_
                         << ", error:" << rd_kafka_err2str( err_code );
            break;
        } else {
            rd_kafka_poll( rk_, producer_config_.common_.poll_timeout_ );
            auto dur = std::chrono::milliseconds( k_kafka_backoff_sleep_ms );
            LOG( WARNING ) << "Kafka queue full, retrying...";
            std::this_thread::sleep_for( dur );
        }
    }
    uint64_t num_updates_val = num_updates_.fetch_add( 1 );
    if( num_updates_val % producer_config_.common_.poll_count_ == 0 ) {
        DCHECK( rk_ );
        DVLOG( 40 ) << "Producer: rd_kafka_poll:" << topic_config_;
        rd_kafka_poll( rk_, producer_config_.common_.poll_timeout_ );
    }

    stop_timer( KAFKA_SEND_UPDATE_TIMER_ID );
}

void kafka_update_destination::init() {
    DVLOG( 20 ) << "Initializing kafka writer:" << conn_config_ << ", "
                << ", " << topic_config_ << producer_config_;

    char errstr[512];

    rd_kafka_conf_t* conf = create_kafka_conf();
    DCHECK( conf );
    rd_kafka_topic_conf_t* topic_conf = create_kafka_topic_conf();
    DCHECK( topic_conf );

    add_producer_configs( conf, topic_conf, conn_config_, producer_config_ );

    rd_kafka_conf_set_dr_msg_cb(
        conf, &kafka_update_destination::message_delivery_callback );

    size_t       entries;
    const char** res = rd_kafka_conf_dump( conf, &entries );
    LOG( INFO ) << "Dumping Kafka Conf:";
    for( size_t i = 0; i < entries; i++ ) {
        LOG( INFO ) << res[i];
    }
    rd_kafka_conf_dump_free( res, entries );

    rk_ = rd_kafka_new( RD_KAFKA_PRODUCER, conf, errstr, sizeof( errstr ) );
    if( !rk_ ) {
        LOG( ERROR ) << "Unable to create new producer, config:" << conn_config_
                     << ", " << producer_config_ << ", error:" << errstr;
        return;
    }

    const char** res2 = rd_kafka_topic_conf_dump( topic_conf, &entries );
    LOG( INFO ) << "Dumping Kafka Topic Conf:";
    for( size_t i = 0; i < entries; i++ ) {
        LOG( INFO ) << res2[i];
    }
    rd_kafka_conf_dump_free( res2, entries );

    topic_ =
        rd_kafka_topic_new( rk_, topic_config_.topic_.c_str(), topic_conf );
}

void kafka_update_destination::shutdown() {
    DVLOG( 20 ) << "Shutting down kafka writer:" << conn_config_ << ", "
                << producer_config_;
    if( rk_ != nullptr ) {
        DVLOG( 30 ) << "Flushing writes";
        rd_kafka_flush( rk_, producer_config_.flush_wait_ms_ );
    }
    if( topic_ != nullptr ) {
        rd_kafka_topic_destroy( topic_ );
        topic_ = nullptr;
    }
    if( rk_ != nullptr ) {
        rd_kafka_destroy( rk_ );
        rk_ = nullptr;
    }
}

// update the last confirmed offset
void kafka_update_destination::message_delivery_callback(
    rd_kafka_t* rk, const rd_kafka_message_t* rkmessage,
    void* no_op_opaque_ptr ) {
    serialized_update update;
    update.length_ = rkmessage->len;
    update.buffer_ = (char*) rkmessage->payload;
    uint64_t message_id = get_serialized_update_message_id( update );

    if( rkmessage->err ) {
        LOG( ERROR ) << "Message delivery for message id:" << message_id
                     << ", failed:" << rd_kafka_err2str( rkmessage->err );
    } else {
        DVLOG( 30 ) << "Message delivery for message id:" << message_id
                    << ", went to:" << rkmessage->partition
                    << " at offset:" << rkmessage->offset << " okay!";
        void* kafka_update_dest_opaque_ptr = rkmessage->_private;
        DCHECK( kafka_update_dest_opaque_ptr );
        kafka_update_destination* dest =
            (kafka_update_destination*) kafka_update_dest_opaque_ptr;

        DVLOG( 10 ) << "Polled Partition:" << dest->topic_config_.partition_
                    << ", set last produced offset:" << rkmessage->offset
                    << " for message id:" << message_id;

        if( rkmessage->offset < 0 ) {
            // RD_KAFKA_OFFSET_INVALID could be
            DVLOG( 5 ) << "Polled Partition:" << dest->topic_config_.partition_
                       << ", last produced offset is negative:"
                       << rkmessage->offset << " for message id:" << message_id;
        } else {
            dest->last_produced_offset_.store( rkmessage->offset );
        }
    }
    // rkmessage is destroyed automatically
}
