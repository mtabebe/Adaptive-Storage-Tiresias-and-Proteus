#include <gflags/gflags.h>
#include <glog/logging.h>

#include "../src/common/constants.h"
#include "../src/common/perf_tracking.h"

#include "../src/data-site/update-propagation/kafka_update_source.h"
#include "../src/data-site/update-propagation/update_queue.h"
#include "../src/data-site/update-propagation/write_buffer_serialization.h"

DEFINE_string( kafka_broker, "localhost:9092", "Kafka broker to listen to" );
DEFINE_string( kafka_topic, "dyna-mast-updates", "Kafka topic to consume" );
DEFINE_int32( kafka_partition, 0, "Kafka partition to consume" );
DEFINE_int32( kafka_start_offset, 0, "Kafka partition offset to consume from" );

void handle_message( const serialized_update& ser_update ) {
    stashed_update update = create_stashed_update( ser_update );
    deserialized_update* deserialized_ptr = update.deserialized_;

    auto pid = deserialized_ptr->pcid_;

    uint32_t is_new_partition = update.deserialized_->is_new_partition_;
    DVLOG( 10 ) << "Enqueue stashed:" << pid
                << ", version:" << update.commit_version_
                << ", is_new_partition:" << is_new_partition
                << ", at:" << deserialized_ptr->txn_ts_;

    for( const partition_column_operation_identifier& poi :
         deserialized_ptr->partition_updates_ ) {
        uint32_t                    op = poi.op_code_;
        uint64_t                    mid_point = poi.data_64_;
        const partition_column_identifier& pid = poi.identifier_;

        DVLOG( 20 ) << "Execute partition operation update:" << op
                    << ", on partition:" << pid << ", mid_point:" << mid_point
                    << ", at:" << deserialized_ptr->txn_ts_;
    }
}

void message_consume_callback( rd_kafka_message_t* rkmessage,
                               void*               consumer_topic_opaque_ptr ) {
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


    handle_message( update );

    DVLOG( 30 ) << "Done applying kafka message:" << message_id;
}

void poll_from_kafka(
    std::shared_ptr<kafka_update_source>  kafka_source,
    std::shared_ptr<kafka_consumer_topic> kafka_topic,
    const kafka_connection_configs&       conn_configs,
    const kafka_consumer_configs&         consumer_configs,
    const kafka_topic_connection_configs& topic_conn_configs ) {

    LOG( INFO ) << "Starting poll from kafka";

    for( ;; ) {
        int32_t num_messages = rd_kafka_consume_callback(
            kafka_topic->topic_, kafka_topic->config_.partition_,
            consumer_configs.consume_timeout_ms_, &message_consume_callback,
            (void*) kafka_topic.get() );
        kafka_topic->num_ops_ = kafka_topic->num_ops_ + 1;
        if( num_messages < 0 ) {
            LOG( INFO ) << "No message found for consumption";
            // old code used to try and do stashed update application here, I
            // don't
            // think we need to do this because, readers will apply stashed
            // updates
        }
        if( kafka_topic->num_ops_ == consumer_configs.common_.poll_count_ ) {
            kafka_topic->num_ops_ = 0;
            LOG( INFO ) << "consumer rd_kafka_poll:" << kafka_topic->config_;
            DCHECK( kafka_topic->rk_ );
            rd_kafka_poll( kafka_topic->rk_,
                           consumer_configs.common_.poll_timeout_ );
        }
    }
}

void start_consumer() {
    kafka_connection_configs conn_configs =
        construct_kafka_connection_configs( FLAGS_kafka_broker );
    kafka_consumer_configs consumer_configs =
        construct_kafka_consumer_configs( construct_kafka_common_configs() );
    kafka_topic_connection_configs topic_conn_configs =
        construct_kafka_topic_connection_configs( FLAGS_kafka_topic,
                                                  FLAGS_kafka_partition,
                                                  FLAGS_kafka_start_offset );

    LOG( INFO ) << "Starting kafka consumer";
    LOG( INFO ) << "Connection configs:" << conn_configs;
    LOG( INFO ) << "Consumer configs:" << consumer_configs;
    LOG( INFO ) << "Topic Connection configs:" << topic_conn_configs;

    auto kafka_source =
        std::make_shared<kafka_update_source>( conn_configs, consumer_configs );
    auto kafka_topic =
        std::make_shared<kafka_consumer_topic>( topic_conn_configs );

    LOG( INFO ) << "Starting consumer topic";
    kafka_source->start_consuming_topic( kafka_topic,
                                         topic_conn_configs.start_offset_ );

    poll_from_kafka( kafka_source, kafka_topic, conn_configs, consumer_configs,
                     topic_conn_configs );
}

int main( int argc, char** argv ) {

    gflags::ParseCommandLineFlags( &argc, &argv, true );
    google::InitGoogleLogging( argv[0] );
    google::InstallFailureSignalHandler();

    init_global_state();

    start_consumer();

    return 0;
}
