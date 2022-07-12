#include "access_stream.h"
#include "tracking_summary.h"
#include <glog/logging.h>
#include <thread>

void access_stream::write_access_to_stream( transaction_partition_accesses &txn_partition_accesses, high_res_time txn_ts, bool is_start_point ) {
    access_stream_record record( txn_partition_accesses, txn_ts, is_start_point );
    //TODO: uncomment so this is legible
    DVLOG( 20 ) << "Writing " << record.txn_partition_accesses_ << "to access_stream";
    std::unique_lock<std::mutex> lock( guard_ );
    bool ok = stream_.write( std::move( record ) );
    if( !ok ) {
        DVLOG( 10 ) << "Could not write to access_stream, queue is full!";
    }
}

void access_stream::access_stream_thread_func() {
    try {
        while( !shutdown_ ) {
            process_next_access_record();
        }
    } catch( std::exception &e ) {
        LOG( FATAL ) << "AccessStream: Got fatal exception: " << e.what();
    }
}

bool access_stream::get_start_record( access_stream_record **record ) {
    // If we have in flight records, process them first
    while( !in_flight_records_.empty() ) {
        //Pop off everything until we hit a start_point
        *record = in_flight_records_.front();
        in_flight_records_.pop_front();
        if( (*record)->is_start_point_ ) {
            return true;
        }
        delete *record;
    }

    // Otherwise, pull a start point entry off the queue
    while( !stream_.isEmpty() ) {
        *record = new access_stream_record( *( stream_.frontPtr() ) );
        stream_.popFront();
        if( (*record)->is_start_point_ ) {
            return true;
        }
        delete *record;
    }

    // No start points, break
    return false;
}

void access_stream::fill_in_flight_buffer( access_stream_record *record ) {
SLEEP_LOOP:
    DVLOG( 20 ) << "Woke up from sleep...";
    auto diff = std::chrono::high_resolution_clock::now() - record->time_;
    // If enough time hasn't elapsed, sleep and retry
    if( diff < tracking_interval_ ) {
        DVLOG( 20 ) << "Not enough time has passed... "
                    << std::chrono::duration_cast<std::chrono::milliseconds>(
                           diff )
                           .count()
                    << " millis";
        std::this_thread::sleep_for( diff );
        goto SLEEP_LOOP;
    }
    DVLOG( 20 ) << "Good to go!";

    // Pull records off the main queue into the in-flight buffer
    access_stream_record *fp;
    while( !stream_.isEmpty() ) {
        fp = stream_.frontPtr();
        if( fp->time_ - record->time_ < tracking_interval_ ) {
            DVLOG( 20 ) << "Found a record within tracking interval...";
            access_stream_record *new_rec = new access_stream_record( *fp );
            stream_.popFront();
            in_flight_records_.push_back( new_rec );
        } else {
            DVLOG( 20 ) << "Done!";
            break;
        }
    }
}

void access_stream::process_next_access_record() {
    access_stream_record *record = nullptr;
START:
    if( shutdown_ ) {
        return;
    }
    //DVLOG( 20 ) << "Trying to pull record out of queue...";

    // Get the start record
    bool ok = get_start_record( &record );
    if( !ok ) {
        //TODO: dock this thread until something gets put in here
        //DVLOG( 20 ) << "Emptied the queue, sleeping!";
        std::this_thread::sleep_for( empty_queue_sleep_time_ );
        goto START;
    }

    DVLOG( 20 ) << "Got record!";

    // fill the in-flight buffer with related items
    fill_in_flight_buffer( record );

    DVLOG( 20 ) << "Filled in-flight buffer..";

    across_transaction_access_correlations cross_txn_correlations;
    cross_txn_correlations.subsequent_txn_accesses_.reserve( in_flight_records_.size() );
    DVLOG( 20 ) << "We have in_flight_records of size: " << in_flight_records_.size();

    for( const access_stream_record *sub_record: in_flight_records_ ) {
        DVLOG( 20 ) << "Received sub_record of: " << sub_record;
        DVLOG( 20 ) << "Subsequent partition accesses: " << sub_record->txn_partition_accesses_.partition_accesses_.size();
        cross_txn_correlations.subsequent_txn_accesses_.push_back( sub_record->txn_partition_accesses_ );
    }

    cross_txn_correlations.origin_accesses_ = record->txn_partition_accesses_;

    DVLOG( 20 ) << "Created cross txn corrs...";


    tracking_summary summary;
    summary.txn_partition_accesses_ = record->txn_partition_accesses_;
    summary.across_txn_accesses_ = cross_txn_correlations;

    DVLOG( 20 ) << "Going to put new summary!";
    sampler_->put_new_sample( std::move( summary ) );
    DVLOG( 20 ) << "Done putting new summary!";

    // Destroy record
    delete record;
}
