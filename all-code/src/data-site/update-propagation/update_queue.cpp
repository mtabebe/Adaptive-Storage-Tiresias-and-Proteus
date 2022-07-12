#include "update_queue.h"

#include <glog/logging.h>

#include "../../common/perf_tracking.h"

update_queue::update_queue()
    : expected_next_update_( 1 ), stashed_updates_(), lock_(), cv_() {}
update_queue::~update_queue() {}

uint32_t update_queue::add_update( stashed_update&& stashed ) {
    DVLOG( 40 ) << "Adding update:" << stashed.commit_version_;
    uint32_t queue_size = 0;
    {
        // lock
        std::unique_lock<std::mutex> guard( lock_ );
        stashed_updates_.push( std::move( stashed ) );
        queue_size = stashed_updates_.size();
        DVLOG( 40 ) << "Adding update:" << stashed.commit_version_
                    << ", stashed top:"
                    << stashed_updates_.top().commit_version_
                    << ", expected next:" << expected_next_update_;
    }
    // notify
    cv_.notify_one();
    return queue_size;
}

void update_queue::set_next_expected_version( uint64_t expected_version ) {
    {
        std::unique_lock<std::mutex> guard( lock_ );
        if ( expected_next_update_ < expected_version) {
            expected_next_update_ = expected_version;
        }
    }
    cv_.notify_all();
}

stashed_update update_queue::get_next_update( bool     wait_for_update,
                                              uint64_t req_version,
                                              uint64_t current_version ) {

    start_timer( UPDATE_QUEUE_GET_NEXT_UPDATE_TIMER_ID );

    stashed_update stashed;
    stashed.commit_version_ = K_NOT_COMMITTED;
    stashed.deserialized_ = nullptr;

    std::chrono::high_resolution_clock::time_point start_point =
        std::chrono::high_resolution_clock::now();

    std::chrono::high_resolution_clock::time_point end_point = start_point;
    std::chrono::duration<double, std::nano> elapsed = end_point - start_point;

    // 5 seconds
    {
        std::unique_lock<std::mutex> guard( lock_ );
        DVLOG( 30 ) << "Getting next update, wait:" << wait_for_update
                    << ", expect next:" << expected_next_update_
                    << ", required version:" << req_version
                    << ", current version:" << current_version;

        if( expected_next_update_ <= current_version ) {
            DVLOG( k_significant_update_propagation_log_level )
                << "Getting next update, wait:" << wait_for_update
                << ", expect next:" << expected_next_update_
                << ", required version:" << req_version
                << ", current version:" << current_version
                << ", updating expected next to:" << current_version + 1;
            expected_next_update_ = current_version + 1;
        }

        while( wait_for_update and ( stashed_updates_.empty() or
                                     ( stashed_updates_.top().commit_version_ >
                                       expected_next_update_ ) ) ) {

            if( req_version < expected_next_update_ ) {
                DVLOG( 30 ) << "Waiting for update:" << wait_for_update
                            << ", expected next:" << expected_next_update_
                            << ", required version:" << req_version
                            << ", breaking out";
                break;
            }

            if( stashed_updates_.empty() ) {
                DVLOG( 30 ) << "Waiting for update:" << wait_for_update
                            << ", expected next:" << expected_next_update_
                            << ", stashed empty";
            } else {
                DVLOG( 30 ) << "Waiting for update:" << wait_for_update
                            << ", expected next:" << expected_next_update_
                            << ", stashed top:"
                            << stashed_updates_.top().commit_version_;
            }
            if( k_timer_log_level_on ) {
                auto ret = cv_.wait_for(
                    guard, std::chrono::nanoseconds(
                               (uint64_t) k_slow_timer_log_time_threshold ) );
                end_point = std::chrono::high_resolution_clock::now();
                elapsed = end_point - start_point;

                if( ret == std::cv_status::timeout ) {
                    VLOG( k_timer_log_level )
                        << "Timed out waiting for update:" << wait_for_update
                        << ", expected next:" << expected_next_update_
                        << ", have waited:" << elapsed.count()
                        << " , so far, this:" << this;
                }
            } else {
                cv_.wait( guard );
                end_point = std::chrono::high_resolution_clock::now();
                elapsed = end_point - start_point;
            }
        }
        // if it isn't empty and commit version is the correct one
        if( ( !stashed_updates_.empty() ) and
            ( stashed_updates_.top().commit_version_ <=
              expected_next_update_ ) ) {
            stashed = stashed_updates_.top();
            stashed_updates_.pop();
            if( stashed.commit_version_ == expected_next_update_ ) {
                expected_next_update_ += 1;
            }
        }
    }

    if( elapsed.count() > k_slow_timer_log_time_threshold ) {
        VLOG( k_slow_timer_error_log_level )
            << "Waiting for update: " << wait_for_update
            << ", expected_next_update_:" << expected_next_update_
            << ", got stashed update:" << stashed.commit_version_
            << ", took:" << elapsed.count() << ", this:" << this;
        if ( elapsed.count() > k_slow_timer_log_time_threshold * 3) {
            LOG( WARNING ) << "Killing myself waiting";
        }
    }


    DVLOG( 30 ) << "Got stashed update:" << stashed.commit_version_;


    stop_timer( UPDATE_QUEUE_GET_NEXT_UPDATE_TIMER_ID );

    return stashed;
}

void update_queue::add_dummy_op( uint64_t version ) {
    stashed_update stashed;
    stashed.commit_version_ = version;
    stashed.deserialized_ = nullptr;

    DVLOG( 40 ) << "Adding dummy op for version:" << version;

    add_update( std::move( stashed ) );
}

stashed_update create_stashed_update( const serialized_update& update ) {
    stashed_update stashed;

    stashed.serialized_.length_ = update.length_;
    stashed.serialized_.buffer_ =
        (char*) malloc( update.length_ * sizeof( char ) );
    std::memcpy( (void*) stashed.serialized_.buffer_, (void*) update.buffer_,
                 stashed.serialized_.length_ );

    stashed.deserialized_ = deserialize_update( stashed.serialized_ );

    stashed.commit_version_ = get_snapshot_version(
        stashed.deserialized_->commit_vv_, stashed.deserialized_->pcid_ );

    return std::move( stashed );
}

void destroy_stashed_update( stashed_update& update ) {
    if( update.deserialized_ != nullptr ) {
        delete update.deserialized_;
        free( (void*) update.serialized_.buffer_ );
    }
    update.deserialized_ = nullptr;
}
