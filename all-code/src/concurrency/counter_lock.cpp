#include "counter_lock.h"
#include "../common/constants.h"
#include "update_spinner.h"
#include <glog/logging.h>
#include <thread>

counter_lock::counter_lock() : value_( 0 ), cv_slab_( 30 ) {
    for( size_t i = 0; i < cv_slab_.size(); i++ ) {
        std::unique_ptr<counter_lock_cv_entry> ent =
            std::make_unique<counter_lock_cv_entry>();
        ent->value_to_wait_for_ = 0;
        cv_slab_[i] = std::move( ent );
    }
}

void counter_lock::wake_up_waiters() {
    for( size_t i = 0; i < cv_slab_.size(); i++ ) {
        std::unique_ptr<counter_lock_cv_entry> &ent = cv_slab_[i];
        if( ent->value_to_wait_for_ != 0 &&
            ent->value_to_wait_for_ <= value_ ) {
            ent->cv_.notify_one();
        }
    }
}

uint64_t counter_lock::update_counter( uint64_t new_count ) {
    std::unique_lock<std::mutex> lk( mut_ );
    value_ = new_count;
    DVLOG( 40 ) << "set[" << this << "]=" << value_;
    wake_up_waiters();
    return value_;
}

uint64_t counter_lock::update_counter_if_less_than( uint64_t new_count ) {
    std::unique_lock<std::mutex> lk( mut_ );
    if( value_ < new_count ) {
        value_ = new_count;
        DVLOG( 40 ) << "set[" << this << "]=" << value_;
        wake_up_waiters();
    }
    return value_;
}

uint64_t counter_lock::incr_counter() {
    std::unique_lock<std::mutex> lk( mut_ );
    value_++;
    DVLOG( 40 ) << "incr[" << this << "]=" << value_;
    wake_up_waiters();
    return value_;
}

uint64_t counter_lock::get_counter() {
    std::unique_lock<std::mutex> lk( mut_ );
    DVLOG( 40 ) << "get[" << this << "]=" << value_;
    return value_;
}

uint64_t counter_lock::get_counter_no_lock() { return value_; }

uint64_t counter_lock::wait_until( uint64_t count ) {

    std::unique_lock<std::mutex> lk( mut_ );
    if( value_ >= count ) {
        return value_;
    }
    // Insert myself into list
    size_t i;

    // Use an empty slot if one is available
    for( i = 0; i < cv_slab_.size(); i++ ) {
        if( cv_slab_[i]->value_to_wait_for_ == 0 ) {
            cv_slab_[i]->value_to_wait_for_ = count;
            cv_slab_[i]->cv_.wait( lk,
                                   [this, count] { return value_ >= count; } );
            cv_slab_[i]->value_to_wait_for_ = 0;
            return value_;
        }
    }
    // Add a new slot if none available
    std::unique_ptr<counter_lock_cv_entry> ent =
        std::make_unique<counter_lock_cv_entry>();
    ent->value_to_wait_for_ = count;
    cv_slab_.emplace_back( std::move( ent ) );
    cv_slab_[i]->cv_.wait( lk, [this, count] { return value_ >= count; } );

    // Okay, we woke up.
    // Zero our entry and return
    cv_slab_[i]->value_to_wait_for_ = 0;
    return value_;
}
