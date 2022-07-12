#include "vector_update_destination.h"

#include "../../common/string_utils.h"

vector_update_destination::vector_update_destination( int32_t id )
    : id_( id ), stored_updates_(), lock_() {}
vector_update_destination::~vector_update_destination() {
    lock_.lock();
    clear_serialized_updates( stored_updates_ );
    lock_.unlock();
}

void vector_update_destination::send_update(
    serialized_update &&update, const partition_column_identifier &pid ) {
    lock_.lock();
    stored_updates_.push_back( std::move( update ) );
    lock_.unlock();
}

propagation_configuration
    vector_update_destination::get_propagation_configuration() {
    propagation_configuration config;
    config.type = propagation_type::VECTOR;

    lock_.lock();
    config.partition = id_;
    config.offset = stored_updates_.size();
    lock_.unlock();

    return config;
}

uint64_t vector_update_destination::get_num_updates() {
    return stored_updates_.size();
}

bool equal_buffers_ignore_ts_if_present( size_t buff_len, const char *buff1,
                                         size_t buff_len2, const char *buff2 ) {
// Timestamps may differ because they are set by the internal db code, and
// aren't visible
// until serialized
#if defined( RECORD_COMMIT_TS )
    uint64_t offset = get_serialized_update_txn_timestamp_offset();
    uint64_t *data = (uint64_t *) (buff1+offset);
    uint64_t *data2 = (uint64_t *) (buff2+offset);
    //Set our timestamp to their timestamp
    *data = *data2;
#endif
    return c_strings_equal( buff_len, buff1, buff_len2, buff2 );
}

bool vector_update_destination::do_stored_and_expected_match(
    const std::vector<serialized_update> &expected ) {
    lock_.lock();

    if( stored_updates_.size() != expected.size() ) {
        DVLOG( 5 ) << "Uneven update sizes, expected:" << expected.size()
                   << ", actual:" << stored_updates_.size();
        lock_.unlock();
        return false;
    }

    for( uint32_t pos = 0; pos < stored_updates_.size(); pos++ ) {
        const serialized_update& actual_update = stored_updates_.at( pos );
        const serialized_update &expected_update = expected.at( pos );

        bool str_eq = equal_buffers_ignore_ts_if_present(
            expected_update.length_, expected_update.buffer_,
            actual_update.length_, actual_update.buffer_ );
        if( !str_eq ) {
            DVLOG( 5 ) << "Expected buffers do not match in pos:" << pos;
            lock_.unlock();
            return false;
        }
    }

    lock_.unlock();
    return true;
}

