#include "vector_update_source.h"

vector_update_source::vector_update_source()
    : expected_(),
      prop_configs_(),
      sub_info_( nullptr ),
      next_read_pos_( 0 ),
      lock_() {}
vector_update_source::~vector_update_source() {
  lock_.lock();
  clear_serialized_updates( expected_ );
  /* do not delete sub info, it is not our pointer */
  lock_.unlock();
}

void vector_update_source::start() {}
void vector_update_source::stop() {}

void vector_update_source::enqueue_updates( void* enqueuer_opaque_ptr ) {
    lock_.lock();
    if( next_read_pos_ >= expected_.size() ) {
        lock_.unlock();
        return;
    }
    DVLOG( 20 ) << "Enqueue update from vector pos:" << next_read_pos_;
    const serialized_update& update = expected_.at( next_read_pos_ );
    next_read_pos_ += 1;
    lock_.unlock();

    begin_enqueue_batch( enqueuer_opaque_ptr );

    enqueue_update( update, enqueuer_opaque_ptr );

    end_enqueue_batch( enqueuer_opaque_ptr );
}
bool vector_update_source::add_source( const propagation_configuration& config,
                                       const partition_column_identifier&      pid,
                                       bool                             do_seek,
                                       const std::string& cause ) {
    return add_sources( config, {pid}, do_seek, cause );
}
bool vector_update_source::add_sources(
    const propagation_configuration&         config,
    const std::vector<partition_column_identifier>& pids, bool do_seek,
    const std::string& cause ) {
    lock_.lock();

    prop_configs_.insert( config );

    if( ( config.offset < next_read_pos_ ) and do_seek ) {
        DCHECK( sub_info_ );
        int64_t sub_offset =
            sub_info_->get_max_offset_of_partitions_for_config( pids, config );
        if( sub_offset >= next_read_pos_ ) {
            DLOG( INFO ) << "Seeking back in vector stream, cause:" << cause;
            next_read_pos_ = config.offset;
        }
    }

    lock_.unlock();

    return false;
}

std::tuple<bool, int64_t> vector_update_source::remove_source(
    const propagation_configuration& config, const partition_column_identifier& pid,
    const std::string& cause ) {
    // do nothing
    return std::make_tuple<>( false, next_read_pos_ );
}

void vector_update_source::add_update( const serialized_update& serialized ) {
    lock_.lock();
    DVLOG(20) << "Adding update";
    expected_.push_back( serialized );
    lock_.unlock();
}

void vector_update_source::build_subscription_offsets(
    std::unordered_map<propagation_configuration, int64_t /*offset*/,
                       propagation_configuration_ignore_offset_hasher,
                       propagation_configuration_ignore_offset_equal_functor>&
        offsets ) {

    for( const auto& prop_config : prop_configs_ ) {
        offsets[prop_config] = next_read_pos_;
    }
}
void vector_update_source::set_subscription_info(
    update_enqueuer_subscription_information* partition_subscription_info ) {
    sub_info_ = partition_subscription_info;
}

