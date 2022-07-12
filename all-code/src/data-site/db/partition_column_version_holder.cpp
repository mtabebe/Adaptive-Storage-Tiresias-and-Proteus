#include "partition_column_version_holder.h"

#include <glog/logging.h>

partition_column_version_holder::partition_column_version_holder(
    const partition_column_identifier& pcid, uint64_t version,
    uint64_t poll_epoch )
    : pcid_( pcid ), version_( version ), poll_epoch_( poll_epoch ) {
    DVLOG( 40 ) << "Init partition column version holder:" << pcid_
                << ", version_:" << version_ << ", poll_epoch_:" << poll_epoch_;
}

void partition_column_version_holder::set_version_and_epoch(
    uint64_t new_version, uint64_t new_epoch ) {
    version_ = new_version;
    poll_epoch_ = new_epoch;

    DVLOG( 40 ) << "Set version and epoch:" << pcid_
                << ", version_:" << version_ << ", poll_epoch_:" << poll_epoch_;
}
void partition_column_version_holder::set_version( uint64_t new_version ) {
    version_ = new_version;
    DVLOG( 40 ) << "Set version:" << pcid_ << ", version_:" << version_
                << ", poll_epoch_:" << poll_epoch_;
}
void partition_column_version_holder::set_epoch( uint64_t new_epoch ) {
    poll_epoch_ = new_epoch;

    DVLOG( 40 ) << "Set epoch:" << pcid_ << ", version_:" << version_
                << ", poll_epoch_:" << poll_epoch_;
}

uint64_t partition_column_version_holder::get_version() const {
    uint64_t v = version_;
    DVLOG( 40 ) << "Get version:" << pcid_ << ", version_:" << v;
    return v;
}
uint64_t partition_column_version_holder::get_epoch() const {
    uint64_t p = poll_epoch_;
    DVLOG( 40 ) << "Get epoch:" << pcid_ << ", epoch_:" << p;
    return p;
}

partition_column_identifier
    partition_column_version_holder::get_partition_column() const {
    return pcid_;
}

void partition_column_version_holder::get_version_and_epoch(
    const partition_column_identifier& pcid_to_check, uint64_t* version_to_set,
    uint64_t* poll_epoch_to_set ) const {
    DCHECK_EQ( pcid_, pcid_to_check );
    *version_to_set = version_;
    *poll_epoch_to_set = poll_epoch_;

    DVLOG( 40 ) << "Get version and epoch:" << pcid_
                << ", version_:" << *version_to_set
                << ", poll_epoch_:" << *poll_epoch_to_set;
}

