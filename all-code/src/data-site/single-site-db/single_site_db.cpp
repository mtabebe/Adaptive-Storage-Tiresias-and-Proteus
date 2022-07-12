#include "single_site_db.h"

#include <glog/logging.h>
#include <glog/stl_logging.h>

partition_column_identifier_set build_pid_set(
    const std::vector<std::shared_ptr<partition_payload>>& payloads ) {
    partition_column_identifier_set ret_set;

    for( auto payload : payloads ) {
        ret_set.emplace( payload->identifier_ );
    }

    return ret_set;
}

