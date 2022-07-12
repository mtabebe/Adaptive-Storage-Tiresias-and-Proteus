#pragma once

#include "../../common/bucket_funcs.h"
#include "../../common/hw.h"
#include "twitter_configs.h"
#include "twitter_table_ids.h"

ALWAYS_INLINE std::vector<table_partition_information> get_twitter_data_sizes(
    const twitter_configs& configs );


#include "twitter_table_sizes-inl.h"


