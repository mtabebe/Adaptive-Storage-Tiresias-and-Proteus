#pragma once

#include "../../common/bucket_funcs.h"
#include "../../common/hw.h"
#include "smallbank_configs.h"
#include "smallbank_table_ids.h"

ALWAYS_INLINE std::vector<table_partition_information> get_smallbank_data_sizes(
    const smallbank_configs& configs );


#include "smallbank_table_sizes-inl.h"

