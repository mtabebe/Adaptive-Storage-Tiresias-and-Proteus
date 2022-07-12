#pragma once

#include "col_partition.h"
#include "multi_col_partition.h"
#include "partition.h"
#include "row_partition.h"

std::shared_ptr<internal_partition> create_internal_partition(
    const partition_type::type& p_type );
std::shared_ptr<partition> create_partition(
    const partition_type::type& p_type );
partition* create_partition_ptr( const partition_type::type& p_type );
