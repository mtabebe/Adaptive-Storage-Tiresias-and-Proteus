#include "partition_util.h"

std::shared_ptr<internal_partition> create_internal_partition(
    const partition_type::type& p_type ) {
    std::shared_ptr<internal_partition> part = nullptr;
    switch( p_type ) {
        case partition_type::type::ROW: {
            part = std::make_shared<row_partition>();
            break;
        }
        case partition_type::type::COLUMN: {
            part = std::make_shared<col_partition>();
            break;
        }
        case partition_type::type::SORTED_COLUMN: {
            part = std::make_shared<sorted_col_partition>();
            break;
        }
        case partition_type::type::MULTI_COLUMN: {
            part = std::make_shared<multi_col_partition>();
            break;
        }
        case partition_type::type::SORTED_MULTI_COLUMN: {
            part = std::make_shared<sorted_multi_col_partition>();
            break;
        }

    }

    return part;
}

std::shared_ptr<partition> create_partition(
    const partition_type::type& p_type ) {
    auto int_part = create_internal_partition( p_type );
    auto ret = std::make_shared<partition>( int_part );
    return ret;
}

partition* create_partition_ptr( const partition_type::type& p_type ) {
    auto int_part = create_internal_partition( p_type );
    partition* p = new partition( int_part );
    return p;
}
