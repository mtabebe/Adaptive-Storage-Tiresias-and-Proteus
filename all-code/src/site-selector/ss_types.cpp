#include "ss_types.h"

mastering_type_configs::mastering_type_configs(
    const ss_mastering_type& master_type )
    : master_type_( master_type ),
      is_mastering_type_fully_replicated_( false ),
      is_single_site_transaction_system_( false ) {

    if( ( master_type_ == ss_mastering_type::DYNAMIC_MASTERING ) or
        ( master_type_ == ss_mastering_type::SINGLE_MASTER_MULTI_SLAVE ) ) {
        is_mastering_type_fully_replicated_ = true;
    }
    if( ( master_type_ == ss_mastering_type::DYNAMIC_MASTERING ) or
        ( master_type_ == ss_mastering_type::DRP ) or
        ( master_type_ == ss_mastering_type::SINGLE_MASTER_MULTI_SLAVE ) or
        ( master_type_ == ss_mastering_type::ADAPT ) ) {
        is_single_site_transaction_system_ = true;
    }
}
