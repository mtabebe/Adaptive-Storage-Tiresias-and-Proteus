#include "site_selector_executor.h"

#include "../benchmark/smallbank/smallbank_configs.h"
#include "../benchmark/ycsb/ycsb_configs.h"
#include "../benchmark/tpcc/tpcc_configs.h"
#if 0 // MTODO-HTAP
#include "../benchmark/twitter/twitter_configs.h"
#endif

std::shared_ptr<partition_data_location_table>
    construct_partition_data_location_table(
        const partition_data_location_table_configs& configs,
        const workload_type                          w_type ) {
    std::shared_ptr<partition_data_location_table> data_loc_tab =
        make_partition_data_location_table( configs );

    switch( w_type ) {
        case YCSB:
            add_ycsb_tables(data_loc_tab);
            break;
        case TPCC:
            add_tpcc_tables( data_loc_tab );
            break;
        case SMALLBANK:
            add_smallbank_tables( data_loc_tab );
        case TWITTER:
            add_twitter_tables( data_loc_tab );
        case UNKNOWN_WORKLOAD:
            LOG( WARNING ) << "Trying to run unknown workload type";
    }

    return data_loc_tab;
}

void add_tables_to_query_arrival_predictor(
    std::shared_ptr<query_arrival_predictor>&             qap,
    const std::shared_ptr<partition_data_location_table>& table ) {
    for( uint32_t table_id = 0; table_id < table->get_num_tables();
         table_id++ ) {
        qap->add_table_metadata( table->get_table_metadata( table_id ) );
    }
}

std::shared_ptr<partition_data_location_table>
    make_partition_data_location_table(
        const partition_data_location_table_configs& configs ) {
    return std::make_shared<partition_data_location_table>( configs );
}

void add_ycsb_tables(
    std::shared_ptr<partition_data_location_table> data_loc_tab ) {
    data_loc_tab->create_table( create_ycsb_table_metadata() );
}

void add_tpcc_tables(
    std::shared_ptr<partition_data_location_table> data_loc_tab ) {
    auto table_metas = create_tpcc_table_metadata();
    for( auto meta : table_metas ) {
        data_loc_tab->create_table( meta );
    }
}

void add_smallbank_tables(
    std::shared_ptr<partition_data_location_table> data_loc_tab ) {
    auto table_metas = create_smallbank_table_metadata();
    for( auto meta : table_metas ) {
        data_loc_tab->create_table( meta );
    }
}

void add_twitter_tables(
    std::shared_ptr<partition_data_location_table> data_loc_tab ) {

#if 0  // MTODO-HTAP
    auto table_metas = create_twitter_table_metadata();
    for( auto meta : table_metas ) {
        data_loc_tab->create_table( meta );
    }
#endif
}

