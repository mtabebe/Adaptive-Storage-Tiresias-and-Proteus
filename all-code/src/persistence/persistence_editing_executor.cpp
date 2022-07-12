#include "persistence_editing_executor.h"

#include <glog/logging.h>

#if 0 // MTODO-BENCH
#include "../benchmark/tpcc/tpcc_location_rewriter.h"
#include "../benchmark/twitter/twitter_location_rewriter.h"
#endif
#include "generic_location_rewriter.h"

void run_rewriter( data_location_rewriter_interface* rewriter,
                   const std::string& in_ss, const std::string& in_sm,
                   const std::string& in_part, const std::string& out_ss,
                   const std::string& out_sm, const std::string& out_part ) {

    // no point using a lock
    DVLOG( 5 ) << "Creating editor";
    persistence_editor editor( rewriter, in_ss, in_sm, in_part, out_ss, out_sm,
                               out_part );
    DVLOG( 5 ) << "Rewriting...";
    editor.rewrite();
    DVLOG( 5 ) << "Rewriting okay!";

    delete rewriter;
}

data_location_rewriter_interface* create_rewriter(
    const rewriter_type re_type, int num_sites,
    const std::vector<table_metadata> tables_metadata,
    const std::unordered_map<int, std::unordered_set<int>>& replica_locs,
    const partition_type::type&    master_type,
    const storage_tier_type::type& master_storage_type,
    const std::unordered_map<int, partition_type::type>&    replica_types,
    const std::unordered_map<int, storage_tier_type::type>& storage_types,
    const std::vector<std::vector<propagation_configuration>>& prop_configs,
    const persistence_configs&                                 per_configs,
    std::shared_ptr<update_destination_generator>              update_generator,
    std::shared_ptr<update_enqueuers>                          enqueuers,
    double prob_range_rewriter_threshold ) {

    switch( re_type ) {
      case RANGE_REWRITER:
          return new generic_range_based_editor(
              tables_metadata, num_sites, prop_configs, per_configs,
              update_generator, enqueuers, replica_locs, master_type,
              master_storage_type, replica_types, storage_types,
              true /*is fixed*/, prob_range_rewriter_threshold );
      case PROBABILITY_RANGE_REWRITER:
          return new generic_range_based_editor(
              tables_metadata, num_sites, prop_configs, per_configs,
              update_generator, enqueuers, replica_locs, master_type,
              master_storage_type, replica_types, storage_types,
              false /*is fixed*/, prob_range_rewriter_threshold );
      case HASH_REWRITER:
          return new generic_location_hash_based_editor(
              tables_metadata, num_sites, prop_configs, per_configs,
              update_generator, enqueuers, replica_locs, master_type,
              master_storage_type, replica_types, storage_types );
      case SINGLE_MASTER_REWRITER:
          return new generic_single_master_based_editor(
              tables_metadata, num_sites, prop_configs, per_configs,
              update_generator, enqueuers, true /*fully replicate*/,
              master_type, master_storage_type, replica_types, storage_types );
      case SINGLE_NODE_REWRITER:
          return new generic_single_master_based_editor(
              tables_metadata, num_sites, prop_configs, per_configs,
              update_generator, enqueuers, false /*fully replicate*/,
              master_type, master_storage_type, replica_types, storage_types );
      case WAREHOUSE_REWRITER:
          return nullptr;
#if 0 // MTODO-BENCH
          return new tpcc_location_warehouse_based_editor(
              tables_metadata, num_sites, prop_configs, per_configs,
              update_generator, enqueuers, replica_locs,
              master_type, replica_types,
              construct_tpcc_configs(),
              k_tpcc_should_fully_replicate_warehouses,
              k_tpcc_should_fully_replicate_districts );
#endif
      case TWITTER_REWRITER:
          return nullptr;
#if 0 // MTODO-BENCH
          return new twitter_location_user_based_editor(
              tables_metadata, num_sites, prop_configs, per_configs,
              update_generator, enqueuers, replica_locs,
              master_type, replica_types,
              construct_twitter_configs(),
              k_twitter_should_fully_replicate_users,
              k_twitter_should_fully_replicate_follows,
              k_twitter_should_fully_replicate_tweets );
#endif
      case NO_OP_REWRITER:
          return new generic_no_op_based_editor( tables_metadata, num_sites,
                                                 prop_configs, per_configs,
                                                 update_generator, enqueuers );
      case UNKNOWN_REWRITER:
          LOG( WARNING ) << "Trying to run with unknown rewriter type";
    }
    return nullptr;
}

void run_ycsb_rewriter(
    int num_sites, const std::vector<table_metadata>&       tables_metadata,
    const std::unordered_map<int, std::unordered_set<int>>& replica_locs,
    const partition_type::type&    master_type,
    const storage_tier_type::type& master_storage_type,
    const std::unordered_map<int, partition_type::type>&    replica_types,
    const std::unordered_map<int, storage_tier_type::type>& storage_types,
    const std::vector<std::vector<propagation_configuration>>& prop_configs,
    const persistence_configs&                                 per_configs,
    std::shared_ptr<update_destination_generator>              update_generator,
    std::shared_ptr<update_enqueuers>                          enqueuers,
    double prob_range_rewriter_threshold, const std::string& in_ss,
    const std::string& in_sm, const std::string& in_part,
    const std::string& out_ss, const std::string& out_sm,
    const std::string& out_part, const rewriter_type re_type ) {
    if( ( re_type == WAREHOUSE_REWRITER ) or ( re_type == TWITTER_REWRITER ) ) {
        LOG( WARNING ) << "Unsupported rewriter type "
                       << rewriter_type_string( re_type ) << " for ycsb";
        return;
    }

    run_rewriter(
        create_rewriter( re_type, num_sites, tables_metadata, replica_locs,
                         master_type, master_storage_type, replica_types,
                         storage_types, prop_configs, per_configs,
                         update_generator, enqueuers,
                         prob_range_rewriter_threshold ),
        in_ss, in_sm, in_part, out_ss, out_sm, out_part );
}

void run_tpcc_rewriter(
    int num_sites, const std::vector<table_metadata>&       tables_metadata,
    const std::unordered_map<int, std::unordered_set<int>>& replica_locs,
    const partition_type::type&    master_type,
    const storage_tier_type::type& master_storage_type,
    const std::unordered_map<int, partition_type::type>&    replica_types,
    const std::unordered_map<int, storage_tier_type::type>& storage_types,
    const std::vector<std::vector<propagation_configuration>>& prop_configs,
    const persistence_configs&                                 per_configs,
    std::shared_ptr<update_destination_generator>              update_generator,
    std::shared_ptr<update_enqueuers>                          enqueuers,
    double prob_range_rewriter_threshold, const std::string& in_ss,
    const std::string& in_sm, const std::string& in_part,
    const std::string& out_ss, const std::string& out_sm,
    const std::string& out_part, const rewriter_type re_type ) {
#if 0 // MTODO-BENCH
    if( ( re_type == RANGE_REWRITER ) or
        ( re_type == PROBABILITY_RANGE_REWRITER ) or
        ( re_type == TWITTER_REWRITER ) ) {
        LOG( WARNING ) << "Unsupported rewriter type "
                       << rewriter_type_string( re_type ) << " for tpcc";
        return;
    }

    run_rewriter(
        create_rewriter( re_type, num_sites, tables_metadata, replica_locs,
                         master_type, replica_types,
                         prop_configs, per_configs, update_generator, enqueuers,
                         prob_range_rewriter_threshold ),
        in_ss, in_sm, in_part, out_ss, out_sm, out_part );
#endif
}

void run_smallbank_rewriter(
    int num_sites, const std::vector<table_metadata>&       tables_metadata,
    const std::unordered_map<int, std::unordered_set<int>>& replica_locs,
    const partition_type::type&    master_type,
    const storage_tier_type::type& master_storage_type,
    const std::unordered_map<int, partition_type::type>&    replica_types,
    const std::unordered_map<int, storage_tier_type::type>& storage_types,
    const std::vector<std::vector<propagation_configuration>>& prop_configs,
    const persistence_configs&                                 per_configs,
    std::shared_ptr<update_destination_generator>              update_generator,
    std::shared_ptr<update_enqueuers>                          enqueuers,
    double prob_range_rewriter_threshold, const std::string& in_ss,
    const std::string& in_sm, const std::string& in_part,
    const std::string& out_ss, const std::string& out_sm,
    const std::string& out_part, const rewriter_type re_type ) {
    if( ( re_type == WAREHOUSE_REWRITER ) or ( re_type == TWITTER_REWRITER ) ) {
        LOG( WARNING ) << "Unsupported rewriter type "
                       << rewriter_type_string( re_type ) << " for smallbank";
        return;
    }

    run_rewriter(
        create_rewriter( re_type, num_sites, tables_metadata, replica_locs,
                         master_type, master_storage_type, replica_types,
                         storage_types, prop_configs, per_configs,
                         update_generator, enqueuers,
                         prob_range_rewriter_threshold ),
        in_ss, in_sm, in_part, out_ss, out_sm, out_part );
}

void run_twitter_rewriter(
    int num_sites, const std::vector<table_metadata>&       tables_metadata,
    const std::unordered_map<int, std::unordered_set<int>>& replica_locs,
    const partition_type::type&    master_type,
    const storage_tier_type::type& master_storage_type,
    const std::unordered_map<int, partition_type::type>&    replica_types,
    const std::unordered_map<int, storage_tier_type::type>& storage_types,
    const std::vector<std::vector<propagation_configuration>>& prop_configs,
    const persistence_configs&                                 per_configs,
    std::shared_ptr<update_destination_generator>              update_generator,
    std::shared_ptr<update_enqueuers>                          enqueuers,
    double prob_range_rewriter_threshold, const std::string& in_ss,
    const std::string& in_sm, const std::string& in_part,
    const std::string& out_ss, const std::string& out_sm,
    const std::string& out_part, const rewriter_type re_type ) {
#if 0 // MTODO-BENCH
    if( re_type == WAREHOUSE_REWRITER ) {
        LOG( WARNING ) << "Unsupported rewriter type "
                       << rewriter_type_string( re_type ) << " for twitter";
        return;
    }

    // MTODO-TWITTER (think through optimization)

    run_rewriter(
        create_rewriter( re_type, num_sites, tables_metadata,
                         replica_locs,
                         master_type, replica_types,
                         prop_configs, per_configs, update_generator, enqueuers,
                         prob_range_rewriter_threshold ),
        in_ss, in_sm, in_part, out_ss, out_sm, out_part );
#endif
}

void execute_rewriter(
    int num_sites, const std::vector<table_metadata>&       tables_metadata,
    const std::unordered_map<int, std::unordered_set<int>>& replica_locs,
    const partition_type::type&    master_type,
    const storage_tier_type::type& master_storage_type,
    const std::unordered_map<int, partition_type::type>&    replica_types,
    const std::unordered_map<int, storage_tier_type::type>& storage_types,
    const std::vector<std::vector<propagation_configuration>>& prop_configs,
    const persistence_configs&                                 per_configs,
    std::shared_ptr<update_destination_generator>              update_generator,
    std::shared_ptr<update_enqueuers> enqueuers, const std::string& in_ss,
    const std::string& in_sm, const std::string& in_part,
    const std::string& out_ss, const std::string& out_sm,
    const std::string& out_part, const workload_type w_type,
    const rewriter_type re_type, double prob_range_rewriter_threshold ) {
    DVLOG( 1 ) << "Running rewriter with workload type:"
               << workload_type_string( w_type )
               << ", with rewriter type:" << rewriter_type_string( re_type );
    DVLOG( 1 ) << "Directories: input SS directory:" << in_ss
               << ", input SM directory:" << in_sm
               << ", input PART directory:" << in_part
               << ", output SS directory:" << out_ss
               << ", output SM directory:" << out_sm
               << ", output PART directory:" << out_part;

    switch( w_type ) {
        case YCSB:
            return run_ycsb_rewriter(
                num_sites, tables_metadata, replica_locs, master_type,
                master_storage_type, replica_types, storage_types, prop_configs,
                per_configs, update_generator, enqueuers,
                prob_range_rewriter_threshold, in_ss, in_sm, in_part, out_ss,
                out_sm, out_part, re_type );
        case TPCC:
            return run_tpcc_rewriter(
                num_sites, tables_metadata, replica_locs, master_type,
                master_storage_type, replica_types, storage_types, prop_configs,
                per_configs, update_generator, enqueuers,
                prob_range_rewriter_threshold, in_ss, in_sm, in_part, out_ss,
                out_sm, out_part, re_type );
        case SMALLBANK:
            return run_smallbank_rewriter(
                num_sites, tables_metadata, replica_locs, master_type,
                master_storage_type, replica_types, storage_types, prop_configs,
                per_configs, update_generator, enqueuers,
                prob_range_rewriter_threshold, in_ss, in_sm, in_part, out_ss,
                out_sm, out_part, re_type );
        case TWITTER:
            return run_twitter_rewriter(
                num_sites, tables_metadata, replica_locs, master_type,
                master_storage_type, replica_types, storage_types, prop_configs,
                per_configs, update_generator, enqueuers,
                prob_range_rewriter_threshold, in_ss, in_sm, in_part, out_ss,
                out_sm, out_part, re_type );

        case UNKNOWN_WORKLOAD:
            LOG( WARNING ) << "Trying to run unknown workload type";
            return;
    }

    DVLOG( 1 ) << "Running rewriter with workload type:"
               << workload_type_string( w_type )
               << ", with rewriter type:" << rewriter_type_string( re_type )
               << " okay!";
}

