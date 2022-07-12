#include <gflags/gflags.h>
#include <glog/logging.h>

#include "../src/benchmark/benchmark_executor.h"
#include "../src/persistence/persistence_editing_executor.h"

DEFINE_string( input_ss_dir, "~/persistent_ss_in_dir/",
               "The directory of persistent SS data to read" );
DEFINE_string( input_sm_dir, "~/persistent_sm_in_dir/",
               "The directory of persistent SM data to read" );
DEFINE_string( input_part_dir, "~/persistent_part_in_dir/",
               "The directory of persistent partition data to read" );
DEFINE_string( output_ss_dir, "~/persistent_ss_out_dir/",
               "The directory of persistent SS data to write" );
DEFINE_string( output_sm_dir, "~/persistent_sm_out_dir/",
               "The directory of persistent SM data to write" );
DEFINE_string( output_part_dir, "~/persistent_part_out_dir/",
               "The directory of persistent partition data to write" );
DEFINE_string( replica_locs_file, "~/.replica_locs_file.cfg",
               "The set of sites that should be replicated" );
DEFINE_string( replica_types_file, "~/.replica_type_file.cfg",
               "The replica types at sites" );
DEFINE_string( replica_storage_types_file, "~/.replica_storage_type_file.cfg",
               "The replica storage types at sites" );
DEFINE_string( rewritten_master_type, "ROW", "The master partition type" );
DEFINE_string( rewritten_master_storage_type, "MEMORY", "The master storage type" );

std::unordered_map<int, std::unordered_set<int>> build_replica_locs(
    const std::string& file_name, int num_sites ) {
    std::ifstream infile( file_name );

    std::unordered_map<int, std::unordered_set<int>> replica_locs;

    // line format:
    // replicas,

    std::string line_str;
    std::string site_replica_str;
    int site_id = 0;

    for( ;; ) {
        bool ok = (bool) std::getline( infile, line_str, '\n' );
        if( !ok ) {
            break;
        }

        DVLOG( 40 ) << "Line line_str:" << line_str;
        DCHECK( ok );

        std::unordered_set<int> replicas;

        std::stringstream linestream( line_str );

        for( ;; ) {
            bool ok = (bool) std::getline( linestream, site_replica_str, ',' );
            if ( !ok) {
                break;
            }
            DVLOG( 40 ) << "Replica str:" << site_replica_str;
            int replica = std::stoi( site_replica_str );
            DCHECK_LT( replica, num_sites );
            replicas.insert( replica );
        }

        DCHECK_LT( site_id, num_sites );
        replica_locs.emplace( site_id, replicas );

        site_id = site_id + 1;

    }

    return replica_locs;
}

std::unordered_map<int, partition_type::type> build_replica_types(
    const std::string& file_name, int num_sites ) {
    std::ifstream infile( file_name );

    std::unordered_map<int, partition_type::type> replica_types;

    // line format:
    // partition_type (e.g. ROW)

    std::string line_str;
    std::string site_replica_str;
    int         site_id = 0;

    for( ;; ) {
        bool ok = (bool) std::getline( infile, line_str, '\n' );
        if( !ok ) {
            break;
        }

        DVLOG( 40 ) << "Line line_str:" << line_str;
        DCHECK( ok );

        partition_type::type replica_type =
            string_to_partition_type( line_str );

        DCHECK_LT( site_id, num_sites );
        replica_types.emplace( site_id, replica_type );

        site_id = site_id + 1;
    }

    return replica_types;
}

std::unordered_map<int, storage_tier_type::type> build_replica_storage_types(
    const std::string& file_name, int num_sites ) {
    std::ifstream infile( file_name );

    std::unordered_map<int, storage_tier_type::type> replica_types;

    // line format:
    // storage_tier_type (e.g. MEMORY)

    std::string line_str;
    std::string site_replica_str;
    int         site_id = 0;

    for( ;; ) {
        bool ok = (bool) std::getline( infile, line_str, '\n' );
        if( !ok ) {
            break;
        }

        DVLOG( 40 ) << "Line line_str:" << line_str;
        DCHECK( ok );

        storage_tier_type::type replica_type =
            string_to_storage_type( line_str );

        DCHECK_LT( site_id, num_sites );
        replica_types.emplace( site_id, replica_type );

        site_id = site_id + 1;
    }

    return replica_types;
}

std::vector<std::vector<propagation_configuration>> build_prop_configs(
    int num_sites, int num_dests ) {
    int partition = 0;

    std::vector<std::vector<propagation_configuration>> prop_configs;
    for( int site_id = 0; site_id < num_sites; site_id++ ) {
        std::vector<propagation_configuration> site_props;
        for( int dest = 0; dest < num_dests; dest++ ) {

            propagation_configuration config;
            config.type = propagation_type::VECTOR;
            config.partition = partition;

            site_props.push_back( config );

            partition = partition + 1;
        }
        prop_configs.push_back( site_props );
    }

    return prop_configs;
}

void run_rewriter() {
    std::string in_ss = FLAGS_input_ss_dir;
    std::string out_ss = FLAGS_output_ss_dir;
    std::string in_sm = FLAGS_input_sm_dir;
    std::string out_sm = FLAGS_output_sm_dir;
    std::string in_part = FLAGS_input_part_dir;
    std::string out_part = FLAGS_output_part_dir;

    partition_type::type master_type =
        string_to_partition_type( FLAGS_rewritten_master_type );
    storage_tier_type::type master_storage_type =
        string_to_storage_type( FLAGS_rewritten_master_storage_type );

    uint32_t num_sites = k_num_sites;
    uint32_t num_dests = k_num_update_destinations;

    std::vector<table_metadata> tables_metadata =
        get_benchmark_tables_metadata();

    std::unordered_map<int, std::unordered_set<int>> replica_locs =
        build_replica_locs( FLAGS_replica_locs_file, num_sites );

    std::unordered_map<int, partition_type::type> replica_types =
        build_replica_types( FLAGS_replica_types_file, num_sites );
    std::unordered_map<int, storage_tier_type::type> replica_storage_types =
        build_replica_storage_types( FLAGS_replica_storage_types_file,
                                     num_sites );

    std::vector<std::vector<propagation_configuration>> prop_configs =
        build_prop_configs( num_sites, num_dests );
    std::shared_ptr<update_destination_generator> update_generator =
        std::make_shared<update_destination_generator>(
            construct_update_destination_generator_configs( num_dests ) );
    for( int id = 0; id < (int) num_dests; id++ ) {
        update_generator->add_update_destination(
            std::make_shared<no_op_update_destination>( id ) );
    }
    std::shared_ptr<update_enqueuers> enqueuers = make_update_enqueuers();

    execute_rewriter( num_sites, tables_metadata, replica_locs, master_type,
                      master_storage_type, replica_types, replica_storage_types,
                      prop_configs, construct_persistence_configs(),
                      update_generator, enqueuers, in_ss, in_sm, in_part,
                      out_ss, out_sm, out_part );
}

int main( int argc, char **argv ) {
    google::InitGoogleLogging( argv[0] );
    google::ParseCommandLineFlags( &argc, &argv, true );
    google::InstallFailureSignalHandler();

    init_global_state();

    run_rewriter();

    return 0;
}

