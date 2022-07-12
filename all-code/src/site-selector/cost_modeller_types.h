#pragma once

#include <folly/Hash.h>

#include "../common/constants.h"

#include "../common/predictor2.h"

double sum_widths( const std::vector<double>& widths );
double bucketize_double( double in, double bucket_size );
int bucketize_int( int in, int bucket_size );

typedef std::tuple<partition_type::type, partition_type::type,
                   storage_tier_type::type, storage_tier_type::type>
    split_partition_types;
struct split_partition_types_equal_functor {
    bool operator()( const split_partition_types& a1,
                     const split_partition_types& a2 ) const {
        return ( ( std::get<0>( a1 ) == std::get<0>( a2 ) ) and
                 ( std::get<1>( a1 ) == std::get<1>( a2 ) ) and
                 ( std::get<2>( a1 ) == std::get<2>( a2 ) ) and
                 ( std::get<3>( a1 ) == std::get<3>( a2 ) ) );
    }
};

struct split_partition_types_hasher {
    std::size_t operator()( const split_partition_types& a ) const {
        std::size_t seed = 0;
        seed = folly::hash::hash_combine( seed, std::get<0>( a ) );
        seed = folly::hash::hash_combine( seed, std::get<1>( a ) );
        seed = folly::hash::hash_combine( seed, std::get<2>( a ) );
        seed = folly::hash::hash_combine( seed, std::get<3>( a ) );

        return seed;
    }
};

typedef std::tuple<partition_type::type, storage_tier_type::type>
    partition_type_tuple;
struct partition_type_tuple_equal_functor {
    bool operator()( const partition_type_tuple& a1,
                     const partition_type_tuple& a2 ) const {
        return ( ( std::get<0>( a1 ) == std::get<0>( a2 ) ) and
                 ( std::get<1>( a1 ) == std::get<1>( a2 ) ) );
    }
};

struct partition_type_tuple_hasher {
    std::size_t operator()( const partition_type_tuple& a ) const {
        std::size_t seed = 0;
        seed = folly::hash::hash_combine( seed, std::get<0>( a ) );
        seed = folly::hash::hash_combine( seed, std::get<1>( a ) );
        return seed;
    }
};


class site_load_information {
   public:
    site_load_information();
    site_load_information( double write_count, double read_count,
                           double update_count, double cpu_load );

    double write_count_;
    double read_count_;
    double update_count_;
    double cpu_load_;
};

enum cost_model_component_type {
    READ_EXECUTION = 0,
    WRITE_EXECUTION = 1,
    SCAN_EXECUTION = 2,
    LOCK_ACQUISITION = 3,
    COMMIT_SERIALIZE_UPDATE = 4,
    COMMIT_BUILD_SNAPSHOT = 5,
    WAIT_FOR_SERVICE = 6,
    WAIT_FOR_SESSION_VERSION = 7,
    WAIT_FOR_SESSION_SNAPSHOT = 8,
    SITE_OPERATION_COUNT = 9,
    SITE_LOAD = 10,
    MEMORY_ALLOCATION = 11,
    MEMORY_DEALLOCATION = 12,
    MEMORY_ASSIGNMENT = 13,
    EVICT_TO_DISK_EXECUTION = 14,
    PULL_FROM_DISK_EXECUTION = 15,
    READ_DISK_EXECUTION = 16,
    SCAN_DISK_EXECUTION = 17,
    DISTRIBUTED_SCAN = 18,

    UNKNOWN_TYPE = 666,
};

static const cost_model_component_type
    k_cost_model_multi_model_prediction_type =
        cost_model_component_type::SITE_LOAD;

static const partition_type::type k_num_partition_types =
    partition_type::type::SORTED_MULTI_COLUMN;
static const storage_tier_type::type k_num_storage_types =
    storage_tier_type::type::DISK;

std::vector<std::tuple<cost_model_component_type, partition_type::type>>
    get_multiple_cost_model_component_types_from_timer_id( int timer_id );
std::tuple<cost_model_component_type, partition_type::type>
    get_cost_model_component_type_from_timer_id( int timer_id );

std::string get_cost_model_component_type_name(
    const cost_model_component_type& model_type );

int transform_timer_id_to_load_input_position( int timer_id );

class cost_modeller_configs {
   public:
    bool   is_static_model_;
    double learning_rate_;
    double regularization_;
    double bias_regularization_;

    double momentum_;
    double kernel_gamma_;

    uint32_t max_internal_model_size_scalar_;
    uint32_t layer_1_nodes_scalar_;
    uint32_t layer_2_nodes_scalar_;

    double max_time_prediction_scale_;

    double         num_reads_weight_;
    double         num_reads_normalization_;
    double         num_reads_max_input_;
    double         read_bias_;
    predictor_type read_predictor_type_;

    double         num_reads_disk_weight_;
    double         read_disk_bias_;
    predictor_type read_disk_predictor_type_;

    double         scan_num_rows_weight_;
    double         scan_num_rows_normalization_;
    double         scan_num_rows_max_input_;
    double         scan_selectivity_weight_;
    double         scan_selectivity_normalization_;
    double         scan_selectivity_max_input_;
    double         scan_bias_;
    predictor_type scan_predictor_type_;

    double         scan_disk_num_rows_weight_;
    double         scan_disk_selectivity_weight_;
    double         scan_disk_bias_;
    predictor_type scan_disk_predictor_type_;

    double         num_writes_weight_;
    double         num_writes_normalization_;
    double         num_writes_max_input_;
    double         write_bias_;
    predictor_type write_predictor_type_;

    double         lock_weight_;
    double         lock_normalization_;
    double         lock_max_input_;
    double         lock_bias_;
    predictor_type lock_predictor_type_;

    double         commit_serialize_weight_;
    double         commit_serialize_normalization_;
    double         commit_serialize_max_input_;
    double         commit_serialize_bias_;
    predictor_type commit_serialize_predictor_type_;

    double         commit_build_snapshot_weight_;
    double         commit_build_snapshot_normalization_;
    double         commit_build_snapshot_max_input_;
    double         commit_build_snapshot_bias_;
    predictor_type commit_build_snapshot_predictor_type_;

    double         wait_for_service_weight_;
    double         wait_for_service_normalization_;
    double         wait_for_service_max_input_;
    double         wait_for_service_bias_;
    predictor_type wait_for_service_predictor_type_;

    double         wait_for_session_version_weight_;
    double         wait_for_session_version_normalization_;
    double         wait_for_session_version_max_input_;
    double         wait_for_session_version_bias_;
    predictor_type wait_for_session_version_predictor_type_;

    double         wait_for_session_snapshot_weight_;
    double         wait_for_session_snapshot_normalization_;
    double         wait_for_session_snapshot_max_input_;
    double         wait_for_session_snapshot_bias_;
    predictor_type wait_for_session_snapshot_predictor_type_;

    double         site_load_prediction_write_weight_;
    double         site_load_prediction_write_normalization_;
    double         site_load_prediction_write_max_input_;
    double         site_load_prediction_read_weight_;
    double         site_load_prediction_read_normalization_;
    double         site_load_prediction_read_max_input_;
    double         site_load_prediction_update_weight_;
    double         site_load_prediction_update_normalization_;
    double         site_load_prediction_update_max_input_;
    double         site_load_prediction_bias_;
    double         site_load_prediction_max_scale_;
    predictor_type site_load_predictor_type_;

    double         site_operation_count_weight_;
    double         site_operation_count_normalization_;
    double         site_operation_count_max_input_;
    double         site_operation_count_max_scale_;
    double         site_operation_count_bias_;
    predictor_type site_operation_count_predictor_type_;

    double         distributed_scan_max_weight_;
    double         distributed_scan_min_weight_;
    double         distributed_scan_normalization_;
    double         distributed_scan_max_input_;
    double         distributed_scan_bias_;
    predictor_type distributed_scan_predictor_type_;

    double memory_num_rows_normalization_;
    double memory_num_rows_max_input_;

    double         memory_allocation_num_rows_weight_;
    double         memory_allocation_bias_;
    predictor_type memory_allocation_predictor_type_;

    double         memory_deallocation_num_rows_weight_;
    double         memory_deallocation_bias_;
    predictor_type memory_deallocation_predictor_type_;

    double         memory_assignment_num_rows_weight_;
    double         memory_assignment_bias_;
    predictor_type memory_assignment_predictor_type_;

    double disk_num_rows_normalization_;
    double disk_num_rows_max_input_;

    double         evict_to_disk_num_rows_weight_;
    double         evict_to_disk_bias_;
    predictor_type evict_to_disk_predictor_type_;

    double         pull_from_disk_num_rows_weight_;
    double         pull_from_disk_bias_;
    predictor_type pull_from_disk_predictor_type_;

    double widths_weight_;
    double widths_normalization_;
    double widths_max_input_;

    double wait_for_session_remaster_default_pct_;
};

cost_modeller_configs construct_cost_modeller_configs(
    bool     is_static_model = k_cost_model_is_static_model,
    double   learning_rate = k_cost_model_learning_rate,
    double   regularization = k_cost_model_regularization,
    double   bias_regularization = k_cost_model_bias_regularization,
    double   momentum = k_cost_model_momentum,
    double   kernel_gamma = k_cost_model_kernel_gamma,
    uint32_t max_internal_model_size_scalar =
        k_cost_model_max_internal_model_size_scalar,
    uint32_t layer_1_nodes_scalar = k_cost_model_layer_1_nodes_scalar,
    uint32_t layer_2_nodes_scalar = k_cost_model_layer_2_nodes_scalar,
    double   max_time_prediction_scale = k_cost_model_log_error_threshold,
    double   num_reads_weight = k_cost_model_num_reads_weight,
    double   num_reads_normalization = k_cost_model_num_reads_normalization,
    double   num_reads_max_input = k_cost_model_num_reads_max_input,
    double   read_bias = k_cost_model_read_bias,
    predictor_type read_predictor_type = k_cost_model_read_predictor_type,
    double         num_reads_disk_weight = k_cost_model_num_reads_disk_weight,
    double         read_disk_bias = k_cost_model_read_disk_bias,
    predictor_type read_disk_predictor_type =
        k_cost_model_read_disk_predictor_type,
    double scan_num_rows_weight = k_cost_model_scan_num_rows_weight,
    double scan_num_rows_normalization =
        k_cost_model_scan_num_rows_normalization,
    double scan_num_rows_max_input = k_cost_model_scan_num_rows_max_input,
    double scan_selectivity_weight = k_cost_model_scan_selectivity_weight,
    double scan_selectivity_normalization =
        k_cost_model_scan_selectivity_normalization,
    double scan_selectivity_max_input = k_cost_model_scan_selectivity_max_input,
    double scan_bias = k_cost_model_scan_bias,
    predictor_type scan_predictor_type = k_cost_model_scan_predictor_type,
    double scan_disk_num_rows_weight = k_cost_model_scan_disk_num_rows_weight,
    double scan_disk_selectivity_weight =
        k_cost_model_scan_disk_selectivity_weight,
    double         scan_disk_bias = k_cost_model_scan_disk_bias,
    predictor_type scan_disk_predictor_type =
        k_cost_model_scan_disk_predictor_type,
    double num_writes_weight = k_cost_model_num_writes_weight,
    double num_writes_normalization = k_cost_model_num_writes_normalization,
    double num_writes_max_input = k_cost_model_num_writes_max_input,
    double write_bias = k_cost_model_write_bias,
    predictor_type write_predictor_type = k_cost_model_write_predictor_type,
    double         lock_weight = k_cost_model_lock_weight,
    double         lock_normalization = k_cost_model_lock_normalization,
    double         lock_max_input = k_cost_model_lock_max_input,
    double         lock_bias = k_cost_model_lock_bias,
    predictor_type lock_predictor_type = k_cost_model_lock_predictor_type,
    double commit_serialize_weight = k_cost_model_commit_serialize_weight,
    double commit_serialize_normalization =
        k_cost_model_commit_serialize_normalization,
    double commit_serialize_max_input = k_cost_model_commit_serialize_max_input,
    double commit_serialize_bias = k_cost_model_commit_serialize_bias,
    predictor_type commit_serialize_predictor_type =
        k_cost_model_commit_serialize_predictor_type,
    double commit_build_snapshot_weight =
        k_cost_model_commit_build_snapshot_weight,
    double commit_build_snapshot_normalization =
        k_cost_model_commit_build_snapshot_normalization,
    double commit_build_snapshot_max_input =
        k_cost_model_commit_build_snapshot_max_input,
    double commit_build_snapshot_bias = k_cost_model_commit_build_snapshot_bias,
    predictor_type commit_build_snapshot_predictor_type =
        k_cost_model_commit_build_snapshot_predictor_type,
    double wait_for_service_weight = k_cost_model_wait_for_service_weight,
    double wait_for_service_normalization =
        k_cost_model_wait_for_service_normalization,
    double wait_for_service_max_input = k_cost_model_wait_for_service_max_input,
    double wait_for_service_bias = k_cost_model_wait_for_service_bias,
    predictor_type wait_for_service_predictor_type =
        k_cost_model_wait_for_service_predictor_type,
    double wait_for_session_version_weight =
        k_cost_model_wait_for_session_version_weight,
    double wait_for_session_version_normalization =
        k_cost_model_wait_for_session_version_normalization,
    double wait_for_session_version_max_input =
        k_cost_model_wait_for_session_version_max_input,
    double wait_for_session_version_bias =
        k_cost_model_wait_for_session_version_bias,
    predictor_type wait_for_session_version_predictor_type =
        k_cost_model_wait_for_session_version_predictor_type,
    double wait_for_session_snapshot_weight =
        k_cost_model_wait_for_session_snapshot_weight,
    double wait_for_session_snapshot_normalization =
        k_cost_model_wait_for_session_snapshot_normalization,
    double wait_for_session_snapshot_max_input =
        k_cost_model_wait_for_session_snapshot_max_input,
    double wait_for_session_snapshot_bias =
        k_cost_model_wait_for_session_snapshot_bias,
    predictor_type wait_for_session_snapshot_predictor_type =
        k_cost_model_wait_for_session_snapshot_predictor_type,
    double site_load_prediction_write_weight =
        k_cost_model_site_load_prediction_write_weight,
    double site_load_prediction_write_normalization =
        k_cost_model_site_load_prediction_write_normalization,
    double site_load_prediction_write_max_input =
        k_cost_model_site_load_prediction_write_max_input,
    double site_load_prediction_read_weight =
        k_cost_model_site_load_prediction_read_weight,
    double site_load_prediction_read_normalization =
        k_cost_model_site_load_prediction_read_normalization,
    double site_load_prediction_read_max_input =
        k_cost_model_site_load_prediction_read_max_input,
    double site_load_prediction_update_weight =
        k_cost_model_site_load_prediction_update_weight,
    double site_load_prediction_update_normalization =
        k_cost_model_site_load_prediction_update_normalization,
    double site_load_prediction_update_max_input =
        k_cost_model_site_load_prediction_update_max_input,
    double site_load_prediction_bias = k_cost_model_site_load_prediction_bias,
    double site_load_prediction_max_scale =
        k_cost_model_site_load_prediction_max_scale,
    predictor_type site_load_predictor_type =
        k_cost_model_site_load_predictor_type,
    double site_operation_count_weight =
        k_cost_model_site_operation_count_weight,
    double site_operation_count_normalization =
        k_cost_model_site_operation_count_normalization,
    double site_operation_count_max_input =
        k_cost_model_site_operation_count_max_input,
    double site_operation_count_max_scale =
        k_cost_model_site_operation_count_max_scale,
    double site_operation_count_bias = k_cost_model_site_operation_count_bias,
    predictor_type site_operation_count_predictor_type =
        k_cost_model_site_operation_count_predictor_type,
    double distributed_scan_max_weight =
        k_cost_model_distributed_scan_max_weight,
    double distributed_scan_min_weight =
        k_cost_model_distributed_scan_min_weight,
    double distributed_scan_normalization =
        k_cost_model_distributed_scan_normalization,
    double distributed_scan_max_input = k_cost_model_distributed_scan_max_input,
    double distributed_scan_bias = k_cost_model_distributed_scan_bias,
    predictor_type distributed_scan_predictor_type =
        k_cost_model_distributed_scan_predictor_type,
    double memory_num_rows_normalization =
        k_cost_model_memory_num_rows_normalization,
    double memory_num_rows_max_input = k_cost_model_memory_num_rows_max_input,
    double memory_allocation_num_rows_weight =
        k_cost_model_memory_allocation_num_rows_weight,
    double         memory_allocation_bias = k_cost_model_memory_allocation_bias,
    predictor_type memory_allocation_predictor_type =
        k_cost_model_memory_allocation_predictor_type,
    double memory_deallocation_num_rows_weight =
        k_cost_model_memory_deallocation_num_rows_weight,
    double memory_deallocation_bias = k_cost_model_memory_deallocation_bias,
    predictor_type memory_deallocation_predictor_type =
        k_cost_model_memory_deallocation_predictor_type,
    double memory_assignment_num_rows_weight =
        k_cost_model_memory_assignment_num_rows_weight,
    double         memory_assignment_bias = k_cost_model_memory_assignment_bias,
    predictor_type memory_assignment_predictor_type =
        k_cost_model_memory_assignment_predictor_type,
    double disk_num_rows_normalization =
        k_cost_model_disk_num_rows_normalization,
    double disk_num_rows_max_input = k_cost_model_disk_num_rows_max_input,
    double evict_to_disk_num_rows_weight =
        k_cost_model_evict_to_disk_num_rows_weight,
    double         evict_to_disk_bias = k_cost_model_evict_to_disk_bias,
    predictor_type evict_to_disk_predictor_type =
        k_cost_model_evict_to_disk_predictor_type,
    double pull_from_disk_num_rows_weight =
        k_cost_model_pull_from_disk_num_rows_weight,
    double         pull_from_disk_bias = k_cost_model_pull_from_disk_bias,
    predictor_type pull_from_disk_predictor_type =
        k_cost_model_pull_from_disk_predictor_type,
    double widths_weight = k_cost_model_widths_weight,
    double widths_normalization = k_cost_model_widths_normalization,
    double widths_max_input = k_cost_model_widths_max_input,
    double wait_for_session_remaster_default_pct =
        k_cost_model_wait_for_session_remaster_default_pct );

std::ostream& operator<<( std::ostream&                os,
                          const cost_modeller_configs& configs );
std::ostream& operator<<( std::ostream&                os,
                          const site_load_information& load_info );

class transaction_prediction_stats {
   public:
    transaction_prediction_stats();
    transaction_prediction_stats( const partition_type::type&    part_type,
                                  const storage_tier_type::type& storage_type,
                                  double contention, double num_updates_needed,
                                  const std::vector<double>& avg_cell_widths,
                                  uint32_t num_entries, bool is_scan,
                                  double scan_selectivity, bool is_point_read,
                                  uint32_t num_point_reads,
                                  bool     is_point_update,
                                  uint32_t num_point_updates );

    transaction_prediction_stats bucketize(
        const transaction_prediction_stats& other ) const;

    partition_type::type part_type_;
    storage_tier_type::type storage_type_;

    double contention_;

    double num_updates_needed_;

    std::vector<double> avg_cell_widths_;
    uint32_t            num_entries_;

    bool   is_scan_;
    double scan_selectivity_;

    bool     is_point_read_;
    uint32_t num_point_reads_;

    bool     is_point_update_;
    uint32_t num_point_updates_;
};
std::ostream& operator<<( std::ostream&                       os,
                          const transaction_prediction_stats& stats );

struct transaction_prediction_stats_equal_functor {
    bool operator()( const transaction_prediction_stats& a1,
                     const transaction_prediction_stats& a2 ) const {
        return ( ( a1.part_type_ == a2.part_type_ ) and
                 ( a1.storage_type_ == a2.storage_type_ ) and
                 ( a1.contention_ == a2.contention_ ) and
                 ( a1.avg_cell_widths_ == a2.avg_cell_widths_ ) and
                 ( a1.num_entries_ == a2.num_entries_ ) and
                 ( a1.is_scan_ == a2.is_scan_ ) and
                 ( a1.scan_selectivity_ == a2.scan_selectivity_ ) and
                 ( a1.is_point_read_ == a2.is_point_read_ ) and
                 ( a1.num_point_reads_ == a2.num_point_reads_ ) and
                 ( a1.is_point_update_ == a2.is_point_update_ ) and
                 ( a1.num_point_updates_ == a2.num_point_updates_ ) );
    }
};

struct transaction_prediction_stats_hasher {
    std::size_t operator()( const transaction_prediction_stats& a ) const {
        std::size_t seed = 0;
        seed = folly::hash::hash_combine( seed, a.part_type_ );
        seed = folly::hash::hash_combine( seed, a.storage_type_ );
        seed = folly::hash::hash_combine( seed, a.contention_ );
        seed =
            folly::hash::hash_combine( seed, sum_widths( a.avg_cell_widths_ ) );
        seed = folly::hash::hash_combine( seed, a.num_entries_ );
        seed = folly::hash::hash_combine( seed, a.is_scan_ );
        seed = folly::hash::hash_combine( seed, a.scan_selectivity_ );
        seed = folly::hash::hash_combine( seed, a.is_point_read_ );
        seed = folly::hash::hash_combine( seed, a.num_point_reads_ );
        seed = folly::hash::hash_combine( seed, a.is_point_update_ );
        seed = folly::hash::hash_combine( seed, a.num_point_updates_ );
        return seed;
    }
};

class add_replica_stats {
   public:
    add_replica_stats();
    add_replica_stats( const partition_type::type&    source_type,
                       const storage_tier_type::type& source_storage_type,
                       const partition_type::type&    dest_type,
                       const storage_tier_type::type& dest_storage_type,
                       double                         contention,
                       const std::vector<double>&     avg_cell_widths,
                       uint32_t                       num_entries );

    add_replica_stats bucketize( const add_replica_stats& other ) const;

    partition_type::type source_type_;
    storage_tier_type::type source_storage_type_;
    partition_type::type dest_type_;
    storage_tier_type::type dest_storage_type_;

    double              contention_;
    std::vector<double> avg_cell_widths_;
    uint32_t            num_entries_;
};

struct add_replica_stats_equal_functor {
    bool operator()( const add_replica_stats& a1,
                     const add_replica_stats& a2 ) const {
        return ( ( a1.source_type_ == a2.source_type_ ) and
                 ( a1.source_storage_type_ == a2.source_storage_type_ ) and
                 ( a1.dest_type_ == a2.dest_type_ ) and
                 ( a1.dest_storage_type_ == a2.dest_storage_type_ ) and
                 ( a1.contention_ == a2.contention_ ) and
                 ( a1.avg_cell_widths_ == a2.avg_cell_widths_ ) and
                 ( a1.num_entries_ == a2.num_entries_ ) );
    }
};

struct add_replica_stats_hasher {
    std::size_t operator()( const add_replica_stats& a ) const {
        std::size_t seed = 0;
        seed = folly::hash::hash_combine( seed, a.source_type_ );
        seed = folly::hash::hash_combine( seed, a.source_storage_type_ );
        seed = folly::hash::hash_combine( seed, a.dest_type_ );
        seed = folly::hash::hash_combine( seed, a.dest_storage_type_ );
        seed = folly::hash::hash_combine( seed, a.contention_ );
        seed =
            folly::hash::hash_combine( seed, sum_widths( a.avg_cell_widths_ ) );
        seed = folly::hash::hash_combine( seed, a.num_entries_ );
        return seed;
    }
};

class remove_replica_stats {
   public:
    remove_replica_stats();
    remove_replica_stats( const partition_type::type& part_type,
                          double                      contention,
                          const std::vector<double>&  avg_cell_widths,
                          uint32_t                    num_entries );

    partition_type::type part_type_;

    double               contention_;
    std::vector<double>  avg_cell_widths_;
    uint32_t             num_entries_;
};

class remaster_stats {
   public:
    remaster_stats();
    remaster_stats( const partition_type::type& source_type,
                    const partition_type::type& dest_type, double num_updates,
                    double contention );

    partition_type::type source_type_;
    partition_type::type dest_type_;
    double               num_updates_;
    double               contention_;
};

class split_stats {
   public:
    split_stats();
    split_stats( const partition_type::type&    ori_type,
                 const partition_type::type&    left_type,
                 const partition_type::type&    right_type,
                 const storage_tier_type::type& ori_storage_type,
                 const storage_tier_type::type& left_storage_type,
                 const storage_tier_type::type& right_storage_type,
                 double contention, const std::vector<double>& avg_cell_widths,
                 uint32_t num_entries, uint32_t split_point );
    split_stats bucketize( const split_stats& other ) const;

    partition_type::type ori_type_;
    partition_type::type left_type_;
    partition_type::type right_type_;

    storage_tier_type::type ori_storage_type_;
    storage_tier_type::type left_storage_type_;
    storage_tier_type::type right_storage_type_;

    double              contention_;
    std::vector<double> avg_cell_widths_;
    uint32_t            num_entries_;
    uint32_t            split_point_;
};
struct split_stats_equal_functor {
    bool operator()( const split_stats& a1, const split_stats& a2 ) const {
        return ( ( a1.ori_type_ == a2.ori_type_ ) and
                 ( a1.left_type_ == a2.left_type_ ) and
                 ( a1.right_type_ == a2.right_type_ ) and
                 ( a1.ori_storage_type_ == a2.ori_storage_type_ ) and
                 ( a1.left_storage_type_ == a2.left_storage_type_ ) and
                 ( a1.right_storage_type_ == a2.right_storage_type_ ) and
                 ( a1.contention_ == a2.contention_ ) and
                 ( a1.avg_cell_widths_ == a2.avg_cell_widths_ ) and
                 ( a1.num_entries_ == a2.num_entries_ ) and
                 ( a1.split_point_ == a2.split_point_ ) );
    }
};

struct split_stats_hasher {
    std::size_t operator()( const split_stats& a ) const {
        std::size_t seed = 0;
        seed = folly::hash::hash_combine( seed, a.ori_type_ );
        seed = folly::hash::hash_combine( seed, a.left_type_ );
        seed = folly::hash::hash_combine( seed, a.right_type_ );

        seed = folly::hash::hash_combine( seed, a.ori_storage_type_ );
        seed = folly::hash::hash_combine( seed, a.left_storage_type_ );
        seed = folly::hash::hash_combine( seed, a.right_storage_type_ );

        seed = folly::hash::hash_combine( seed, a.contention_ );
        seed =
            folly::hash::hash_combine( seed, sum_widths( a.avg_cell_widths_ ) );
        seed = folly::hash::hash_combine( seed, a.num_entries_ );
        seed = folly::hash::hash_combine( seed, a.split_point_ );
        return seed;
    }
};

class merge_stats {
   public:
    merge_stats();
    merge_stats( const partition_type::type&    left_type,
                 const partition_type::type&    right_type,
                 const partition_type::type&    merge_type,
                 const storage_tier_type::type& left_storage_type,
                 const storage_tier_type::type& right_storage_type,
                 const storage_tier_type::type& merge_storage_type,
                 double left_contention, double right_contention,
                 const std::vector<double>& left_cell_widths,
                 const std::vector<double>& right_cell_widths,
                 uint32_t left_num_entries, uint32_t right_num_entries );

    merge_stats bucketize( const merge_stats& other ) const;

    partition_type::type left_type_;
    partition_type::type right_type_;
    partition_type::type merge_type_;

    storage_tier_type::type left_storage_type_;
    storage_tier_type::type right_storage_type_;
    storage_tier_type::type merge_storage_type_;

    double              left_contention_;
    double              right_contention_;

    std::vector<double> left_cell_widths_;
    std::vector<double> right_cell_widths_;

    uint32_t            left_num_entries_;
    uint32_t            right_num_entries_;
};

struct merge_stats_equal_functor {
    bool operator()( const merge_stats& a1, const merge_stats& a2 ) const {
        return ( ( a1.left_type_ == a2.left_type_ ) and
                 ( a1.right_type_ == a2.right_type_ ) and
                 ( a1.merge_type_ == a2.merge_type_ ) and
                 ( a1.left_storage_type_ == a2.left_storage_type_ ) and
                 ( a1.right_storage_type_ == a2.right_storage_type_ ) and
                 ( a1.merge_storage_type_ == a2.merge_storage_type_ ) and
                 ( a1.left_contention_ == a2.left_contention_ ) and
                 ( a1.right_contention_ == a2.right_contention_ ) and
                 ( a1.left_cell_widths_ == a2.left_cell_widths_ ) and
                 ( a1.right_cell_widths_ == a2.right_cell_widths_ ) and
                 ( a1.left_num_entries_ == a2.left_num_entries_ ) and
                 ( a1.right_num_entries_ == a2.right_num_entries_ ) );
    }
};

struct merge_stats_hasher {
    std::size_t operator()( const merge_stats& a ) const {
        std::size_t seed = 0;
        seed = folly::hash::hash_combine( seed, a.left_type_ );
        seed = folly::hash::hash_combine( seed, a.right_type_ );
        seed = folly::hash::hash_combine( seed, a.merge_type_ );

        seed = folly::hash::hash_combine( seed, a.left_storage_type_ );
        seed = folly::hash::hash_combine( seed, a.right_storage_type_ );
        seed = folly::hash::hash_combine( seed, a.merge_storage_type_ );

        seed = folly::hash::hash_combine( seed, a.left_contention_ );
        seed = folly::hash::hash_combine( seed, a.right_contention_ );
        seed = folly::hash::hash_combine( seed,
                                          sum_widths( a.left_cell_widths_ ) );
        folly::hash::hash_combine( seed, sum_widths( a.right_cell_widths_ ) );
        seed = folly::hash::hash_combine( seed, a.left_num_entries_ );
        seed = folly::hash::hash_combine( seed, a.right_num_entries_ );
        return seed;
    }
};

class change_types_stats {
   public:
    change_types_stats();
    change_types_stats( const partition_type::type&    ori_type,
                        const partition_type::type&    new_type,
                        const storage_tier_type::type& ori_storage_type,
                        const storage_tier_type::type& new_storage_type,
                        double                         contention,
                        const std::vector<double>&     avg_cell_widths,
                        uint32_t                       num_entries );

    change_types_stats bucketize( const change_types_stats& other ) const;

    partition_type::type ori_type_;
    partition_type::type new_type_;

    storage_tier_type::type ori_storage_type_;
    storage_tier_type::type new_storage_type_;

    double              contention_;
    std::vector<double> avg_cell_widths_;
    uint32_t            num_entries_;
};

struct change_types_stats_equal_functor {
    bool operator()( const change_types_stats& a1,
                     const change_types_stats& a2 ) const {
        return ( ( a1.ori_type_ == a2.ori_type_ ) and
                 ( a1.new_type_ == a2.new_type_ ) and
                 ( a1.ori_storage_type_ == a2.ori_storage_type_ ) and
                 ( a1.new_storage_type_ == a2.new_storage_type_ ) and
                 ( a1.contention_ == a2.contention_ ) and
                 ( a1.avg_cell_widths_ == a2.avg_cell_widths_ ) and
                 ( a1.num_entries_ == a2.num_entries_ ) );
    }
};

struct change_types_stats_hasher {
    std::size_t operator()( const change_types_stats& a ) const {
        std::size_t seed = 0;
        seed = folly::hash::hash_combine( seed, a.ori_type_ );
        seed = folly::hash::hash_combine( seed, a.new_type_ );

        seed = folly::hash::hash_combine( seed, a.ori_storage_type_ );
        seed = folly::hash::hash_combine( seed, a.new_storage_type_ );

        seed = folly::hash::hash_combine( seed, a.contention_ );
        seed = folly::hash::hash_combine( seed,
                                          sum_widths( a.avg_cell_widths_ ) );
        seed = folly::hash::hash_combine( seed, a.num_entries_ );
        return seed;
    }
};


