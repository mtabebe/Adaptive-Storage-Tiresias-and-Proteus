#pragma once

#include "../../common/partition_funcs.h"
#include "../db/partition_metadata.h"
#include "update_propagation_configs.h"

class update_enqueuer_partition_subscription_information {
   public:
    update_enqueuer_partition_subscription_information(
        const partition_column_identifier& pid,
        const std::unordered_map<
            propagation_configuration, int64_t /*offset*/,
            propagation_configuration_ignore_offset_hasher,
            propagation_configuration_ignore_offset_equal_functor>&
            begin_subscription_configs );

    void add_new_subscription_offset_information(
        const propagation_configuration& config, int64_t offset,
        bool replace_with_new_if_prev_smaller );
    void remove_subscription_offset_information(
        const propagation_configuration& config, int64_t upper_bound_offset );

    int64_t get_subscription_offset(
        const propagation_configuration& config ) const;
    std::unordered_map<propagation_configuration, int64_t,
                       propagation_configuration_ignore_offset_hasher,
                       propagation_configuration_ignore_offset_equal_functor>
        get_subscription_offsets() const;

    void add_in_subscription_offset_information(
        const propagation_configuration& config, int64_t offset );
    void add_new_source_information( const propagation_configuration& config,
                                     int64_t                          offset );

   private:
    partition_column_identifier pid_;
    folly::ConcurrentHashMap<
        propagation_configuration, int64_t /*offset*/,
        propagation_configuration_ignore_offset_hasher,
        propagation_configuration_ignore_offset_equal_functor>
        begin_subscription_configs_;
};

class update_enqueuer_subscription_information {
  public:
   update_enqueuer_subscription_information();
   ~update_enqueuer_subscription_information();

   void create_table( const table_metadata& metadata );
   void set_expected_number_of_tables( uint32_t expected_num_tables );

   void add_partition_subscription_information(
       const partition_column_identifier& pid,
       const std::unordered_map<
           propagation_configuration, int64_t /*offset*/,
           propagation_configuration_ignore_offset_hasher,
           propagation_configuration_ignore_offset_equal_functor>&
           begin_subscription_configs );
   void remove_partition_subscription_information(
       const partition_column_identifier& pid );

   void add_source( const propagation_configuration& prop, int64_t offset );
   void remove_source( const propagation_configuration& prop,
                       int64_t                          upper_bound_offset );

   int64_t get_offset_of_partition_for_config(
       const partition_column_identifier&      pid,
       const propagation_configuration& prop ) const;
   int64_t get_max_offset_of_partitions_for_config(
       const std::vector<partition_column_identifier>& pids,
       const propagation_configuration&         prop ) const;

  private:
   std::vector<partition_column_identifier_folly_concurrent_hash_map_t<
       std::shared_ptr<update_enqueuer_partition_subscription_information>>*>
            pid_to_configs_;
   uint32_t expected_num_tables_;
};

