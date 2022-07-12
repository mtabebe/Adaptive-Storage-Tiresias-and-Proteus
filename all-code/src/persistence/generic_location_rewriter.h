#pragma once

#include "../distributions/distributions.h"
#include "data_location_rewriter_interface.h"

class generic_location_hash_based_editor
    : public data_location_rewriter_interface {
   public:
    generic_location_hash_based_editor(
        const std::vector<table_metadata>& tables_metadata, int num_sites,
        const std::vector<std::vector<propagation_configuration>>& prop_configs,
        const persistence_configs&                                 per_configs,
        std::shared_ptr<update_destination_generator> update_generator,
        std::shared_ptr<update_enqueuers>             enqueuers,
        const std::unordered_map<int, std::unordered_set<int>>& replica_locs,
        const partition_type::type&    master_type,
        const storage_tier_type::type& master_storage_type,
        const std::unordered_map<int, partition_type::type>&    replica_types,
        const std::unordered_map<int, storage_tier_type::type>& storage_types );
    ~generic_location_hash_based_editor();

    void rewrite_partition_locations(
        partition_locations_map& partition_locations );

   private:
    std::unordered_map<int, std::unordered_set<int>> replica_locs_;

    partition_type::type    master_type_;
    storage_tier_type::type master_storage_type_;

    std::unordered_map<int, partition_type::type>    replica_types_;
    std::unordered_map<int, storage_tier_type::type> storage_types_;
};

class generic_single_master_based_editor
    : public data_location_rewriter_interface {
   public:
    generic_single_master_based_editor(
        const std::vector<table_metadata>& tables_metadata, int num_sites,
        const std::vector<std::vector<propagation_configuration>>& prop_configs,
        const persistence_configs&                                 per_configs,
        std::shared_ptr<update_destination_generator> update_generator,
        std::shared_ptr<update_enqueuers> enqueuers, bool replicate_fully,
        const partition_type::type&    master_type,
        const storage_tier_type::type& master_storage_type,
        const std::unordered_map<int, partition_type::type>&    replica_types,
        const std::unordered_map<int, storage_tier_type::type>& storage_types );
    ~generic_single_master_based_editor();

    void rewrite_partition_locations(
        partition_locations_map& partition_locations );

   private:
    bool replicate_fully_;

    std::unordered_map<int, std::unordered_set<int>> replica_locs_;

    partition_type::type    master_type_;
    storage_tier_type::type master_storage_type_;

    std::unordered_map<int, partition_type::type>    replica_types_;
    std::unordered_map<int, storage_tier_type::type> storage_types_;
};

class generic_range_based_editor : public data_location_rewriter_interface {
   public:
    generic_range_based_editor(
        const std::vector<table_metadata>& tables_metadata, int num_sites,
        const std::vector<std::vector<propagation_configuration>>& prop_configs,
        const persistence_configs&                                 per_configs,
        std::shared_ptr<update_destination_generator> update_generator,
        std::shared_ptr<update_enqueuers>             enqueuers,
        const std::unordered_map<int, std::unordered_set<int>>& replica_locs,
        const partition_type::type&    master_type,
        const storage_tier_type::type& master_storage_type,
        const std::unordered_map<int, partition_type::type>&    replica_types,
        const std::unordered_map<int, storage_tier_type::type>& storage_types,
        bool is_fixed_range, double probability_threshold );
    ~generic_range_based_editor();

    void rewrite_partition_locations(
        partition_locations_map& partition_locations );

   private:
    std::unordered_map<int, std::unordered_set<int>> replica_locs_;

    partition_type::type    master_type_;
    storage_tier_type::type master_storage_type_;

    std::unordered_map<int, partition_type::type>    replica_types_;
    std::unordered_map<int, storage_tier_type::type> storage_types_;

    bool          is_fixed_range_;
    double        probability_threshold_;
    distributions dist_;
};

class generic_no_op_based_editor : public data_location_rewriter_interface {
   public:
    generic_no_op_based_editor(
        const std::vector<table_metadata>& tables_metadata, int num_sites,
        const std::vector<std::vector<propagation_configuration>>& prop_configs,
        const persistence_configs&                                 per_configs,
        std::shared_ptr<update_destination_generator> update_generator,
        std::shared_ptr<update_enqueuers>             enqueuers );
    ~generic_no_op_based_editor();

    void rewrite_partition_locations(
        partition_locations_map& partition_locations );

   private:
};
