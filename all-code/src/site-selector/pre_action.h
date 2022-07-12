#pragma once

#include <condition_variable>
#include <memory>
#include <mutex>
#include <vector>

#include "cost_modeller.h"
#include "partition_payload.h"

enum pre_action_type {
    NONE,
    REMASTER,
    ADD_PARTITIONS,
    ADD_REPLICA_PARTITIONS,
    REMOVE_PARTITIONS,
    SPLIT_PARTITION,
    MERGE_PARTITION,
    CHANGE_OUTPUT_DESTINATION,
    CHANGE_PARTITION_TYPE,
    TRANSFER_REMASTER,
};
enum action_outcome { OK, FAILED };

std::string pre_action_type_to_string( const pre_action_type& type );

// if you depend on a partition identifier, then you must depend on earlier
// prior actions, that either modify, or create this partition
class pre_action {
   public:
    pre_action();
    void init( int site, pre_action_type type, void* args, double cost,
               const cost_model_prediction_holder&             model_prediction,
               const std::vector<partition_column_identifier>& relevant_pids,
               const std::vector<std::shared_ptr<pre_action>>& dependencies );
    ~pre_action();

    void mark_complete( const action_outcome& outcome );

    action_outcome wait_to_satisfy_dependencies();

    uint32_t id_;

    int                                      site_;
    pre_action_type                          type_;
    void*                                    args_;
    double                                   cost_;
    cost_model_prediction_holder             model_prediction_;
    std::vector<partition_column_identifier> relevant_pids_;

    std::vector<std::shared_ptr<pre_action>> dependencies_;

   private:
    action_outcome wait_for_completion_or_failure();

    std::mutex              mutex_;
    std::condition_variable cv_;
    bool                    complete_;
    action_outcome          outcome_;
};

struct pre_action_comparator {
    bool operator()( const pre_action& l, const pre_action& r ) {
        return ( l.cost_ > r.cost_ );
    }
};

struct pre_action_shared_ptr_comparator {
    bool operator()( const std::shared_ptr<pre_action>& l,
                     const std::shared_ptr<pre_action>& r ) {
        return ( l->cost_ > r->cost_ );
    }
};

class add_partition_pre_action {
   public:
    add_partition_pre_action(
        int master_site, const std::vector<std::shared_ptr<partition_payload>>&,
        const std::vector<partition_type::type>&    partition_types,
        const std::vector<storage_tier_type::type>& storage_types,
        uint32_t                                    update_destination_slot );
    add_partition_pre_action(
        int master_site, const std::vector<partition_column_identifier>& pids,
        const std::vector<partition_type::type>&    partition_types,
        const std::vector<storage_tier_type::type>& storage_types,
        uint32_t                                    update_destination_slot );

    std::vector<std::shared_ptr<partition_payload>> get_partitions();

    int                                             master_site_;
    std::vector<std::shared_ptr<partition_payload>> partitions_;
    std::vector<partition_column_identifier>        partition_ids_;
    std::vector<partition_type::type>               partition_types_;
    std::vector<storage_tier_type::type>            storage_types_;
    uint32_t                                        update_destination_slot_;
};

class add_replica_partition_pre_action {
   public:
    add_replica_partition_pre_action(
        int master_site, const std::vector<std::shared_ptr<partition_payload>>&,
        const std::vector<partition_type::type>&    partition_types,
        const std::vector<storage_tier_type::type>& storage_types );
    add_replica_partition_pre_action(
        int master_site, const std::vector<partition_column_identifier>& pids,
        const std::vector<partition_type::type>&    partition_types,
        const std::vector<storage_tier_type::type>& storage_types );

    std::vector<std::shared_ptr<partition_payload>> get_partitions();

    int                                             master_site_;
    std::vector<std::shared_ptr<partition_payload>> partitions_;
    std::vector<partition_column_identifier>        partition_ids_;
    std::vector<partition_type::type>               partition_types_;
    std::vector<storage_tier_type::type>            storage_types_;
};

class remaster_partition_pre_action {
   public:
    remaster_partition_pre_action(
        int old_master, int new_master,
        const std::vector<std::shared_ptr<partition_payload>>&,
        const std::vector<uint32_t>& update_destination_slots );
    remaster_partition_pre_action(
        int old_master, int new_master,
        const std::vector<partition_column_identifier>& pids,
        const std::vector<uint32_t>& update_destination_slots );

    std::vector<std::shared_ptr<partition_payload>> get_partitions();

    int                                             old_master_;
    int                                             new_master_;
    std::vector<std::shared_ptr<partition_payload>> partitions_;
    std::vector<partition_column_identifier>        partition_ids_;
    std::vector<uint32_t>                           update_destination_slots_;
};

class split_partition_pre_action {
   public:
    split_partition_pre_action(
        std::shared_ptr<partition_payload> partition,
        const cell_key& split_point, const partition_type::type& low_type,
        const partition_type::type&    high_type,
        const storage_tier_type::type& low_storage_type,
        const storage_tier_type::type& high_storage_type,
        uint32_t                       low_update_destination_slot,
        uint32_t                       high_update_destination_slot );
    split_partition_pre_action(
        const partition_column_identifier& partition_id,
        const cell_key& split_point, const partition_type::type& low_type,
        const partition_type::type&    high_type,
        const storage_tier_type::type& low_storage_type,
        const storage_tier_type::type& high_storage_type,
        uint32_t                       low_update_destination_slot,
        uint32_t                       high_update_destination_slot );

    std::shared_ptr<partition_payload> partition_;
    partition_column_identifier        partition_id_;
    cell_key                           split_point_;

    partition_type::type               low_type_;
    partition_type::type               high_type_;
    storage_tier_type::type            low_storage_type_;
    storage_tier_type::type            high_storage_type_;

    uint32_t low_update_destination_slot_;
    uint32_t high_update_destination_slot_;
};

class merge_partition_pre_action {
   public:
    merge_partition_pre_action(
        const partition_column_identifier& left_pid,
        const partition_column_identifier& right_pid,
        const partition_type::type&        merge_type,
        const storage_tier_type::type&     merge_storage_type,
        uint32_t                           update_destination_slot );

    partition_column_identifier left_pid_;
    partition_column_identifier right_pid_;

    partition_type::type        merge_type_;
    storage_tier_type::type     merge_storage_type_;

    uint32_t                    update_destination_slot_;
};

class change_output_destination_action {
   public:
    change_output_destination_action(
        const std::vector<partition_column_identifier>& pids,
        uint32_t update_destination_slot );
    change_output_destination_action(
        std::vector<std::shared_ptr<partition_payload>> partitions,
        uint32_t update_destination_slot );

    std::vector<std::shared_ptr<partition_payload>> partitions_;
    std::vector<partition_column_identifier>        partition_ids_;
    uint32_t                                        update_destination_slot_;
};

class remove_partition_action {
   public:
    remove_partition_action(
        const std::vector<partition_column_identifier>& pids );
    remove_partition_action(
        std::vector<std::shared_ptr<partition_payload>> partitions );

    std::vector<std::shared_ptr<partition_payload>> partitions_;
    std::vector<partition_column_identifier>        partition_ids_;
};

class change_partition_type_action {
   public:
    change_partition_type_action(
        const std::vector<partition_column_identifier>& pids,
        const std::vector<partition_type::type>&        part_types,
        const std::vector<storage_tier_type::type>&     storage_types );
    change_partition_type_action(
        const std::vector<std::shared_ptr<partition_payload>>& payloads,
        const std::vector<partition_type::type>&               part_types,
        const std::vector<storage_tier_type::type>&            storage_types );

    std::vector<std::shared_ptr<partition_payload>> partitions_;
    std::vector<partition_column_identifier>        partition_ids_;
    std::vector<partition_type::type>               partition_types_;
    std::vector<storage_tier_type::type>            storage_types_;
};

class transfer_remaster_action {
   public:
    transfer_remaster_action(
        int old_master, int new_master,
        const std::vector<partition_column_identifier>&        pids,
        const std::vector<std::shared_ptr<partition_payload>>& partitions,
        const std::vector<partition_type::type>&               partition_types,
        const std::vector<storage_tier_type::type>&            storage_types );

    int                                             old_master_;
    int                                             new_master_;
    std::vector<std::shared_ptr<partition_payload>> partitions_;
    std::vector<partition_column_identifier>        partition_ids_;
    std::vector<partition_type::type>               partition_types_;
    std::vector<storage_tier_type::type>            storage_types_;
};

std::shared_ptr<pre_action> create_add_partition_pre_action(
    int destination, int master_site,
    const std::vector<std::shared_ptr<partition_payload>>& insert_partitions,
    const std::vector<partition_type::type>&               partition_types,
    const std::vector<storage_tier_type::type>&            storage_types,
    uint32_t update_destination_slot );
std::shared_ptr<pre_action> create_add_partition_pre_action(
    int destination, int master_site,
    const std::vector<partition_column_identifier>& add_pids,
    const std::vector<partition_type::type>&        partition_types,
    const std::vector<storage_tier_type::type>&     storage_types,
    const std::vector<std::shared_ptr<pre_action>>& deps, double cost,
    cost_model_prediction_holder& model_prediction,
    uint32_t                      update_destination_slot );

std::shared_ptr<pre_action> create_add_replica_partition_pre_action(
    int destination, int master_site,
    const std::vector<std::shared_ptr<partition_payload>>& insert_partitions,
    const std::vector<partition_type::type>&               partition_types,
    const std::vector<storage_tier_type::type>& storage_types, double cost,
    const cost_model_prediction_holder& model_prediction );

std::shared_ptr<pre_action> create_add_replica_partition_pre_action(
    int destination, int master_site,
    const std::vector<partition_column_identifier>& add_pids,
    const std::vector<partition_type::type>&        partition_types,
    const std::vector<storage_tier_type::type>&     storage_types,
    const std::vector<std::shared_ptr<pre_action>>& deps, double cost,
    const cost_model_prediction_holder& model_prediction );

std::shared_ptr<pre_action> create_remaster_partition_pre_action(
    int old_master, int new_master,
    const std::vector<std::shared_ptr<partition_payload>>& remaster_partitions,
    double op_score, const cost_model_prediction_holder& model_prediction,
    const std::vector<uint32_t> update_destination_slots );
std::shared_ptr<pre_action> create_remaster_partition_pre_action(
    int old_master, int new_master,
    const std::vector<partition_column_identifier>& remaster_pids,
    const std::vector<std::shared_ptr<pre_action>>& deps, double op_score,
    const cost_model_prediction_holder& model_prediction,
    const std::vector<uint32_t>         update_destination_slots );

std::shared_ptr<pre_action> create_split_partition_pre_action(
    int master_site, std::shared_ptr<partition_payload> partition,
    const cell_key& split_point, double cost,
    const cost_model_prediction_holder& model_prediction,
    const partition_type::type& low_type, const partition_type::type& high_type,
    const storage_tier_type::type& low_storage_type,
    const storage_tier_type::type& high_storage_type,
    uint32_t                       low_update_destination_slot,
    uint32_t                       high_update_destination_slot );
std::shared_ptr<pre_action> create_split_partition_pre_action(
    int master_site, const partition_column_identifier& pid,
    const cell_key& split_point, const partition_type::type& low_type,
    const partition_type::type&                     high_type,
    const storage_tier_type::type&                  low_storage_type,
    const storage_tier_type::type&                  high_storage_type,
    const std::vector<std::shared_ptr<pre_action>>& deps, double cost,
    const cost_model_prediction_holder& model_prediction,
    uint32_t                            low_update_destination_slot,
    uint32_t                            high_update_destination_slot );

std::shared_ptr<pre_action> create_merge_partition_pre_action(
    int master_site, const partition_column_identifier& left_pid,
    const partition_column_identifier&              right_pid,
    const partition_column_identifier&              merged_pid,
    const partition_type::type&                     merge_type,
    const storage_tier_type::type&                  merge_storage_type,
    const std::vector<std::shared_ptr<pre_action>>& deps, double cost,
    const cost_model_prediction_holder& model_prediction,
    uint32_t                            update_destination_slot );

std::shared_ptr<pre_action> create_change_output_destination_action(
    int master_site, const std::vector<partition_column_identifier>& pids,
    const std::vector<std::shared_ptr<pre_action>>& deps, double cost,
    const cost_model_prediction_holder& model_prediction,
    uint32_t                            update_destination_slot );
std::shared_ptr<pre_action> create_change_output_destination_action(
    int                                                    master_site,
    const std::vector<std::shared_ptr<partition_payload>>& partitions,
    const std::vector<std::shared_ptr<pre_action>>& deps, double cost,
    const cost_model_prediction_holder& model_prediction,
    uint32_t                            update_destination_slot );

std::shared_ptr<pre_action> create_remove_partition_action(
    int site, const std::vector<std::shared_ptr<partition_payload>>& partitions,
    const std::vector<std::shared_ptr<pre_action>>& deps, double cost,
    const cost_model_prediction_holder& model_prediction );
std::shared_ptr<pre_action> create_remove_partition_action(
    int site, const std::vector<partition_column_identifier>& pids,
    const std::vector<std::shared_ptr<pre_action>>& deps, double cost,
    const cost_model_prediction_holder& model_prediction );

std::shared_ptr<pre_action> create_transfer_remaster_partition_action(
    int old_master, int new_master, const partition_column_identifier& pid,
    std::shared_ptr<partition_payload>              payload,
    const std::vector<partition_type::type>&        partition_types,
    const std::vector<storage_tier_type::type>&     storage_types,
    const std::vector<std::shared_ptr<pre_action>>& deps );
std::shared_ptr<pre_action> create_transfer_remaster_partitions_action(
    int old_master, int new_master,
    const std::vector<partition_column_identifier>&        pids,
    const std::vector<std::shared_ptr<partition_payload>>& payloads,
    const std::vector<partition_type::type>&               partition_types,
    const std::vector<storage_tier_type::type>&            storage_types,
    const std::vector<std::shared_ptr<pre_action>>&        deps );

std::shared_ptr<pre_action> create_change_partition_type_action(
    int site, const std::vector<partition_column_identifier>& pids,
    const std::vector<partition_type::type>&    part_types,
    const std::vector<storage_tier_type::type>& storage_types, double cost,
    const cost_model_prediction_holder&             model_prediction,
    const std::vector<std::shared_ptr<pre_action>>& deps );
std::shared_ptr<pre_action> create_change_partition_type_action(
    int site, const std::vector<std::shared_ptr<partition_payload>>& payloads,
    const std::vector<partition_type::type>&    part_types,
    const std::vector<storage_tier_type::type>& storage_types, double cost,
    const cost_model_prediction_holder&             model_prediction,
    const std::vector<std::shared_ptr<pre_action>>& deps );

