#pragma once

#include "stat_tracked_enumerator_types.h"

class stat_tracked_enumerator_decision_holder {
   public:
    stat_tracked_enumerator_decision_holder(
        const add_replica_type_stat_tracked_enumerator_decision_tracker&
            add_replica_type_enumerator_decision_tracker,
        const merge_partition_type_stat_tracked_enumerator_decision_tracker&
            merge_partition_type_enumerator_decision_tracker,
        const split_partition_type_stat_tracked_enumerator_decision_tracker&
            split_partition_type_enumerator_decision_tracker,
        const new_partition_type_stat_tracked_enumerator_decision_tracker&
            new_partition_type_enumerator_decision_tracker,
        const change_partition_type_stat_tracked_enumerator_decision_tracker&
            change_partition_type_enumerator_decision_tracker );

    void clear_decisions();

    void add_replica_types_decision( const add_replica_stats&    stats,
                                     const partition_type_tuple& decision );

    void add_merge_type_decision( const merge_stats&          merge_stat,
                                  const partition_type_tuple& decision );

    void add_split_types_decision( const split_stats&           split_stat,
                                   const split_partition_types& decision );

    void add_new_types_decision( const transaction_prediction_stats& txn_stat,
                                 const partition_type_tuple&         decision );

    void add_change_types_decision( const change_types_stats&   stat,
                                    const partition_type_tuple& decision );

    add_replica_type_stat_tracked_enumerator_decision_tracker
        add_replica_type_enumerator_decision_tracker_;
    merge_partition_type_stat_tracked_enumerator_decision_tracker
        merge_partition_type_enumerator_decision_tracker_;
    split_partition_type_stat_tracked_enumerator_decision_tracker
        split_partition_type_enumerator_decision_tracker_;
    new_partition_type_stat_tracked_enumerator_decision_tracker
        new_partition_type_enumerator_decision_tracker_;
    change_partition_type_stat_tracked_enumerator_decision_tracker
        change_partition_type_enumerator_decision_tracker_;
};

class stat_tracked_enumerator_holder {
   public:
    stat_tracked_enumerator_holder(
        const stat_tracked_enumerator_configs& configs );

    std::vector<partition_type_tuple> get_add_replica_types(
        const add_replica_stats& stats ) const;
    void add_replica_types_decision( const add_replica_stats&    stats,
                                     const partition_type_tuple& decision );

    std::vector<partition_type_tuple> get_merge_type(
        const merge_stats& merge_stat ) const;
    void add_merge_type_decision( const merge_stats&          merge_stat,
                                  const partition_type_tuple& decision );

    std::vector<split_partition_types> get_split_types(
        const split_stats& split_stat ) const;
    void add_split_types_decision( const split_stats&           split_stat,
                                   const split_partition_types& decision );

    std::vector<partition_type_tuple> get_new_types(
        const transaction_prediction_stats& txn_stat ) const;
    void add_new_types_decision( const transaction_prediction_stats& txn_stat,
                                 const partition_type_tuple&         decision );

    std::vector<partition_type_tuple> get_change_types(
        const change_types_stats& stat ) const;
    void add_change_types_decision( const change_types_stats&   stat,
                                    const partition_type_tuple& decision );

    void add_decisions( const stat_tracked_enumerator_decision_holder& holder );
    void add_decisions( const std::shared_ptr<
                        stat_tracked_enumerator_decision_holder>& holder );
    stat_tracked_enumerator_decision_holder construct_decision_holder() const;
    std::shared_ptr<stat_tracked_enumerator_decision_holder>
        construct_decision_holder_ptr() const;

   private:
    stat_tracked_enumerator_configs configs_;

    std::unique_ptr<add_replica_type_stat_tracked_enumerator>
        add_replica_type_enumerator_;
    std::unique_ptr<merge_partition_type_stat_tracked_enumerator>
        merge_partition_type_enumerator_;
    std::unique_ptr<split_partition_type_stat_tracked_enumerator>
        split_partition_type_enumerator_;
    std::unique_ptr<new_partition_type_stat_tracked_enumerator>
        new_partition_type_enumerator_;

    std::unique_ptr<change_partition_type_stat_tracked_enumerator>
        change_partition_type_enumerator_;
};

