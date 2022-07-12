#pragma once
#include <vector>

#include "../distributions/adaptive_reservoir.h"
#include "../distributions/distributions.h"
#include "partition_data_location_table.h"
#include "tracking_summary.h"
#include <atomic>
#include <memory>

class adaptive_reservoir_sampler {

   public:
    adaptive_reservoir_sampler(
        size_t reservoir_size, float orig_mult, float weight_incr,
        float                                          decay_rate,
        std::shared_ptr<partition_data_location_table> data_loc_tab );

    /* Check if we should sample a tracking summary */
    bool should_sample();

    /* Add the tracking sample to the reservoir (should call should_sample()
       first).
       Note that the sample can have references to partitions that no longer
       exist
       due to merging or splitting.

       If the base partition we are referencing no longer exists, we don't add
       those references. However, we do no verification of destination
       references
       because otherwise we would need to do a boatload of tracking and undo ops
       on split/merge to keep destination references valid anyways. The idea is
       that these will eventually be sampled out or fixed by the optimizer. */
    virtual void put_new_sample( tracking_summary&& sample );

    void remove_counts_for_sample( tracking_summary& sample );
    void add_counts_for_sample( tracking_summary& sample );
    void decay();

    std::vector<tracking_summary>& get_reservoir_ref() { return reservoir_; }

   protected:
    void remove_across_transaction_correlation_sample( tracking_summary& sample );
    void remove_within_transaction_correlation_sample( tracking_summary& sample );
    void remove_sample_access_counts( tracking_summary& sample );

    void add_across_transaction_correlation_sample( tracking_summary& sample );
    void add_within_transaction_correlation_sample( tracking_summary& sample );
    void add_sample_access_counts( tracking_summary& sample );

    void modify_across_partition_accesses(
        const transaction_partition_accesses orig_partition_accesses_of_one_type,
        const transaction_partition_accesses subsequent_partition_acceses_of_one_type,
        bool orig_accesses_are_writes,
        bool subsequent_accesses_are_writes,
        bool is_add
    );

    void modify_within_transitions_of_single_type(
        const transaction_partition_accesses &partition_accesses,
        bool is_write,
        bool is_add
    );

    void modify_within_transitions_for_mixed_type(
        const transaction_partition_accesses &orig_partition_accesses,
        const transaction_partition_accesses &dest_partition_accesses,
        bool source_is_write,
        bool is_add
    );


    void modify_within_transitions_for_rw_wr_types(
        const transaction_partition_accesses &read_partition_accesses,
        const transaction_partition_accesses &write_partition_accesses,
        bool is_add
    );



    void modify_across_transaction_correlation_sample( tracking_summary& sample, bool is_add );
    void modify_within_transaction_correlation_sample( tracking_summary& sample, bool is_add );
    void modify_sample_access_counts( tracking_summary& sample, bool is_add );

    std::vector<tracking_summary>                  reservoir_;
    adaptive_reservoir                             adaptive_;
    std::shared_ptr<partition_data_location_table> data_loc_tab_;
};
