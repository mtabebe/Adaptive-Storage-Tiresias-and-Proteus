#include "adaptive_reservoir_sampler.h"

#include "../common/perf_tracking.h"
#include "tracking_summary.h"
#include "partition_data_location_table.h"
#include <glog/logging.h>

adaptive_reservoir_sampler::adaptive_reservoir_sampler(
    size_t reservoir_size, float orig_mult, float weight_incr, float decay_rate,
    std::shared_ptr<partition_data_location_table> data_loc_tab )
    : reservoir_( reservoir_size ),
      adaptive_( reservoir_size, orig_mult, weight_incr, decay_rate ),
      data_loc_tab_( data_loc_tab ) {}

bool adaptive_reservoir_sampler::should_sample() {
  return adaptive_.should_sample();
}

void adaptive_reservoir_sampler::decay() { return adaptive_.decay(); }

void adaptive_reservoir_sampler::remove_across_transaction_correlation_sample(
    tracking_summary &sample ) {
    start_timer( ARS_REMOVE_FROM_ACROSS_CORR_TIMER_ID );
    modify_across_transaction_correlation_sample( sample, false );
    stop_timer( ARS_REMOVE_FROM_ACROSS_CORR_TIMER_ID );
}

static void stratify_partition_accesses(
    const transaction_partition_accesses &part_accesses,
    transaction_partition_accesses &      read_partition_accesses,
    transaction_partition_accesses &      write_partition_accesses ) {
    for( const auto &part_access : part_accesses.partition_accesses_ ) {
        if( part_access.is_write_ ) {
            write_partition_accesses.partition_accesses_.push_back( part_access );
        } else {
            read_partition_accesses.partition_accesses_.push_back( part_access );
        }
    }
}

#define modify_transitions_via_partition_accesses( within_across, tracking_type, is_add, partition_accesses ) \
    do { \
        if( is_add ) { \
            partition->within_across ## _ ## tracking_type ## _txn_statistics_.add_transaction_accesses( partition_accesses ); \
        } else { \
            partition->within_across ## _ ## tracking_type ## _txn_statistics_.remove_transaction_accesses( partition_accesses ); \
        }\
    } while( 0 );

void adaptive_reservoir_sampler::modify_across_partition_accesses(
    const transaction_partition_accesses orig_partition_accesses_of_one_type,
    const transaction_partition_accesses subsequent_partition_acceses_of_one_type,
    bool orig_accesses_are_writes,
    bool subsequent_accesses_are_writes,
    bool is_add
) {

    for( const auto &orig_part_access : orig_partition_accesses_of_one_type.partition_accesses_ ) {
        std::shared_ptr<partition_payload> partition =
            data_loc_tab_->get_partition( orig_part_access.partition_id_,
                                          partition_lock_mode::no_lock );

        if( partition != nullptr ) {
            // W->?
            if( orig_accesses_are_writes ) {
                // W->W
                if( subsequent_accesses_are_writes ) {
                    modify_transitions_via_partition_accesses(
                        across, ww, is_add,
                        subsequent_partition_acceses_of_one_type );
                    // W->R
                } else {
                    modify_transitions_via_partition_accesses(
                        across, wr, is_add,
                        subsequent_partition_acceses_of_one_type );
                }
                // R->?
            } else {
                // R->W
                if( subsequent_accesses_are_writes ) {
                    modify_transitions_via_partition_accesses(
                        across, rw, is_add,
                        subsequent_partition_acceses_of_one_type );
                    // R->R
                } else {
                    modify_transitions_via_partition_accesses(
                        across, rr, is_add,
                        subsequent_partition_acceses_of_one_type );
                }
            }
            partition->unlock( partition_lock_mode::no_lock );
        }
    }

}

void adaptive_reservoir_sampler::modify_across_transaction_correlation_sample( tracking_summary& sample, bool is_add ) {
    //Map from everything in the orig partition accesses to all unique partition accesses in subsequent
    //transactions
    across_transaction_access_correlations &across_txn_accesses = sample.across_txn_accesses_;

    transaction_partition_accesses orig_partition_accesses = across_txn_accesses.origin_accesses_;

    transaction_partition_accesses orig_read_partition_accesses;
    transaction_partition_accesses orig_write_partition_accesses;

    stratify_partition_accesses(
        orig_partition_accesses,
        orig_read_partition_accesses,
        orig_write_partition_accesses
    );

    transaction_partition_accesses subsequent_partition_accesses = across_txn_accesses.get_unique_subsequent_txn_accesses();

    transaction_partition_accesses subsequent_read_partition_accesses;
    transaction_partition_accesses subsequent_write_partition_accesses;

    stratify_partition_accesses(
        subsequent_partition_accesses,
        subsequent_read_partition_accesses,
        subsequent_write_partition_accesses
    );

    modify_across_partition_accesses(
        orig_read_partition_accesses,
        subsequent_read_partition_accesses,
        false,
        false,
        is_add
    );

    modify_across_partition_accesses(
        orig_read_partition_accesses,
        subsequent_write_partition_accesses,
        false,
        true,
        is_add
    );

    modify_across_partition_accesses(
        orig_write_partition_accesses,
        subsequent_read_partition_accesses,
        true,
        false,
        is_add
    );

    modify_across_partition_accesses(
        orig_write_partition_accesses,
        subsequent_write_partition_accesses,
        true,
        true,
        is_add
    );
}

void adaptive_reservoir_sampler::add_across_transaction_correlation_sample( tracking_summary& sample ) {
    start_timer( ARS_ADD_TO_ACROSS_CORR_TIMER_ID );
    modify_across_transaction_correlation_sample( sample, true );
    stop_timer( ARS_ADD_TO_ACROSS_CORR_TIMER_ID );
}

void adaptive_reservoir_sampler::remove_within_transaction_correlation_sample( tracking_summary& sample ) {
    start_timer( ARS_REMOVE_FROM_WITHIN_CORR_TIMER_ID );
    modify_within_transaction_correlation_sample( sample, false );
    stop_timer( ARS_REMOVE_FROM_WITHIN_CORR_TIMER_ID );
}

void adaptive_reservoir_sampler::modify_within_transitions_of_single_type(
    const transaction_partition_accesses &partition_accesses,
    bool is_write,
    bool is_add
) {
    for( const auto &part_access : partition_accesses.partition_accesses_ ) {
        std::shared_ptr<partition_payload> partition =
            data_loc_tab_->get_partition( part_access.partition_id_,
                                          partition_lock_mode::no_lock );
        if( partition != nullptr ) {
            transaction_partition_accesses dup_accesses( partition_accesses );
            dup_accesses.drop_partition_access( part_access );

            if( is_write ) {
                modify_transitions_via_partition_accesses( within, ww, is_add, dup_accesses );
            } else {
                modify_transitions_via_partition_accesses( within, rr, is_add, dup_accesses );
            }
            partition->unlock( partition_lock_mode::no_lock );
        }
    }
}

void adaptive_reservoir_sampler::modify_within_transitions_for_mixed_type(
    const transaction_partition_accesses &orig_partition_accesses,
    const transaction_partition_accesses &dest_partition_accesses,
    bool source_is_write,
    bool is_add
) {
    for( const auto &orig_part_access : orig_partition_accesses.partition_accesses_ ) {
        std::shared_ptr<partition_payload> partition =
            data_loc_tab_->get_partition( orig_part_access.partition_id_,
                                          partition_lock_mode::no_lock );
        if( partition != nullptr ) {
            // W->R Transitions
            if( source_is_write ) {
                modify_transitions_via_partition_accesses( within, wr, is_add, dest_partition_accesses );
            // R -> W transitions
            } else {
                modify_transitions_via_partition_accesses( within, rw, is_add, dest_partition_accesses );
            }
            partition->unlock( partition_lock_mode::no_lock );
        }
    }
}

void adaptive_reservoir_sampler::modify_within_transitions_for_rw_wr_types(
    const transaction_partition_accesses &read_partition_accesses,
    const transaction_partition_accesses &write_partition_accesses,
    bool is_add
) {

    // RW Transitions
    modify_within_transitions_for_mixed_type(
        read_partition_accesses,
        write_partition_accesses,
        false,
        is_add
    );

    // WR Transitions
    modify_within_transitions_for_mixed_type(
        write_partition_accesses,
        read_partition_accesses,
        true,
        is_add
    );

}


void adaptive_reservoir_sampler::modify_within_transaction_correlation_sample( tracking_summary& sample, bool is_add ) {
    const transaction_partition_accesses &part_accesses = sample.txn_partition_accesses_;

    transaction_partition_accesses read_partition_accesses;
    transaction_partition_accesses write_partition_accesses;
    stratify_partition_accesses(
        part_accesses,
        read_partition_accesses,
        write_partition_accesses
    );

    modify_within_transitions_of_single_type( read_partition_accesses, false, is_add );
    modify_within_transitions_of_single_type( write_partition_accesses, true, is_add );

    modify_within_transitions_for_rw_wr_types(
        read_partition_accesses,
        write_partition_accesses,
        is_add
    );

}

void adaptive_reservoir_sampler::add_within_transaction_correlation_sample( tracking_summary& sample ) {
    start_timer( ARS_ADD_TO_WITHIN_CORR_TIMER_ID );
    modify_within_transaction_correlation_sample( sample, true );
    stop_timer( ARS_ADD_TO_WITHIN_CORR_TIMER_ID );
}

void adaptive_reservoir_sampler::remove_sample_access_counts( tracking_summary& summary ) {
    start_timer( ARS_REMOVE_SKEW_COUNTS_FROM_SITES_TIMER_ID );
    transaction_partition_accesses &part_accesses = summary.txn_partition_accesses_;
    for( const auto &part_access : part_accesses.partition_accesses_ ) {
        if( part_access.is_write_ ) {
            data_loc_tab_->decrease_sample_based_site_write_access_count(
                part_access.site_, 1 );
        } else {
            data_loc_tab_->decrease_sample_based_site_read_access_count(
                part_access.site_, 1 );
        }
        std::shared_ptr<partition_payload> partition =
            data_loc_tab_->get_partition( part_access.partition_id_,
                                          partition_lock_mode::no_lock );
        if( partition != nullptr ) {
            if( part_access.is_write_ ) {
                partition->sample_based_write_accesses_--;
            } else {
                partition->sample_based_read_accesses_--;
            }
            partition->unlock( partition_lock_mode::no_lock );
        }
    }

    stop_timer( ARS_REMOVE_SKEW_COUNTS_FROM_SITES_TIMER_ID );
}

void adaptive_reservoir_sampler::remove_counts_for_sample( tracking_summary& sample ) {
    if( !sample.is_placeholder_sample() ) {
        DVLOG( 50 ) << "Removing a sample!";
        remove_across_transaction_correlation_sample( sample );
        remove_within_transaction_correlation_sample( sample );
    }
}

void adaptive_reservoir_sampler::add_counts_for_sample( tracking_summary& sample ) {
    DCHECK( !sample.is_placeholder_sample() );
    add_across_transaction_correlation_sample( sample );
    add_within_transaction_correlation_sample( sample );
}

void adaptive_reservoir_sampler::put_new_sample( tracking_summary&& sample ) {
    start_timer( ARS_PUT_NEW_SAMPLE_TIMER_ID );

    int draw = adaptive_.rand_dist_.get_uniform_int( 0, reservoir_.size() );
    add_counts_for_sample( sample );

    remove_counts_for_sample( reservoir_[ draw ] );

    reservoir_[ draw ] = std::move( sample );

    stop_timer( ARS_PUT_NEW_SAMPLE_TIMER_ID );
}
