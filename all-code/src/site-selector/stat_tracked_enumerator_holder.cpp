#include "stat_tracked_enumerator_holder.h"

stat_tracked_enumerator_decision_holder::
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
            change_partition_type_enumerator_decision_tracker )
    : add_replica_type_enumerator_decision_tracker_(
          add_replica_type_enumerator_decision_tracker ),
      merge_partition_type_enumerator_decision_tracker_(
          merge_partition_type_enumerator_decision_tracker ),
      split_partition_type_enumerator_decision_tracker_(
          split_partition_type_enumerator_decision_tracker ),
      new_partition_type_enumerator_decision_tracker_(
          new_partition_type_enumerator_decision_tracker ),
      change_partition_type_enumerator_decision_tracker_(
          change_partition_type_enumerator_decision_tracker ) {}

void stat_tracked_enumerator_decision_holder::clear_decisions() {
    add_replica_type_enumerator_decision_tracker_.clear_decisions();
    merge_partition_type_enumerator_decision_tracker_.clear_decisions();
    split_partition_type_enumerator_decision_tracker_.clear_decisions();
    new_partition_type_enumerator_decision_tracker_.clear_decisions();
    change_partition_type_enumerator_decision_tracker_.clear_decisions();
}

void stat_tracked_enumerator_decision_holder::add_replica_types_decision(
    const add_replica_stats& stats, const partition_type_tuple& decision ) {
    add_replica_type_enumerator_decision_tracker_.add_decision( stats,
                                                                decision );
}

void stat_tracked_enumerator_decision_holder::add_merge_type_decision(
    const merge_stats& merge_stat, const partition_type_tuple& decision ) {
    merge_partition_type_enumerator_decision_tracker_.add_decision( merge_stat,
                                                                    decision );
}

void stat_tracked_enumerator_decision_holder::add_split_types_decision(
    const split_stats& split_stat, const split_partition_types& decision ) {
    split_partition_type_enumerator_decision_tracker_.add_decision( split_stat,
                                                                    decision );
}

void stat_tracked_enumerator_decision_holder::add_new_types_decision(
    const transaction_prediction_stats& txn_stat,
    const partition_type_tuple&         decision ) {
    new_partition_type_enumerator_decision_tracker_.add_decision( txn_stat,
                                                                  decision );
}

void stat_tracked_enumerator_decision_holder::add_change_types_decision(
    const change_types_stats& stat, const partition_type_tuple& decision ) {
    change_partition_type_enumerator_decision_tracker_.add_decision( stat,
                                                                     decision );
}

stat_tracked_enumerator_holder::stat_tracked_enumerator_holder(
    const stat_tracked_enumerator_configs& configs )
    : configs_( configs ),
      add_replica_type_enumerator_( nullptr ),
      merge_partition_type_enumerator_( nullptr ),
      split_partition_type_enumerator_( nullptr ),
      new_partition_type_enumerator_( nullptr ),
      change_partition_type_enumerator_( nullptr ) {
    add_replica_stats add_replica_bucket = add_replica_stats(
        partition_type::type::ROW, storage_tier_type::type::MEMORY,
        partition_type::type::ROW, storage_tier_type::type::MEMORY,
        configs_.contention_bucket_, configs_.cell_width_bucket_,
        configs_.num_entries_bucket_ );

    std::vector<partition_type_tuple> acceptable_types;
    for( const auto& part : configs_.acceptable_partition_types_ ) {
        for( const auto& storage : configs_.acceptable_storage_types_ ) {
            acceptable_types.emplace_back( std::make_tuple<>( part, storage ) );
        }
    }

    add_replica_type_enumerator_ =
        std::make_unique<add_replica_type_stat_tracked_enumerator>(
            add_replica_bucket, acceptable_types, configs_.track_stats_,
            configs_.prob_of_returning_default_, configs_.min_threshold_,
            configs_.min_count_threshold_ );

    merge_stats merge_partition_bucket =
        merge_stats( partition_type::type::ROW, partition_type::type::ROW,
                     partition_type::type::ROW,
                     storage_tier_type::type::MEMORY,
                     storage_tier_type::type::MEMORY,
                     storage_tier_type::type::MEMORY,
                     configs_.contention_bucket_,
                     configs_.contention_bucket_, configs_.cell_width_bucket_,
                     configs_.cell_width_bucket_, configs_.num_entries_bucket_,
                     configs_.num_entries_bucket_ );

    merge_partition_type_enumerator_ =
        std::make_unique<merge_partition_type_stat_tracked_enumerator>(
            merge_partition_bucket, acceptable_types, configs_.track_stats_,
            configs_.prob_of_returning_default_, configs_.min_threshold_,
            configs_.min_count_threshold_ );

    split_stats split_partition_bucket = split_stats(
        partition_type::type::ROW, partition_type::type::ROW,
        partition_type::type::ROW, storage_tier_type::type::MEMORY,
        storage_tier_type::type::MEMORY, storage_tier_type::type::MEMORY,
        configs_.contention_bucket_, configs_.cell_width_bucket_,
        configs_.num_entries_bucket_, 1 /* split point */ );

    std::vector<split_partition_types> acceptable_split_types;
    for( const auto& left_split_type : configs_.acceptable_partition_types_ ) {
        for( const auto& right_split_type :
             configs_.acceptable_partition_types_ ) {
            for( const auto& left_store_type :
                 configs_.acceptable_storage_types_ ) {
                for( const auto& right_store_type :
                     configs_.acceptable_storage_types_ ) {
                    acceptable_split_types.push_back(
                        std::make_tuple( left_split_type, right_split_type,
                                         left_store_type, right_store_type ) );
                }
            }
        }
    }

    split_partition_type_enumerator_ =
        std::make_unique<split_partition_type_stat_tracked_enumerator>(
            split_partition_bucket, acceptable_split_types,
            configs_.track_stats_, configs_.prob_of_returning_default_,
            configs_.min_threshold_, configs_.min_count_threshold_ );

    transaction_prediction_stats change_partition_bucket =
        transaction_prediction_stats(
            partition_type::type::ROW, storage_tier_type::type::MEMORY,
            configs_.num_updates_needed_bucket_, configs_.contention_bucket_,
            configs_.cell_width_bucket_, configs_.num_entries_bucket_,
            true /* is scan */, configs_.scan_selectivity_bucket_,
            true /* is point read */, configs_.num_point_reads_bucket_,
            true /* is point update */, configs_.num_point_updates_bucket_ );

    std::vector<partition_type_tuple> acceptable_new_types;
    for( const auto& part : configs_.acceptable_new_partition_types_ ) {
        for( const auto& store : configs_.acceptable_new_storage_types_ ) {
            acceptable_new_types.emplace_back(
                std::make_tuple<>( part, store ) );
        }
    }

    new_partition_type_enumerator_ =
        std::make_unique<new_partition_type_stat_tracked_enumerator>(
            change_partition_bucket, acceptable_new_types,
            configs_.track_stats_, configs_.prob_of_returning_default_,
            configs_.min_threshold_, configs_.min_count_threshold_ );

    change_types_stats change_type_bucket = change_types_stats(
        partition_type::type::ROW, partition_type::type::ROW,
        storage_tier_type::type::MEMORY, storage_tier_type::type::MEMORY,
        configs_.contention_bucket_, configs_.cell_width_bucket_,
        configs_.num_entries_bucket_ );

    change_partition_type_enumerator_ =
        std::make_unique<change_partition_type_stat_tracked_enumerator>(
            change_type_bucket, acceptable_types, configs_.track_stats_,
            configs_.prob_of_returning_default_, configs_.min_threshold_,
            configs_.min_count_threshold_ );
}

std::vector<partition_type_tuple>
    stat_tracked_enumerator_holder::get_add_replica_types(
        const add_replica_stats& stats ) const {
    return add_replica_type_enumerator_->get_enumerations_from_stats( stats );
}
void stat_tracked_enumerator_holder::add_replica_types_decision(
    const add_replica_stats& stats, const partition_type_tuple& decision ) {
    add_replica_type_enumerator_->add_decision( stats, decision );
}

std::vector<partition_type_tuple>
    stat_tracked_enumerator_holder::get_merge_type(
        const merge_stats& merge_stat ) const {
    return merge_partition_type_enumerator_->get_enumerations_from_stats(
        merge_stat );
}
void stat_tracked_enumerator_holder::add_merge_type_decision(
    const merge_stats& merge_stat, const partition_type_tuple& decision ) {
    merge_partition_type_enumerator_->add_decision( merge_stat, decision );
}

std::vector<split_partition_types> stat_tracked_enumerator_holder::get_split_types(
    const split_stats& split_stat ) const {
    return split_partition_type_enumerator_->get_enumerations_from_stats(
        split_stat );
}
void stat_tracked_enumerator_holder::add_split_types_decision(
    const split_stats& split_stat, const split_partition_types& decision ) {
    split_partition_type_enumerator_->add_decision( split_stat, decision );
}

std::vector<partition_type_tuple> stat_tracked_enumerator_holder::get_new_types(
    const transaction_prediction_stats& txn_stat ) const {
    return new_partition_type_enumerator_->get_enumerations_from_stats(
        txn_stat );
}
void stat_tracked_enumerator_holder::add_new_types_decision(
    const transaction_prediction_stats& txn_stat,
    const partition_type_tuple&         decision ) {
    new_partition_type_enumerator_->add_decision( txn_stat, decision );
}

std::vector<partition_type_tuple>
    stat_tracked_enumerator_holder::get_change_types(
        const change_types_stats& stat ) const {
    return change_partition_type_enumerator_->get_enumerations_from_stats(
        stat );
}
void stat_tracked_enumerator_holder::add_change_types_decision(
    const change_types_stats& stat, const partition_type_tuple& decision ) {
    return change_partition_type_enumerator_->add_decision( stat, decision );
}

void stat_tracked_enumerator_holder::add_decisions(
    const stat_tracked_enumerator_decision_holder& holder ) {
    if( !configs_.track_stats_ ) {
        return;
    }
    add_replica_type_enumerator_->add_decisions(
        holder.add_replica_type_enumerator_decision_tracker_ );
    merge_partition_type_enumerator_->add_decisions(
        holder.merge_partition_type_enumerator_decision_tracker_ );
    split_partition_type_enumerator_->add_decisions(
        holder.split_partition_type_enumerator_decision_tracker_ );
    new_partition_type_enumerator_->add_decisions(
        holder.new_partition_type_enumerator_decision_tracker_ );
    change_partition_type_enumerator_->add_decisions(
        holder.change_partition_type_enumerator_decision_tracker_ );
}

void stat_tracked_enumerator_holder::add_decisions(
    const std::shared_ptr<stat_tracked_enumerator_decision_holder>& holder ) {
    if( !configs_.track_stats_ ) {
        return;
    }
    add_replica_type_enumerator_->add_decisions(
        holder->add_replica_type_enumerator_decision_tracker_ );
    merge_partition_type_enumerator_->add_decisions(
        holder->merge_partition_type_enumerator_decision_tracker_ );
    split_partition_type_enumerator_->add_decisions(
        holder->split_partition_type_enumerator_decision_tracker_ );
    new_partition_type_enumerator_->add_decisions(
        holder->new_partition_type_enumerator_decision_tracker_ );
    change_partition_type_enumerator_->add_decisions(
        holder->change_partition_type_enumerator_decision_tracker_ );
}

stat_tracked_enumerator_decision_holder
    stat_tracked_enumerator_holder::construct_decision_holder() const {
    return stat_tracked_enumerator_decision_holder(
        add_replica_type_enumerator_->construct_decision_tracker(),
        merge_partition_type_enumerator_->construct_decision_tracker(),
        split_partition_type_enumerator_->construct_decision_tracker(),
        new_partition_type_enumerator_->construct_decision_tracker(),
        change_partition_type_enumerator_->construct_decision_tracker() );
}

std::shared_ptr<stat_tracked_enumerator_decision_holder>
    stat_tracked_enumerator_holder::construct_decision_holder_ptr() const {
    return std::make_shared<stat_tracked_enumerator_decision_holder>(
        add_replica_type_enumerator_->construct_decision_tracker(),
        merge_partition_type_enumerator_->construct_decision_tracker(),
        split_partition_type_enumerator_->construct_decision_tracker(),
        new_partition_type_enumerator_->construct_decision_tracker(),
        change_partition_type_enumerator_->construct_decision_tracker() );
}

