#pragma once

#include <glog/logging.h>

#define stat_tracked_enumerator_decision_tracker_types                \
    template <typename B, typename E, typename BHash, typename BPred, \
              typename EHash, typename EPred>
#define stat_tracked_enumerator_decision_tracker_T \
    stat_tracked_enumerator_decision_tracker<B, E, BHash, BPred, EHash, EPred>

#define stat_tracked_enumerator_types                                 \
    template <typename B, typename E, typename BHash, typename BPred, \
              typename EHash, typename EPred>
#define stat_tracked_enumerator_T \
    stat_tracked_enumerator<B, E, BHash, BPred, EHash, EPred>

#define stat_tracked_enumerator_entry_types template <typename B>
#define stat_tracked_enumerator_entry_T stat_tracked_enumerator_entry<B>

stat_tracked_enumerator_decision_tracker_types
    stat_tracked_enumerator_decision_tracker_T::
        stat_tracked_enumerator_decision_tracker(
            bool track_stats,
            const std::unordered_map<E, uint32_t, EHash, EPred>&
                     enumerator_to_pos,
            const B& bucketizer )
    : counts_(),
      totals_(),
      enumerator_to_pos_( enumerator_to_pos ),
      track_stats_( track_stats ),
      bucketizer_( bucketizer ) {}

stat_tracked_enumerator_decision_tracker_types void
    stat_tracked_enumerator_decision_tracker_T::add_decision(
        const B& stats, const E& decision ) {
    if( !track_stats_ ) {
        return;
    }
    auto     b_stat = bucketizer_.bucketize( stats );
    auto     pos_found = enumerator_to_pos_.find( decision );
    uint32_t pos = pos_found->second;

    auto found = counts_.find( b_stat );
    if ( found == counts_.end()) {
        totals_.emplace( b_stat, 1 );

        std::vector<uint32_t> vec_counts( enumerator_to_pos_.size(), 0 );
        vec_counts.at( pos ) = 1;
        counts_.emplace( b_stat, vec_counts );
    } else {
        DCHECK_EQ( enumerator_to_pos_.size(), found->second.size() );
        found->second.at( pos ) = found->second.at( pos ) + 1;

        auto total_found = totals_.find( b_stat );
        total_found->second = total_found->second + 1;
    }
}
stat_tracked_enumerator_decision_tracker_types void
    stat_tracked_enumerator_decision_tracker_T::clear_decisions() {
    if( !track_stats_ ) {
        return;
    }
    counts_.clear();
    totals_.clear();
}

stat_tracked_enumerator_entry_types
    stat_tracked_enumerator_entry_T::stat_tracked_enumerator_entry(
        const B& entry, uint32_t num_entries )
    : stat_(),
      total_count_( 0 ),
      per_pos_count_( num_entries ),
      default_( num_entries, 0 ) {
    for( uint32_t pos = 0; pos < num_entries; pos++ ) {
        per_pos_count_.at( pos ) = 0;
        default_.at( pos ) = pos;
    }
}

stat_tracked_enumerator_entry_types void
    stat_tracked_enumerator_entry_T::add_decision( uint32_t pos ) {

    DCHECK_LT( pos, per_pos_count_.size() );
    total_count_.fetch_add( 1 );
    per_pos_count_.at( pos ).fetch_add( 1 );
}

stat_tracked_enumerator_entry_types void
    stat_tracked_enumerator_entry_T::add_decisions(
        uint32_t total_count, const std::vector<uint32_t>& counts ) {

    total_count_.fetch_add( total_count );

    DCHECK_EQ( counts.size(), per_pos_count_.size() );

    uint32_t accum = 0;

    for( uint32_t pos = 0; pos < counts.size(); pos++ ) {
        if( counts.at( pos ) > 0 ) {
            per_pos_count_.at( pos ).fetch_add( counts.at( pos ) );
            accum += counts.at( pos );
        }
    }

    DCHECK_EQ( total_count, accum );
}

stat_tracked_enumerator_entry_types std::vector<uint32_t>
                                    stat_tracked_enumerator_entry_T::get_decisions_above_threshold(
        double threshold, uint64_t count_threshold ) const {
    std::vector<uint32_t> ret;

    uint64_t total = total_count_;
    if( total < count_threshold ) {
        return default_;
    }
    for( uint32_t pos = 0; pos < per_pos_count_.size(); pos++ ) {
        uint64_t cnt = per_pos_count_.at( pos );
        double   pct = ( (double) cnt ) / ( (double) total );
        if( pct >= threshold ) {
            ret.emplace_back( pos );
        }
    }

    return ret;
}

stat_tracked_enumerator_types
    stat_tracked_enumerator_T::stat_tracked_enumerator(
        const B& stat_bucketizer, const std::vector<E> default_values,
        bool track_stats, double prob_of_returning_default,
        double min_threshold, uint64_t min_count_threshold )
    : stat_bucketizer_( stat_bucketizer ),
      default_values_( default_values ),
      entries_(),
      enumerator_to_pos_(),
      dist_( nullptr ),
      track_stats_( track_stats ),
      prob_of_returning_default_( prob_of_returning_default ),
      min_threshold_( min_threshold ),
      min_count_threshold_( min_count_threshold ) {
    for( uint32_t pos = 0; pos < default_values_.size(); pos++ ) {
        enumerator_to_pos_[default_values_.at( pos )] = pos;
    }
}

stat_tracked_enumerator_types std::vector<E>
                              stat_tracked_enumerator_T::get_enumerations_from_stats(
        const B& stats ) const {
    if( !track_stats_ ) {
        return default_values_;
    }

    double prob = dist_.get_uniform_double_in_range( 0.0, 1.0 );
    if( prob > prob_of_returning_default_ ) {
        auto entry = get_entry( stats );
        if( entry ) {
            auto decisions = entry->get_decisions_above_threshold(
                min_threshold_, min_count_threshold_ );
            if( !decisions.empty() ) {
                std::vector<E> rets;
                for( uint32_t pos : decisions ) {
                    DCHECK_LT( pos, default_values_.size() );
                    rets.emplace_back( default_values_.at( pos ) );
                }
                return rets;
            }  // found decisions
        }      // entry
    }          // prob outside defaults

    return default_values_;
}

stat_tracked_enumerator_types void stat_tracked_enumerator_T::add_decision(
    const B& stats, const E& decision ) {
    if( !track_stats_ ) {
        return;
    }
    auto     found = enumerator_to_pos_.find( decision );
    uint32_t pos = found->second;

    auto entry = get_or_create_entry( stats );
    entry->add_decision( pos );
}

stat_tracked_enumerator_types void stat_tracked_enumerator_T::add_decisions(
    const stat_tracked_enumerator_decision_tracker<B, E, BHash, BPred, EHash,
                                                   EPred>& tracker ) {
    if( !track_stats_ ) {
        return;
    }
    DCHECK_EQ( tracker.counts_.size(), tracker.totals_.size() );
    for( const auto& tracker_entry : tracker.counts_ ) {
        const auto& stats = tracker_entry.first;
        const auto& decision = tracker_entry.second;
        const auto& total_count = tracker.totals_.at( stats );

        auto entry = get_or_create_entry( stats );
        entry->add_decisions( total_count, decision );
    }
}

stat_tracked_enumerator_types
    stat_tracked_enumerator_decision_tracker<B, E, BHash, BPred, EHash, EPred>
    stat_tracked_enumerator_T::construct_decision_tracker() const {
    return stat_tracked_enumerator_decision_tracker<B, E, BHash, BPred, EHash,
                                                    EPred>(
        track_stats_, enumerator_to_pos_, stat_bucketizer_ );
}

stat_tracked_enumerator_types std::shared_ptr<stat_tracked_enumerator_entry<B>>
    stat_tracked_enumerator_T::get_entry( const B& stats ) const {

    B bucketized = get_bucketized_stat( stats );
    return get_entry_from_bucketized(bucketized );
}
stat_tracked_enumerator_types std::shared_ptr<stat_tracked_enumerator_entry<B>>
                              stat_tracked_enumerator_T::get_entry_from_bucketized(
        const B& stats ) const {

    std::shared_ptr<stat_tracked_enumerator_entry<B>> ret = nullptr;

    auto found = entries_.find( stats );
    if( found != entries_.cend() ) {
        ret = found->second;
    }

    return ret;
}

stat_tracked_enumerator_types std::shared_ptr<stat_tracked_enumerator_entry<B>>
    stat_tracked_enumerator_T::get_or_create_entry( const B& stats ) {
    B    bucketized = get_bucketized_stat( stats );
    auto entry = get_entry_from_bucketized( bucketized );
    if( entry ) {
        return entry;
    }
    auto entry_ptr = std::make_shared<stat_tracked_enumerator_entry<B>>(
        bucketized, default_values_.size() );
    auto insertion = entries_.insert( std::move( bucketized ), entry_ptr );
    if( !insertion.second ) {
        entry = insertion.first->second;
    } else {
        entry = entry_ptr;
    }

    return entry;
}

stat_tracked_enumerator_types B
    stat_tracked_enumerator_T::get_bucketized_stat( const B& stats ) const {
    return stat_bucketizer_.bucketize( stats );
}
stat_tracked_enumerator_types std::unordered_map<E, uint32_t, EHash, EPred>
                              stat_tracked_enumerator_T::get_enumerator_to_pos() const {
    return enumerator_to_pos_;
}
stat_tracked_enumerator_types B
                              stat_tracked_enumerator_T::get_bucketizer() const {
    return stat_bucketizer_;
}
