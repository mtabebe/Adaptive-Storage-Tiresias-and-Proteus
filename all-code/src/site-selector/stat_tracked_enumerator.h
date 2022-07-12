#pragma once

#include <atomic>
#include <functional>
#include <vector>

#include <folly/concurrency/ConcurrentHashMap.h>

#include "../distributions/distributions.h"

template <typename B, typename E, typename BHash = std::hash<B>,
          typename BPred = std::equal_to<B>, typename EHash = std::hash<E>,
          typename EPred = std::equal_to<E>>
class stat_tracked_enumerator_decision_tracker {
   public:
    stat_tracked_enumerator_decision_tracker(
        bool track_stats,
        const std::unordered_map<E, uint32_t, EHash, EPred>& enumerator_to_pos,
        const B& bucketizer );

    void add_decision( const B& stats, const E& decision );
    void clear_decisions();

    std::unordered_map<B, std::vector<uint32_t>, BHash, BPred> counts_;
    std::unordered_map<B, uint32_t, BHash, BPred>              totals_;

   private:
    // default return values
    std::unordered_map<E, uint32_t, EHash, EPred> enumerator_to_pos_;

    bool track_stats_;
    B    bucketizer_;
};

template <typename B>
class stat_tracked_enumerator_entry {
   public:
    stat_tracked_enumerator_entry( const B& entry, uint32_t num_entries );

    void add_decision( uint32_t pos );
    void add_decisions( uint32_t                     total_count,
                        const std::vector<uint32_t>& counts );

    std::vector<uint32_t> get_decisions_above_threshold(
        double threshold, uint64_t count_threshold ) const;

   private:
    // bucketized stat
    B stat_;

    // total count
    std::atomic<uint64_t> total_count_;
    // count per position
    std::vector<std::atomic<uint64_t>> per_pos_count_;
    std::vector<uint32_t>             default_;
};

template <typename B, typename E, typename BHash = std::hash<B>,
          typename BPred = std::equal_to<B>, typename EHash = std::hash<E>,
          typename EPred = std::equal_to<E>>
class stat_tracked_enumerator {
   public:
    stat_tracked_enumerator( const B&             stat_bucketizer,
                             const std::vector<E> default_values,
                             bool track_stats, double prob_of_returning_default,
                             double   min_threshold,
                             uint64_t min_count_threshold );

    std::vector<E> get_enumerations_from_stats( const B& stats ) const;
    void add_decision( const B& stats, const E& decision );
    void add_decisions( const stat_tracked_enumerator_decision_tracker<
                        B, E, BHash, BPred, EHash, EPred>& tracker );
    stat_tracked_enumerator_decision_tracker<B, E, BHash, BPred, EHash, EPred>
        construct_decision_tracker() const;

   private:
    std::unordered_map<E, uint32_t, EHash, EPred> get_enumerator_to_pos() const;

    B get_bucketizer() const;


    std::shared_ptr<stat_tracked_enumerator_entry<B>> get_entry(
        const B& stats ) const;
    std::shared_ptr<stat_tracked_enumerator_entry<B>> get_or_create_entry(
        const B& stats );

    std::shared_ptr<stat_tracked_enumerator_entry<B>> get_entry_from_bucketized(
        const B& stats ) const;

    B get_bucketized_stat( const B& stats ) const;

    // stat bucketizer
    B stat_bucketizer_;

    // default return values
    std::vector<E> default_values_;

    folly::ConcurrentHashMap<
        B, std::shared_ptr<stat_tracked_enumerator_entry<B>>, BHash, BPred>
        entries_;

    std::unordered_map<E, uint32_t, EHash, EPred> enumerator_to_pos_;

    mutable distributions dist_;

    bool track_stats_;
    // prob of returning default
    double prob_of_returning_default_;
    // min threshold
    double   min_threshold_;
    uint64_t min_count_threshold_;
};

#include "stat_tracked_enumerator.tcc"
