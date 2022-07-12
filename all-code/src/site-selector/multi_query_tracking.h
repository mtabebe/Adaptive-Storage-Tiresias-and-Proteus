#pragma once

#include <mutex>
#include <condition_variable>

#include <folly/concurrency/ConcurrentHashMap.h>

#include "../common/partition_funcs.h"

enum multi_query_plan_type {
    WAITING,
    PLANNING,
    EXECUTING,
    CACHED,
};

class multi_query_plan {
   public:
    multi_query_plan( int destination );

    multi_query_plan(
        const partition_column_identifier_map_t<int>& pids_to_site );

    bool                                   is_multi_site_transaction_;
    int                                    destination_;
    partition_column_identifier_map_t<int> pids_to_site_;
};

class multi_query_plan_entry {
   public:
    multi_query_plan_entry();

    double get_plan_overlap(
        const partition_column_identifier_set& write_pids,
        const partition_column_identifier_set& read_pids ) const;

    std::shared_ptr<multi_query_plan>     get_plan() const;
    multi_query_plan_type                 get_plan_type() const;
    bool is_pid_in_plan( const partition_column_identifier& pid,
                         bool                               is_write ) const;

    uint64_t get_id() const;

    void set_plan_type( const multi_query_plan_type& plan_type );
    void set_plan( std::shared_ptr<multi_query_plan>& plan,
                   const multi_query_plan_type&       plan_type );
    void set_plan_destination( int                          destination,
                               const multi_query_plan_type& plan_type );
    void set_scan_plan_destination(
        const partition_column_identifier_map_t<int>& pids_to_site,
        const multi_query_plan_type&                  plan_type );

    std::tuple<bool, int> get_plan_destination() const;
    std::tuple<bool, partition_column_identifier_map_t<int>>
        get_scan_plan_destination() const;

    void set_pid_sets(
        const partition_column_identifier_unordered_set& write_set,
        const partition_column_identifier_unordered_set& read_set );
    void set_pid_sets( const partition_column_identifier_set& write_set,
                       const partition_column_identifier_set& read_set );


    bool wait_for_plan_to_be_generated();

   private:
    void add_to_pids( const partition_column_identifier&         pid,
                      partition_column_identifier_unordered_set& pid_set,
                      bool                                       is_write );

    std::shared_ptr<multi_query_plan>     plan_;
    std::atomic<multi_query_plan_type>    plan_type_;

    partition_column_identifier_unordered_set write_set_;
    partition_column_identifier_unordered_set read_set_;

    uint64_t id_;

    partition_column_identifier_key_hasher hasher_;

    mutable std::mutex      mutex_;
    std::condition_variable cv_;
};

class transaction_query_entry {
   public:
    transaction_query_entry();

    bool contains_plan( uint64_t id ) const;

    std::vector<
        std::tuple<double, uint64_t, std::shared_ptr<multi_query_plan_entry>>>
        get_sorted_plans() const;

    std::unordered_map<uint64_t, std::shared_ptr<multi_query_plan_entry>>
        entries_;
    std::unordered_map<uint64_t, double> overlaps_;

   private:
};

class multi_query_partition_entry {
   public:
    multi_query_partition_entry();
    multi_query_partition_entry( const partition_column_identifier& pid );

    void set_pid( const partition_column_identifier& pid );

    void update_transaction_query_entry(
        transaction_query_entry&               entry,
        const partition_column_identifier_set& write_pids,
        const partition_column_identifier_set& read_pids ) const;
    void add_entry( std::shared_ptr<multi_query_plan_entry>& entry );

   private:
    partition_column_identifier pid_;
    folly::ConcurrentHashMap<uint64_t, std::shared_ptr<multi_query_plan_entry>>
        entries_;
};


