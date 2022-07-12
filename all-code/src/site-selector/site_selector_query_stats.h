#pragma once

#include <vector>

#include "../data-site/db/partition_metadata.h"

typedef std::unordered_map<uint32_t, std::unordered_map<uint32_t, double>>
    cached_cell_sizes_by_table;
typedef std::unordered_map<uint32_t, std::unordered_map<uint32_t, double>>
    cached_accesses_by_table;
typedef std::unordered_map<uint32_t, std::unordered_map<uint32_t, double>>
    cached_selectivity_by_table;

class cached_ss_stat_holder {
   public:
    cached_ss_stat_holder();

    cached_cell_sizes_by_table  cached_cell_sizes_;
    cached_selectivity_by_table cached_selectivity_;

    cached_accesses_by_table cached_num_reads_;
    cached_accesses_by_table cached_num_writes_;
    cached_accesses_by_table cached_contention_;
};

class table_width_stats {
   public:
    table_width_stats( const table_metadata& meta );

    void set_cell_width( uint32_t table_id, uint32_t col_id, double width );

    double get_cell_width( uint32_t col_id ) const;
    std::vector<double> get_cell_widths(
        const partition_column_identifier& pid ) const;
    std::vector<double> get_cell_widths( uint32_t col_start,
                                         uint32_t col_end ) const;
    std::vector<double> get_and_cache_cell_widths(
        const partition_column_identifier& pid,
        cached_cell_sizes_by_table&        col_sizes ) const;

   private:
    uint32_t                         table_id_;
    std::vector<std::atomic<double>> widths_;
};
class table_selectivity_stats {
   public:
    table_selectivity_stats( const table_metadata& meta );

    void set_cell_selectivity( uint32_t table_id, uint32_t col_id, double sel );

    double get_cell_selectivity( uint32_t col_id ) const;
    double get_selectivity( const partition_column_identifier& pid ) const;
    double get_selectivity( uint32_t col_start, uint32_t col_end ) const;

    double get_and_cache_cell_selectivity(
        uint32_t                     col_id,
        cached_selectivity_by_table& cached_selectivity ) const;
    double get_and_cache_selectivity(
        const partition_column_identifier& pid,
        cached_selectivity_by_table&       cached_selectivity ) const;
    double get_and_cache_selectivity(
        uint32_t col_start, uint32_t col_end,
        cached_selectivity_by_table& cached_selectivity ) const;

   private:
    uint32_t                         table_id_;
    std::vector<std::atomic<double>> selectivity_;
};

class table_access_frequency {
   public:
    table_access_frequency( const table_metadata&  meta,
                            std::atomic<uint64_t>* total_num_updates,
                            std::atomic<uint64_t>* total_num_reads );

    double get_average_contention(
        const partition_column_identifier& pid ) const;
    double get_average_contention( uint32_t col ) const;
    double get_and_cache_average_contention(
        uint32_t col, cached_accesses_by_table& cached_contention ) const;
    double get_and_cache_average_contention(
        const partition_column_identifier& pid,
        cached_accesses_by_table&          cached_contention ) const;

    double get_average_num_reads( uint32_t col ) const;
    double get_average_num_reads(
        const partition_column_identifier& pid ) const;
    double get_and_cache_average_num_reads(
        uint32_t col, cached_accesses_by_table& cached_reads ) const;
    double get_and_cache_average_num_reads(
        const partition_column_identifier& pid,
        cached_accesses_by_table&          cached_reads ) const;

    double get_average_num_updates(
        const partition_column_identifier& pid ) const;
    double get_average_num_updates( uint32_t col ) const;
    double get_and_cache_average_num_updates(
        uint32_t col, cached_accesses_by_table& cached_updates ) const;
    double get_and_cache_average_num_updates(
        const partition_column_identifier& pid,
        cached_accesses_by_table&          cached_updates ) const;

    void add_accesses( const std::vector<uint64_t>& writes,
                       const std::vector<uint64_t>& reads );

   private:
    double get_average_num_ops(
        const partition_column_identifier&        pid,
        const std::vector<std::atomic<uint64_t>>& running_num_ops,
        const std::vector<std::atomic<uint64_t>>& num_txns ) const;
    double get_average_num_ops(
        uint32_t col, const std::vector<std::atomic<uint64_t>>& running_num_ops,
        const std::vector<std::atomic<uint64_t>>& num_txns ) const;

    void add_accesses_internal(
        std::atomic<uint64_t>*              total_num,
        std::vector<std::atomic<uint64_t>>& running_num_ops,
        std::vector<std::atomic<uint64_t>>& num_op_txns,
        const std::vector<uint64_t>&        counts );

    uint32_t table_id_;

    std::atomic<uint64_t>* total_num_updates_;
    std::atomic<uint64_t>* total_num_reads_;

    std::vector<std::atomic<uint64_t>> running_num_updates_;
    std::vector<std::atomic<uint64_t>> num_update_txns_;

    std::vector<std::atomic<uint64_t>> running_num_reads_;
    std::vector<std::atomic<uint64_t>> num_read_txns_;
};

class site_selector_query_stats {
   public:
    site_selector_query_stats();

    void create_table( const table_metadata& meta );

    std::vector<double> get_cell_widths(
        const partition_column_identifier& pid ) const;
    std::vector<double> get_and_cache_cell_widths(
        const partition_column_identifier& pid,
        cached_cell_sizes_by_table&        col_sizes ) const;
    std::vector<double> get_and_cache_cell_widths(
        const partition_column_identifier& pid,
        cached_ss_stat_holder&             cached ) const;

    double get_average_scan_selectivity(
        const partition_column_identifier& pid ) const;
    double get_and_cache_average_scan_selectivity(
        const partition_column_identifier& pid,
        cached_selectivity_by_table&       cached ) const;
    double get_and_cache_average_scan_selectivity(
        const partition_column_identifier& pid,
        cached_ss_stat_holder&             cached ) const;

    double get_average_num_reads(
        const partition_column_identifier& pid ) const;
    double get_and_cache_average_num_reads(
        const partition_column_identifier& pid,
        cached_accesses_by_table&          cached ) const;
    double get_and_cache_average_num_reads(
        const partition_column_identifier& pid,
        cached_ss_stat_holder&             cached ) const;

    double get_average_num_updates(
        const partition_column_identifier& pid ) const;
    double get_and_cache_average_num_updates(
        const partition_column_identifier& pid,
        cached_accesses_by_table&          cached ) const;
    double get_and_cache_average_num_updates(
        const partition_column_identifier& pid,
        cached_ss_stat_holder&             cached ) const;

    double get_average_contention(
        const partition_column_identifier& pid ) const;
    double get_and_cache_average_contention(
        const partition_column_identifier& pid,
        cached_accesses_by_table&          cached ) const;
    double get_and_cache_average_contention(
        const partition_column_identifier& pid,
        cached_ss_stat_holder&             cached ) const;

    void set_cell_width( uint32_t table_id, uint32_t col_id, double width );
    void set_column_selectivity( uint32_t table_id, uint32_t col_id,
                                 double selectivity );

    void add_transaction_accesses( const std::vector<cell_key_ranges>& writes,
                                   const std::vector<cell_key_ranges>& reads );

   private:
    std::vector<std::vector<uint64_t>> fill_observations(
        const std::vector<cell_key_ranges>& ckrs ) const;

    uint32_t              num_tables_;
    std::vector<uint32_t> num_columns_;

    std::atomic<uint64_t> total_num_updates_;
    std::atomic<uint64_t> total_num_reads_;

    std::vector<table_width_stats>       table_widths_;
    std::vector<table_selectivity_stats> table_selectivity_;
    std::vector<table_access_frequency>  table_accesses_;
};
