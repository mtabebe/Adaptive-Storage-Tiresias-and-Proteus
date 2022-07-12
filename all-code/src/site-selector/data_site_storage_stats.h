#pragma once

#include <atomic>
#include <unordered_map>
#include <vector>

#include "../data-site/db/partition_metadata.h"
#include "cost_modeller_types.h"
#include "site_selector_query_stats.h"

class data_site_storage_stat {
  public:
   data_site_storage_stat( uint32_t                                    site_id,
                           const std::vector<storage_tier_type::type>& tiers );
   ~data_site_storage_stat();

   void create_table( uint32_t table_id, uint32_t num_columns );

   double get_storage_tier_size(
       const storage_tier_type::type&          tier,
       const std::vector<std::vector<double>>& widths ) const;

   void change_count_in_tier( uint32_t                           site,
                              const storage_tier_type::type&     tier,
                              const partition_column_identifier& pid,
                              int64_t                            multiplier );

  private:
   uint32_t site_id_;
   // tier<table<cols>>
   std::unordered_map<storage_tier_type::type,
                      std::vector<std::atomic<int64_t>*>>
       per_tier_num_rows_per_column_per_table_;
};

class data_site_storage_stats {
  public:
   data_site_storage_stats( uint32_t num_sites,
                            const std::unordered_map<storage_tier_type::type,
                                                     double>& storage_limits );
   ~data_site_storage_stats();

   void create_table( const table_metadata& meta );

   double get_storage_tier_size( uint32_t                       site,
                                 const storage_tier_type::type& tier ) const;
   std::unordered_map<storage_tier_type::type, std::vector<double>>
       get_storage_tier_sizes() const;

   double get_partition_size( const partition_column_identifier& pid ) const;

   void update_table_widths( uint32_t                   table_id,
                             const std::vector<double>& widths );
   void update_table_widths_from_stats(
       const site_selector_query_stats& stats );

   void add_partition_to_tier( uint32_t                           site,
                               const storage_tier_type::type&     tier,
                               const partition_column_identifier& pid );
   void remove_partition_from_tier( uint32_t                           site,
                                    const storage_tier_type::type&     tier,
                                    const partition_column_identifier& pid );

   void change_partition_tier( uint32_t                           site,
                               const storage_tier_type::type&     old_tier,
                               const storage_tier_type::type&     new_tier,
                               const partition_column_identifier& pid );

   double get_storage_limit( uint32_t                       site,
                             const storage_tier_type::type& tier ) const;
   double get_storage_ratio( uint32_t                       site,
                             const storage_tier_type::type& tier ) const;
   double compute_storage_ratio( double size, double limit ) const;

   std::unordered_map<storage_tier_type::type, std::vector<double>>
       get_storage_ratios() const;

   std::unordered_map<storage_tier_type::type, std::vector<double>>
       get_storage_ratios_from_sizes(
           const std::unordered_map<storage_tier_type::type,
                                    std::vector<double>>& sizes ) const;

  private:
   std::tuple<uint32_t, uint32_t> get_site_iter_bounds( uint32_t site ) const;

   uint32_t num_sites_;
   uint32_t num_tables_;

   std::vector<std::vector<double>>    table_widths_;
   std::vector<data_site_storage_stat> per_site_stats_;
   std::unordered_map<storage_tier_type::type, double> storage_limits_;
};
