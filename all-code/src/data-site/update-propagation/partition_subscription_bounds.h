#pragma once

#include "../../common/partition_funcs.h"
#include "../db/partition_column_version_holder.h"
#include "../db/partition_metadata.h"

class subscription_bound {
   public:
    subscription_bound( uint64_t bound, uint64_t version );

    uint64_t bound_;
    uint64_t version_;
};

inline bool operator==( const subscription_bound& lhs,
                        const subscription_bound& rhs ) {
    return ( ( lhs.bound_ == rhs.bound_ ) and
             ( lhs.version_ == rhs.version_ ) );
}
inline bool operator!=( const subscription_bound& lhs,
                        const subscription_bound& rhs ) {
    return !( lhs == rhs );
}

typedef partition_column_identifier_folly_concurrent_hash_map_t<subscription_bound>
    concurrent_partition_id_to_subscription_bound_map;
typedef partition_column_identifier_folly_concurrent_hash_map_t<
    std::shared_ptr<partition_column_version_holder>>
    concurrent_partition_id_to_version_map;

class partition_subscription_bounds {
   public:
    partition_subscription_bounds();
    ~partition_subscription_bounds();

    void create_table( const table_metadata& metadata);
    void set_expected_number_of_tables( uint32_t expected_num_tables );

    // upper bound
    bool set_upper_bound( const partition_column_identifier& pid,
                          const subscription_bound&   bound );
    bool remove_upper_bound( const partition_column_identifier& pid,
                             uint64_t                    version );
    bool look_up_upper_bound( const partition_column_identifier& pid,
                              uint64_t*                   upper_bound ) const;


    // lower bounds
    void set_lower_bound_of_source(
        const partition_column_identifier&               pid,
        std::shared_ptr<partition_column_version_holder> version_holder );
    void insert_lower_bound( const partition_column_identifier& pid,
                             uint64_t                    version );
    bool look_up_lower_bound( const partition_column_identifier& pid,
                              uint64_t* lower_bound ) const;

   private:
    std::vector<concurrent_partition_id_to_version_map*>
        active_pids_lower_bound_;
    std::vector<concurrent_partition_id_to_subscription_bound_map*>
        active_pids_upper_bound_;

    uint32_t expected_num_tables_;
};
