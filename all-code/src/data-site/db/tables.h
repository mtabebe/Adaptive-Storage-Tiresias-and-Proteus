#pragma once

#include "table.h"

#include "transaction_partition_holder.h"

class tables {
   public:
    tables();
    ~tables();

    void init( const tables_metadata&                        metadata,
               std::shared_ptr<update_destination_generator> update_gen,
               std::shared_ptr<update_enqueuers>             enqueuers );

    /*
    Creates a table with at most num_records in it, with locked partitions of
    size
    partition_size
    TODO: not thread safe, not sure if it matters
    */
    uint32_t create_table( const table_metadata& metadata );

    transaction_partition_holder* get_partition_holder(
        const partition_column_identifier_set&   write_pids,
        const partition_column_identifier_set&   read_pids,
        const partition_lookup_operation& lookup_operation );

    ALWAYS_INLINE transaction_partition_holder* get_partitions(
        const partition_column_identifier_set& write_pids,
        const partition_column_identifier_set& read_pids );
    ALWAYS_INLINE transaction_partition_holder* get_or_create_partitions(
        const partition_column_identifier_set& write_pids,
        const partition_column_identifier_set& read_pids );

    std::shared_ptr<partition> get_partition_if_active(
        const partition_column_identifier& pid ) const;
    std::shared_ptr<partition> get_partition(
        const partition_column_identifier& pid ) const;
    std::shared_ptr<partition> get_or_create_partition(
        const partition_column_identifier& pid );
    std::shared_ptr<partition> get_or_create_partition_and_pin(
        const partition_column_identifier& pid );

    ALWAYS_INLINE std::shared_ptr<partition> add_partition(
        const partition_column_identifier& pid,
        const partition_type::type&        p_type,
        const storage_tier_type::type&     s_type );
    ALWAYS_INLINE std::shared_ptr<partition> add_partition(
        const partition_column_identifier& pid,
        const partition_type::type&        p_type,
        const storage_tier_type::type& s_type, bool do_pin, uint64_t version,
        bool                                          do_lock,
        std::shared_ptr<update_destination_interface> update_destination );

    ALWAYS_INLINE bool insert_partition( const partition_column_identifier& pid,
                                         std::shared_ptr<partition>  part );
    ALWAYS_INLINE bool set_partition( const partition_column_identifier& pid,
                                      std::shared_ptr<partition>         part );

    ALWAYS_INLINE void remove_partition( const partition_column_identifier& pid );

    ALWAYS_INLINE partition* create_partition_without_adding_to_table(
        const partition_column_identifier& pid,
        const partition_type::type&        p_type,
        const storage_tier_type::type& s_type, bool do_pin, uint64_t version );
    ALWAYS_INLINE partition* create_partition_without_adding_to_table(
        const partition_column_identifier& pid,
        const partition_type::type&        p_type,
        const storage_tier_type::type& s_type, bool do_pin, uint64_t version,
        std::shared_ptr<update_destination_interface> update_destination );

    ALWAYS_INLINE split_partition_result split_partition(
        const snapshot_vector&             snapshot,
        const partition_column_identifier& old_pid, uint64_t split_row_point,
        uint32_t split_col_point, const partition_type::type& low_type,
        const partition_type::type&                   high_type,
        const storage_tier_type::type&                low_storage_type,
        const storage_tier_type::type&                high_storage_type,
        std::shared_ptr<update_destination_interface> low_update_destination,
        std::shared_ptr<update_destination_interface> high_update_destination );
    ALWAYS_INLINE split_partition_result merge_partition(
        const snapshot_vector&                        snapshot,
        const partition_column_identifier&            low_pid,
        const partition_column_identifier&            high_pid,
        const partition_type::type&                   merge_type,
        const storage_tier_type::type&                merge_storage_type,
        std::shared_ptr<update_destination_interface> update_destination );

    void stop_subscribing(
        const std::vector<update_propagation_information>& subscriptions,
        const snapshot_vector& snapshot, const std::string& cause );
    void start_subscribing(
        const std::vector<update_propagation_information>& subscriptions,
        const snapshot_vector& snapshot, const std::string& cause );
    void start_subscribing_and_create_partition_if_necessary(
        const std::vector<update_propagation_information>& subscriptions,
        const snapshot_vector& snapshot, const std::string& cause );
    void mark_subscription_end(
        const std::vector<update_propagation_information>& subscriptions,
        const snapshot_vector&                             snapshot );
    void switch_subscription(
        const std::vector<update_propagation_information>& subscription_switch,
        const std::string&                                 cause );

    void get_update_partition_states(
        std::vector<polled_partition_column_version_information>&
            polled_versions );

    void gc_inactive_partitions();

    table* get_table( const std::string& name ) const;
    ALWAYS_INLINE table* get_table( uint32_t table_id ) const;

    uint32_t get_num_tables() const;
    ALWAYS_INLINE uint64_t
        get_approx_total_number_of_partitions( uint32_t table_id );

    tables_metadata get_tables_metadata() const;

    void persist_db_table( uint32_t                   table_id,
                           data_site_table_persister* persister );

    ALWAYS_INLINE cell_data_type
        get_cell_data_type( const cell_identifier& cid ) const;

    std::vector<std::vector<cell_widths_stats>> get_column_widths() const;
    std::vector<std::vector<selectivity_stats>> get_selectivities() const;
    partition_column_identifier_map_t<storage_tier_type::type>
        get_storage_tier_changes() const;

   private:
    std::shared_ptr<partition> get_partition_via_lookup_op(
        const partition_column_identifier& pid,
        const partition_lookup_operation& op, bool is_read );

    std::vector<table*>                           tables_;
    tables_metadata                               metadata_;
    std::shared_ptr<update_destination_generator> update_gen_;
    std::shared_ptr<update_enqueuers>             enqueuers_;
};

#include "tables-inl.h"
