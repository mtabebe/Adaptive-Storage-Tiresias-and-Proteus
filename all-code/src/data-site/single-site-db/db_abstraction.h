#pragma once

#include "../../common/constants.h"
#include "../../site-selector/site_selector_metadata.h"
#include "../../templates/single_site_db_types.h"
#include "../db/db.h"
#include "db_abstraction_types.h"
#include "single_site_db.h"

class db_abstraction {
   public:
    virtual ~db_abstraction(){};

    virtual void init( std::shared_ptr<update_destination_generator> update_gen,
                       std::shared_ptr<update_enqueuers>             enqueuers,
                       const tables_metadata& t_meta ) = 0;

    virtual uint32_t create_table( const table_metadata& metadata ) = 0;

    virtual transaction_partition_holder* get_partitions_with_begin(
        uint32_t client_id, const snapshot_vector& begin_timestamp,
        std::vector<cell_key_ranges>& write_keys,
        std::vector<cell_key_ranges>& read_keys, bool allow_missing ) = 0;

    virtual transaction_partition_holder* get_partitions_with_begin(
        uint32_t client_id, const snapshot_vector& begin_timestamp,
        const partition_column_identifier_set& write_pids,
        const partition_column_identifier_set& read_pids,
        const partition_column_identifier_set& inflight_pids,
        bool                                   allow_missing ) = 0;

    virtual void change_partition_types(
        uint32_t                                          client_id,
        const std::vector<::partition_column_identifier>& pids,
        const std::vector<partition_type::type>&          part_types,
        const std::vector<storage_tier_type::type>&       storage_types ) = 0;

    virtual void add_partition( uint32_t                           client_id,
                                const partition_column_identifier& pid,
                                uint32_t                       master_location,
                                const partition_type::type&    p_type,
                                const storage_tier_type::type& s_type ) = 0;
    virtual void remove_partition( uint32_t client_id, const cell_key& ck ) = 0;

    virtual snapshot_vector merge_partition( uint32_t               client_id,
                                             const snapshot_vector& snapshot,
                                             const cell_key&        merge_point,
                                             bool merge_vertically ) = 0;
    virtual snapshot_vector split_partition( uint32_t               client_id,
                                             const snapshot_vector& snapshot,
                                             const cell_key&        split_point,
                                             bool split_vertically ) = 0;
    virtual snapshot_vector remaster_partitions(
        uint32_t client_id, const snapshot_vector& snapshot,
        std::vector<cell_key_ranges>& ckrs, uint32_t new_master ) = 0;

    virtual uint32_t get_site_location() = 0;
    virtual partition_type::type get_default_partition_type(
        uint32_t table_id ) = 0;
    virtual storage_tier_type::type get_default_storage_type(
        uint32_t table_id ) = 0;
};

class plain_db_wrapper : public db_abstraction {
   public:
    plain_db_wrapper();
    ~plain_db_wrapper();

    void init( std::shared_ptr<update_destination_generator> update_gen,
               std::shared_ptr<update_enqueuers>             enqueuers,
               const tables_metadata&                        t_meta ) override;
    void init( db* db );

    uint32_t create_table( const table_metadata& metadata ) override;

    transaction_partition_holder* get_partitions_with_begin(
        uint32_t client_id, const snapshot_vector& begin_timestamp,
        std::vector<cell_key_ranges>& write_keys,
        std::vector<cell_key_ranges>& read_keys, bool allow_missing ) override;
    transaction_partition_holder* get_partitions_with_begin(
        uint32_t client_id, const snapshot_vector& begin_timestamp,
        const partition_column_identifier_set& write_pids,
        const partition_column_identifier_set& read_pids,
        const partition_column_identifier_set& inflight_pids,
        bool                                   allow_missing ) override;

    void change_partition_types(
        uint32_t                                          client_id,
        const std::vector<::partition_column_identifier>& pids,
        const std::vector<partition_type::type>&          part_types,
        const std::vector<storage_tier_type::type>& storage_types ) override;

    void add_partition( uint32_t                           client_id,
                        const partition_column_identifier& pid,
                        uint32_t                           master_location,
                        const partition_type::type&        p_type,
                        const storage_tier_type::type&     s_type ) override;
    void remove_partition( uint32_t client_id, const cell_key& ck ) override;

    snapshot_vector merge_partition( uint32_t               client_id,
                                     const snapshot_vector& snapshot,
                                     const cell_key&        merge_point,
                                     bool merge_vertically ) override;
    snapshot_vector split_partition( uint32_t               client_id,
                                     const snapshot_vector& snapshot,
                                     const cell_key&        split_point,
                                     bool split_vertically ) override;
    snapshot_vector remaster_partitions( uint32_t               client_id,
                                         const snapshot_vector& snapshot,
                                         std::vector<cell_key_ranges>& cks,
                                         uint32_t new_master ) override;

    uint32_t get_site_location() override;
    partition_type::type get_default_partition_type(
        uint32_t table_id ) override;
    storage_tier_type::type get_default_storage_type(
        uint32_t table_id ) override;


   private:
    partition_column_identifier_set generate_partition_column_identifiers(
        std::vector<cell_key>& cks );
    partition_column_identifier_set generate_partition_column_identifiers(
        std::vector<cell_key_ranges>& cks );
    partition_column_identifier generate_partition_column_identifier(
        const cell_key& ck );

    uint64_t get_partition_size( uint32_t table_id );
    uint32_t get_column_size( uint32_t table_id );
    partition_column_identifier generate_partition_column_identifier(
        const cell_key& ck, uint64_t partition_size, uint32_t col_size );
    partition_column_identifier_set generate_partition_column_identifier(
        const cell_key_ranges& ck, uint64_t partition_size, uint32_t col_size );

    db*                       db_;
    bool                      own_;
    propagation_configuration prop_config_;
};

single_site_db_wrapper_t_types class single_site_db_wrapper_t
    : public db_abstraction {
   public:
    single_site_db_wrapper_t();

    void init( std::shared_ptr<update_destination_generator> update_gen,
               std::shared_ptr<update_enqueuers>             enqueuers,
               const tables_metadata&                        t_meta ) override;

    uint32_t create_table( const table_metadata& metadata ) override;

    transaction_partition_holder* get_partitions_with_begin(
        uint32_t client_id, const snapshot_vector& begin_timestamp,
        std::vector<cell_key_ranges>& write_keys,
        std::vector<cell_key_ranges>& read_keys, bool allow_missing ) override;

    transaction_partition_holder* get_partitions_with_begin(
        uint32_t client_id, const snapshot_vector& begin_timestamp,
        const partition_column_identifier_set& write_pids,
        const partition_column_identifier_set& read_pids,
        const partition_column_identifier_set& inflight_pids,
        bool                                   allow_missing ) override;

    void change_partition_types(
        uint32_t                                          client_id,
        const std::vector<::partition_column_identifier>& pids,
        const std::vector<partition_type::type>&          part_types,
        const std::vector<storage_tier_type::type>& storage_types ) override;

    void add_partition( uint32_t                           client_id,
                        const partition_column_identifier& pid,
                        uint32_t                           master_location,
                        const partition_type::type&        p_type,
                        const storage_tier_type::type&     s_type ) override;
    void remove_partition( uint32_t client_id, const cell_key& ck ) override;

    snapshot_vector merge_partition( uint32_t               client_id,
                                     const snapshot_vector& snapshot,
                                     const cell_key&        merge_point,
                                     bool merge_vertically ) override;
    snapshot_vector split_partition( uint32_t               client_id,
                                     const snapshot_vector& snapshot,
                                     const cell_key&        split_point,
                                     bool split_vertically ) override;
    snapshot_vector remaster_partitions( uint32_t               client_id,
                                         const snapshot_vector& snapshot,
                                         std::vector<cell_key_ranges>& cks,
                                         uint32_t new_master ) override;

    uint32_t get_site_location() override;
    partition_type::type get_default_partition_type(
        uint32_t table_id ) override;
    storage_tier_type::type get_default_storage_type(
        uint32_t table_id ) override;

   private:
    single_site_db<enable_db> db_;
};

typedef single_site_db_wrapper_t<true /*enable db*/>   single_site_db_wrapper;
typedef single_site_db_wrapper_t<false /*disable db*/> ss_db_wrapper;

class db_abstraction_configs {
   public:
    db_abstraction_type     db_type_;
};

db_abstraction_configs construct_db_abstraction_configs(
    const db_abstraction_type& type = k_db_abstraction_type );

db_abstraction* create_db_abstraction( const db_abstraction_configs& configs =
                                           construct_db_abstraction_configs() );


#include "db_abstraction.tcc"
