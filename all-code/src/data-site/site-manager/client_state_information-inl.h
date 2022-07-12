#pragma once
#include <cstdlib>
#include <glog/logging.h>

const static uint32_t K_INACTIVE_CLIENT = 0;
const static uint32_t K_ACTIVE_CLIENT = 1;
const static uint32_t K_MAX_SESSION_CLIENT = 2;

#if 0  // HDB-OUT
inline const client_version_vector&
    client_state_information::get_client_session( uint32_t client_id ) const {
    DCHECK_LT( client_id, client_session_state_.size() );
    const client_version_vector& ret = client_session_state_.at( client_id );
    return ret;
}

inline void client_state_information::set_client_session(
    uint32_t client_id, const client_version_vector& cli_version ) {
    DCHECK_LT( client_id, client_session_state_.size() );
    DCHECK_LE( cli_version.size(), num_sites_ );

    for( uint32_t loc = 0; loc < cli_version.size(); loc++ ) {
        client_session_state_.at( client_id ).at( loc ) = cli_version.at( loc );
    }
}

#endif

inline void client_state_information::reset_partition_holder(
    uint32_t client_id ) {
    DCHECK_LT( client_id, client_partition_holders_.size() );
    if (client_partition_holders_[client_id]) {
        delete client_partition_holders_[client_id];
        client_partition_holders_[client_id] = nullptr;
    }
}
inline void client_state_information::set_partition_holder(
    uint32_t client_id, transaction_partition_holder* holder ) {
    reset_partition_holder( client_id );
    client_partition_holders_[client_id] = holder;
}

inline transaction_partition_holder*
    client_state_information::get_partition_holder( uint32_t client_id ) const {
    DCHECK_LT( client_id, client_partition_holders_.size() );
    transaction_partition_holder* holder = client_partition_holders_[client_id];
    return holder;
}

#if 0 // HDB-OUT

inline const std::map<uint32_t, std::vector<uint64_t>>&
    client_state_information::get_per_table_partitions(
        uint32_t client_id ) const {
    DCHECK_LT( client_id, per_client_partitions_.size() );
    return per_client_partitions_.at( client_id );
}
inline void client_state_information::store_per_table_partitions(
    uint32_t client_id,
    const std::map<uint32_t, std::vector<uint64_t>>& per_table_partitions ) {
    DCHECK_LT( client_id, per_client_partitions_.size() );
    per_client_partitions_.at( client_id ) = per_table_partitions;
}
inline const std::vector<record_identifier>&
    client_state_information::get_write_set( uint32_t client_id ) const {
    DCHECK_LT( client_id, per_client_write_sets_.size() );
    return per_client_write_sets_.at( client_id );
}
inline void client_state_information::store_write_set(
    uint32_t client_id, const std::vector<record_identifier>& write_set ) {
    DCHECK_LT( client_id, per_client_write_sets_.size() );
    per_client_write_sets_.at( client_id ) = write_set;
}

inline void client_state_information::set_client_active_state(
    uint32_t client_id, uint32_t state ) {
    DCHECK_LT( client_id, active_clients_.size() );
    active_clients_.at( client_id ).store( state, std::memory_order_release );
}
inline void client_state_information::set_client_active( uint32_t client_id ) {
    set_client_active_state( client_id, K_ACTIVE_CLIENT );
};
inline void client_state_information::set_client_inactive(
    uint32_t client_id ) {
    set_client_active_state( client_id, K_INACTIVE_CLIENT );
}
inline void client_state_information::set_client_max_session(
    uint32_t client_id ) {
    set_client_active_state( client_id, K_MAX_SESSION_CLIENT );
}

inline const client_version_vector&
    client_state_information::get_clients_low_watermark(
        uint32_t client_id ) const {
    DCHECK_LT( client_id, client_low_watermark_.size() );
    const client_version_vector& ret = client_low_watermark_.at( client_id );
    return ret;
}
inline void client_state_information::set_clients_low_watermark(
    uint32_t client_id, client_version_vector lwm ) {
    DCHECK_LT( client_id, client_low_watermark_.size() );
    DCHECK_LE( lwm.size(), num_sites_ );

    for( uint32_t loc = 0; loc < lwm.size(); loc++ ) {
        client_low_watermark_.at( client_id ).at( loc ) = lwm.at( loc );
    }
}

#endif

