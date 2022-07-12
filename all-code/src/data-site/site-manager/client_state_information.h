#pragma once

#include "../../common/hw.h"
#include "../../concurrency/counter_lock.h"
#include "../db/transaction_partition_holder.h"
#include <map>
#include <memory>
#include <vector>

class client_state_information {
   public:
    client_state_information();
    ~client_state_information();

    void init( uint64_t num_clients, uint32_t site_identifier );

#if 0 // HDB-OUT
    ALWAYS_INLINE void set_client_session(
        uint32_t client_id, const client_version_vector& cli_version );
    // must be set before they load the site version vector
    ALWAYS_INLINE void set_client_active( uint32_t client_id );
    // must be set after they set their site version vector
    ALWAYS_INLINE void set_client_inactive( uint32_t client_id );
    // must be set after they set their site version vector
    ALWAYS_INLINE void set_client_max_session( uint32_t client_id );


    client_version_vector get_low_watermark(
        std::vector<counter_lock>* site_version_vector ) const;

    ALWAYS_INLINE const client_version_vector& get_clients_low_watermark(
        uint32_t client_id ) const;
    ALWAYS_INLINE void set_clients_low_watermark( uint32_t client_id,
                                                  client_version_vector lwm );
#endif

    ALWAYS_INLINE void reset_partition_holder( uint32_t client_id );
    ALWAYS_INLINE void set_partition_holder(
        uint32_t client_id, transaction_partition_holder* holder );
    ALWAYS_INLINE transaction_partition_holder* get_partition_holder(
        uint32_t client_id ) const;

   private:
#if 0 // HDB-OUT
    ALWAYS_INLINE void set_client_active_state( uint32_t client_id,
                                                uint32_t state );
#endif

    std::vector<transaction_partition_holder*>  client_partition_holders_;
    uint32_t                                    num_sites_;
    uint32_t                                    site_identifier_;


#if 0 // HDB-OUT
    std::vector<std::atomic<std::uint32_t>> active_clients_;
    std::vector<client_version_vector>          client_session_state_;
    std::vector<client_version_vector>          client_low_watermark_;

    mutable std::vector<uint64_t>               computed_low_watermark_;
    mutable std::vector<uint64_t>               new_watermark_tmp_;
#endif
};

#include "client_state_information-inl.h"
