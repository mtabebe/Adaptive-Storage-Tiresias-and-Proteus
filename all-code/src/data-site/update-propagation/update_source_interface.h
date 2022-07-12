#pragma once

#include "../../gen-cpp/gen-cpp/proto_types.h"
#include "update_enqueuer_subscription_information.h"
#include "update_propagation_configs.h"
#include "write_buffer_serialization.h"

class update_source_interface {
   public:
    virtual ~update_source_interface(){};

    virtual void start() = 0;
    virtual void stop() = 0;

    virtual void enqueue_updates( void* enqueuer_opaque_ptr ) = 0;

    virtual bool add_source( const propagation_configuration&   config,
                             const partition_column_identifier& pid,
                             bool do_seek, const std::string& cause ) = 0;
    virtual bool add_sources(
        const propagation_configuration&                config,
        const std::vector<partition_column_identifier>& pids, bool do_seek,
        const std::string& cause ) = 0;

    virtual std::tuple<bool, int64_t> remove_source(
        const propagation_configuration&   config,
        const partition_column_identifier& pid, const std::string& cause ) = 0;

    virtual void build_subscription_offsets(
        std::unordered_map<
            propagation_configuration, int64_t /*offset*/,
            propagation_configuration_ignore_offset_hasher,
            propagation_configuration_ignore_offset_equal_functor>&
            offsets ) = 0;

    virtual void set_subscription_info(
        update_enqueuer_subscription_information*
            partition_subscription_info ) = 0;
};

void enqueue_update( const serialized_update& update,
                     void*                    enqueuer_opaque_ptr );
uint32_t get_enqueuer_id( const propagation_configuration& config );
bool propagation_configurations_match_without_offset(
    const propagation_configuration& a, const propagation_configuration& b );

void begin_enqueue_batch( void* enqueuer_opaque_ptr );
void end_enqueue_batch( void* enqueuer_opaque_ptr );

const std::string k_remaster_at_master_cause_string = "remaster_at_master";
const std::string k_remaster_at_replica_cause_string = "remaster_at_replica";
const std::string k_split_cause_string = "split";
const std::string k_merge_cause_string = "merge";
const std::string k_add_replica_cause_string = "add_replica";
const std::string k_remove_replica_cause_string = "remove_replica";
const std::string k_destination_change_cause_string = "destination_change";

