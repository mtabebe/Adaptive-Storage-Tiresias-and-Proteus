#pragma once

#include "update_source_interface.h"

#include "../../concurrency/semaphore.h"

class vector_update_source : public update_source_interface {
  public:
   vector_update_source();
   ~vector_update_source();

    void start();
    void stop();

    void enqueue_updates( void* enqueuer_opaque_ptr );

    bool add_source( const propagation_configuration& config,
                     const partition_column_identifier& pid, bool do_seek,
                     const std::string& cause );
    bool add_sources( const propagation_configuration&         config,
                      const std::vector<partition_column_identifier>& pids,
                      bool do_seek, const std::string& cause );

    std::tuple<bool, int64_t> remove_source(
        const propagation_configuration&   config,
        const partition_column_identifier& pid, const std::string& cause );

    void add_update( const serialized_update& serialized );

    void build_subscription_offsets(
        std::unordered_map<
            propagation_configuration, int64_t /*offset*/,
            propagation_configuration_ignore_offset_hasher,
            propagation_configuration_ignore_offset_equal_functor>& offsets );
    void set_subscription_info(
        update_enqueuer_subscription_information* partition_subscription_info );

   private:
    std::vector<serialized_update> expected_;
    std::unordered_set<propagation_configuration,
                       propagation_configuration_ignore_offset_hasher,
                       propagation_configuration_ignore_offset_equal_functor>
                                              prop_configs_;
    update_enqueuer_subscription_information* sub_info_;

    uint32_t                       next_read_pos_;
    semaphore                      lock_;
};
