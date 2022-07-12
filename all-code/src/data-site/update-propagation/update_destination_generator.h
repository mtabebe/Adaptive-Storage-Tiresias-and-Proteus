#pragma once

#include "no_op_update_destination.h"
#include "update_destination_interface.h"
#include "update_propagation_configs.h"
#include "vector_update_destination.h"

#include "../../common/partition_funcs.h"

class update_destination_generator {
   public:
    update_destination_generator(
        const update_destination_generator_configs& configs );
    ~update_destination_generator();

    void init();

    std::shared_ptr<update_destination_interface>
        get_update_destination_by_hashing_partition_id(
            const partition_column_identifier& pid ) const;
    std::shared_ptr<update_destination_interface>
        get_update_destination_by_propagation_configuration(
            const propagation_configuration& cfg ) const;
    std::shared_ptr<update_destination_interface>
        get_update_destination_by_position( uint32_t pos ) const;

    void add_update_destination(
        std::shared_ptr<update_destination_interface> dest );

    std::vector<propagation_configuration> get_propagation_configurations();
    std::vector<int64_t>                   get_propagation_counts();

   private:
    uint32_t lookup_propagation_configuration(
        const propagation_configuration& cfg ) const;

    update_destination_generator_configs                       configs_;
    std::vector<std::shared_ptr<update_destination_interface>> destinations_;
    std::unordered_map<propagation_configuration, uint32_t,
                       propagation_configuration_ignore_offset_hasher,
                       propagation_configuration_ignore_offset_equal_functor>
                                           prop_config_to_destination_;
    partition_column_identifier_key_hasher hasher_;
};

std::shared_ptr<update_destination_generator>
    make_no_op_update_destination_generator();

bool should_seek_topic( const update_propagation_information& new_prop,
                        const update_propagation_information& old_prop );
bool should_seek_topic( const propagation_configuration& new_prop,
                        const propagation_configuration& old_prop );
