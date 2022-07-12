#include "update_destination_generator.h"

#include <glog/logging.h>

// an interface that stores all the update destinations, this allows
// multiplexing of output streams across partitions

update_destination_generator::update_destination_generator(
    const update_destination_generator_configs& configs )
    : configs_( configs ),
      destinations_(),
      prop_config_to_destination_(),
      hasher_() {}

update_destination_generator::~update_destination_generator() {}

void update_destination_generator::init() {
    DCHECK_EQ( destinations_.size(), configs_.num_update_destinations_ );
}

std::shared_ptr<update_destination_interface> update_destination_generator::
    get_update_destination_by_hashing_partition_id(
        const partition_column_identifier& pid ) const {
    uint32_t pos = hasher_( pid ) % destinations_.size();
    return destinations_.at( pos );
}

std::shared_ptr<update_destination_interface> update_destination_generator::
    get_update_destination_by_propagation_configuration(
        const propagation_configuration& cfg ) const {
    uint32_t pos = lookup_propagation_configuration( cfg );
    return destinations_.at( pos );
}

uint32_t update_destination_generator::lookup_propagation_configuration(
    const propagation_configuration& cfg ) const {
    auto found = prop_config_to_destination_.find( cfg );
    if( found == prop_config_to_destination_.end() ) {
        DLOG( FATAL ) << "Unable to find prop_config:" << cfg
                      << " in existing destinations:";
    }

    uint32_t pos = found->second;
    DCHECK_LT( pos, prop_config_to_destination_.size() );
    return pos;
}

std::shared_ptr<update_destination_interface>
    update_destination_generator::get_update_destination_by_position(
        uint32_t pos ) const {
    DCHECK_LT( pos, destinations_.size() );
    return destinations_.at( pos );
}

void update_destination_generator::add_update_destination(
    std::shared_ptr<update_destination_interface> dest ) {
    DCHECK_LT( destinations_.size(), configs_.num_update_destinations_ );
    destinations_.push_back( dest );

    propagation_configuration lookup_cfg =
        dest->get_propagation_configuration();
    // store this state
    prop_config_to_destination_[lookup_cfg] = destinations_.size() - 1;
}

std::vector<propagation_configuration>
    update_destination_generator::get_propagation_configurations() {

    std::vector<propagation_configuration> prop_configs;
    for( auto dest : destinations_ ) {
        prop_configs.push_back( dest->get_propagation_configuration() );
    }

    return prop_configs;
}

std::vector<int64_t> update_destination_generator::get_propagation_counts() {
    std::vector<int64_t> counts;
    for( auto dest : destinations_ ) {
        counts.push_back( (int64_t) dest->get_num_updates() );
    }
    return counts;
}

std::shared_ptr<update_destination_generator>
    make_no_op_update_destination_generator() {
    auto update_gen = std::make_shared<update_destination_generator>(
        construct_update_destination_generator_configs( 1 ) );
    update_gen->add_update_destination(
        std::make_shared<no_op_update_destination>( 0 ) );
    return update_gen;
}

bool should_seek_topic( const update_propagation_information& new_prop,
                        const update_propagation_information& old_prop ) {

    return should_seek_topic( new_prop.propagation_config_,
                              old_prop.propagation_config_ );
}

bool should_seek_topic( const propagation_configuration& new_prop,
                        const propagation_configuration& old_prop ) {

    bool should_seek = true;
    if( new_prop.type != old_prop.type ) {
        should_seek = true;
    }
    switch( new_prop.type ) {
        case propagation_type::NO_OP:
            should_seek = false;
        case propagation_type::VECTOR:
            should_seek = !( ( new_prop.partition == old_prop.partition ) );
        case propagation_type::KAFKA:
            should_seek = !( ( new_prop.partition == old_prop.partition ) and
                             ( new_prop.topic == old_prop.topic ) );
    }

    DVLOG( 40 ) << "Should_seek_topic: new_prop:" << new_prop
                << ", old_prop:" << old_prop << ", should_seek:" << should_seek;

    return should_seek;
}
