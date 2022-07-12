#include "update_source_interface.h"

#include <folly/Hash.h>

#include "update_enqueuer.h"

void enqueue_update( const serialized_update& update,
                     void*                    enqueuer_opaque_ptr ) {
    stashed_update stashed = create_stashed_update( update );
    update_enqueuer* enqueuer = (update_enqueuer*) enqueuer_opaque_ptr;

    enqueuer->enqueue_stashed_update( std::move( stashed ) );
}

void begin_enqueue_batch( void* enqueuer_opaque_ptr ) {
    update_enqueuer* enqueuer = (update_enqueuer*) enqueuer_opaque_ptr;
    enqueuer->begin_enqueue_batch();
}
void end_enqueue_batch( void* enqueuer_opaque_ptr ) {
    update_enqueuer* enqueuer = (update_enqueuer*) enqueuer_opaque_ptr;
    enqueuer->end_enqueue_batch();
}

uint32_t get_enqueuer_id( const propagation_configuration& config ) {
    propagation_configuration        config_c = config;
    config_c.offset = 0;
    propagation_configuration_hasher hasher;
    uint32_t id = (uint32_t) hasher( config_c );
    return id;
}

bool propagation_configurations_match_without_offset(
    const propagation_configuration& a, const propagation_configuration& b ) {
    propagation_configuration        config_a = a;
    propagation_configuration        config_b = b;

    config_a.offset = 0;
    config_b.offset = 0;

    propagation_configuration_equal_functor equaler;
    bool equals = equaler( config_a, config_b );
    return equals;
}
