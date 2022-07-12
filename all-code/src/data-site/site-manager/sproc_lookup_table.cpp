#include "sproc_lookup_table.h"

#include <glog/logging.h>

sproc_lookup_table::sproc_lookup_table( size_t max_registerable_functions,
                                        size_t max_registerable_scan_functions )
    : sproc_map_( max_registerable_functions ),
      map_size_( max_registerable_functions ),
      scan_map_( max_registerable_scan_functions ),
      scan_map_size_( max_registerable_scan_functions ) {}
void sproc_lookup_table::register_function( function_identifier func_id,
                                            function_skeleton   skel ) {
    auto hasher = function_identifier_hasher();
    DVLOG( 5 ) << "inserting into sproc table:" << func_id.name;
    int64_t hash = hasher( func_id );

    // If the map is overfull, it will fall back to probing multiple maps.
    // This sucks, but isn't an error
    if( sproc_map_.size() >= map_size_ ) {
        LOG( WARNING ) << "Registering sproc in overfull map, have "
                       << sproc_map_.size() << " but supports " << map_size_;
    }
    sproc_map_.insert( {hash, skel} );
}

void sproc_lookup_table::register_scan_function( function_identifier    func_id,
                                                 scan_function_skeleton skel ) {
    auto hasher = function_identifier_hasher();
    DVLOG( 5 ) << "inserting into scan table:" << func_id.name;
    int64_t hash = hasher( func_id );

    // If the map is overfull, it will fall back to probing multiple maps.
    // This sucks, but isn't an error
    if( scan_map_.size() >= scan_map_size_ ) {
        LOG( WARNING ) << "Registering scan in overfull map, have "
                       << scan_map_.size() << " but supports " << scan_map_size_;
    }
    scan_map_.insert( {hash, skel} );
}

function_skeleton sproc_lookup_table::lookup_function(
    const function_identifier &func_id ) {
    DVLOG( 20 ) << "looking up (sproc):" << func_id.name;
    auto    hasher = function_identifier_hasher();
    int64_t hash = hasher( func_id );

    auto search = sproc_map_.find( hash );
    return search != sproc_map_.end() ? search->second : NULL;
}

scan_function_skeleton sproc_lookup_table::lookup_scan_function(
    const function_identifier &func_id ) {
    DVLOG( 20 ) << "looking up (scan):" << func_id.name;
    auto    hasher = function_identifier_hasher();
    int64_t hash = hasher( func_id );

    auto search = scan_map_.find( hash );
    return search != scan_map_.end() ? search->second : NULL;
}
