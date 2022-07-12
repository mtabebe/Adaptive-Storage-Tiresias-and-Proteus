#pragma once

#include "distributions.h"
#include <vector>
template <class T>
std::vector<T> subsample_keys( const std::vector<T> &keys, distributions &dist, int32_t num_keys_to_select ) {

    //No need to sample, return
    if( keys.size() <= (unsigned) num_keys_to_select ) {
        return std::vector<T>( keys );
    }

    //Set up sampling arr
    T const *keys_to_sample[ keys.size() ];
    for( unsigned int i = 0; i < keys.size(); i++ ) {
        keys_to_sample[ i ] = &(keys[i]);
    }


    //Do sample
    std::vector<T> sampled_keys;
    sampled_keys.reserve( num_keys_to_select );
    for( int32_t i = 0; i < num_keys_to_select; i++ ) {
        int32_t end_pos = (int32_t) (keys.size()-i-1);
        int32_t choice = dist.get_uniform_int( 0, end_pos );
        T const* item = keys_to_sample[ choice ];

        //Copy
        sampled_keys.push_back( *item );

        //Swap around to reduce array size
        keys_to_sample[ choice ] = keys_to_sample[ end_pos ];
        
    }

    return sampled_keys;
}

