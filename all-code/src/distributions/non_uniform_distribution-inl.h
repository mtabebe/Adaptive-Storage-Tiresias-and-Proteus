#pragma once

inline non_uniform_distribution::non_uniform_distribution()
    : dist_( nullptr ), c_last_( 0 ), c_id_( 0 ), ol_i_id_( 0 ) {
    c_last_ = dist_.get_uniform_int( 0, 255 );
    c_id_ = dist_.get_uniform_int( 0, 1023 );
    ol_i_id_ = dist_.get_uniform_int( 0, 8191 );
}
inline non_uniform_distribution::non_uniform_distribution( uint32_t c_last,
                                                           uint32_t c_id,
                                                           uint32_t ol_i_id )
    : dist_( nullptr ), c_last_( c_last ), c_id_( c_id ), ol_i_id_( ol_i_id ) {}

inline uint32_t non_uniform_distribution::nu_rand_c_last( uint32_t x,
                                                          uint32_t y ) {
    return nu_rand( 255, c_last_, x, y );
}
inline uint32_t non_uniform_distribution::nu_rand_c_id( uint32_t x,
                                                        uint32_t y ) {
    return nu_rand( 1023, c_id_, x, y );
}
inline uint32_t non_uniform_distribution::nu_rand_ol_i_id( uint32_t x,
                                                           uint32_t y ) {
    return nu_rand( 8191, ol_i_id_, x, y );
}

inline uint32_t non_uniform_distribution::nu_rand( uint32_t A, uint32_t C,
                                                   uint32_t x, uint32_t y ) {
    uint32_t a_rand = dist_.get_uniform_int( 0, A );
    uint32_t x_y_rand = dist_.get_uniform_int( x, y );
    uint32_t diff = y - x + 1;
    uint32_t nur = ( ( ( a_rand | x_y_rand ) + C ) % diff ) + x;
    return nur;
}
