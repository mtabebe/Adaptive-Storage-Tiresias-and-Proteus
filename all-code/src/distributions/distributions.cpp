#include "distributions.h"

#include <cstring>

distributions::distributions( zipf_distribution_cdf* dist_cdf )
    : dist_cdf_( dist_cdf ),
      generator_(),
      real_distribution_(),
      int_distribution_() {}

std::string distributions::write_uniform_str( uint32_t lower_len,
                                              uint32_t upper_len ) {
    uint32_t    len = get_uniform_int( lower_len, upper_len );
    std::string dest;
    dest.reserve( len );
    for( uint32_t i = 0; i < len; i++ ) {
        dest.push_back( get_uniform_char() );
    }
    return dest;
}

std::string distributions::write_uniform_nstr(
                                        uint32_t lower_len,
                                        uint32_t upper_len ) {
    uint32_t len = get_uniform_int( lower_len, upper_len );
    std::string dest;
    dest.reserve( len );
    for( uint32_t i = 0; i < len; i++ ) {
        dest.push_back( get_uniform_nchar() );
    }
    return dest;
}

std::string distributions::write_last_name( uint32_t id ) {
    static const char* const SYLLABLES[] = {
        "BAR", "OUGHT", "ABLE",  "PRI",   "PRES",
        "ESE", "ANTI",  "CALLY", "ATION", "EING",
    };
    static const uint32_t LENGTHS[] = {
        3, 5, 4, 3, 4, 3, 4, 5, 5, 4,
    };
    DCHECK_GE( id, 0 );
    DCHECK_LE( id, 999 );

    std::vector<uint32_t> indices = {id / 100, ( id / 10 ) % 10, id % 10};
    std::string           dest;
    for( uint32_t index : indices ) {
        dest.insert( dest.size(), SYLLABLES[index], LENGTHS[index] );
    }
    return dest;
}

void distributions::write_uniform_str( uint32_t* dest_len, char* dest,
                                       uint32_t lower_len,
                                       uint32_t upper_len ) {
    uint32_t len = get_uniform_int( lower_len, upper_len );
    *dest_len = len;
    for( uint32_t i = 0; i < len; i++ ) {
        dest[i] = get_uniform_char();
    }
    dest[len] = '\0';
}

void distributions::write_uniform_nstr( uint32_t* dest_len, char* dest,
                                        uint32_t lower_len,
                                        uint32_t upper_len ) {
    uint32_t len = get_uniform_int( lower_len, upper_len );
    *dest_len = len;
    for( uint32_t i = 0; i < len; i++ ) {
        dest[i] = get_uniform_nchar();
    }
    dest[len] = '\0';
}

void distributions::write_last_name( uint32_t* len, char* dest, uint32_t id ) {
    static const char* const SYLLABLES[] = {
        "BAR", "OUGHT", "ABLE",  "PRI",   "PRES",
        "ESE", "ANTI",  "CALLY", "ATION", "EING",
    };
    static const uint32_t LENGTHS[] = {
        3, 5, 4, 3, 4, 3, 4, 5, 5, 4,
    };
    DCHECK_GE( id, 0 );
    DCHECK_LE( id, 999 );

    uint32_t              offset = 0;
    std::vector<uint32_t> indices = {id / 100, ( id / 10 ) % 10, id % 10};
    for( uint32_t index : indices ) {
        std::memcpy( dest + offset, SYLLABLES[index], LENGTHS[index] );
        offset += LENGTHS[index];
    }
    *len = offset;
    dest[offset] = '\0';
}

std::string distributions::generate_string_for_selectivity(
    double selectivity, uint32_t max_len, const std::string& str ) const {
    DCHECK_GE( selectivity, 0.0 );
    DCHECK_LE( selectivity, 1.0 );

    std::string s;
    double      selectivity_so_far = 0;

    double acceptable_low_range =
        std::max( (double) 0, selectivity - ( selectivity * 0.01 ) );
    double acceptable_high_range =
        std::min( (double) 1, selectivity + ( selectivity * 0.01 ) );

    while( s.size() < max_len ) {
        DVLOG( 40 ) << "Selectivity so far:" << selectivity_so_far << ", s:" << s;
        if( ( selectivity_so_far >= acceptable_low_range ) and
            ( selectivity_so_far <= acceptable_high_range ) ) {
            break;
        }
        if( s.size() == 0 ) {
            selectivity_so_far = 1;
        }
        uint32_t pos =
            ( (double) ( selectivity / selectivity_so_far ) ) * str.size();
        pos = std::min( pos, ( uint32_t )( str.size() - 1 ) );
        s.push_back( str.at( pos ) );
        double selectivity_of_op =
            ( (double) ( pos + 1 ) ) / ( (double) str.size() );
        selectivity_so_far = selectivity_so_far * selectivity_of_op;
        if( pos == str.size() - 1 ) {
            break;
        }
        DVLOG( 40 ) << "Updated selectivity so far:" << selectivity_so_far << ", s:" << s;
    }

    return s;
}
