#include "sproc_helpers.h"

#include <glog/logging.h>

void check_args( const std::vector<arg_code> &expected_codes,
                 const std::vector<arg_code> &actual_codes,
                 const std::vector<void *> &  values ) {
#ifdef NDEBUG
    // release
    (void) expected_codes;
    (void) actual_codes;
    (void) values;
#else

    DCHECK_EQ( expected_codes.size(), actual_codes.size() );
    DCHECK_EQ( expected_codes.size(), values.size() );

    for( uint32_t pos = 0; pos < expected_codes.size(); pos++ ) {
        const arg_code &expected_code = expected_codes.at( pos );
        const arg_code &actual_code = actual_codes.at( pos );

        DCHECK_EQ( expected_code.code, actual_code.code );
        DCHECK_EQ( expected_code.array_flag, actual_code.array_flag );
    }
#endif
}

void serialize_sproc_result( sproc_result &               res,
                             std::vector<void *> &        result_values,
                             const std::vector<arg_code> &result_codes ) {
    char * buff = NULL;
    size_t serialize_len =
        serialize_for_sproc( result_codes, result_values, &buff );
    DCHECK_GE( serialize_len, 4 );
    res.res_args.assign( buff, serialize_len );
}
