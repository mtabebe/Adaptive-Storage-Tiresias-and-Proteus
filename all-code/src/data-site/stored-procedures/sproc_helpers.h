#pragma once

#include "../site-manager/serialize.h"

void check_args( const std::vector<arg_code> &expected_codes,
                 const std::vector<arg_code> &actual_codes,
                 const std::vector<void *> &  values );

void serialize_sproc_result( sproc_result &               res,
                             std::vector<void *> &        result_values,
                             const std::vector<arg_code> &result_codes );

