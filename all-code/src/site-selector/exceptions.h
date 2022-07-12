#pragma once

#include "../gen-cpp/gen-cpp/dynamic_mastering_types.h"

struct site_selector_exception {
    site_selector_exception( exec_status_type::type code ) {
        exception_code_ = code;
    }

    exec_status_type::type exception_code_;
};
