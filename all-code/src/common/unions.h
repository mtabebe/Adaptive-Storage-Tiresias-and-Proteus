#pragma once

/*
 * It turns out that casting between types that are not char* or is undefined
 * in C/C++ because of pointer aliasing.
 * Therefore aggresive compilation gives you errors when you try to convert
 * between types.
 * Instead use a union. Technically this is not defined in the spec either,
 * but GCC supports it so it is the best that you can do.
 * https://blog.regehr.org/archives/959
 */

union _4_bytes {
    int          as_int;
    float        as_float;
    unsigned int as_uint;
};

union _8_bytes {
    long     as_long;
    double   as_double;
    uint64_t as_uint64;
};
