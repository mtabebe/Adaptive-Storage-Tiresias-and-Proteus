#pragma once

#include "vector_util.h"

template<typename T>
std::ostream& operator<<( std::ostream& os, const std::vector<T>& v ) {
    os << "{ ";
    for (const T& d: v) {
        os << d << " ";
    }
    os << "}";
    return os;
}

template<typename T>
std::ostream& operator<<( std::ostream& os, const std::valarray<T>& v ) {
    os << "{ ";
    for (const T& d: v) {
        os << d << " ";
    }
    os << "}";
    return os;
}

template<long N>
std::ostream& operator<<( std::ostream& os, const dvector<N>& v ) {
    os << "{ ";
    for (const double& d: v) {
        os << d << " ";
    }
    os << "}";
    return os;
}
