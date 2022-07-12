#pragma once

#define aligned_atomic_types template <typename T>
#define aligned_atomic_T aligned_atomic<T>

aligned_atomic_types inline aligned_atomic_T::aligned_atomic() {}
aligned_atomic_types void inline aligned_atomic_T::store(
    T value, std::memory_order order ) {
    value_.store( value, order );
}

aligned_atomic_types T inline aligned_atomic_T::load(
    std::memory_order order ) {
    return value_.load( order );
}
