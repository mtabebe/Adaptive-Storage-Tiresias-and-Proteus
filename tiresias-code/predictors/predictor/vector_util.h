#pragma once

#include <iostream>
#include <vector>
#include <valarray>
#include <dlib/matrix.h>

template <long N>
using dvector = dlib::matrix<double, N, 1>;

using d_var_vector = dlib::matrix<double>;

d_var_vector dvector_from_vector( const std::vector<double>& vec );

template <typename T>
std::ostream& operator<<( std::ostream& os, const std::vector<T>& v );

template<typename T>
std::ostream& operator<<( std::ostream& os, const std::valarray<T>& v );

template<long N>
std::ostream& operator<<( std::ostream& os, const dvector<N>& v );

std::ostream& operator<<( std::ostream& os, const d_var_vector& v );

#include "vector_util.tcc"
