#pragma once

#include <cstdint>
#include <vector>

// 2^47 ==> 140 trillion
// http://www.tsm-resources.com/alists/pow2.html
// can have at most 140 trillion updates
const static uint64_t K_2_48 = 281474976710656;
// the max number of updates
const static uint64_t K_2_47 = K_2_48 / 2;

// store the flags (< 16 bits) in the top part of transaction state counter
const static uint64_t K_ABORTED_FLAG = 2;
// K_ABORTED_FLAG needs to be less than K_COMMITTED so that don't trigger any
// asserts

const static uint64_t K_COMMITTED_FLAG = K_ABORTED_FLAG * 2;        // 4
const static uint64_t K_IN_COMMIT_FLAG = K_COMMITTED_FLAG * 2;      // 8
const static uint64_t K_NOT_COMMITTED_FLAG = K_IN_COMMIT_FLAG * 2;  // 16

// Define the constants so that we can add flags to the K_2_47 to get the value
const static uint64_t K_NOT_COMMITTED =
    K_2_47 + K_NOT_COMMITTED_FLAG;                              // 2^47 + 16
const static uint64_t K_IN_COMMIT = K_2_47 + K_IN_COMMIT_FLAG;  // 2^47 + 8
const static uint64_t K_COMMITTED = K_2_47 + K_COMMITTED_FLAG;  // 2^47 + 4
const static uint64_t K_ABORTED = K_2_47 + K_ABORTED_FLAG;      // 2^47 + 2

static_assert( K_ABORTED < K_COMMITTED,
               "K_COMMITTED should be less than K_IN_COMMIT" );
static_assert( K_COMMITTED < K_IN_COMMIT,
               "K_COMMITTED should be less than K_IN_COMMIT" );
static_assert( K_COMMITTED < K_NOT_COMMITTED,
               "K_COMMITTED should be less than K_NOT_COMMITTED" );
static_assert( K_IN_COMMIT < K_NOT_COMMITTED,
               "K_IN_COMMIT should be less than K_NOT_COMMITTED" );

typedef std::vector<uint64_t> client_version_vector;
