#pragma once

/* TODO: choose these hyperparameters based on the number of updates we expect
 * to buffer before sending and the number of updates we expect to buffer after
 * recv before processing.
 *
 * We want to overprovision these significantly since the cost is just a few
 * kb of space and we can avoid using locks. These numbers must be bigger than
 * the number of threads in the system (plus some significant overhead) to make
 * concurrency problems statistically impossible.
 */

//#define KAFKA_OUT_BUFFER_SIZE 1024*1024*10
#define KAFKA_OUT_BUFFER_SIZE 1024 * 1024 * 20
#define KAFKA_IN_BUFFER_SIZE 1024

#define TIMER_DEPTH 100

#define SPAR_PERIODIC_QUANTITY 7
#define SPAR_TEMPORAL_QUANTITY 30
