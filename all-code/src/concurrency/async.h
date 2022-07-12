#pragma once

// Async in production, Synchronized for testing purposes
#if !defined( UNIT_TEST )
#define async( func, args... )             \
    do {                                   \
        {                                  \
            std::thread thr( func, args ); \
            thr.detach();                  \
        }                                  \
    } while( 0 );
#else
#define async( func, args... )             \
    do {                                   \
        {                                  \
            std::thread thr( func, args ); \
            thr.join();                    \
        }                                  \
    } while( 0 );
#endif
