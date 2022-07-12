#pragma once

#include <atomic>

#include "../../common/packed_pointer.h"
#include "version_types.h"

// The "in-flight" state of a transaction, i.e. a pointer on the value in the
// MVCC chain
// point to this, which says what the state of that transaction is
// N.B.: This class assumes that only one person can modify the
// transaction_state at a time
//(which is the case because of the partition locks on writes). Failure to obey
// this rule
// is going to result in some mega-bad, ultra-confusing errors.
class transaction_state {
   public:
    transaction_state();
    ~transaction_state();

    void set_aborted();
    void set_not_committed();
    void set_committing();
    void set_committed( uint64_t version );

    uint64_t get_version() const;

   private:
    std::atomic<packed_pointer> version_state_;
};
