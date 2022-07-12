#pragma once

// lock_interface is just an interface to ensure that all the locks follow the
// same API. Don't use lock_interface as a virtual type, instead template to
// avoid the overhead of the virtual function lookup. lookup. lookup. lookup.
class lock_interface {
   public:
    virtual ~lock_interface(){};

    virtual bool lock() = 0;
    virtual bool unlock() = 0;
};
