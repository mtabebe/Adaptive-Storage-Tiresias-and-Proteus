This directory contains the source code for this repository.

We describe the high-level organization, then details of the implementation, followed by notes on potential future optimizations.

# Organization

## [Common](common/)
[common](common/) contains [hardware](common/hw.h) specific details and generic re-usable components like [packed pointers](common/packed_pointer.h), and a [timer](common/timer.h) function. As well as all the definitions of [configurable constants and flags](common/constants.h) used in the source.

## [Concurrency](concurrency/)
[concurrency](concurrency/) contains different implementations of a [lock interface](concurrency/lock_interface.h), such as [semaphores](concurrency/semaphore.h) and [spin locks](concurrency/spinlock.h)

## [Distributions](distributions/)
[distributions](distributions/) contains an implementation of the [zipfian distribution](distributions/zipf_distribution_cdf.h).

## [DB](data-site/db)
This contains our [database](data-site/db) implementation. Which uses two-phased locking (2PL) to lock partitions, and multi-version concurrency control (MVCC) to perform non-blocking reads.

## [Benchmarks](benchmark/)
This contains implementations of benchmarks, which extend from the base [benchmark interface](benchmark/benchmark_interface.h). Specific implementations like [ycsb](benchmark/ycsb/) have their own [implementation](benchmark/ycsb/ycsb_benchmark.h).

## [Thrift interface](thrift-interface)
This defines the RPC interface for the site selector and site manager.

## [Site Manager](data-site/site-manager)
This contains the [site manager implementation](data-site/site-manager/site_manager_handler.h) , which wraps the [database](data-site/db) to perform operations through the thrift interface. To use stored procedures the [sproc lookup table](data-site/site-manager/sproc_lookup_table.h) maps method signatures to functions.

## [Site Selector](site-selector/)
This contains the implementation of the site selector, which is copied from [dynamic mastering](https://github.com/mtabebe/dynamic-mastering/tree/f7f93a25be53486a514cbc1bc7c4ae0fdec0dcde/site_selector) with some slight modifications to conform to style, eliminiate dead code, and fit in line with the constants. More details are in the [readme](site-selector/README.md)

## [Store Procedures](data-site/stored-procedures)
This contains the stored procedures that operate on the database, such as the [ycsb stored procedures](data-site/stored-procedures/ycsb).

## [Update Propagation](data-site/update-propagation/)
This contains all the code that manages applying and propagating updates between the databases.

## [Templates](templates/)
The sheer volume of using templates everywhere in the code means that adding one template to a class, makes it a *huge effort* to change all the places that use it. To avoid that problem we standardize common templates in this directory and strive to use them in places so that initial type changes only need to be done in one place.

# Implementation

## Database Implementation
### Overview
At a high level the design of our database is as follows.

We split the database up into continuous ranges, which we call partitions. Each partition has a lock associated with the partition that must be locked if any record in the partition is written. The [table](data-site/db/table.h) class defines the range and lock mappings.

When a transaction begins it must lock all the partitions it wishes to updates, and then it can perform its reads and writes.

For concurrency control we use an MVCC model, that is integrated with a client session consistency model. Therefore we have a hierarchy of classes to define the different structures in the system. We start from the lowest level and build up to the highest interface.

### Details

***[Packed Records](data-site/db/packed_record.h)***

A packed record is the basic unit of storage for a data item. It is either a pointer to a data item or if the data item is less than 8 bytes, an inline copy of the data item.

***[Record](data-site/db/record.h)***

A record contains a packed record, and some metadata about the version of the record, such as whether the record has been deleted.

The version is either the known version number, or a pointer to some transaction state. Transaction states are in one of three [states](data-site/db/version_types.h): `NOT COMMITTED`, `IN COMMIT` or `COMITTED`. A record is only visible to other transactions if it is `COMITTED`.

***[MVCC Chain](data-site/db/mvcc_chain.h)***

The main logic related to MVCC state is contained in an MVCC Chain. An MVCC chain is a list-like structure, that contains all the versions of a given record. Each version of a record has a known version, which reflects the commit number of the transaction that performed the update of the record, and is tied to the site where the transaction originally took place. Therefore, there is a direct way to translate between a version number on a record, and the session information a client has.

We organize MVCC chains as follows:

<pre>
[Record, Record, Record] [Next MVCC Chain*]
</pre>

That is, an MVCC chain contains an array of records, which may not yet have been updated, and a pointer to the previous chain in the list.

Records are laid out such that the earliest record is at the end of the array and we fill the array from back to front. A record is visible within a window that begins from their version number, and ends at the version of the next record in the chain. Consider the following example where we show only version history for records:

<pre>
[NULL, IN COMMIT, version=7, version=5, version=2].
</pre>

In this example if a transaction wanted to read at version 1, they would not find any record that satisfied them. That is because the record was first written at version 2.

Any read that occurs between versions 2 and 4 will see the version 2, and reads to versions 5 and 6 will see version 5.

The tricky part is what a read at version 8 would see. If the transaction that is currently ends up with version 8, then clearly it should see that version. But otherwise it should see version 7. Therefore we must spin and wait to see the assigned version for the transaction in commit. We have an optimization in the code that avoids spinning on a transaction that is in commit unless it is absolutely necessary. It is for this reason that we store a pointer in the record and change it after the fact. This allows a single atomic update to change the transaction state from `NOT COMMITTED` to `IN COMMIT` and to `COMMITTED` for all the records that have been changed in the transaction.


The above rule does not apply for transactions that are not committed, or committed. If a transaction has not committed, it is clear that the assigned version cannot be earlier than the version that a transaction wishes to read at.

Garbage collection takes place by removing chains that have no visible versions (that is a low water mark that would read the data)

Remastering a data item requires that a new chain is created, as the assumption is that all the records on the same chain are for updates that are mastered at the same data site.

***[Versioned Record](data-site/db/versioned_record.h)***

A versioned record is a permanent structure that simply stores the key of a data item, and a pointer to  the most recent chain. This structure is useful for storing in some sort of index for the data item.

***[Table](data-site/db/table.h)***

A table is simply a hash table of versioned records and performs the mapping of reads and writes to the correct versioned record. Our hash table does not handle collisions, so make sure the table is the correct size.

The table also maintains locks for the records.

***[DB](data-site/db/db.h)***

The database is the interface that clients interact with to perform transactions. The database consists of tables, and client state.

The database also contains the site version vector, and delegates update propagation work, upaate application, and garbage collection to worker threads.

## [Update Propagation](data-site/update-propagation/)

Update application and propagation is implemented on the database as templates. This allows flexible implementations of propagation and application for unit tests, and execution.

***[Write Buffer Serialization](data-site/update-propagation/write_buffer_serialization.h)***

To propagate updates we need to serialize the [write buffer](data-site/db/write_buffer.h) which contains changes made in a database transaction into a format that can be sent across the wire. Write Buffer Serialization converts write buffers into a serialized update structure that can be written to the wire.

On the other side, we convert updates into a *deserialized update* which contains the origin, the commit version vector, and a set of [deserialized update records](data-site/db/deserialized_update_record.h). These records do not copy data, and are copied into the record when they are written.

***[Update Propagation Interface](data-site/update-propagation/update_propagator_interface.h)***

Is the interface for propagating updates.

The [no_op_propagator](data-site/update-propagation/no_op_propagator.h) is the simplest implementation: it simply drops updates.

The [thread_writing_update_propagator](data-site/update-propagation/thread_writing_update_propagator.h) stores write buffers in a [kafka buffer](data-site/update-propagation/kafka_buffer.h), which indexes updates by their commit version. A separate thread is then used to serialize the write buffer, and write the buffer out. Note that this thread needs to maintain it's own watermarks, which is done on a per client basis.

To separate unit tests, how data is written out, is abstracted by the [update_propagation_writer_interface](data-site/update-propagation/update_propagation_writer_interface.h).

For [unit test](../../test/db_test.cpp) purposes one implementation is the [vector_propagation_writer](data-site/update-propagation/vector_propagation_writer.h) which simply stores all the updates in a local vector.

For production use the [kafka_propagation_writer](data-site/update-propagation/kafka_propagation_writer.h) actually pushes data out to kafka using the [librdkafka](https://github.com/edenhill/librdkafka/blob/master/src/rdkafka.h) API.

***[Update Applier Interface](data-site/update-propagation/update_applier_interface.h)***

Is the interface for applying updates.

The [no_op_applier](data-site/update-propagation/no_op_applier.h) is the simplest implementation, it does not apply any updates.

The [thread_reading_update_applier](data-site/update-propagation/thread_reading_update_applier.h) is the true implementation of update application. It takes in a set of [update readers](data-site/update-propagation/update_application_reader_interface.h) and creates a new thread for each reader. Each thread then instructs the reader to apply updates.

Update application is implemented as a series of [helper methods](data-site/update-propagation/update_applier_helper.h). Logically, given a serialized update, the database tables, the transaction state, and the site version vector update applications proceeds by deserializing the update into a *deserialized update*, waiting until the session is satisfied, locking the write set, applying the updates, committing, and unlocking the updates.

Each of the [update readers](data-site/update-propagation/update_application_reader_interface.h) pulls a serialized update, and if it is valid will call these [helpers](data-site/update-propagation/update_applier_helper.h)

The [vector_application_reader](data-site/update-propagation/vector_application_reader.h) has a set of stored updates to applied and is used for [unit tests](../../test/db_test.cpp).

The [kafka_application_reader](data-site/update-propagation/kafka_application_reader.h) pulls updates from Kafka, by consuming updates, and in a callback, calling the update application [helpers](data-site/update-propagation/update_applier_helper.h)

***Kafka Configuration***

To centralize Kafka configuration we keep structures of [configs](data-site/update-propagation/kafka_configs.h) that are used when interfacing with kafka.

We also provide a series of [helpers](data-site/update-propagation/kafka_helpers.h) that are used to set up, and add configuration to kafka.

## [Benchmarks](benchmark/)

Benchmarks share generic [configurations](benchmark/benchmark_configs.h), such as how long the benchmark runs for and how many concurrent clients are used. There is also a common re-usable class for [picking the next operation](benchmark/workload_operation_selector.h). Finally we track how many operations occurred in a re-usable [benchmark statistics](benchmark/benchmark_statistics.h) structure, that counts per client, and then can merge at the end.

Each [benchmark](benchmark/benchmark_interface.h) must support:
* creating a database
* loading the database
* running the actual workload

For [YCSB](benchmark/ycsb/ycsb_benchmark.h) we implement this by creating a [worker](benchmark/ycsb/ycsb_benchmark_worker.h) that does the interaction with the database. Each worker in turn selects the operation and then runs the actual operation on the database.

To run a benchmark simply call `run_benchmark` as defined in the [benchmark executor](benchmark/benchmark_executor.h).

## Other implementation notes

### [Packed pointers](common/packed_pointer.h)

A pointer is an 8 byte integer, however current machines only use a 48 bit (6 byte) addressing scheme. This allows us to have 2 bytes that can be used to store extra flag information. This is useful for tracking some metadata, like whether we are actually storing a pointer, or some 6 byte structure.

We use this information when storing version state by either storing a pointer to the transaction state, or if we are storing an actual version number.

### [Client State Information](data-site/db/)

This class stores the information needed for ongoing transactions for clients like their session information, write buffer (changes performed in the transaction), and the partitions that they have locked.

We also track transaction state, which represents whether the ongoing transaction for the client is not committed, in commit, or committed. We re-use this state, and therefore we need a way for clients to make sure that they are not seeing potentially stale data. Therefore we track a counter in the transaction state that updates with every transaction.

### [Zipfian Distribution](distributions/zipf_distribution_cdf.h)

This class selects items following a zipfian distribution with parameter alpha. Accessing an item with a key operates by picking a number uniformly in the range 0, 1. Then finding the key whoses CDF maps to that key. The CDF follows the zipfian distribution.

Therefore to optimize this lookup, we first compute all the CDF's for the range of keys, and then binary search. Therefore, the zipfian distribution is computed once and passed into implementations of a generic [distribution](distributions/distributions.h). Because random number generators are not thread safe, every thread should create their own distribution.

### [Constants](common/constants.h)

These constants define all the knobs and toggles that are used in the system. They are statically defined and generally of the form `k_constant`. Each constant has an associated [gflag](http://gflags.github.io/gflags/) with it, which has a help message that defines the meaning. Most of the places that use these constants take the constant as a default parameter, which allows overriding of constants for unit tests.

It is important that an executable calls `init_constants` to set up the constants to actually reflect the specified constant.

# Optimizations and Future Optimizations

## Inlining
Any methods that are short and small could be inlined, and forced to be inlined with the [gcc attributed](https://gcc.gnu.org/ml/gcc-help/2007-01/msg00049.html) `__attribute__((always_inline))`.

This directive will force the compiler to inline the method, to avoid popping and pushing state in a function calls. The reason this is necessary is that the `inline` keyword does not force the compiler to actually inline anything, instead it is a hint.

Others argue that `inline` is not really necessary because the compiler is sufficiently good at doing this.

To address readability concerns we separate out inline functions from the header into a `header_name-inl.h` file and `#include the` the inlined file in the header. We use the `ALWAYS_INLINE` macro that is defined in [hw.h](common/hw.h).

## Return rvalues and use move constructors
There are some places in the code where we could return rvalues and use move constructors. For instance when returning a string in a [packed_record](data-site/db/packed_record.h).

For now we have left it because the return value optimization should kick in.

## Templates instead of virtual functions
Virtual functions that result from virtually pointed objects can be expensive. To avoid this overhead we define an interface and derive classes from that type. However, instead of using the virtual object we template the type and instantiate the template with the derived type.

Look at [concurrency](concurrency/) as an example, and [table](data-site/db/table.h) and [lock_test](../test/lock_test.cpp) as an example of how they are used.

## Cache line consciousness
Where possible we assert the sizes of objects (e.g. [records](data-site/db/record.h)) and try to make sure they align as either 16, 32, or 64 byte objects to avoid objects that span cache lines.

The [spinlock](concurrency/spinlock.h) implementation is conscious of cache lines by separating out the `ticket` and `serving` variables from one cache line to another.

## Avoid synchronization wherever possible
In our code, we have two points that require hard synchronization:

1. The partition locks for tables, which are enforced by atomic ticket locks
2. The version counters (counter_locks), which are atomic uint64_t's.

Per the Intel SDM (Volume 3, Section 8.3 and 11.10) and the glibc source code, we can make the following observations:

1. instructions cannot be reordered around the std::atomic instructions.
2. On x86_64, all std::atomic operations translate to asm_volatile ops with LOCK and the "memory" clobber keyword.
3. LOCK/SFENCE/MFENCE flush the CPU store buffers out to memory.
4. the memory clobber keyword forces the compiler to push any values it has in registers out to memory, and afterward to load all values that it previously had in registers back in from memory.

This allows us to aggressively optimize around our two synchronization points because any values written by a previous transaction in the same partition will be visible to the next writer for that partition just by virtue of acquiring/releasing the lock. Similarly, by atomically loading the counter value, any data that was written by another thread to incrementing the counter will be visible to the reader.

These guarantees don't provide everything though --- loads/stores to different counters do not guarantee that threads will see each other's writes, and it is possible for readers to observe partial state from writers in the process of executing transactions. Thankfully, the DM++ version counters encapsulate dependencies and the mvcc_chains were directly built to address the latter problem, so it is a good design fit.

Since there are no true locks for readers, it is important to design functions around possibly seeing state from future writers in other records of the mvcc_chains and to realize that transaction state can change underneath you.

As a set of general rules:
1. If a variable can be changed concurrently by multiple writers (counter_locks) then you need to use atomics to enforce it. Store buffers will be flushed on write to them and other operations cannot be reordered across atomic operation boundaries. You can spin on these.
2. Dependencies are currently enforced by counter_locks's atomics, which means that if you set the state you want a dependent transaction to see and then increment the counter, anyone who sees the incremented count version will also see the state you set. Other people may see the state before you set the counter, but if you want to guarantee that the state is there, rely on the counter. Be careful that you don't rely on "overwritable" state (the mvcc chains and client independent versions keep these concerns to a minimum).
3. If only one person modifies it but you want readers to see the value, you likely want to use volatile UNLESS the value is written on one side of an atomic op and readers won't check it until they've got a certain value from the atomic op (See (2)). Note that volatile does NOT flush store buffers, unlike atomic ops, so you will want to manually SFENCE() if you want the values
to be immediately visible.

Because of sequential consistency on x86, the following desing pattern is quite useful:
If the writer does writes to two variables in pairs (w[x], w[y]), then:
```
r[x] <- get value for x
r[y] <- get value for y
r[x] <- test if value of x changed.
```
If not, then either we read a correct pair (x, y) or we read the y from the previous pair and the x from the next.
But, if we can rule out the second condition because due to ordering guarantees, then we have a consistent read without the use
of any atomic operations. We know that at the start of a transaction (before doing the update for x), we set a flag that says a new transaction is in progress. If we we don't see that flag, then the (x,y) pairing is valid because the next x is only written after that flag is set, and x86 provides sequential consistency.

An example of this is provided in [transaction_state](data-site/db/transaction_state.cpp).
