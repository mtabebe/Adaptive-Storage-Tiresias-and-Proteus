namespace java com.adapt_htap //

typedef i32 clientid

enum exec_status_type
{
    COMMAND_OK,                /* a query command that doesn't return
                                 * anything was executed properly by the
                                 * backend */
    TUPLES_OK,                 /* a query command that returns tuples was
                                 * executed properly by the backend, PGresult
                                 * contains the result tuples */
    NONFATAL_ERROR,            /* notice or warning message */
    FATAL_ERROR,               /* query failed */
    INVALID_SVV,               /* A malformed svv was passed for the transaction */
    ERROR_WITH_WRITE_SET,
    MASTER_CHANGE_OK,
    MASTER_CHANGE_ERROR,
    INVALID_SERIALIZATION,      /* the serialization string sent via rpc_stored_procedure is invalid */
    FUNCTION_NOT_FOUND,         /* the function name with argTypes was not found */
    DATA_NOT_LOCATED_AT_ONE_SITE, /*for one shot sprocs*/
    GENERIC_ERROR,
}

struct primary_key {
    1:required i32 table_id,
    2:required i64 row_id
}

struct cell_key {
    1:required i32 table_id,
    2:required i64 row_id
    3:required i32 col_id
}

struct cell_key_ranges {
    1:required i32 table_id,
    2:required i64 row_id_start,
    3:required i64 row_id_end,
    4:required i32 col_id_start,
    5:required i32 col_id_end,
}

struct bucket_key {
    1:required i32 map_key,
    2:required i64 range_key
}

struct partition_identifier {
    1: required i32 table_id,
    2: required i64 partition_start,
    3: required i64 partition_end,
}

struct partition_column_identifier {
    1: required i32 table_id,
    2: required i64 partition_start,
    3: required i64 partition_end,
    4: required i32 column_start,
    5: required i32 column_end,
}

typedef list<i64> site_version_vector
typedef map<i64, i64> snapshot_vector

struct partition_to_primary_key {
    1: list<i32> present_partitions,
    2: map<i32, list<primary_key>> pks_per_partition,
}

struct partition_col_to_cell_key {
    1: list<i32> present_partitions,
    2: map<i32, list<cell_key>> cks_per_partition,
}


const i8 CTT_LOCK = 84; /* T */
const i8 CDT_LOCK = 68; /* D */
const i8 CMT_LOCK = 77; /* M */
const i8 OPEN_LOCK = 79; /* O */

struct ss_db_value {
  1: i8 lock_value,
  2: i16 counter,
  3: clientid id,
  4: i32 site,
}
