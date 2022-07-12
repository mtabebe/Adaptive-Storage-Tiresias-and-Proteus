#include "benchmark_db_operators.h"

#include <glog/logging.h>

#include "struct_conversion.h"

benchmark_db_operators::benchmark_db_operators(
    uint32_t client_id, db_abstraction* db,
    const db_abstraction_configs& abstraction_configs, bool limit_propagation,
    bool merge_snapshots )
    : client_id_( client_id ),
      abstraction_configs_( abstraction_configs ),
      db_( db ),
      txn_holder_( nullptr ),
      state_(),
      global_state_(),
      limit_propagation_( limit_propagation ),
      merge_snapshots_( merge_snapshots ) {}

void benchmark_db_operators::begin_transaction(
    std::vector<::cell_key_ranges>& write_ckrs,
    std::vector<::cell_key_ranges>& read_ckrs, const std::string& name ) {
    DCHECK( txn_holder_ == nullptr );
    LOG( INFO ) << "TXN INFO:" << client_id_ << ", name:" << name
                << ", write_set:" << write_ckrs << ", read_ckrs:" << read_ckrs;
    txn_holder_ = db_->get_partitions_with_begin(
        client_id_, state_, write_ckrs, read_ckrs, false /* allow missing */ );
}

void benchmark_db_operators::begin_scan_transaction(
    std::vector<::cell_key_ranges>& read_ckrs, const std::string& name ) {
    DCHECK( txn_holder_ == nullptr );
    std::vector<::cell_key_ranges> write_ckrs;
    LOG( INFO ) << "TXN INFO:" << client_id_ << ", name:" << name
                << ", write_set:" << write_ckrs << ", read_ckrs:" << read_ckrs;

    txn_holder_ = db_->get_partitions_with_begin(
        client_id_, state_, write_ckrs, read_ckrs, true /* allow_missing */ );
}

void benchmark_db_operators::abort_transaction() {
    DCHECK( txn_holder_ );
    txn_holder_->abort_transaction();
    delete txn_holder_;
    txn_holder_ = nullptr;
}

void benchmark_db_operators::commit_transaction() {
    DCHECK( txn_holder_ );
    auto loc_state = txn_holder_->commit_transaction();
    if( merge_snapshots_ ) {
        merge_snapshot_versions_if_larger( loc_state, global_state_ );
    }
    delete txn_holder_;
    txn_holder_ = nullptr;
}

void benchmark_db_operators::set_transaction_partition_holder(
    transaction_partition_holder* txn_holder ) {
    txn_holder_ = txn_holder;
}

void benchmark_db_operators::add_partitions(
    const std::vector<partition_column_identifier>& pids ) {
    for( const auto& pid : pids ) {
        db_->add_partition( client_id_, pid, db_->get_site_location(),
                            db_->get_default_partition_type( pid.table_id ),
                            db_->get_default_storage_type( pid.table_id ) );
    }
}

void benchmark_db_operators::change_partition_types(
    const std::vector<partition_column_identifier>& pids,
    const std::vector<partition_type::type>&        part_types,
    const std::vector<storage_tier_type::type>&     storage_types ) {
    db_->change_partition_types( client_id_, pids, part_types, storage_types );
}

void benchmark_db_operators::remove_data( const cell_identifier& cid ) {
  // can assert this happened if we want
    txn_holder_->remove_data( cid );
}

#define DB_WRITE_DATA( _ck, _data, _method )             \
    bool _write_ok = txn_holder_->_method( _ck, _data ); \
    DCHECK( _write_ok );

void benchmark_db_operators::write_uint64( const cell_identifier& ck,
                                           uint64_t               data ) {
    DB_WRITE_DATA( ck, data, update_uint64_data );
}
void benchmark_db_operators::write_int64( const cell_identifier& ck,
                                          int64_t                data ) {
    DB_WRITE_DATA( ck, data, update_int64_data );
}
void benchmark_db_operators::write_double( const cell_identifier& ck,
                                           double                 data ) {
    DB_WRITE_DATA( ck, data, update_double_data );
}
void benchmark_db_operators::write_string( const cell_identifier& ck,
                                           const std::string&     data ) {
    DB_WRITE_DATA( ck, data, update_string_data );
}

void benchmark_db_operators::insert_uint64( const cell_identifier& ck,
                                            uint64_t               data ) {
    DB_WRITE_DATA( ck, data, insert_uint64_data );
}
void benchmark_db_operators::insert_int64( const cell_identifier& ck,
                                           int64_t                data ) {
    DB_WRITE_DATA( ck, data, insert_int64_data );
}
void benchmark_db_operators::insert_double( const cell_identifier& ck,
                                            double                 data ) {
    DB_WRITE_DATA( ck, data, insert_double_data );
}
void benchmark_db_operators::insert_string( const cell_identifier& ck,
                                            const std::string&     data ) {
    DB_WRITE_DATA( ck, data, insert_string_data );
}

// MTODO if !limit_propagation;
#define DB_WRITE_LIMIT_DATA( _ck, _data, _limit_method, _reg_method ) \
    bool _write_ok = txn_holder_->_reg_method( _ck, _data );          \
    DCHECK( _write_ok );

void benchmark_db_operators::write_uint64_no_propagate(
    const cell_identifier& ck, uint64_t data ) {
    DB_WRITE_LIMIT_DATA( ck, data, update_uint64_data_no_propagate,
                         update_uint64_data );
}
void benchmark_db_operators::write_int64_no_propagate(
    const cell_identifier& ck, int64_t data ) {
    DB_WRITE_LIMIT_DATA( ck, data, update_int64_data_no_propagate,
                         update_int64_data );
}
void benchmark_db_operators::write_double_no_propagate(
    const cell_identifier& ck, double data ) {
    DB_WRITE_LIMIT_DATA( ck, data, update_double_data_no_propagate,
                         update_double_data );
}
void benchmark_db_operators::write_string_no_propagate(
    const cell_identifier& ck, const std::string& data ) {
    DB_WRITE_LIMIT_DATA( ck, data, update_string_data_no_propagate,
                         update_string_data );
}

void benchmark_db_operators::insert_uint64_no_propagate(
    const cell_identifier& ck, uint64_t data ) {
    DB_WRITE_LIMIT_DATA( ck, data, insert_uint64_data_no_propagate,
                         insert_uint64_data );
}
void benchmark_db_operators::insert_int64_no_propagate(
    const cell_identifier& ck, int64_t data ) {
    DB_WRITE_LIMIT_DATA( ck, data, insert_int64_data_no_propagate,
                         insert_int64_data );
}
void benchmark_db_operators::insert_double_no_propagate(
    const cell_identifier& ck, double data ) {
    DB_WRITE_LIMIT_DATA( ck, data, insert_double_data_no_propagate,
                         insert_double_data );
}
void benchmark_db_operators::insert_string_no_propagate(
    const cell_identifier& ck, const std::string& data ) {
    DB_WRITE_LIMIT_DATA( ck, data, insert_string_data_no_propagate,
                         insert_string_data );
}

#define DB_LOOKUP_DATA( _ck, _method )         \
    auto _found = txn_holder_->_method( _ck ); \
    DCHECK( std::get<0>( _found ) );           \
    return std::get<1>( _found );

uint64_t benchmark_db_operators::lookup_uint64( const cell_identifier& ck ) {
    DB_LOOKUP_DATA( ck, get_uint64_data );
}
int64_t benchmark_db_operators::lookup_int64( const cell_identifier& ck ) {
    DB_LOOKUP_DATA( ck, get_int64_data );
}
double benchmark_db_operators::lookup_double( const cell_identifier& ck ) {
    DB_LOOKUP_DATA( ck, get_double_data );
}
std::string benchmark_db_operators::lookup_string( const cell_identifier& ck ) {
    DB_LOOKUP_DATA( ck, get_string_data );
}

#define DB_LOOKUP_NULLABLE_DATA( _ck, _method ) \
    return txn_holder_->_method( _ck );
std::tuple<bool, uint64_t> benchmark_db_operators::lookup_nullable_uint64(
    const cell_identifier& ck ) {
    DB_LOOKUP_NULLABLE_DATA( ck, get_uint64_data );
}
std::tuple<bool, int64_t> benchmark_db_operators::lookup_nullable_int64(
    const cell_identifier& ck ) {
    DB_LOOKUP_NULLABLE_DATA( ck, get_int64_data );
}
std::tuple<bool, double> benchmark_db_operators::lookup_nullable_double(
    const cell_identifier& ck ) {
    DB_LOOKUP_NULLABLE_DATA( ck, get_double_data );
}
std::tuple<bool, std::string> benchmark_db_operators::lookup_nullable_string(
    const cell_identifier& ck ) {
    DB_LOOKUP_NULLABLE_DATA( ck, get_string_data );
}

uint64_t benchmark_db_operators::lookup_latest_uint64(
    const cell_identifier& ck ) {
    DB_LOOKUP_DATA( ck, get_latest_uint64_data );
}
int64_t benchmark_db_operators::lookup_latest_int64(
    const cell_identifier& ck ) {
    DB_LOOKUP_DATA( ck, get_latest_int64_data );
}
double benchmark_db_operators::lookup_latest_double(
    const cell_identifier& ck ) {
    DB_LOOKUP_DATA( ck, get_latest_double_data );
}
std::string benchmark_db_operators::lookup_latest_string(
    const cell_identifier& ck ) {
    DB_LOOKUP_DATA( ck, get_latest_string_data );
}

std::tuple<bool, uint64_t>
    benchmark_db_operators::lookup_latest_nullable_uint64(
        const cell_identifier& ck ) {
    DB_LOOKUP_NULLABLE_DATA( ck, get_latest_uint64_data );
}
std::tuple<bool, int64_t> benchmark_db_operators::lookup_latest_nullable_int64(
    const cell_identifier& ck ) {
    DB_LOOKUP_NULLABLE_DATA( ck, get_latest_int64_data );
}
std::tuple<bool, double> benchmark_db_operators::lookup_latest_nullable_double(
    const cell_identifier& ck ) {
    DB_LOOKUP_NULLABLE_DATA( ck, get_latest_double_data );
}
std::tuple<bool, std::string>
    benchmark_db_operators::lookup_latest_nullable_string(
        const cell_identifier& ck ) {
    DB_LOOKUP_NULLABLE_DATA( ck, get_latest_string_data );
}

std::vector<result_tuple> benchmark_db_operators::scan(
    uint32_t table_id, uint64_t low_key, uint64_t high_key,
    const std::vector<uint32_t>& project_cols,
    const predicate_chain&       predicate ) {
    return txn_holder_->scan( table_id, low_key, high_key, project_cols,
                              predicate );
}
void benchmark_db_operators::scan( uint32_t table_id, uint64_t low_key,
                                   uint64_t                     high_key,
                                   const std::vector<uint32_t>& project_cols,
                                   const predicate_chain&       predicate,
                                   std::vector<result_tuple>&   results ) {
    txn_holder_->scan( table_id, low_key, high_key, project_cols, predicate,
                       results );
}


void benchmark_db_operators::split_partition( const cell_identifier& cid,
                                              bool split_vertically ) {
  cell_key ck;
  ck.table_id = cid.table_id_;
  ck.col_id = cid.col_id_;
  ck.row_id = cid.key_;

  db_->split_partition( client_id_, state_, ck, split_vertically );
}

void benchmark_db_operators::merge_partition( const cell_identifier& cid,
                                              bool merge_vertically ) {
    cell_key ck;
    ck.table_id = cid.table_id_;
    ck.col_id = cid.col_id_;
    ck.row_id = cid.key_;

    db_->merge_partition( client_id_, state_, ck, merge_vertically );
}
void benchmark_db_operators::remaster_partition( const cell_identifier& cid,
                                                 uint32_t new_master ) {
    auto ckr = cell_key_ranges_from_cell_identifier( cid );
    std::vector<cell_key_ranges> ckrs = {ckr};
    db_->remaster_partitions( client_id_, state_, ckrs, new_master );
}

uint32_t benchmark_db_operators::get_site_location() {
    return db_->get_site_location();
}
