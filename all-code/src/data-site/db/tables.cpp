#include "tables.h"

#include <glog/logging.h>

tables::tables()
    : tables_(), metadata_(), update_gen_( nullptr ), enqueuers_( nullptr ) {}
tables::~tables() {
    for( uint32_t pos = 0; pos < tables_.size(); pos++ ) {
        delete tables_.at( pos );
        tables_.at( pos ) = nullptr;
    }
    tables_.clear();
}

void tables::init( const tables_metadata&                        metadata,
                   std::shared_ptr<update_destination_generator> update_gen,
                   std::shared_ptr<update_enqueuers>             enqueuers ) {
    metadata_ = metadata;
    tables_.reserve( metadata_.expected_num_tables_ );
    update_gen_ = update_gen;
    enqueuers_ = enqueuers;
}

/*
Creates a table with at most num_records in it, with locked partitions of
size
partition_size
TODO: not thread safe, not sure if it matters
*/
uint32_t tables::create_table( const table_metadata& metadata ) {
    DVLOG( 7 ) << "Tables create table:" << metadata;
    uint32_t table_id = metadata.table_id_;

    DCHECK_LT( table_id, metadata_.expected_num_tables_ );

    table_metadata t_metadata = create_table_metadata(
        metadata.table_name_, table_id, metadata.num_columns_,
        metadata.col_types_, metadata.num_records_in_chain_,
        metadata.num_records_in_snapshot_chain_, metadata_.site_location_,
        metadata.default_partition_size_, metadata.default_column_size_,
        metadata.default_tracking_partition_size_,
        metadata.default_tracking_column_size_,
        metadata.default_partition_type_, metadata.default_storage_type_,
        metadata_.enable_secondary_storage_,
        metadata_.secondary_storage_dir_ + "/" + std::to_string( table_id ) );

    if( table_id < tables_.size() ) {
        return table_id;
    }
    DCHECK_EQ( table_id, tables_.size() );

    enqueuers_->create_table( metadata );

    table* t = new table;
    tables_.emplace_back( t );
    DCHECK_EQ( table_id, tables_.size() - 1 );
    execute_table_operation( init, table_id, t_metadata, update_gen_,
                             enqueuers_ );
    return table_id;
}

transaction_partition_holder* tables::get_partition_holder(
    const partition_column_identifier_set&   write_pids,
    const partition_column_identifier_set&   read_pids,
    const partition_lookup_operation& true_lookup_operation ) {
    transaction_partition_holder* partition_holder =
        new transaction_partition_holder();
    bool allow_missing = false;
    partition_lookup_operation lookup_operation = true_lookup_operation;
    if( lookup_operation == partition_lookup_operation::GET_ALLOW_MISSING ) {
        allow_missing = true;
        lookup_operation = partition_lookup_operation::GET;
    }
    for( const auto& pid : write_pids ) {
        std::shared_ptr<partition> partition =
            std::move( get_partition_via_lookup_op( pid, lookup_operation,
                                                    false /*not a read*/ ) );
        if ( partition ) {
            partition_holder->add_write_partition( pid,
                                                   std::move( partition ) );
        } else if( !allow_missing ) {
            DLOG( WARNING ) << "Unable to get write set partition:" << pid
                            << ", lookup_operation:" << lookup_operation;
            delete partition_holder;
            return nullptr;
        }
    }
    if( write_pids.size() > 0 ) {
        auto write_tables = partition_holder->get_tables();
        for( const auto& tid : write_tables ) {
            partition_holder->add_table_poll_epoch(
                tid, tables_.at( tid )->get_poll_epoch() );
        }
    }

    for( const auto& pid : read_pids ) {
        std::shared_ptr<partition> partition =
            std::move( get_partition_via_lookup_op( pid, lookup_operation,
                                                    true /*read*/ ) );
        if( partition ) {
            partition_holder->add_read_partition( pid, std::move( partition ) );
        } else if( !allow_missing ) {
            DLOG( WARNING ) << "Unable to get read set partition:" << pid
                            << ", lookup_operation:" << lookup_operation;
            delete partition_holder;
            return nullptr;
        }
    }

    return partition_holder;
}

std::shared_ptr<partition> tables::get_partition_via_lookup_op(
    const partition_column_identifier& pid, const partition_lookup_operation& op,
    bool is_read ) {

    partition_lookup_operation rewritten_op = op;

    if( op == partition_lookup_operation::GET_WRITE_MAY_CREATE_READS ) {
        rewritten_op = partition_lookup_operation::GET;
        if (is_read) {
            rewritten_op = partition_lookup_operation::GET_OR_CREATE;
        }
    } else if( op == partition_lookup_operation::
                         GET_WRITE_IF_ACTIVE_MAY_CREATE_READS ) {
        rewritten_op = partition_lookup_operation::GET_IF_ACTIVE;
        if (is_read) {
            rewritten_op = partition_lookup_operation::GET_OR_CREATE;
        }
    }

    std::shared_ptr<partition> part = nullptr;
    if ( rewritten_op == partition_lookup_operation::GET_IF_ACTIVE) {
        return get_partition_if_active( pid );
    } else if( rewritten_op == partition_lookup_operation::GET ) {
        return get_partition( pid );
    }
    DCHECK_EQ( rewritten_op, partition_lookup_operation::GET_OR_CREATE );
    return get_or_create_partition( pid );
}

std::shared_ptr<partition> tables::get_partition_if_active(
    const partition_column_identifier& pid ) const {
    uint32_t                   table_id = pid.table_id;
    return std::move(
        execute_table_operation( get_partition_if_active, table_id, pid ) );
}

std::shared_ptr<partition> tables::get_partition(
    const partition_column_identifier& pid ) const {
    uint32_t                   table_id = pid.table_id;
    return std::move( execute_table_operation( get_partition, table_id, pid ) );
}

std::shared_ptr<partition> tables::get_or_create_partition(
    const partition_column_identifier& pid ) {
    uint32_t table_id = pid.table_id;
    return std::move(
        execute_table_operation( get_or_create_partition, table_id, pid ) );
}
std::shared_ptr<partition> tables::get_or_create_partition_and_pin(
    const partition_column_identifier& pid ) {
    uint32_t table_id = pid.table_id;
    return std::move( execute_table_operation( get_or_create_partition_and_pin,
                                               table_id, pid ) );
}

void tables::stop_subscribing(
    const std::vector<update_propagation_information>& subscriptions,
    const snapshot_vector& snapshot, const std::string& cause ) {
    return execute_table_operation( stop_subscribing, 0, subscriptions,
                                    snapshot, cause );
}
void tables::start_subscribing(
    const std::vector<update_propagation_information>& subscriptions,
    const snapshot_vector& snapshot, const std::string& cause ) {
    return execute_table_operation( start_subscribing, 0, subscriptions,
                                    snapshot, cause );
}
void tables::start_subscribing_and_create_partition_if_necessary(
    const std::vector<update_propagation_information>& subscriptions,
    const snapshot_vector& snapshot, const std::string& cause ) {
    for( const auto info : subscriptions ) {
        get_or_create_partition( info.identifier_ );
    }
    start_subscribing( subscriptions, snapshot, cause );
}

void tables::mark_subscription_end(
    const std::vector<update_propagation_information>& subscriptions,
    const snapshot_vector&                             snapshot ) {
    return execute_table_operation( mark_subscription_end, 0, subscriptions,
                                    snapshot );
}

void tables::switch_subscription(
    const std::vector<update_propagation_information>& subscription_switch,
    const std::string&                                 cause ) {
    return execute_table_operation( switch_subscription, 0, subscription_switch,
                                    cause );
}

table* tables::get_table( const std::string& name ) const {
    table* found = nullptr;

    for( table* t : tables_ ) {
        const std::string& t_name = t->get_metadata().table_name_;
        if( t_name.compare( name ) == 0 ) {
            found = t;
            break;
        }
    }
    return found;
}

void tables::gc_inactive_partitions() {
    for( table* t : tables_ ) {
        t->gc_inactive_partitions();
    }
}

uint32_t        tables::get_num_tables() const { return tables_.size(); }
tables_metadata tables::get_tables_metadata() const { return metadata_; }

void tables::persist_db_table( uint32_t                   table_id,
                               data_site_table_persister* persister ) {
    return execute_table_operation( persist_table, table_id, persister );
}

void tables::get_update_partition_states(
    std::vector<polled_partition_column_version_information>&
        polled_versions ) {
    for( table* t : tables_ ) {
        t->get_update_partition_states( polled_versions );
    }
}

std::vector<std::vector<cell_widths_stats>> tables::get_column_widths() const {
    std::vector<std::vector<cell_widths_stats>> cws;
    for( const table* t: tables_) {
        cws.emplace_back( t->get_stats()->get_average_cell_widths() );
    }
    return cws;
}
std::vector<std::vector<selectivity_stats>> tables::get_selectivities() const {
    std::vector<std::vector<selectivity_stats>> ss;
    for( const table* t : tables_ ) {
        ss.emplace_back( t->get_stats()->get_selectivity_stats() );
    }
    return ss;
}

partition_column_identifier_map_t<storage_tier_type::type>
    tables::get_storage_tier_changes() const {
    partition_column_identifier_map_t<storage_tier_type::type> ret;
    for( const table* t : tables_ ) {
        t->get_stats()->get_storage_tier_changes( ret );
    }
    return ret;
}
