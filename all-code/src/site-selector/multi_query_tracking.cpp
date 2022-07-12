#include "multi_query_tracking.h"

#include <glog/logging.h>
#include <folly/Hash.h>

multi_query_plan::multi_query_plan( int destination )
    : is_multi_site_transaction_( false ),
      destination_( destination ),
      pids_to_site_() {}
multi_query_plan::multi_query_plan(
    const partition_column_identifier_map_t<int>& pids_to_site )
    : is_multi_site_transaction_( true ),
      destination_( -1 ),
      pids_to_site_( pids_to_site ) {}

multi_query_plan_entry::multi_query_plan_entry()
    : plan_( nullptr ),
      plan_type_( multi_query_plan_type::WAITING ),
      write_set_(),
      read_set_(),
      id_( 0 ),
      hasher_(),
      mutex_(),
      cv_() {}

std::shared_ptr<multi_query_plan> multi_query_plan_entry::get_plan() const {
    return plan_;
}
multi_query_plan_type                 multi_query_plan_entry::get_plan_type() const {
    return plan_type_;
}
bool multi_query_plan_entry::is_pid_in_plan(
    const partition_column_identifier& pid, bool is_write ) const {
    bool ret = false;
    {  // guard
        std::unique_lock<std::mutex> lock( mutex_ );
        if( is_write ) {
            ret = ( write_set_.count( pid ) == 1 );
        } else {
            ret = ( read_set_.count( pid ) == 1 );
        }
    }
    return ret;
}

uint64_t multi_query_plan_entry::get_id() const { return id_; }

uint32_t count_overlaps(
    const partition_column_identifier_unordered_set& needle,
    const partition_column_identifier_unordered_set& haystack ) {
    uint32_t count = 0;
    for( const auto& pid : needle ) {
        count += haystack.count( pid );
    }
    return count;
}
uint32_t count_overlaps(
    const partition_column_identifier_set&           needle,
    const partition_column_identifier_unordered_set& haystack ) {
    uint32_t count = 0;
    for( const auto& pid : needle ) {
        count += haystack.count( pid );
    }
    return count;
}
uint32_t count_overlaps( const partition_column_identifier_set& needle,
                         const partition_column_identifier_set& haystack ) {
    uint32_t count = 0;
    for( const auto& pid : needle ) {
        count += haystack.count( pid );
    }
    return count;
}
uint32_t count_overlaps(
    const partition_column_identifier_unordered_set& needle,
    const partition_column_identifier_set&           haystack ) {
    uint32_t count = 0;
    for( const auto& pid : needle ) {
        count += haystack.count( pid );
    }
    return count;
}

double multi_query_plan_entry::get_plan_overlap(
    const partition_column_identifier_set& write_pids,
    const partition_column_identifier_set& read_pids ) const {

    uint32_t write_overlaps = 0;
    if( write_set_.size() > write_pids.size() ) {
        write_overlaps = count_overlaps( write_pids, write_set_ );
    } else {
        write_overlaps = count_overlaps( write_set_, write_pids );
    }
    DCHECK_LE( write_overlaps, write_pids.size() );
    uint32_t read_overlaps = 0;
    if( read_set_.size() > read_pids.size() ) {
        read_overlaps = count_overlaps( read_pids, read_set_ );
    } else {
        read_overlaps = count_overlaps( read_set_, read_pids );
    }
    DCHECK_LE( read_overlaps, read_pids.size() );

    double overlapped = (double) ( write_overlaps + read_overlaps );
    double total_size = (double) ( read_pids.size() + write_pids.size() );

    return ( overlapped / total_size );
}

void multi_query_plan_entry::set_plan(
    std::shared_ptr<multi_query_plan>& plan,
    const multi_query_plan_type&       plan_type ) {
    {
        std::unique_lock<std::mutex> lock( mutex_ );
        // guard notify
        plan_ = plan;
        plan_type_ = plan_type;
        lock.unlock();
        cv_.notify_all();
    }
}

void multi_query_plan_entry::set_plan_destination(
    int destination,
    const multi_query_plan_type&       plan_type ) {
    {
        std::unique_lock<std::mutex> lock( mutex_ );
        if ( plan_ == nullptr) {
            plan_ = std::make_shared<multi_query_plan>( destination );
        }
        plan_->destination_ = destination;
        plan_type_ = plan_type;
        lock.unlock();
        cv_.notify_all();
    }
}

void multi_query_plan_entry::set_scan_plan_destination(
    const partition_column_identifier_map_t<int>& pids_to_site,
    const multi_query_plan_type&                  plan_type ) {
    {
        std::unique_lock<std::mutex> lock( mutex_ );
        plan_ = std::make_shared<multi_query_plan>( pids_to_site );
        plan_type_ = plan_type;
        lock.unlock();
        cv_.notify_all();
    }
}

std::tuple<bool, partition_column_identifier_map_t<int>>
    multi_query_plan_entry::get_scan_plan_destination() const {
    auto plan_type = get_plan_type();
    bool ret = false;
    partition_column_identifier_map_t<int> destinations;
    if( ( plan_type == multi_query_plan_type::EXECUTING ) or
        ( plan_type == multi_query_plan_type::CACHED ) ) {
        auto plan = get_plan();
        if( plan ) {
            if( plan->is_multi_site_transaction_ ) {
                destinations = plan->pids_to_site_;
                ret = ( destinations.size() >= 0 );
            }
        }
    }
    return std::make_tuple<>( ret, destinations );
}

std::tuple<bool, int> multi_query_plan_entry::get_plan_destination() const {
    auto plan_type = get_plan_type();
    bool ret = false;
    int destination = -1;
    if( ( plan_type == multi_query_plan_type::EXECUTING ) or
        ( plan_type == multi_query_plan_type::CACHED ) ) {
        auto plan = get_plan();
        if( plan ) {
            if( !plan->is_multi_site_transaction_ ) {
                destination = plan->destination_;
                ret = ( destination >= 0 );
            }
        }
    }
    return std::make_tuple<>( ret, destination );
}

void multi_query_plan_entry::add_to_pids(
    const partition_column_identifier&         pid,
    partition_column_identifier_unordered_set& pid_set, bool is_write ) {
    pid_set.insert( pid );
    id_ = folly::hash::hash_combine( id_, ( uint32_t( is_write ) + 1 ) );
    id_ = folly::hash::hash_combine( id_, hasher_( pid ) );
}

void multi_query_plan_entry::set_pid_sets(
    const partition_column_identifier_unordered_set& write_set,
    const partition_column_identifier_unordered_set& read_set ) {
    {  // guard
        std::unique_lock<std::mutex> lock( mutex_ );

        for( const auto& pid : write_set ) {
            add_to_pids( pid, write_set_, true );
        }
        for( const auto& pid : read_set ) {
            add_to_pids( pid, read_set_, false );
        }
    }
}

void multi_query_plan_entry::set_pid_sets(
    const partition_column_identifier_set& write_set,
    const partition_column_identifier_set& read_set ) {
    {  // guard
        std::unique_lock<std::mutex> lock( mutex_ );

        for( const auto& pid : write_set ) {
            add_to_pids( pid, write_set_, true );
        }
        for( const auto& pid : read_set ) {
            add_to_pids( pid, read_set_, false );
        }
    }
}


void multi_query_plan_entry::set_plan_type(
    const multi_query_plan_type& plan_type ) {
    {
        std::unique_lock<std::mutex> lock( mutex_ );
        // guard notify
        plan_type_ = plan_type;
        lock.unlock();
        cv_.notify_all();
    }
}

bool multi_query_plan_entry::wait_for_plan_to_be_generated() {
    if( ( plan_type_ == multi_query_plan_type::EXECUTING ) or
        ( plan_type_ == multi_query_plan_type::CACHED ) ) {
        return true;
    }

    {
        std::unique_lock<std::mutex> lock( mutex_ );
        // guard
        while( !( ( plan_type_ == multi_query_plan_type::EXECUTING ) or
                  ( plan_type_ == multi_query_plan_type::CACHED ) ) ) {
            cv_.wait( lock );
        }
    }
    DCHECK( ( plan_type_ == multi_query_plan_type::EXECUTING ) or
            ( plan_type_ == multi_query_plan_type::CACHED ) );
    return true;
}

transaction_query_entry::transaction_query_entry() : entries_(), overlaps_() {}

bool sort_overlap(
    const std::tuple<double, uint64_t, std::shared_ptr<multi_query_plan_entry>>&
        left,
    const std::tuple<double, uint64_t, std::shared_ptr<multi_query_plan_entry>>&
        right ) {
    return ( std::get<0>( left ) > std::get<0>( right ) );
}

bool transaction_query_entry::contains_plan( uint64_t id ) const {
    return ( entries_.count( id ) == 1 );
}

std::vector<
    std::tuple<double, uint64_t, std::shared_ptr<multi_query_plan_entry>>>
    transaction_query_entry::get_sorted_plans() const {
    std::vector<
        std::tuple<double, uint64_t, std::shared_ptr<multi_query_plan_entry>>>
        ret;

    DCHECK_EQ( entries_.size(), overlaps_.size() );

    for ( const auto& entry : entries_) {
        uint64_t id = entry.first;
        double   overlap = overlaps_.at( id );
        ret.emplace_back( std::make_tuple<>( overlap, id, entry.second ) );
    }

    std::sort( ret.begin(), ret.end(), sort_overlap );

    return ret;
}

multi_query_partition_entry::multi_query_partition_entry()
    : pid_(), entries_() {}

multi_query_partition_entry::multi_query_partition_entry(
    const partition_column_identifier& pid )
    : pid_( pid ), entries_() {}

void multi_query_partition_entry::set_pid(
    const partition_column_identifier& pid ) {
    pid_ = pid;
}

void multi_query_partition_entry::update_transaction_query_entry(
    transaction_query_entry&               entry,
    const partition_column_identifier_set& write_pids,
    const partition_column_identifier_set& read_pids ) const {

    if( ( write_pids.count( pid_ ) == 0 ) and
        ( read_pids.count( pid_ ) == 0 ) ) {
        return;
    }

    DCHECK( ( write_pids.count( pid_ ) == 1 ) or
            ( read_pids.count( pid_ ) == 1 ) );

    for( auto it = entries_.cbegin(); it != entries_.cend(); ++it ) {
        uint64_t                                id = it->first;
        std::shared_ptr<multi_query_plan_entry> mqp_entry = it->second;

        if( !entry.contains_plan( id ) ) {
            double overlap =
                mqp_entry->get_plan_overlap( write_pids, read_pids );
            entry.entries_[id] = mqp_entry;
            entry.overlaps_[id] = overlap;
        }
    }
}

void multi_query_partition_entry::add_entry(
    std::shared_ptr<multi_query_plan_entry>& entry ) {
    entries_.insert( entry->get_id(), entry );
}

