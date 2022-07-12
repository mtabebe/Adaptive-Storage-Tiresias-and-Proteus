#pragma once

#include <glog/logging.h>
#ifndef GLOG_STL_LOGGING_FOR_UNORDERED
#define GLOG_STL_LOGGING_FOR_UNORDERED
#endif
#include <glog/stl_logging.h>

#include "../../common/perf_tracking.h"
#include "predicate.h"

#define column_stats_types template <typename T>
#define column_stats_T column_stats<T>

#define packed_column_data_types template <typename T>
#define packed_column_data_T packed_column_data<T>

#define packed_column_records_types template <typename T>
#define packed_column_records_multi_types template <typename L, typename R>

column_stats_types column_stats_T::column_stats()
    : min_(), max_(), average_(), sum_(), count_( 0 ) {
    sum_ = get_zero<T>();
    average_ = get_zero<T>();
}

template <typename C>
std::ostream& operator<<( std::ostream& os, const column_stats<C>& stats ) {
    os << "[ min_:" << stats.min_ << ", max_:" << stats.max_
       << ", average_:" << stats.average_ << ", sum_:" << stats.sum_
       << ", count_:" << stats.count_ << " ]";

    return os;
}

packed_column_data_types packed_column_data_T::packed_column_data()
    : stats_(), data_() {}

packed_column_data_types void packed_column_data_T::deep_copy(
    const packed_column_data<T>& other ) {
    stats_ = other.stats_;
    data_ = other.data_;
}

template <typename C>
std::ostream& operator<<( std::ostream&                os,
                          const packed_column_data<C>& data ) {
    os << "[ stats_:" << data.stats_ << ", data_: {" << data.data_ << "} ]";

    return os;
}

packed_column_data_types void packed_column_data_T::insert_data_at_position(
    int32_t pos, const T& val, uint32_t count, bool do_maintenance,
    bool is_sorted ) {

    DVLOG( 40 ) << "Insert data at position:" << pos << ", val:" << val
                << ", count:" << count << ", do_maintenance:" << do_maintenance
                << ", is_sorted:" << is_sorted;

    insert_data_at_position_no_metadata_update( pos, val );

    add_data_to_metadata( pos, count );
    if( do_maintenance ) {
        recompute_stats( val, is_sorted );
    }
}

packed_column_data_types void
    packed_column_data_T::insert_data_at_position_no_metadata_update(
        int32_t pos, const T& val ) {
    DVLOG( 40 ) << "Insert data at position:" << pos << ", val:" << val;
    DCHECK_LE( pos, data_.size() );
    if( (uint32_t) pos == data_.size() ) {
        data_.emplace_back( val );
    } else {
        data_.insert( data_.begin() + pos, val );
    }

    DCHECK_LT( pos, data_.size() );
}

packed_column_data_types void packed_column_data_T::remove_data_from_position(
    int pos, bool do_maintenance, bool is_sorted ) {
    DCHECK_LT( pos, data_.size() );

    T val = data_.at( pos );
    remove_data_from_metadata( pos );
    remove_data_from_position_no_metadata_update( pos );

    if( do_maintenance ) {
        recompute_stats( val, is_sorted );
    }
}

packed_column_records_types void
    packed_column_data_T::remove_data_from_position_no_metadata_update(
        int pos ) {
    DCHECK_LT( pos, data_.size() );
    DVLOG( 40 ) << "Remove data from position:" << pos
                << ", val:" << data_.at( pos );

    data_.erase( data_.begin() + pos );
}

packed_column_data_types void packed_column_data_T::overwrite_data(
    int pos, const T& val, bool do_maintenance, bool is_sorted ) {
    DCHECK_LT( pos, data_.size() );

    DVLOG( 40 ) << "Overwrite data from position:" << pos
                << ", old val:" << data_.at( pos ) << ", new val:" << val;

    T old_val = data_.at( pos );
    data_.at( pos ) = val;

    switch_data_in_metadata( old_val, val, do_maintenance, is_sorted );
}

packed_column_data_types void packed_column_data_T::recompute_stats(
    const T& val, bool is_sorted ) {
    DVLOG( 40 ) << "Recompute stats";

    if( stats_.count_ == 0 ) {
        stats_.sum_ = get_zero<T>();
        stats_.count_ = 0;
        return;
    }
    if( stats_.count_ == 1 ) {
        stats_.min_ = data_.at( 0 );
        stats_.max_ = data_.at( 0 );
    }
    bool update_min = ( val <= stats_.min_ );
    bool update_max = ( val >= stats_.max_ );

    if( update_min or update_max ) {
        update_statistics( is_sorted );
    }

    DVLOG( 40 ) << "Recomputed stats:" << *this;
}

packed_column_data_types void packed_column_data_T::update_statistics(
    bool is_sorted ) {
    DVLOG( 40 ) << "Updating stats";

    if( data_.size() == 0) {
        stats_.sum_ = get_zero<T>();
        stats_.count_ = 0;
        return;
    }

    if( is_sorted ) {
        stats_.min_ = data_.at( 0 );
        stats_.max_ = data_.at( data_.size() - 1 );
        return;
    }
    T cur_min = data_.at(0);
    T cur_max = data_.at( 0 );
    for( const T& v : data_ ) {
        if( v < cur_min ) {
            cur_min = v;
        }
        if( v > cur_max ) {
            cur_max = v;
        }
    }
    stats_.min_ = cur_min;
    stats_.max_ = cur_max;
}

template <typename T>
T get_zero() {
    return (T) 0;
}

template <typename T>
std::tuple<T, T> update_average_and_total( const T& old_total,
                                           uint32_t old_count,
                                           uint32_t new_count, const T& val,
                                           int multiplier ) {
    DVLOG( 40 ) << "update average and total:, old_total:" << old_total
                << ", old_count:" << old_count << ", new_count:" << new_count
                << ", val:" << val << ", multiplier:" << multiplier;
    T new_total = old_total;
    new_total = new_total + ( T )( multiplier * val );
    T new_avg = (T)0;
    if (new_count > 0){
        new_avg = new_total / (T) new_count;
    }

    DVLOG( 40 ) << "update average and total: new average:" << new_avg
                << ", new_total:" << new_total;
    return std::make_tuple<>( new_avg, new_total );
}

packed_column_data_types void packed_column_data_T::remove_data_from_metadata(
    int pos ) {
    DCHECK_LT( pos, data_.size() );
    DVLOG( 40 ) << "Remove data from metadata:" << pos
                << ", val:" << data_.at( pos );

    uint32_t old_count = stats_.count_;
    stats_.count_ = old_count - 1;

    if( !std::is_same<T, std::string>::value ) {
        auto avg_total = update_average_and_total(
            stats_.sum_, old_count, stats_.count_, data_.at( pos ), -1 );
        stats_.average_ = std::get<0>( avg_total );
        stats_.sum_ = std::get<1>( avg_total );
    }
}

packed_column_data_types void packed_column_data_T::add_data_to_metadata(
    int pos, uint32_t count ) {
    DVLOG( 40 ) << "Add data to metadata:" << pos << ", val:" << data_.at( pos )
                << ", count:" << count;

    DCHECK_LT( pos, data_.size() );

    uint32_t old_count = stats_.count_;
    stats_.count_ = old_count + count;

    if( !std::is_same<T, std::string>::value ) {
        auto avg_total = update_average_and_total(
            stats_.sum_, old_count, stats_.count_, data_.at( pos ), count );

        stats_.average_ = std::get<0>( avg_total );
        stats_.sum_ = std::get<1>( avg_total );
    }
}

packed_column_data_types void packed_column_data_T::switch_data_in_metadata(
    const T& old_val, const T& new_val, bool do_maintenance, bool is_sorted ) {

    DVLOG( 40 ) << "Switch data in metadata old_val:" << old_val
                << ", new_val:" << new_val;

    if( !std::is_same<T, std::string>::value ) {
        auto rm_avg_total = update_average_and_total(
            stats_.sum_, stats_.count_, stats_.count_, old_val, -1 );

        auto add_avg_total = update_average_and_total(
            std::get<1>( rm_avg_total ), stats_.count_, stats_.count_, new_val,
            1 );

        stats_.average_ = std::get<0>( add_avg_total );
        stats_.sum_ = std::get<1>( add_avg_total );
    }

    if( do_maintenance ) {
        update_statistics( is_sorted );
    }
}

packed_column_data_types std::tuple<bool, int32_t>
    packed_column_data_T::sorted_find_data_position( const T& val ) const {

    if( data_.empty() ) {
        return std::make_tuple<>( false, 0 );
    }
    // binary search
    int32_t left = 0;
    int32_t right = data_.size() - 1;
    int32_t mid = 0;

    while( left <= right ) {
        mid = ( left + right ) / 2;
        if( data_.at( mid ) == val ) {
            DVLOG( 40 ) << "Find data position val:" << val
                        << ", is_sorted:" << true << ", found returning [ "
                        << true << ", " << mid << " ]";
            return std::make_tuple<>( true, mid );
        } else if( data_.at( mid ) < val ) {
            left = mid + 1;
        } else {
            // mid > val
            right = mid - 1;
        }
    }
    if ( data_.at(mid) < val) {
        mid += 1;
    }
    DVLOG( 40 ) << "Find data position val:" << val << ", is_sorted:" << true
                << ", not found returning [ " << false << ", " << mid << " ]";
    return std::make_tuple<>( false, mid );
}

packed_column_data_types std::tuple<bool, int32_t>
    packed_column_data_T::find_data_position( const T& val,
                                              bool     is_sorted ) const {

    DVLOG( 40 ) << "Find data position val:" << val
                << ", is_sorted:" << is_sorted;
    if( data_.empty() ) {
        DVLOG( 40 ) << "Find data position val:" << val
                    << ", is_sorted:" << is_sorted << ", empty returning [ "
                    << false << ", " << 0 << " ]";

        return std::make_tuple<>( false, 0 );
    }

    if( is_sorted ) {
        return sorted_find_data_position( val );
    }
    for( int32_t pos = data_.size() - 1; pos >= 0; pos-- ) {
        if( val == data_.at( pos ) ) {
            DVLOG( 40 ) << "Find data position val:" << val
                        << ", is_sorted:" << is_sorted << ", found returning [ "
                        << true << ", " << pos << " ]";

            return std::make_tuple<>( true, pos );
        }
    }
    DVLOG( 40 ) << "Find data position val:" << val
                << ", is_sorted:" << is_sorted << ", not found returning [ "
                << false << ", " << data_.size() << " ]";
    return std::make_tuple<>( false, data_.size() );
}

packed_column_records_types bool packed_column_records::internal_remove_data(
    uint64_t key, bool do_statistics_maintenance,
    packed_column_data<T>& col_data ) {
    int32_t index_position = get_index_position( key );
    int32_t data_position = index_positions_.at( index_position );
    DCHECK_GT( data_counts_.size(), 0 );
    DCHECK_LT( data_position, data_counts_.size() );

    if( data_counts_.at( data_position ) == 1 ) {
        col_data.remove_data_from_position(
            data_position, do_statistics_maintenance, is_column_sorted_ );

        check_key_only_key_in_keys( data_position, key );

        remove_from_data_counts_and_update_index_positions( data_position );
    } else {
        col_data.remove_data_from_metadata( data_position );
        data_counts_.at( data_position ) = data_counts_.at( data_position ) - 1;
        remove_key_from_keys( data_position, key );
    }

    index_positions_.at( index_position ) = -1;

    return true;
}

packed_column_records_types bool
    packed_column_records::internal_insert_sorted_data(
        uint64_t key, const T& data, bool do_statistics_maintenance,
        packed_column_data<T>& col_data ) {
    DVLOG( 40 ) << "Internal insert sorted data:" << key;
    int32_t index_position = get_index_position( key );

    auto    found_info = col_data.find_data_position( data, is_column_sorted_ );
    bool found = std::get<0>( found_info );
    int32_t data_pos = std::get<1>( found_info );
    if( !found ) {
        return internal_insert_data_at_position<T>(
            data_pos, key, data, do_statistics_maintenance, col_data );
    } else {
        col_data.add_data_to_metadata( data_pos, 1 );
        data_counts_.at( data_pos ) = data_counts_.at( data_pos ) + 1;
        index_positions_.at( index_position ) = data_pos;
        insert_key_to_keys(data_pos, key);
    }
    return true;
}

packed_column_records_types bool
    packed_column_records::internal_insert_data_at_position(
        int32_t data_position, uint64_t key, const T& data,
        bool do_statistics_maintenance, packed_column_data<T>& col_data ) {
    DVLOG( 40 ) << "Internal insert data:" << key
                << ", at position:" << data_position;

    std::unordered_set<uint64_t> keys = {key};
    DVLOG( 40 ) << "Key size:" << keys.size();
    return internal_insert_data_at_position(
        data_position, keys, data, do_statistics_maintenance, col_data );
}
packed_column_records_types bool
    packed_column_records::internal_insert_data_at_position(
        int32_t data_position, const std::unordered_set<uint64_t>& keys,
        const T& data, bool do_statistics_maintenance,
        packed_column_data<T>& col_data ) {

    DVLOG( 40 ) << "Keys size:" << keys.size();

    col_data.insert_data_at_position( data_position, data, keys.size(),
                                      do_statistics_maintenance,
                                      is_column_sorted_ );
    add_to_data_counts_and_update_index_positions( data_position, keys.size() );
    for( uint64_t key : keys ) {
        uint32_t index_pos = get_index_position( key );
        DCHECK_LT( index_pos, index_positions_.size() );
        index_positions_.at( index_pos ) = data_position;
    }
    insert_keys_to_keys( data_position, keys );
    return true;
}

packed_column_records_types bool packed_column_records::internal_insert_data(
    uint64_t key, const T& data, bool do_statistics_maintenance,
    packed_column_data<T>& col_data ) {
    if( is_column_sorted_ ) {
        return internal_insert_sorted_data<T>(
            key, data, do_statistics_maintenance, col_data );
    }
    return internal_insert_data_at_position(
        data_counts_.size(), key, data, do_statistics_maintenance, col_data );
}

packed_column_records_types bool
    packed_column_records::internal_update_sorted_data(
        uint64_t key, const T& data, bool do_statistics_maintenance,
        packed_column_data<T>& col_data ) {
    DVLOG( 40 ) << "Internal update sorted data:" << key;

    int32_t index_pos = get_index_position( key );
    int32_t old_data_position = index_positions_.at( index_pos );
    DCHECK_GE( old_data_position, 0 );

    DVLOG( 40 ) << "Index pos:" << index_pos
                << ", old data position:" << old_data_position;
    T old_data = col_data.data_.at( old_data_position );

    // new pos
    auto    found_info = col_data.find_data_position( data, is_column_sorted_ );
    bool    new_found = std::get<0>( found_info );
    int32_t new_data_pos = std::get<1>( found_info );

    uint32_t old_count = data_counts_.at( old_data_position );

    DVLOG( 40 ) << "Old count:" << old_count << ", new found:" << new_found
                << ", new data pos:" << new_data_pos;

    if( old_count > 1 ) {
        DVLOG( 40 ) << "old_count > 1, " << old_count;
        // no need to delete old data, or insert
        data_counts_.at( old_data_position ) = old_count - 1;
        remove_key_from_keys( old_data_position, key );

        if( new_found ) {
            data_counts_.at( new_data_pos ) =
                data_counts_.at( new_data_pos ) + 1;
            DCHECK_EQ( data, col_data.data_.at( new_data_pos ) );

        } else {
            // add data to pos
            // insert in new_data_pos
            col_data.insert_data_at_position_no_metadata_update( new_data_pos,
                                                                 data );

            add_to_data_counts_and_update_index_positions( new_data_pos );
        }
        index_positions_.at( index_pos ) = new_data_pos;
        insert_key_to_keys( new_data_pos, key );
    } else if( new_found ) {
        DVLOG( 40 ) << "new found:" << new_found
                    << ", new_data_pos:" << new_data_pos
                    << ", old_data_position:" << old_data_position;
        if( new_data_pos != old_data_position ) {
            data_counts_.at( new_data_pos ) =
                data_counts_.at( new_data_pos ) + 1;
            DCHECK_EQ( data, col_data.data_.at( new_data_pos ) );
            index_positions_.at( index_pos ) = new_data_pos;
            check_key_only_key_in_keys( old_data_position, key );
            insert_key_to_keys( new_data_pos, key );

            // now delete the old one
            DCHECK_EQ( 1, old_count );
            col_data.remove_data_from_position_no_metadata_update(
                old_data_position );
            remove_from_data_counts_and_update_index_positions(
                old_data_position );
        }
    } else {
        DVLOG( 40 ) << "Else, new_found:" << new_found
                    << ", old_count:" << old_count;
        // delete the old one
        col_data.remove_data_from_position_no_metadata_update(
            old_data_position );
        data_counts_.erase( data_counts_.begin() + old_data_position );

        check_key_only_key_in_keys( old_data_position, key );
        keys_.erase( keys_.begin() + old_data_position );

        found_info = col_data.find_data_position( data, is_column_sorted_ );
        new_found = std::get<0>( found_info );
        DCHECK_EQ( false, new_found );
        new_data_pos = std::get<1>( found_info );

        col_data.insert_data_at_position_no_metadata_update( new_data_pos,
                                                             data );
        index_positions_.at( index_pos ) = new_data_pos;
        update_data_counts_and_update_index_positions( old_data_position,
                                                       new_data_pos );
        index_positions_.at( index_pos ) = new_data_pos;
        insert_key_to_keys( new_data_pos, key );
    }

    col_data.switch_data_in_metadata( old_data, data, do_statistics_maintenance,
                                      is_column_sorted_ );

    return true;
}

packed_column_records_types bool packed_column_records::internal_update_data(
    uint64_t key, const T& data, bool do_statistics_maintenance,
    packed_column_data<T>& col_data ) {
    DVLOG( 40 ) << "Internal update data:" << key;
    DVLOG( 40 ) << "This:" << *this;

    if( is_column_sorted_ ) {
        return internal_update_sorted_data(
            key, data, do_statistics_maintenance, col_data );
    }

    int32_t index_pos = get_index_position( key );
    int32_t data_position = index_positions_.at( index_pos );
    DCHECK_GE( data_position, 0 );

    // overwrite
    col_data.overwrite_data( data_position, data, do_statistics_maintenance,
                             false );
    return true;
}

packed_column_records_types bool packed_column_records::templ_update_data(
    uint64_t key, const T& data, bool do_statistics_maintenance,
    packed_column_data<T>& col_data ) {

    DVLOG( 40 ) << "Update data:" << key << ", data:" << data
                << ", do_statistics_maintenance:" << do_statistics_maintenance;
    DVLOG( 40 ) << "Updating this:" << *this;

    int32_t pos = get_index_position( key );
    int32_t cur_index = index_positions_.at( pos );
    bool    ret = false;
    if( cur_index < 0 ) {
        ret = internal_insert_data<T>( key, data, do_statistics_maintenance,
                                       col_data );
    } else {
        ret = internal_update_data<T>( key, data, do_statistics_maintenance,
                                       col_data );
    }

    DVLOG( 40 ) << "Update data:" << key << ", data:" << data
                << ", do_statistics_maintenance:" << do_statistics_maintenance
                << ", returning:" << ret;

    DVLOG( 40 ) << "Update resulted in this:" << *this;

    return ret;
}

packed_column_records_types bool packed_column_records::templ_insert_data(
    uint64_t key, const T& data, bool do_statistics_maintenance,
    packed_column_data<T>& col_data ) {

    DVLOG( 40 ) << "Insert data:" << key << ", data:" << data
                << ", do_statistics_maintenance:" << do_statistics_maintenance;
    DVLOG( 40 ) << "Insert to this:" << *this;

    int32_t pos = get_index_position( key );
    int32_t cur_index = index_positions_.at( pos );

    bool ret = false;
    if( cur_index >= 0 ) {
        ret = internal_update_data<T>( key, data, do_statistics_maintenance,
                                       col_data );
    } else {
        ret = internal_insert_data<T>( key, data, do_statistics_maintenance,
                                       col_data );
    }
    DVLOG( 40 ) << "Insert data:" << key << ", data:" << data
                << ", do_statistics_maintenance:" << do_statistics_maintenance
                << ", returning:" << ret;

    DVLOG( 40 ) << "Insert resulted in this:" << *this;
    return ret;
}

packed_column_records_types bool packed_column_records::templ_remove_data(
    uint64_t key, bool do_statistics_maintenance,
    packed_column_data<T>& col_data ) {

    DVLOG( 40 ) << "Remove data:" << key
                << ", do_statistics_maintenance:" << do_statistics_maintenance;

    int32_t pos = get_index_position( key );
    int32_t cur_index = index_positions_.at( pos );
    if( cur_index < 0 ) {
        DVLOG( 40 ) << "Remove data:" << key << ", do_statistics_maintenance:"
                    << do_statistics_maintenance
                    << ", not found, returning:" << false;

        return false;
    }

    bool ret =  internal_remove_data<T>( key, do_statistics_maintenance, col_data );

    DVLOG( 40 ) << "Remove data:" << key
                << ", do_statistics_maintenance:" << do_statistics_maintenance
                << ", found, returning:" << ret;

    return ret;
}

packed_column_records_types void packed_column_records::templ_update_statistics(
    packed_column_data<T>& col_data ) const {
    col_data.update_statistics( is_column_sorted_ );
}

packed_column_records_types void packed_column_records::templ_deep_copy(
    packed_column_data<T>&       col_data,
    const packed_column_data<T>& other_col_data ) {
    col_data.deep_copy( other_col_data );
}

packed_column_records_types column_stats<T>
                            packed_column_records::templ_get_column_stats(
        const packed_column_data<T>& col_data ) const {
    return col_data.stats_;
}
packed_column_records_types uint32_t
                            packed_column_records::templ_get_value_count(
        const T& data, const packed_column_data<T>& col_data ) const {
    DVLOG( 40 ) << "Get Value count:" << data;
    int32_t count = 0;
    if( col_data.stats_.count_ == 0 ) {
        DVLOG( 40 ) << "Get Value count:" << data
                    << ", empty returning: " << count;
        return count;
    } else if( ( data < col_data.stats_.min_ ) or
               ( data > col_data.stats_.max_ ) ) {
        DVLOG( 40 ) << "Get Value count:" << data << ", outside range ("
                    << col_data.stats_.min_ << ", " << col_data.stats_.max_
                    << "), returning: " << count;
        return count;
    }

    auto found = col_data.find_data_position( data, is_column_sorted_ );
    if( !std::get<0>( found ) ) {
        DVLOG( 40 ) << "Get Value count:" << data
                    << ", not found, returning: " << count;

        return count;
    }

    int32_t found_pos = std::get<1>( found );

    if( is_column_sorted_ ) {
        DCHECK_LT( found_pos, data_counts_.size() );
        count = data_counts_.at( found_pos );

        DVLOG( 40 ) << "Get Value count:" << data
                    << ", sorted, found in pos:" << found_pos
                    << ", returning: " << count;
        return count;
    }

    DCHECK_EQ( data_counts_.size(), col_data.data_.size() );
    for( int32_t pos = found_pos; pos >= 0; pos-- ) {
        if( col_data.data_.at( pos ) == data ) {
            count += data_counts_.at( pos );
        }
    }
    DVLOG( 40 ) << "Get Value count:" << data
                << ", unsorted, found in pos:" << found_pos
                << ", returning: " << count;
    return count;
}
packed_column_records_types std::tuple<bool, T>
                            packed_column_records::templ_get_data(
        uint64_t key, const packed_column_data<T>& col_data ) const {
    DVLOG( 40 ) << "Get data:" << key << ", this:" << *this;

    T def;

    uint32_t index_pos = get_index_position( key );
    int32_t  data_pos = index_positions_.at( index_pos );
    if ( data_pos < 0) {
        DVLOG( 40 ) << "Get data:" << key << ", not found, returning:" << false;
        return std::make_tuple<>( false, def );
    }

    DCHECK_LT( data_pos, col_data.data_.size() );
    DVLOG( 40 ) << "Get data:" << key
                << ", found, returning:" << col_data.data_.at( data_pos );
    return std::make_tuple<>( true, col_data.data_.at( data_pos ) );
}

packed_column_records_types uint32_t packed_column_records::templ_get_count(
    const packed_column_data<T>& col_data ) const {
    return col_data.stats_.count_;
}

packed_column_records_types void
    packed_column_records::templ_merge_col_records_horizontally(
        packed_column_data<T>* col_data, packed_column_records* low,
        packed_column_data<T>* low_data, packed_column_records* high,
        packed_column_data<T>* high_data ) {
    if( !is_column_sorted_ ) {
        start_timer( MERGE_COLUMN_RECORDS_HORIZONTALLY_TIMER_ID );
        // just append low and high
        // remember to update positions and stats
        keys_ = low->keys_;
        keys_.insert( keys_.end(), high->keys_.begin(), high->keys_.end() );

        data_counts_ = low->data_counts_;
        data_counts_.insert( data_counts_.end(), high->data_counts_.begin(),
                             high->data_counts_.end() );
        index_positions_ = low->index_positions_;
        uint32_t index_offset = low_data->data_.size();
        for( int32_t pos : high->index_positions_ ) {
            int32_t recompute_pos = pos;
            if( pos >= 0 ) {
                recompute_pos = recompute_pos + index_offset;
            }
            index_positions_.emplace_back( recompute_pos );
        }

        col_data->data_ = low_data->data_;
        col_data->data_.insert( col_data->data_.end(), high_data->data_.begin(),
                                high_data->data_.end() );
        stop_timer( MERGE_COLUMN_RECORDS_HORIZONTALLY_TIMER_ID );
    } else {
        start_timer( MERGE_SORTED_COLUMN_RECORDS_HORIZONTALLY_TIMER_ID );
        templ_merge_sorted_col_data( col_data, low, low_data, high, high_data );
        stop_timer( MERGE_SORTED_COLUMN_RECORDS_HORIZONTALLY_TIMER_ID );
    }

    // merge the stats
    col_data->stats_ = low_data->stats_;
    if( high_data->stats_.min_ < col_data->stats_.min_ ) {
        col_data->stats_.max_ = high_data->stats_.min_;
    }
    if( high_data->stats_.max_ > col_data->stats_.max_ ) {
        col_data->stats_.max_ = high_data->stats_.max_;
    }

    auto avg_total = update_average_and_total(
        low_data->stats_.sum_, low_data->stats_.count_,
        low_data->stats_.count_ + high_data->stats_.count_,
        high_data->stats_.sum_, 1 );
    col_data->stats_.average_ = std::get<0>( avg_total );
    col_data->stats_.sum_ = std::get<1>( avg_total );

    col_data->stats_.count_ =
        col_data->stats_.count_ + high_data->stats_.count_;
}

packed_column_records_types void
    packed_column_records::templ_merge_sorted_col_data(
        packed_column_data<T>* col_data, packed_column_records* low,
        packed_column_data<T>* low_data, packed_column_records* high,
        packed_column_data<T>* high_data ) {
    if( low->is_column_sorted_ and high->is_column_sorted_ ) {
        templ_merge_insert( col_data, low, low_data, high, high_data );
    } else if( low->is_column_sorted_ ) {
        templ_copy_in_sorted_data( col_data, low, low_data );
        templ_insert_data_one_at_a_time( col_data, high, high_data );
    } else if( high->is_column_sorted_ ) {
        templ_copy_in_sorted_data( col_data, high, high_data );
        templ_insert_data_one_at_a_time( col_data, low, low_data );
    } else {
        // just insert it all
        templ_insert_data_one_at_a_time( col_data, high, high_data );
        templ_insert_data_one_at_a_time( col_data, high, high_data );
    }
}

packed_column_records_types void packed_column_records::templ_merge_insert(
    packed_column_data<T>* col_data, packed_column_records* low,
    packed_column_data<T>* low_data, packed_column_records* high,
    packed_column_data<T>* high_data ) {
    DVLOG( 40 ) << "templ_merge_insert";

    uint32_t low_pos = 0;
    uint32_t high_pos = 0;

    DCHECK_EQ( low->keys_.size(), low_data->data_.size() );
    DCHECK_EQ( high->keys_.size(), high_data->data_.size() );

    while( ( low_pos < low->keys_.size() ) or
           ( high_pos < high->keys_.size() ) ) {
        DVLOG( 40 ) << "templ merge insert low_pos:" << low_pos
                    << ", high_pos:" << high_pos;
        if ( low_pos >= low->keys_.size() ) {
            templ_append_from_other_position( col_data, high_pos, high,
                                              high_data );
            high_pos += 1;
        } else if ( high_pos >= high->keys_.size()) {
            templ_append_from_other_position( col_data, low_pos, low,
                                              low_data );
            low_pos += 1;
        } else if( low_data->data_.at( low_pos ) <
                   high_data->data_.at( high_pos ) ) {
            templ_append_from_other_position( col_data, low_pos, low,
                                              low_data );
            low_pos += 1;
        } else {  // low >= high
            DCHECK_GE( low_data->data_.at( low_pos ),
                       high_data->data_.at( high_pos ) );

            templ_append_from_other_position( col_data, high_pos, high,
                                              high_data );
            if( low_data->data_.at( low_pos ) ==
                high_data->data_.at( high_pos ) ) {
                uint32_t pos = data_counts_.size() - 1;

                data_counts_.at( pos ) =
                    data_counts_.at( pos ) + low->data_counts_.at( low_pos );
                for( uint64_t key : low->keys_.at( low_pos ) ) {
                    index_positions_.at( get_index_position( key ) ) =
                        (int32_t) pos;
                    keys_.at( pos ).insert( key );
                }

                low_pos += 1;
            }
            high_pos += 1;
        }
    }
    DVLOG( 40 ) << "templ_merge_insert, done:" << *this;
}

packed_column_records_types void
    packed_column_records::templ_append_from_other_position(
        packed_column_data<T>* col_data, uint32_t pos,
        packed_column_records* consider_records,
        packed_column_data<T>* consider_data ) {
    DCHECK_LT( pos, consider_records->keys_.size() );
    DCHECK_LT( pos, consider_data->data_.size() );

    keys_.emplace_back( consider_records->keys_.at( pos ) );
    data_counts_.emplace_back( consider_records->data_counts_.at( pos ) );
    col_data->data_.emplace_back( consider_data->data_.at( pos ) );
    uint32_t index_val = col_data->data_.size() - 1;
    for( uint64_t key : keys_.at( index_val ) ) {
        index_positions_.at( get_index_position( key ) ) = index_val;
    }
}

packed_column_records_types void
    packed_column_records::templ_copy_in_sorted_data(
        packed_column_data<T>* col_data, packed_column_records* other,
        packed_column_data<T>* other_data ) {

    DCHECK( keys_.empty() );
    DCHECK( data_counts_.empty() );

    keys_ = other->keys_;
    data_counts_ = other->data_counts_;
    col_data->data_ = other_data->data_;

    for( uint64_t key = other->key_start_; key <= other->key_end_; key++ ) {
        int32_t other_index_pos = other->get_index_position( key );
        int32_t index_val = other->index_positions_.at( other_index_pos );
        int32_t my_index_pos = get_index_position( key );
        index_positions_.at( my_index_pos ) = index_val;
    }
}

packed_column_records_types void
    packed_column_records::templ_insert_data_one_at_a_time(
        packed_column_data<T>* col_data, packed_column_records* other,
        packed_column_data<T>* other_data ) {
    for( uint64_t key = other->key_start_; key <= other->key_end_; key++ ) {
        int32_t index_pos = other->get_index_position( key );
        int32_t index_val = other->index_positions_.at( index_pos );
        if( index_val >= 0 ) {
            DCHECK_LT( index_val, other_data->data_.size() );
            templ_insert_data( key, other_data->data_.at( index_val ),
                               false /* no stats maintenance */, *col_data );
        }
    }
}

packed_column_records_types void
    packed_column_records::templ_split_col_records_horizontally(
        packed_column_data<T>* col_data, packed_column_records* low,
        packed_column_data<T>* low_data, packed_column_records* high,
        packed_column_data<T>* high_data, uint64_t split_point ) {

    DCHECK_EQ( keys_.size(), data_counts_.size() );
    DCHECK_EQ( keys_.size(), col_data->data_.size() );

    std::unordered_set<uint64_t> low_keys;
    std::unordered_set<uint64_t> high_keys;

    DCHECK_EQ( split_point, high->key_start_ );

    for( uint32_t pos = 0; pos < keys_.size(); pos++ ) {
        for( uint64_t key : keys_.at( pos ) ) {
            if( key < high->key_start_ ) {
                DCHECK_GE( key, low->key_start_ );
                DCHECK_LE( key, low->key_end_ );

                low_keys.insert( key );
            } else {
                DCHECK_GE( key, high->key_start_ );
                DCHECK_LE( key, high->key_end_ );

                high_keys.insert( key );
            }
        }

        if( low_keys.size() > 0 ) {
            templ_append_keys_from_other_position_and_update_stats(
                col_data, low, low_data, low_keys, pos );
        }
        if( high_keys.size() > 0 ) {
            templ_append_keys_from_other_position_and_update_stats(
                col_data, high, high_data, high_keys, pos );
        }

        low_keys.clear();
        high_keys.clear();
    }
}
template <typename T>
void packed_column_records::templ_append_data(
    packed_column_data<T>* other_data, const T& data,
    const std::unordered_set<uint64_t>& keys ) {

    DCHECK_EQ( keys_.size(), other_data->data_.size() );
    DCHECK_EQ( keys_.size(), data_counts_.size() );
    int32_t index_pos = keys_.size();

    // update indices
    uint32_t count = keys.size();
    data_counts_.emplace_back( count );
    keys_.emplace_back( keys );
    other_data->data_.emplace_back( data );
    for( uint64_t key : keys ) {
        index_positions_.at( get_index_position( key ) ) = index_pos;
    }

    templ_update_stats_after_append( other_data, data, count );
}

template <typename T>
void templ_update_stats_after_append( packed_column_data<T>* other_data,
                                      const T& data, int count ) {
    if( other_data->stats_.count_ == 0 ) {
        other_data->stats_.min_ = data;
        other_data->stats_.max_ = data;
    } else {
        if( data < other_data->stats_.min_ ) {
            other_data->stats_.min_ = data;
        } else if( data > other_data->stats_.max_ ) {
            other_data->stats_.max_ = data;
        }
    }

    // update avg, sum, count
    auto updated_stats = update_average_and_total(
        other_data->stats_.sum_, other_data->stats_.count_,
        other_data->stats_.count_ + count, data, count /* multiplier*/ );
    other_data->stats_.average_ = std::get<0>( updated_stats );
    other_data->stats_.sum_ = std::get<1>( updated_stats );
    other_data->stats_.count_ = other_data->stats_.count_ + count;
}

packed_column_records_types void packed_column_records::
    templ_append_keys_from_other_position_and_update_stats(
        packed_column_data<T>* col_data, packed_column_records* other,
        packed_column_data<T>*              other_data,
        const std::unordered_set<uint64_t>& keys, int32_t pos ) {
    DCHECK_LT( pos, col_data->data_.size() );

    // append
    other->templ_append_data( other_data, col_data->data_.at( pos ), keys );
}

template <typename L>
void packed_column_records::templ_merge_col_records_vertically_left(
    packed_column_data<multi_column_data>*     col_data,
    const std::vector<multi_column_data_type>& col_types,
    packed_column_records* left, packed_column_data<L>* left_col_data,
    const std::vector<cell_data_type>& left_types, packed_column_records* right,
    const std::vector<cell_data_type>& right_types ) {

    switch( right->col_type_ ) {
        case cell_data_type::UINT64:
            templ_merge_col_records_vertically(
                col_data, col_types, left, left_col_data, left_types, right,
                (packed_column_data<uint64_t>*) right->packed_data_,
                right_types );
            break;
        case cell_data_type::INT64:
            templ_merge_col_records_vertically(
                col_data, col_types, left, left_col_data, left_types, right,
                (packed_column_data<int64_t>*) right->packed_data_,
                right_types );
            break;
        case cell_data_type::DOUBLE:
            templ_merge_col_records_vertically(
                col_data, col_types, left, left_col_data, left_types, right,
                (packed_column_data<double>*) right->packed_data_,
                right_types );
            break;
        case cell_data_type::STRING:
            templ_merge_col_records_vertically(
                col_data, col_types, left, left_col_data, left_types, right,
                (packed_column_data<std::string>*) right->packed_data_,
                right_types );
            break;
        case cell_data_type::MULTI_COLUMN:
            templ_merge_col_records_vertically(
                col_data, col_types, left, left_col_data, left_types, right,
                (packed_column_data<multi_column_data>*) right->packed_data_,
                right_types );
            break;
    }
}
packed_column_records_multi_types void
    packed_column_records::templ_merge_col_records_vertically(
        packed_column_data<multi_column_data>*     col_data,
        const std::vector<multi_column_data_type>& col_types,
        packed_column_records* left, packed_column_data<L>* left_col_data,
        const std::vector<cell_data_type>& left_types,
        packed_column_records* right, packed_column_data<R>* right_col_data,
        const std::vector<cell_data_type>& right_types ) {

    if( !is_column_sorted_ ) {
        start_timer( MERGE_COL_RECORDS_VERTICALLY_UNSORTED_TIMER_ID );
        templ_merge_col_records_vertically_unsorted(
            col_data, col_types, left, left_col_data, left_types, right,
            right_col_data, right_types );
        stop_timer( MERGE_COL_RECORDS_VERTICALLY_UNSORTED_TIMER_ID );
    } else {
        if( left->is_column_sorted_ and right->is_column_sorted_ ) {
            start_timer( MERGE_COL_RECORDS_VERTICALLY_SORTED_TIMER_ID );
            templ_merge_col_records_vertically_sorted(
                col_data, col_types, left, left_col_data, left_types, right,
                right_col_data, right_types );
            stop_timer( MERGE_COL_RECORDS_VERTICALLY_SORTED_TIMER_ID );
        } else {
            start_timer(
                MERGE_COL_RECORDS_VERTICALLY_SORTED_LEFT_RIGHT_UNSORTED_TIMER_ID );
            templ_merge_col_records_vertically_sorted_left_right_unsorted(
                col_data, col_types, left, left_col_data, left_types, right,
                right_col_data, right_types );
            stop_timer(
                MERGE_COL_RECORDS_VERTICALLY_SORTED_LEFT_RIGHT_UNSORTED_TIMER_ID );
        }
    }
}

packed_column_records_multi_types multi_column_data
                                  construct_vertical_merge_data(
        const std::vector<multi_column_data_type>& col_types, int32_t left_pos,
        packed_column_data<L>*             left_col_data,
        const std::vector<cell_data_type>& left_types, int32_t right_pos,
        packed_column_data<R>*             right_col_data,
        const std::vector<cell_data_type>& right_types ) {
    multi_column_data mcd( col_types );

    if( left_pos >= 0) {
        DCHECK_LT( left_pos, left_col_data->data_.size() );
        write_in_mcd_data( &mcd, 0, left_col_data->data_.at( left_pos ),
                           left_types );
    }
    if( right_pos >= 0 ) {
        DCHECK_LT( right_pos, right_col_data->data_.size() );
        write_in_mcd_data( &mcd, left_types.size(),
                           right_col_data->data_.at( right_pos ), right_types );
    }

    return mcd;
}

packed_column_records_multi_types void
    packed_column_records::templ_merge_col_records_vertically_unsorted(
        packed_column_data<multi_column_data>*     col_data,
        const std::vector<multi_column_data_type>& col_types,
        packed_column_records* left, packed_column_data<L>* left_col_data,
        const std::vector<cell_data_type>& left_types,
        packed_column_records* right, packed_column_data<R>* right_col_data,
        const std::vector<cell_data_type>& right_types ) {
    int32_t pos = 0;
    for( uint64_t key = key_start_; key <= key_end_; key++ ) {
        int32_t key_pos = get_index_position( key );
        if( ( left->index_positions_.at( key_pos ) != -1 ) or
            ( right->index_positions_.at( key_pos ) != -1 ) ) {

            multi_column_data mcd = construct_vertical_merge_data(
                col_types, left->index_positions_.at( key_pos ), left_col_data,
                left_types, right->index_positions_.at( key_pos ),
                right_col_data, right_types );

            std::unordered_set<uint64_t> key_set = {key};
            keys_.emplace_back( key_set );
            index_positions_.at( key_pos ) = pos;
            data_counts_.emplace_back( 1 );
            col_data->data_.emplace_back( mcd );

            templ_update_stats_after_append( col_data, mcd, 1 );

            pos += 1;
        }
    }
}

packed_column_records_multi_types void
    packed_column_records::templ_merge_col_records_vertically_sorted(
        packed_column_data<multi_column_data>*     col_data,
        const std::vector<multi_column_data_type>& col_types,
        packed_column_records* left, packed_column_data<L>* left_col_data,
        const std::vector<cell_data_type>& left_types,
        packed_column_records* right, packed_column_data<R>* right_col_data,
        const std::vector<cell_data_type>& right_types ) {

    std::unordered_set<uint64_t> inserted_keys;

    for( uint32_t left_pos = 0; left_pos < left->keys_.size(); left_pos++ ) {
        for( uint64_t key : left->keys_.at( left_pos ) ) {
            inserted_keys.insert( key );
        }
    }

    uint32_t insert_pos = 0;
    for( uint32_t pos = 0; pos < right->keys_.size(); pos++ ) {
        std::unordered_set<uint64_t> to_insert;
        for( uint64_t key : right->keys_.at( pos ) ) {
            if( inserted_keys.count( key ) == 0 ) {
                to_insert.insert( key );
            }
        }
        if( to_insert.size() > 0 ) {
            // construct a mcd and do the insert
            sorted_merge_insert( col_data, col_types, to_insert, insert_pos,
                                 left_col_data, left_types, -1, right_col_data,
                                 right_types, pos );
            insert_pos += 1;
        }
    }

    for( uint32_t left_pos = 0; left_pos < left->keys_.size(); left_pos++ ) {
        std::map<int32_t, std::unordered_set<uint64_t>> key_pos_in_right;
        for( uint64_t key : left->keys_.at( left_pos ) ) {
            uint32_t index_pos = right->get_index_position( key );
            int32_t  pos_in_right = right->index_positions_.at( index_pos );
            key_pos_in_right[pos_in_right].insert( key );
        }

        for( const auto& right_entry : key_pos_in_right ) {
            sorted_merge_insert( col_data, col_types, right_entry.second,
                                 insert_pos, left_col_data, left_types,
                                 left_pos, right_col_data, right_types,
                                 right_entry.first );

            insert_pos += 1;
        }
    }
}
packed_column_records_multi_types void
    packed_column_records::sorted_merge_insert(
        packed_column_data<multi_column_data>*     col_data,
        const std::vector<multi_column_data_type>& col_types,
        const std::unordered_set<uint64_t>& keys, uint32_t insert_pos,
        packed_column_data<L>*             left_col_data,
        const std::vector<cell_data_type>& left_types, int32_t left_pos,
        packed_column_data<R>*             right_col_data,
        const std::vector<cell_data_type>& right_types, int32_t right_pos ) {

    multi_column_data mcd = construct_vertical_merge_data(
        col_types, left_pos, left_col_data, left_types, right_pos,
        right_col_data, right_types );

    DCHECK_EQ( insert_pos, col_data->data_.size() );



    col_data->data_.push_back( mcd );
    keys_.push_back( keys );
    data_counts_.push_back( keys.size() );
    for( uint64_t key : keys ) {
        uint32_t index_pos = get_index_position( key );
        index_positions_.at( index_pos ) = insert_pos;
    }

    templ_update_stats_after_append( col_data, mcd, keys.size() );
}

packed_column_records_multi_types void packed_column_records::
    templ_merge_col_records_vertically_sorted_left_right_unsorted(
        packed_column_data<multi_column_data>*     col_data,
        const std::vector<multi_column_data_type>& col_types,
        packed_column_records* left, packed_column_data<L>* left_col_data,
        const std::vector<cell_data_type>& left_types,
        packed_column_records* right, packed_column_data<R>* right_col_data,
        const std::vector<cell_data_type>& right_types ) {

    for( uint64_t key = key_start_; key <= key_end_; key++ ) {
        int32_t key_pos = get_index_position( key );
        if( ( left->index_positions_.at( key_pos ) != -1 ) or
            ( right->index_positions_.at( key_pos ) != -1 ) ) {

            multi_column_data mcd = construct_vertical_merge_data(
                col_types, left->index_positions_.at( key_pos ), left_col_data,
                left_types, right->index_positions_.at( key_pos ),
                right_col_data, right_types );

            insert_data( key, mcd, true /* maintain stats */ );
        }
    }
}

template <typename L>
void packed_column_records::templ_split_col_records_vertically_left(
    uint32_t col_split_point, packed_column_data<multi_column_data>* col_data,
    const std::vector<cell_data_type>& col_types, packed_column_records* left,
    packed_column_data<L>*                     left_col_data,
    const std::vector<multi_column_data_type>& left_types,
    packed_column_records*                     right,
    const std::vector<multi_column_data_type>& right_types ) {

    switch( right->col_type_ ) {
        case cell_data_type::UINT64:
            templ_split_col_records_vertically(
                col_split_point, col_data, col_types, left, left_col_data,
                left_types, right,
                (packed_column_data<uint64_t>*) right->packed_data_,
                right_types );
            break;
        case cell_data_type::INT64:
            templ_split_col_records_vertically(
                col_split_point, col_data, col_types, left, left_col_data,
                left_types, right,
                (packed_column_data<int64_t>*) right->packed_data_,
                right_types );
            break;
        case cell_data_type::DOUBLE:
            templ_split_col_records_vertically(
                col_split_point, col_data, col_types, left, left_col_data,
                left_types, right,
                (packed_column_data<double>*) right->packed_data_,
                right_types );
            break;
        case cell_data_type::STRING:
            templ_split_col_records_vertically(
                col_split_point, col_data, col_types, left, left_col_data,
                left_types, right,
                (packed_column_data<std::string>*) right->packed_data_,
                right_types );
            break;
        case cell_data_type::MULTI_COLUMN:
            templ_split_col_records_vertically(
                col_split_point, col_data, col_types, left, left_col_data,
                left_types, right,
                (packed_column_data<multi_column_data>*) right->packed_data_,
                right_types );
            break;
    }
}

packed_column_records_multi_types void
    packed_column_records::templ_split_col_records_vertically(
        uint32_t                               col_split_point,
        packed_column_data<multi_column_data>* col_data,
        const std::vector<cell_data_type>&     col_types,
        packed_column_records* left, packed_column_data<L>* left_col_data,
        const std::vector<multi_column_data_type>& left_types,
        packed_column_records* right, packed_column_data<R>* right_col_data,
        const std::vector<multi_column_data_type>& right_types ) {

    if( is_column_sorted_ and left->is_column_sorted_ ) {
        return templ_split_col_records_vertically_sorted(
            col_split_point, col_data, col_types, left, left_col_data,
            left_types, right, right_col_data, right_types );
    }

    start_timer( SPLIT_COL_RECORDS_VERTICALLY_TIMER_ID );

    DCHECK_EQ( keys_.size(), data_counts_.size() );
    DCHECK_EQ( keys_.size(), col_data->data_.size() );
    for( uint32_t pos = 0; pos < keys_.size(); pos++ ) {
        std::tuple<bool, L> left_rec = split_col_record_vertically(
            col_data->data_.at( pos ), 0, col_split_point - 1, col_types,
            left_types, left_col_data );
        std::tuple<bool, R> right_rec = split_col_record_vertically(
            col_data->data_.at( pos ), col_split_point, col_types.size() - 1,
            col_types, right_types, right_col_data );

        if( std::get<0>( left_rec ) ) {
            left->templ_split_insert( left_col_data, std::get<1>( left_rec ),
                                      keys_.at( pos ) );
        }
        if( std::get<0>( right_rec ) ) {
            right->templ_split_insert( right_col_data, std::get<1>( right_rec ),
                                       keys_.at( pos ) );
        }
    }

    stop_timer( SPLIT_COL_RECORDS_VERTICALLY_TIMER_ID );
}

packed_column_records_multi_types void
    packed_column_records::templ_split_col_records_vertically_sorted(
        uint32_t                               col_split_point,
        packed_column_data<multi_column_data>* col_data,
        const std::vector<cell_data_type>&     col_types,
        packed_column_records* left, packed_column_data<L>* left_col_data,
        const std::vector<multi_column_data_type>& left_types,
        packed_column_records* right, packed_column_data<R>* right_col_data,
        const std::vector<multi_column_data_type>& right_types ) {

    start_timer( SPLIT_COL_RECORDS_VERTICALLY_SORTED_TIMER_ID );

    DCHECK_EQ( keys_.size(), data_counts_.size() );
    DCHECK_EQ( keys_.size(), col_data->data_.size() );

    int32_t last_left_pos = -1;
    for( uint32_t pos = 0; pos < keys_.size(); pos++ ) {
        std::tuple<bool, L> left_rec = split_col_record_vertically(
            col_data->data_.at( pos ), 0, col_split_point - 1, col_types,
            left_types, left_col_data );
        std::tuple<bool, R> right_rec = split_col_record_vertically(
            col_data->data_.at( pos ), col_split_point, col_types.size() - 1,
            col_types, right_types, right_col_data );

        if( std::get<0>( left_rec ) ) {
            if( ( last_left_pos >= 0 ) and
                ( left_col_data->data_.at( last_left_pos ) ==
                  std::get<1>( left_rec ) ) ) {
                // update stats
                left->keys_.at( last_left_pos )
                    .insert( keys_.at( pos ).begin(), keys_.at( pos ).end() );
                templ_update_stats_after_append( left_col_data,
                                                 std::get<1>( left_rec ),
                                                 keys_.at( pos ).size() );
            } else {
                left->templ_append_data( left_col_data, std::get<1>( left_rec ),
                                         keys_.at( pos ) );
            }
        }
        if( std::get<0>( right_rec ) ) {
            right->templ_split_insert( right_col_data, std::get<1>( right_rec ),
                                       keys_.at( pos ) );
        }
    }

    stop_timer( SPLIT_COL_RECORDS_VERTICALLY_SORTED_TIMER_ID );
}

packed_column_records_types
void packed_column_records::templ_split_insert(
    packed_column_data<T>* col_data, const T& data,
    const std::unordered_set<uint64_t>& keys ) {
    if( !is_column_sorted_ ) {
        // just append
        templ_append_data( col_data, data, keys );
    } else {
        auto found_info =
            col_data->find_data_position( data, is_column_sorted_ );
        bool    found = std::get<0>( found_info );
        int32_t data_pos = std::get<1>( found_info );
        if( !found ) {
            internal_insert_data_at_position<T>(
                data_pos, keys, data, true /* do_statistics_maintenance */,
                *col_data );
        } else {
            col_data->add_data_to_metadata( data_pos, keys.size() );
            data_counts_.at( data_pos ) =
                data_counts_.at( data_pos ) + keys.size();
            for( uint64_t key : keys ) {
                index_positions_.at( get_index_position( key ) ) = data_pos;
            }
            keys_.at( data_pos ).insert( keys.begin(), keys.end() );
        }
    }
}

packed_column_records_types
void packed_column_records::templ_snapshot_state(
    snapshot_partition_column_state& snapshot,
    std::vector<cell_data_type>&     col_types,
    packed_column_data<T>&           col_data ) const {

    DCHECK_EQ( keys_.size(), col_data.data_.size() );

    for( uint32_t pos = 0; pos < keys_.size(); pos++ ) {
        add_to_snapshot( snapshot, col_types, col_data.data_.at( pos ),
                         keys_.at( pos ) );
    }
}

packed_column_records_types void packed_column_records::templ_scan(
    const partition_column_identifier& pid, uint64_t low_key, uint64_t high_key,
    const predicate_chain& predicate, const std::vector<uint32_t>& project_cols,
    std::vector<result_tuple>&   result_tuples,
    const packed_column_data<T>& col_data ) const {

    if( keys_.empty() ) {
        return;
    }
    DCHECK_GT( col_data.stats_.count_, 0 );
    DVLOG( 40 ) << "Evaluate predicate on partition: " << pid
                << "stats:" << col_data.stats_ << ", predicate:" << predicate;
    bool pred_ok_on_stats =
        evaluate_predicate_on_column_stats( col_data.stats_, predicate );
    DVLOG( 40 ) << "Evaluate predicate on partition: " << pid
                << ", ret:" << pred_ok_on_stats;
    if( !pred_ok_on_stats ) {
        return;
    }
    DVLOG( 40 ) << "Search bounds on partition:" << pid << ", data_:" << *this
                << ", predicate:" << predicate;
    auto     bounds = templ_get_predicate_bounds( predicate, col_data );
    bool     bound_ok = std::get<0>( bounds );
    uint32_t lower_bound = std::get<1>( bounds );
    uint32_t upper_bound = std::get<2>( bounds );

    DVLOG( 40 ) << "Search bounds on partition:" << pid
                << ", bound_ok:" << bound_ok << ", bounds:" << lower_bound
                << " to " << upper_bound;
    DCHECK_EQ( keys_.size(), col_data.data_.size() );
    if( ( lower_bound > keys_.size() ) or !bound_ok ) {
        return;
    }
    if( upper_bound >= keys_.size() ) {
        upper_bound = keys_.size() - 1;
    }

    DCHECK_GE( lower_bound, 0 );
    DCHECK_GE( upper_bound, 0 );
    DCHECK_LT( lower_bound, keys_.size() );
    DCHECK_LT( upper_bound, keys_.size() );

    for( uint32_t pos = lower_bound; pos <= upper_bound; pos++ ) {
        DVLOG( 40 ) << "Evaluate predicate on:" << col_data.data_.at( pos )
                    << ", pos:" << pos;
        bool pred_eval = evaluate_predicate_on_col_data(
            col_data.data_.at( pos ), predicate );
        DVLOG( 40 ) << "Evaluate predicate on pos:" << pos
                    << ", ret:" << pred_eval;
        if( pred_eval ) {
            templ_generate_result_tuples( pos, col_data.data_.at( pos ),
                                          low_key, high_key, pid, project_cols,
                                          result_tuples );
        }
    }
}

packed_column_records_types void
    packed_column_records::templ_generate_result_tuples(
        uint32_t pos, const T& col_data, uint64_t low_key, uint64_t high_key,
        const partition_column_identifier& pid,
        const std::vector<uint32_t>&       project_cols,
        std::vector<result_tuple>&         result_tuples ) const {

    std::vector<result_cell> cells =
        templ_generate_result_cells( col_data, pid, project_cols );

    for( uint64_t key : keys_.at( pos ) ) {
        if( ( key >= low_key ) and ( key <= high_key ) ) {
            DVLOG( 40 ) << "Adding key:" << key << ", to result tuple";
            result_tuple res;

            res.table_id = pid.table_id;
            res.row_id = key;

            res.cells = cells;

            result_tuples.emplace_back( res );
        }
    }
}

packed_column_records_types
std::tuple<bool, uint32_t, uint32_t>
    packed_column_records::templ_get_predicate_bounds(
        const predicate_chain&       predicate,
        const packed_column_data<T>& col_data ) const {
    if( !is_column_sorted_ ) {
        return std::make_tuple<>( true, 0, keys_.size() - 1 );
    }
    uint32_t low_pos = 0;
    uint32_t high_pos = keys_.size() - 1;

    uint32_t and_low_pos = 0;
    uint32_t and_high_pos = keys_.size() - 1;
    bool     and_set_pos = false;

    for( const auto& pred : predicate.and_predicates ) {
        if( pred.predicate == predicate_type::type::INEQUALITY ) {
            continue;
        }

        auto pred_pos =
            col_data.get_bounded_position_from_cell_predicate( pred );
        // the info can't be satisfied
        if( !std::get<0>( pred_pos ) ) {
            return pred_pos;
        }
        // predicate can't be satisfied here
        if( !and_set_pos ) {
            // we are the first
            and_low_pos = std::get<1>( pred_pos );
            and_high_pos = std::get<2>( pred_pos );
            and_set_pos = true;
        } else {
            // expand
            and_low_pos = std::max( and_low_pos, std::get<1>( pred_pos ) );
            and_high_pos = std::min( and_high_pos, std::get<2>( pred_pos ) );
        }

        if( and_low_pos > and_high_pos ) {
            return std::make_tuple<>( false, and_low_pos, and_high_pos );
        }
    }

    if( ( and_low_pos > and_high_pos ) and predicate.is_and_join_predicates ) {
        return std::make_tuple<>( false, and_low_pos, and_high_pos );
    }
    if( predicate.or_predicates.empty() ) {
        return std::make_tuple<>( ( and_low_pos <= and_high_pos ), and_low_pos,
                                  and_high_pos );
    }

    uint32_t or_low_pos = 0;
    uint32_t or_high_pos = keys_.size() - 1;
    bool     or_set_pos = false;

    for( const auto& pred : predicate.or_predicates ) {
        if( pred.predicate == predicate_type::type::INEQUALITY ) {
            continue;
        }
        auto pred_pos =
            col_data.get_bounded_position_from_cell_predicate( pred );
        // don't know the info
        if( !std::get<0>( pred_pos ) ) {
            continue;
        }
        // predicate can't be satisfied here
        if( std::get<1>( pred_pos ) > std::get<2>( pred_pos ) ) {
            continue;
        }

        if( !or_set_pos ) {
            // we are the first
            or_low_pos = std::get<1>( pred_pos );
            or_low_pos = std::get<2>( pred_pos );
            or_set_pos = true;
        } else {
            // expand
            or_low_pos = std::min( or_low_pos, std::get<1>( pred_pos ) );
            or_high_pos = std::max( or_high_pos, std::get<2>( pred_pos ) );
        }
    }

    low_pos = or_low_pos;
    high_pos = or_high_pos;
    if( predicate.is_and_join_predicates ) {
        low_pos = std::max( or_low_pos, and_low_pos );
        high_pos = std::min( or_high_pos, and_high_pos );
    } else if( !predicate.and_predicates.empty() ) {
        low_pos = std::min( or_low_pos, and_low_pos );
        high_pos = std::max( or_high_pos, and_high_pos );
    }

    return std::make_tuple<>( ( low_pos <= high_pos ), low_pos, high_pos );
}

packed_column_records_types void
    packed_column_records::templ_install_packed_data_from_sorted(
        packed_column_data<T>* data, const packed_column_records* other_records,
        const packed_column_data<T>* other_data ) {

    DCHECK_EQ( other_records->data_counts_.size(),
               other_records->keys_.size() );
    DCHECK_EQ( other_data->data_.size(), other_records->data_counts_.size() );

    for( uint32_t pos = 0; pos < other_records->keys_.size(); pos++ ) {
        for( uint64_t key : other_records->keys_.at( pos ) ) {
            uint32_t new_pos = data->data_.size();
            data->data_.emplace_back( other_data->data_.at( pos ) );
            data_counts_.emplace_back( 1 );
            std::unordered_set<uint64_t> key_set = {key};
            keys_.emplace_back( key_set );
            index_positions_.at( get_index_position( key ) ) = new_pos;
        }
    }

    DCHECK_EQ( data_counts_.size(), keys_.size() );
    DCHECK_EQ( data->data_.size(), data_counts_.size() );
    data->stats_ = other_data->stats_;
}

packed_column_records_types std::vector<uint32_t>
                            packed_column_records::templ_get_data_positions(
        const packed_column_data<T>& data ) const {
    DCHECK_EQ( data.data_.size(), keys_.size() );
    return data.get_persistence_positions();
}

packed_column_records_types void
    packed_column_records::templ_persist_data_to_disk(
        data_persister* persister, const std::vector<uint32_t>& data_positions,
        uint32_t data_start_pos, const packed_column_data<T>& data ) const {
    data.persist_data_to_disk( persister, data_positions, data_start_pos );
}

packed_column_records_types void
    packed_column_records::templ_restore_data_from_disk(
        data_reader* reader, uint32_t num_data_items,
        packed_column_data<T>& data ) {
    data.restore_data_from_disk( reader, num_data_items );
}

packed_column_records_types void
    packed_column_records::templ_persist_stats_to_disk(
        data_persister* persister, column_stats<multi_column_data>& stats,
        const packed_column_data<T>& data ) const {
    data.persist_stats_to_disk( persister, stats );
}

packed_column_records_types void
    packed_column_records::templ_restore_stats_from_disk(
        data_reader* reader, packed_column_data<T>& data ) {
    data.restore_stats_from_disk( reader );
}

