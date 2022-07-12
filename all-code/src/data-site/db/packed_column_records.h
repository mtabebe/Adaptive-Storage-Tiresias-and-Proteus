#pragma once

#include <cstdint>
#include <string>
#include <tuple>
#include <unordered_set>
#include <vector>

#include "../../common/cell_data_type.h"
#include "../../persistence/data_persister.h"
#include "../../persistence/data_reader.h"
#include "multi_column_data.h"

template <typename T>
class column_stats {
   public:
    column_stats();

    template <class C>
    friend std::ostream& operator<<( std::ostream&          os,
                                     const column_stats<C>& data );

    T min_;
    T max_;
    T average_;
    T sum_;

    uint32_t count_;
};

template <typename T>
class packed_column_data {
   public:
    packed_column_data();

    void deep_copy( const packed_column_data<T>& other );

    std::tuple<bool, int32_t> find_data_position( const T& val,
                                                  bool is_sorted ) const;
    std::tuple<bool, int32_t> sorted_find_data_position( const T& val ) const;

    void insert_data_at_position( int32_t pos, const T& val, uint32_t count,
                                  bool do_maintenance, bool is_sorted );
    void insert_data_at_position_no_metadata_update( int32_t  pos,
                                                     const T& val );

    void remove_data_from_position( int pos, bool do_maintenance,
                                    bool is_sorted );
    void remove_data_from_position_no_metadata_update( int pos );

    void remove_data_from_metadata( int pos );
    void add_data_to_metadata( int pos, uint32_t count );
    void switch_data_in_metadata( const T& old_val, const T& new_val,
                                  bool do_maintenance, bool is_sorted );

    void overwrite_data( int pos, const T& val, bool do_maintenance,
                         bool is_sorted );

    void update_statistics( bool is_sorted );

    // given val recompute
    void recompute_stats( const T& val, bool is_sorted );

    std::tuple<bool, uint32_t, uint32_t>
        get_bounded_position_from_cell_predicate(
            const cell_predicate& c_pred ) const;

    std::vector<uint32_t> get_persistence_positions() const;
    void persist_data_to_disk( data_persister*              persister,
                               const std::vector<uint32_t>& data_positions,
                               uint32_t data_start_pos ) const;
    void restore_data_from_disk( data_reader* reader, uint32_t num_data_items );

    void persist_stats_to_disk( data_persister*                  persister,
                                column_stats<multi_column_data>& stats ) const;
    void restore_stats_from_disk( data_reader* reader );

    template <class C>
    friend std::ostream& operator<<( std::ostream&                os,
                                     const packed_column_data<C>& data );



    column_stats<T> stats_;
    std::vector<T> data_;
};

class packed_column_records {
   public:
    packed_column_records( bool                  is_column_sorted,
                           const cell_data_type& col_type, uint64_t key_start,
                           uint64_t key_end );
    ~packed_column_records();

    void deep_copy( const packed_column_records& other );

    // const because it's on a void*
    void update_statistics() const;

    bool remove_data( uint64_t key, bool do_statistics_maintenance );

    bool insert_data( uint64_t key, uint64_t data,
                      bool do_statistics_maintenance );
    bool insert_data( uint64_t key, int64_t data,
                      bool do_statistics_maintenance );
    bool insert_data( uint64_t key, const std::string& data,
                      bool do_statistics_maintenance );
    bool insert_data( uint64_t key, double data,
                      bool do_statistics_maintenance );
    bool insert_data( uint64_t key, const multi_column_data& data,
                      bool do_statistics_maintenance );

    bool update_data( uint64_t key, uint64_t data,
                      bool do_statistics_maintenance );
    bool update_data( uint64_t key, int64_t data,
                      bool do_statistics_maintenance );
    bool update_data( uint64_t key, const std::string& data,
                      bool do_statistics_maintenance );
    bool update_data( uint64_t key, double data,
                      bool do_statistics_maintenance );
    bool update_data( uint64_t key, const multi_column_data& data,
                      bool do_statistics_maintenance );

    std::tuple<bool, uint64_t> get_uint64_data( uint64_t key ) const;
    std::tuple<bool, int64_t> get_int64_data( uint64_t key ) const;
    std::tuple<bool, double> get_double_data( uint64_t key ) const;
    std::tuple<bool, std::string> get_string_data( uint64_t key ) const;
    std::tuple<bool, multi_column_data> get_multi_column_data(
        uint64_t key ) const;

    void scan( const partition_column_identifier& pid, uint64_t low_key,
               uint64_t high_key, const predicate_chain& predicate,
               const std::vector<uint32_t>& project_cols,
               std::vector<result_tuple>&   result_tuples ) const;

    uint32_t get_count() const;

    uint32_t get_value_count( uint64_t data ) const;
    uint32_t get_value_count( int64_t data ) const;
    uint32_t get_value_count( double data ) const;
    uint32_t get_value_count( const std::string& data ) const;
    uint32_t get_value_count( const multi_column_data& data ) const;

    column_stats<uint64_t>    get_uint64_column_stats() const;
    column_stats<int64_t>     get_int64_column_stats() const;
    column_stats<double>      get_double_column_stats() const;
    column_stats<std::string> get_string_column_stats() const;
    column_stats<multi_column_data> get_multi_column_stats() const;

    void snapshot_state( snapshot_partition_column_state& snapshot,
                         std::vector<cell_data_type>&     col_types ) const;

    void install_from_sorted_data( const packed_column_records* other );

    void split_col_records_horizontally( packed_column_records* low,
                                         packed_column_records* high,
                                         uint64_t row_split_point );
    void split_col_records_vertically(
        uint32_t col_split_point, const std::vector<cell_data_type>& col_types,
        packed_column_records*                     left,
        const std::vector<multi_column_data_type>& left_types,
        packed_column_records*                     right,
        const std::vector<multi_column_data_type>& right_types );

    void merge_col_records_horizontally( packed_column_records* low,
                                         packed_column_records* high );
    void merge_col_records_vertically(
        const std::vector<multi_column_data_type>& col_types,
        packed_column_records*                     left,
        const std::vector<cell_data_type>&         left_types,
        packed_column_records*                     right,
        const std::vector<cell_data_type>&         right_types );

    bool is_sorted() const;

    void persist_data( data_persister* part_persister, uint64_t record_pid,
                       uint64_t version, uint64_t pid_hash,
                       uint32_t num_cols ) const;

    column_stats<multi_column_data> persist_to_disk(
        data_persister* part_persister ) const;
    void restore_from_disk( data_reader* part_reader );

    friend std::ostream& operator<<( std::ostream&                os,
                                     const packed_column_records& packed );

   private:
    template <typename T>
    void templ_update_statistics( packed_column_data<T>& col_data ) const;

    template <typename T>
    bool templ_remove_data( uint64_t key, bool do_statistics_maintenance,
                            packed_column_data<T>& col_data );
    template <typename T>
    bool templ_update_data( uint64_t key, const T& data,
                            bool                   do_statistics_maintenance,
                            packed_column_data<T>& col_data );
    template <typename T>
    bool templ_insert_data( uint64_t key, const T& data,
                            bool                   do_statistics_maintenance,
                            packed_column_data<T>& col_data );

    template <typename T>
    bool internal_remove_data( uint64_t key, bool do_statistics_maintenance,
                               packed_column_data<T>& col_data );
    template <typename T>
    bool internal_update_data( uint64_t key, const T& data,
                               bool                   do_statistics_maintenance,
                               packed_column_data<T>& col_data );
    template <typename T>
    bool internal_insert_data( uint64_t key, const T& data,
                               bool                   do_statistics_maintenance,
                               packed_column_data<T>& col_data );
    template <typename T>
    bool internal_insert_sorted_data( uint64_t key, const T& data,
                                      bool do_statistics_maintenance,
                                      packed_column_data<T>& col_data );
    template <typename T>
    bool internal_insert_data_at_position( int32_t position, uint64_t key,
                                           const T& data,
                                           bool     do_statistics_maintenance,
                                           packed_column_data<T>& col_data );
    template <typename T>
    bool internal_insert_data_at_position(
        int32_t position, const std::unordered_set<uint64_t>& keys,
        const T& data, bool do_statistics_maintenance,
        packed_column_data<T>& col_data );

    template <typename T>
    bool internal_update_sorted_data( uint64_t key, const T& data,
                                      bool do_statistics_maintenance,
                                      packed_column_data<T>& col_data );

    template <typename T>
    void templ_snapshot_state( snapshot_partition_column_state& snapshot,
                               std::vector<cell_data_type>&     col_types,
                               packed_column_data<T>& col_data ) const;

    void add_to_data_counts_and_update_index_positions( int32_t new_data_pos );
    void add_to_data_counts_and_update_index_positions( int32_t  new_data_pos,
                                                        uint32_t add );
    void remove_from_data_counts_and_update_index_positions(
        int32_t new_data_pos );
    void update_data_counts_and_update_index_positions( int32_t old_data_pos,
                                                        int32_t new_data_pos );

    template <typename T>
    uint32_t templ_get_count( const packed_column_data<T>& col_data ) const;

    template <typename T>
    column_stats<T> templ_get_column_stats(
        const packed_column_data<T>& col_data ) const;
    template <typename T>
    uint32_t templ_get_value_count(
        const T& data, const packed_column_data<T>& col_data ) const;
    template <typename T>
    std::tuple<bool, T> templ_get_data(
        uint64_t key, const packed_column_data<T>& col_data ) const;

    template <typename T>
    void templ_deep_copy( packed_column_data<T>&       col_data,
                          const packed_column_data<T>& other_col_data );

    int32_t get_index_position( uint64_t key ) const;

    void remove_key_from_keys( uint32_t old_data_position, uint64_t key );
    void insert_key_to_keys( uint32_t new_data_pos, uint64_t key );
    void insert_keys_to_keys( uint32_t                            new_data_pos,
                              const std::unordered_set<uint64_t>& keys );
    void check_key_only_key_in_keys( uint32_t data_position, uint64_t key );

    template <typename T>
    void templ_merge_col_records_horizontally(
        packed_column_data<T>* col_data, packed_column_records* low,
        packed_column_data<T>* low_data, packed_column_records* high,
        packed_column_data<T>* high_data );
    template <typename T>
    void templ_merge_sorted_col_data( packed_column_data<T>* col_data,
                                      packed_column_records* low,
                                      packed_column_data<T>* low_data,
                                      packed_column_records* high,
                                      packed_column_data<T>* high_data );

    template <typename T>
    void templ_merge_insert( packed_column_data<T>* col_data,
                             packed_column_records* low,
                             packed_column_data<T>* low_data,
                             packed_column_records* high,
                             packed_column_data<T>* high_data );

    template <typename T>
    void templ_copy_in_sorted_data( packed_column_data<T>* col_data,
                                    packed_column_records* other,
                                    packed_column_data<T>* other_data );
    template <typename T>
    void templ_insert_data_one_at_a_time( packed_column_data<T>* col_data,
                                          packed_column_records* other,
                                          packed_column_data<T>* other_data );
    template <typename T>
    void templ_append_from_other_position(
        packed_column_data<T>* col_data, uint32_t pos,
        packed_column_records* consider_records,
        packed_column_data<T>* consider_data );
    template <typename T>
    void templ_append_data( packed_column_data<T>* other_data, const T& data,
                            const std::unordered_set<uint64_t>& keys );

    void internal_split_col_records_horizontally( packed_column_records* low,
                                                  packed_column_records* high,
                                                  uint64_t row_split_point );

    template <typename T>
    void templ_split_col_records_horizontally( packed_column_data<T>* col_data,
                                               packed_column_records* low,
                                               packed_column_data<T>* low_data,
                                               packed_column_records* high,
                                               packed_column_data<T>* high_data,
                                               uint64_t split_point );
    template <typename T>
    void templ_append_keys_from_other_position_and_update_stats(
        packed_column_data<T>* col_data, packed_column_records* other,
        packed_column_data<T>*              other_data,
        const std::unordered_set<uint64_t>& keys, int32_t pos );

    template <typename L>
    void templ_merge_col_records_vertically_left(
        packed_column_data<multi_column_data>*     col_data,
        const std::vector<multi_column_data_type>& col_types,
        packed_column_records* left, packed_column_data<L>* left_col_data,
        const std::vector<cell_data_type>& left_types,
        packed_column_records*             right,
        const std::vector<cell_data_type>& right_types );

    template <typename L, typename R>
    void templ_merge_col_records_vertically(
        packed_column_data<multi_column_data>*     col_data,
        const std::vector<multi_column_data_type>& col_types,
        packed_column_records* left, packed_column_data<L>* left_col_data,
        const std::vector<cell_data_type>& left_types,
        packed_column_records* right, packed_column_data<R>* right_col_data,
        const std::vector<cell_data_type>& right_types );

    template <typename L, typename R>
    void templ_merge_col_records_vertically_unsorted(
        packed_column_data<multi_column_data>*     col_data,
        const std::vector<multi_column_data_type>& col_types,
        packed_column_records* left, packed_column_data<L>* left_col_data,
        const std::vector<cell_data_type>& left_types,
        packed_column_records* right, packed_column_data<R>* right_col_data,
        const std::vector<cell_data_type>& right_types );

    template <typename L, typename R>
    void templ_merge_col_records_vertically_sorted(
        packed_column_data<multi_column_data>*     col_data,
        const std::vector<multi_column_data_type>& col_types,
        packed_column_records* left, packed_column_data<L>* left_col_data,
        const std::vector<cell_data_type>& left_types,
        packed_column_records* right, packed_column_data<R>* right_col_data,
        const std::vector<cell_data_type>& right_types );

    template <typename L, typename R>
    void templ_merge_col_records_vertically_sorted_left_right_unsorted(
        packed_column_data<multi_column_data>*     col_data,
        const std::vector<multi_column_data_type>& col_types,
        packed_column_records* left, packed_column_data<L>* left_col_data,
        const std::vector<cell_data_type>& left_types,
        packed_column_records* right, packed_column_data<R>* right_col_data,
        const std::vector<cell_data_type>& right_types );
    template <typename L, typename R>
    void sorted_merge_insert(
        packed_column_data<multi_column_data>*     col_data,
        const std::vector<multi_column_data_type>& col_types,
        const std::unordered_set<uint64_t>& keys, uint32_t insert_pos,
        packed_column_data<L>*             left_col_data,
        const std::vector<cell_data_type>& left_types, int32_t left_pos,
        packed_column_data<R>*             right_col_data,
        const std::vector<cell_data_type>& right_types, int32_t right_pos );

    template <typename L>
    void templ_split_col_records_vertically_left(
        uint32_t                               col_split_point,
        packed_column_data<multi_column_data>* col_data,
        const std::vector<cell_data_type>&     col_types,
        packed_column_records* left, packed_column_data<L>* left_col_data,
        const std::vector<multi_column_data_type>& left_types,
        packed_column_records*                     right,
        const std::vector<multi_column_data_type>& right_types );

    template <typename T>
    std::tuple<bool, T> split_col_record_vertically(
        const multi_column_data& mcd, uint32_t col_start, uint32_t col_end,
        const std::vector<cell_data_type>&         overall_type,
        const std::vector<multi_column_data_type>& new_type,
        packed_column_data<T>*                     col_data );

    template <typename L, typename R>
    void templ_split_col_records_vertically(
        uint32_t                               col_split_point,
        packed_column_data<multi_column_data>* col_data,
        const std::vector<cell_data_type>&     col_types,
        packed_column_records* left, packed_column_data<L>* left_col_data,
        const std::vector<multi_column_data_type>& left_types,
        packed_column_records* right, packed_column_data<R>* right_col_data,
        const std::vector<multi_column_data_type>& right_types );

    template <typename L, typename R>
    void templ_split_col_records_vertically_sorted(
        uint32_t                               col_split_point,
        packed_column_data<multi_column_data>* col_data,
        const std::vector<cell_data_type>&     col_types,
        packed_column_records* left, packed_column_data<L>* left_col_data,
        const std::vector<multi_column_data_type>& left_types,
        packed_column_records* right, packed_column_data<R>* right_col_data,
        const std::vector<multi_column_data_type>& right_types );

    template <typename T>
    void templ_split_insert( packed_column_data<T>* col_data, const T& data,
                             const std::unordered_set<uint64_t>& keys );

    template <typename T>
    void templ_scan( const partition_column_identifier& pid, uint64_t low_key,
                     uint64_t high_key, const predicate_chain& predicate,
                     const std::vector<uint32_t>& project_cols,
                     std::vector<result_tuple>&   result_tuples,
                     const packed_column_data<T>& col_data ) const;
    template <typename T>
    void templ_generate_result_tuples(
        uint32_t pos, const T& col_data, uint64_t low_key, uint64_t high_key,
        const partition_column_identifier& pid,
        const std::vector<uint32_t>&       project_cols,
        std::vector<result_tuple>&         result_tuples ) const;
    template <typename T>
    std::tuple<bool, uint32_t, uint32_t> templ_get_predicate_bounds(
        const predicate_chain&       predicate,
        const packed_column_data<T>& col_data ) const;

    template <typename T>
    void templ_install_packed_data_from_sorted(
        packed_column_data<T>* data, const packed_column_records* other_records,
        const packed_column_data<T>* other_data );

    void persist_cell_data( data_persister* part_persister, uint32_t pos,
                            uint32_t num_cols ) const;

    std::vector<uint32_t> get_data_positions() const;

    template <typename T>
    std::vector<uint32_t> templ_get_data_positions(
        const packed_column_data<T>& data ) const;

    void persist_data_to_disk( data_persister*              part_persister,
                               const std::vector<uint32_t>& data_positions,
                               uint32_t data_start_pos ) const;
    template <typename T>
    void templ_persist_data_to_disk(
        data_persister* persister, const std::vector<uint32_t>& data_positions,
        uint32_t data_start_pos, const packed_column_data<T>& data ) const;

    void restore_data_from_disk( data_reader* part_reader,
                                 uint32_t     num_data_items );
    template <typename T>
    void templ_restore_data_from_disk( data_reader*           part_reader,
                                       uint32_t               num_data_items,
                                       packed_column_data<T>& data );

    void persist_stats_to_disk( data_persister*                  persister,
                                column_stats<multi_column_data>& stats ) const;
    void restore_stats_from_disk( data_reader* reader );

    template <typename T>
    void templ_persist_stats_to_disk( data_persister* persister,
                                      column_stats<multi_column_data>& stats,
                                      const packed_column_data<T>& data ) const;
    template <typename T>
    void templ_restore_stats_from_disk( data_reader*           persister,
                                        packed_column_data<T>& data );

    bool is_column_sorted_;

    std::vector<std::unordered_set<uint64_t>> keys_;
    std::vector<int32_t> index_positions_;
    uint64_t             key_start_;
    uint64_t             key_end_;

    std::vector<uint32_t> data_counts_;

    cell_data_type col_type_;

    void* packed_data_;
};

template <typename T>
std::tuple<T, T> update_average_and_total( const T& old_total,
                                           uint32_t old_count,
                                           uint32_t new_count, const T& val,
                                           int multiplier );
template <>
std::tuple<std::string, std::string> update_average_and_total<std::string>(
    const std::string& old_total, uint32_t old_count, uint32_t new_count,
    const std::string& val, int multiplier );
template <>
std::tuple<multi_column_data, multi_column_data>
    update_average_and_total<multi_column_data>(
        const multi_column_data& old_total, uint32_t old_count,
        uint32_t new_count, const multi_column_data& val, int multiplier );

template <typename T>
void write_in_mcd_data( multi_column_data* mcd, uint32_t start_col,
                        const T&                           data,
                        const std::vector<cell_data_type>& types );

template <typename T>
std::vector<result_cell> templ_generate_result_cells(
    const T& data, const partition_column_identifier& pid,
    const std::vector<uint32_t>& project_cols );

template <typename T>
T get_zero();

template <>
std::string get_zero();
template <>
multi_column_data get_zero();

template <typename T>
void add_to_snapshot( snapshot_partition_column_state& snapshot,
                      std::vector<cell_data_type>& col_types, const T& data,
                      const std::unordered_set<uint64_t>& keys );

#include "packed_column_records.tcc"
