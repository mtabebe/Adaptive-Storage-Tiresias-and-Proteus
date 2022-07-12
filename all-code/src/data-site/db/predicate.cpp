#include "predicate.h"

#include <glog/logging.h>

#include "../../common/string_conversion.h"

bool evaluate_predicate_on_cell_data( const packed_cell_data& pcd,
                                      const cell_predicate&   pred ) {
    DCHECK( pcd.is_present() );
    switch( pred.type ) {
        case data_type::type::UINT64: {
            return evaluate_predicate_on_uint64_data(
                pcd.get_uint64_data(), string_to_uint64( pred.data ),
                pred.predicate );
            break;
        }
        case data_type::type::INT64: {
            return evaluate_predicate_on_int64_data(
                pcd.get_int64_data(), string_to_int64( pred.data ),
                pred.predicate );
            break;
        }
        case data_type::type::DOUBLE: {
            return evaluate_predicate_on_double_data(
                pcd.get_double_data(), string_to_double( pred.data ),
                pred.predicate );
            break;
        }
        case data_type::type::STRING: {
            return evaluate_predicate_on_string_data(
                pcd.get_string_data(), pred.data, pred.predicate );
            break;
        }
    }
    return false;
}

bool evaluate_predicate_on_row_record( const row_record*      row_rec,
                                       const predicate_chain& predicate ) {
    DCHECK( row_rec );
    DCHECK( row_rec->is_present() );

    const packed_cell_data* pcd = row_rec->get_row_data();
    uint32_t                num_cols = row_rec->get_num_columns();

    bool and_pred_ok = true;

    for( const auto& pred : predicate.and_predicates ) {
        int32_t col_id = pred.col_id;
        if( ( col_id >= (int32_t) num_cols ) or
            ( !pcd[col_id].is_present() ) ) {
            and_pred_ok = false;
            break;
        }
        bool pred_ok = evaluate_predicate_on_cell_data( pcd[col_id], pred );
        if( !pred_ok ) {
            and_pred_ok = false;
            break;
        }
    }

    if( ( !and_pred_ok ) and predicate.is_and_join_predicates ) {
        return false;
    }

    if( predicate.or_predicates.empty() ) {
        return and_pred_ok;
    }

    bool or_pred_ok = false;
    for( const auto& pred : predicate.or_predicates ) {
        int32_t col_id = pred.col_id;
        if( ( col_id >= (int32_t) num_cols ) or
            ( !pcd[col_id].is_present() ) ) {
            continue;
        }
        bool pred_ok = evaluate_predicate_on_cell_data( pcd[col_id], pred );
        if( pred_ok ) {
            or_pred_ok = true;
            break;
        }
    }

    bool ret = or_pred_ok;
    if( predicate.is_and_join_predicates ) {
        ret = and_pred_ok and or_pred_ok;
    } else if( !predicate.and_predicates.empty() ) {
        ret = and_pred_ok or or_pred_ok;
    }

    return ret;
}

#define TEMPL_EVALUATE_CELL_PRED_ON_COLUMN_DATA_STATS(                       \
    _stats, _c_pred, _expect_type, _eval_method, _conv_method )              \
    if( _stats.count_ == 0 ) {                                               \
        return false;                                                        \
    }                                                                        \
    DCHECK_NE( _c_pred.predicate, predicate_type::type::INEQUALITY );        \
    DCHECK_EQ( _c_pred.type, _expect_type );                                 \
    auto _pred_data = _conv_method( _c_pred.data );                          \
    switch( _c_pred.predicate ) {                                            \
        case predicate_type::type::EQUALITY: {                               \
            return _eval_method(                                             \
                       _stats.min_, _pred_data,                              \
                       predicate_type::type::LESS_THAN_OR_EQUAL ) and        \
                   _eval_method(                                             \
                       _stats.max_, _pred_data,                              \
                       predicate_type::type::GREATER_THAN_OR_EQUAL );        \
            break;                                                           \
        }                                                                    \
        case predicate_type::type::INEQUALITY: {                             \
            DLOG( FATAL ) << "Shouldn't fall here";                          \
            break;                                                           \
        }                                                                    \
        case predicate_type::type::LESS_THAN: {                              \
            return _eval_method( _stats.min_, _pred_data,                    \
                                 predicate_type::type::LESS_THAN );          \
            break;                                                           \
        }                                                                    \
        case predicate_type::type::GREATER_THAN: {                           \
            return _eval_method( _stats.max_, _pred_data,                    \
                                 predicate_type::type::GREATER_THAN );       \
            break;                                                           \
        }                                                                    \
        case predicate_type::type::LESS_THAN_OR_EQUAL: {                     \
            return _eval_method( _stats.min_, _pred_data,                    \
                                 predicate_type::type::LESS_THAN_OR_EQUAL ); \
            break;                                                           \
        }                                                                    \
        case predicate_type::type::GREATER_THAN_OR_EQUAL: {                  \
            return _eval_method(                                             \
                _stats.max_, _pred_data,                                     \
                predicate_type::type::GREATER_THAN_OR_EQUAL );               \
            break;                                                           \
        }                                                                    \
    }                                                                        \
    return false;

bool evaluate_cell_predicate_on_uint64_stats( const column_stats<uint64_t>& stats,
                                         const cell_predicate& predicate ) {
    TEMPL_EVALUATE_CELL_PRED_ON_COLUMN_DATA_STATS(
        stats, predicate, data_type::type::UINT64,
        evaluate_predicate_on_uint64_data, string_to_uint64 );
}
bool evaluate_cell_predicate_on_int64_stats( const column_stats<int64_t>& stats,
                                        const cell_predicate& predicate ) {

    TEMPL_EVALUATE_CELL_PRED_ON_COLUMN_DATA_STATS(
        stats, predicate, data_type::type::INT64,
        evaluate_predicate_on_int64_data, string_to_int64 );
}
bool evaluate_cell_predicate_on_double_stats( const column_stats<double>& stats,
                                         const cell_predicate& predicate ) {
    TEMPL_EVALUATE_CELL_PRED_ON_COLUMN_DATA_STATS(
        stats, predicate, data_type::type::DOUBLE,
        evaluate_predicate_on_double_data, string_to_double );
}
bool evaluate_cell_predicate_on_string_stats(
    const column_stats<std::string>& stats, const cell_predicate& predicate ) {
    TEMPL_EVALUATE_CELL_PRED_ON_COLUMN_DATA_STATS(
        stats, predicate, data_type::type::STRING,
        evaluate_predicate_on_string_data, string_to_string );
}

#define TEMPL_EVALUATE_PRED_ON_COLUMN_STATS( _stats, _pred, _expect_type, \
                                             _eval_method )               \
    int32_t num_cols = 1;                                                 \
    bool    and_pred_ok = true;                                           \
    if( _stats.count_ == 0 ) {                                               \
        return false;                                                        \
    }                                                                        \
    for( const auto& _c_pred : _pred.and_predicates ) {                   \
        int32_t col_id = _c_pred.col_id;                                  \
        if( ( col_id >= (int32_t) num_cols ) or                           \
            ( _c_pred.type != _expect_type ) ) {                          \
            and_pred_ok = false;                                          \
            break;                                                        \
        }                                                                 \
        if( _c_pred.predicate != predicate_type::type::INEQUALITY ) {     \
            bool _pred_eval = _eval_method( _stats, _c_pred );            \
            if( !_pred_eval ) {                                           \
                and_pred_ok = false;                                      \
                break;                                                    \
            }                                                             \
        }                                                                 \
    }                                                                     \
    if( ( !and_pred_ok ) and _pred.is_and_join_predicates ) {             \
        return false;                                                     \
    }                                                                     \
    if( predicate.or_predicates.empty() ) {                               \
        return and_pred_ok;                                               \
    }                                                                     \
    bool or_pred_ok = false;                                              \
    for( const auto& _c_pred : _pred.or_predicates ) {                    \
        int32_t col_id = _c_pred.col_id;                                  \
        if( ( col_id >= (int32_t) num_cols ) or                           \
            ( _c_pred.type != _expect_type ) ) {                          \
            continue;                                                     \
        }                                                                 \
        if( _c_pred.predicate != predicate_type::type::INEQUALITY ) {     \
            bool _pred_eval = _eval_method( _stats, _c_pred );            \
            if( _pred_eval ) {                                            \
                or_pred_ok = true;                                        \
                break;                                                    \
            }                                                             \
        } else {                                                          \
            or_pred_ok = true;                                            \
            break;                                                        \
        }                                                                 \
    }                                                                     \
    bool ret = or_pred_ok;                                                \
    if( _pred.is_and_join_predicates ) {                                  \
        ret = and_pred_ok and or_pred_ok;                                 \
    } else if( !predicate.and_predicates.empty() ) {                      \
        ret = and_pred_ok or or_pred_ok;                                  \
    }                                                                     \
    return ret;

bool evaluate_predicate_on_column_stats( const column_stats<uint64_t>& stats,
                                         const predicate_chain& predicate ) {
    TEMPL_EVALUATE_PRED_ON_COLUMN_STATS(
        stats, predicate, data_type::type::UINT64,
        evaluate_cell_predicate_on_uint64_stats );
}
bool evaluate_predicate_on_column_stats( const column_stats<int64_t>& stats,
                                         const predicate_chain& predicate ) {
    TEMPL_EVALUATE_PRED_ON_COLUMN_STATS(
        stats, predicate, data_type::type::INT64,
        evaluate_cell_predicate_on_int64_stats );
}
bool evaluate_predicate_on_column_stats( const column_stats<double>& stats,
                                         const predicate_chain& predicate ) {
    TEMPL_EVALUATE_PRED_ON_COLUMN_STATS(
        stats, predicate, data_type::type::DOUBLE,
        evaluate_cell_predicate_on_double_stats );
}
bool evaluate_predicate_on_column_stats( const column_stats<std::string>& stats,
                                         const predicate_chain& predicate ) {
    TEMPL_EVALUATE_PRED_ON_COLUMN_STATS(
        stats, predicate, data_type::type::STRING,
        evaluate_cell_predicate_on_string_stats );
}

#define TEMPL_EVALUATE_CELL_PRED_ON_MULTI_COLUMN_DATA_STATS( \
    _data, _pred, _type, _eval_method, _get_method )         \
    if( _data.count_ == 0 ) {                                \
        return false;                                        \
    }                                                        \
    column_stats<_type> _cons_stats;                         \
    _cons_stats.count_ = _data.count_;                       \
    uint32_t _col_id = _pred.col_id;                         \
    DCHECK_LT( _col_id, _data.min_.get_num_columns() );      \
    DCHECK_LT( _col_id, _data.max_.get_num_columns() );      \
    auto _min_data = _data.min_._get_method( _col_id );      \
    DCHECK( std::get<0>( _min_data ) );                      \
    _cons_stats.min_ = std::get<1>( _min_data );             \
    auto _max_data = _data.max_._get_method( _col_id );      \
    DCHECK( std::get<0>( _max_data ) );                      \
    _cons_stats.max_ = std::get<1>( _max_data );             \
    auto _avg_data = _data.average_._get_method( _col_id );  \
    if( std::get<0>( _avg_data ) ) {                         \
        _cons_stats.average_ = std::get<1>( _avg_data );     \
    }                                                        \
    return _eval_method( _cons_stats, _pred );

bool evaluate_cell_predicate_on_multi_column_stats(
    const column_stats<multi_column_data>& stats, const cell_predicate& pred ) {
    switch( pred.type ) {
        case data_type::type::UINT64: {
            TEMPL_EVALUATE_CELL_PRED_ON_MULTI_COLUMN_DATA_STATS(
                stats, pred, uint64_t, evaluate_cell_predicate_on_uint64_stats,
                get_uint64_data );
            break;
        }
        case data_type::type::INT64: {
            TEMPL_EVALUATE_CELL_PRED_ON_MULTI_COLUMN_DATA_STATS(
                stats, pred, int64_t, evaluate_cell_predicate_on_int64_stats,
                get_int64_data );
            break;
        }
        case data_type::type::DOUBLE: {
            TEMPL_EVALUATE_CELL_PRED_ON_MULTI_COLUMN_DATA_STATS(
                stats, pred, double, evaluate_cell_predicate_on_double_stats,
                get_double_data );
            break;
        }
        case data_type::type::STRING: {
            TEMPL_EVALUATE_CELL_PRED_ON_MULTI_COLUMN_DATA_STATS(
                stats, pred, std::string,
                evaluate_cell_predicate_on_string_stats, get_string_data );
            break;
        }
    }
    return false;
}

bool evaluate_predicate_on_column_stats(
    const column_stats<multi_column_data>& stats,
    const predicate_chain&                 predicate ) {
    if( stats.count_ == 0 ) {
        return false;
    }
    bool and_pred_ok = true;
    uint32_t num_cols = stats.min_.get_num_columns();
    DCHECK_EQ( num_cols, stats.max_.get_num_columns() );
    for( const auto& c_pred : predicate.and_predicates ) {
        int32_t col_id = c_pred.col_id;
        if( col_id >= (int32_t) num_cols ) {
            and_pred_ok = false;
            break;
        }
        if( c_pred.predicate != predicate_type::type::INEQUALITY ) {
            bool pred_eval =
                evaluate_cell_predicate_on_multi_column_stats( stats, c_pred );
            if( !pred_eval ) {
                and_pred_ok = false;
                break;
            }
        }
    }
    if( ( !and_pred_ok ) and predicate.is_and_join_predicates ) {
        return false;
    }
    if( predicate.or_predicates.empty() ) {
        DVLOG( 40 ) << "Returning:" << and_pred_ok;
        return and_pred_ok;
    }
    bool or_pred_ok = false;
    for( const auto& c_pred : predicate.or_predicates ) {
        int32_t col_id = c_pred.col_id;
        if( col_id >= (int32_t) num_cols ) {
            continue;
        }
        if( c_pred.predicate != predicate_type::type::INEQUALITY ) {
            bool pred_eval =
                evaluate_cell_predicate_on_multi_column_stats( stats, c_pred );
            if( pred_eval ) {
                or_pred_ok = true;
                break;
            }
        } else {
            or_pred_ok = true;
            break;
        }
    }
    bool ret = or_pred_ok;
    if( predicate.is_and_join_predicates ) {
        ret = and_pred_ok and or_pred_ok;
    } else if( !predicate.and_predicates.empty() ) {
        ret = and_pred_ok or or_pred_ok;
    }
    return ret;
}

#define TEMPL_EVALUATE_PRED_ON_COLUMN_DATA( _data, _c_pred, _exp_type,   \
                                            _eval_method, _conv_method ) \
    _eval_method( _data, _conv_method( _c_pred.data ), _c_pred.predicate );

#define TEMPL_EVALUATE_PRED_ON_COLUMN( _data, _pred, _expect_type,      \
                                       _eval_method, _conv_method )     \
    int32_t num_cols = 1;                                               \
    bool    and_pred_ok = true;                                         \
    for( const auto& _c_pred : _pred.and_predicates ) {                 \
        int32_t col_id = _c_pred.col_id;                                \
        if( ( col_id >= (int32_t) num_cols ) or                         \
            ( _c_pred.type != _expect_type ) ) {                        \
            and_pred_ok = false;                                        \
            break;                                                      \
        }                                                               \
        bool _pred_eval = TEMPL_EVALUATE_PRED_ON_COLUMN_DATA(           \
            _data, _c_pred, _expect_type, _eval_method, _conv_method ); \
        if( !_pred_eval ) {                                             \
            and_pred_ok = false;                                        \
            break;                                                      \
        }                                                               \
    }                                                                   \
    if( ( !and_pred_ok ) and _pred.is_and_join_predicates ) {           \
        return false;                                                   \
    }                                                                   \
    if( predicate.or_predicates.empty() ) {                             \
        return and_pred_ok;                                             \
    }                                                                   \
    bool or_pred_ok = false;                                            \
    for( const auto& _c_pred : _pred.or_predicates ) {                  \
        int32_t col_id = _c_pred.col_id;                                \
        if( ( col_id >= (int32_t) num_cols ) or                         \
            ( _c_pred.type != _expect_type ) ) {                        \
            continue;                                                   \
        }                                                               \
        bool _pred_eval = TEMPL_EVALUATE_PRED_ON_COLUMN_DATA(           \
            _data, _c_pred, _expect_type, _eval_method, _conv_method ); \
        if( _pred_eval ) {                                              \
            or_pred_ok = true;                                          \
            break;                                                      \
        }                                                               \
    }                                                                   \
    bool ret = or_pred_ok;                                              \
    if( _pred.is_and_join_predicates ) {                                \
        ret = and_pred_ok and or_pred_ok;                               \
    } else if( !predicate.and_predicates.empty() ) {                    \
        ret = and_pred_ok or or_pred_ok;                                \
    }                                                                   \
    return ret;

bool evaluate_predicate_on_col_data( const uint64_t&        data,
                                     const predicate_chain& predicate ) {
    TEMPL_EVALUATE_PRED_ON_COLUMN( data, predicate, data_type::type::UINT64,
                                   evaluate_predicate_on_uint64_data,
                                   string_to_uint64 );
}
bool evaluate_predicate_on_col_data( const int64_t&        data,
                                     const predicate_chain& predicate ) {
    TEMPL_EVALUATE_PRED_ON_COLUMN( data, predicate, data_type::type::INT64,
                                   evaluate_predicate_on_int64_data,
                                   string_to_int64 );
}
bool evaluate_predicate_on_col_data( const double&        data,
                                     const predicate_chain& predicate ) {
    TEMPL_EVALUATE_PRED_ON_COLUMN( data, predicate, data_type::type::DOUBLE,
                                   evaluate_predicate_on_int64_data,
                                   string_to_int64 );
}
bool evaluate_predicate_on_col_data( const std::string&     data,
                                     const predicate_chain& predicate ) {

    TEMPL_EVALUATE_PRED_ON_COLUMN( data, predicate, data_type::type::STRING,
                                   evaluate_predicate_on_string_data,
                                   string_to_string );
}

#define TEMPL_EVALUATE_PRED_ON_MULTI_COL_DATA( _data, _pred, _pred_func,    \
                                               _get_func, _conv_func )      \
    DCHECK_LT( pred.col_id, data.get_num_columns() );                       \
    auto _get_tuple = _data._get_func( pred.col_id );                       \
    if( !std::get<0>( _get_tuple ) ) {                                      \
        return false;                                                       \
    }                                                                       \
    return _pred_func( std::get<1>( _get_tuple ), _conv_func( _pred.data ), \
                       _pred.predicate );

bool evaluate_cell_predicate_on_multi_column_data( const multi_column_data& data,
                                              const cell_predicate&    pred ) {
    switch( pred.type ) {
        case data_type::type::UINT64: {
            TEMPL_EVALUATE_PRED_ON_MULTI_COL_DATA(
                data, pred, evaluate_predicate_on_uint64_data, get_uint64_data,
                string_to_uint64 );
            break;
        }
        case data_type::type::INT64: {
            TEMPL_EVALUATE_PRED_ON_MULTI_COL_DATA(
                data, pred, evaluate_predicate_on_int64_data, get_int64_data,
                string_to_int64 );
            break;
        }
        case data_type::type::DOUBLE: {
            TEMPL_EVALUATE_PRED_ON_MULTI_COL_DATA(
                data, pred, evaluate_predicate_on_double_data, get_double_data,
                string_to_double );
            break;
        }
        case data_type::type::STRING: {
            TEMPL_EVALUATE_PRED_ON_MULTI_COL_DATA(
                data, pred, evaluate_predicate_on_string_data, get_string_data,
                string_to_string );
            break;
        }
    }
    return false;
}

bool evaluate_predicate_on_col_data( const multi_column_data& data,
                                     const predicate_chain&   predicate ) {
    uint32_t num_cols = data.get_num_columns();

    bool and_pred_ok = true;

    for( const auto& pred : predicate.and_predicates ) {
        int32_t col_id = pred.col_id;
        if( col_id >= (int32_t) num_cols ) {
            and_pred_ok = false;
            break;
        }
        bool pred_ok =
            evaluate_cell_predicate_on_multi_column_data( data, pred );
        if( !pred_ok ) {
            and_pred_ok = false;
            break;
        }
    }

    if( ( !and_pred_ok ) and predicate.is_and_join_predicates ) {
        return false;
    }

    if( predicate.or_predicates.empty() ) {
        return and_pred_ok;
    }

    bool or_pred_ok = false;
    for( const auto& pred : predicate.or_predicates ) {
        int32_t col_id = pred.col_id;
        if( col_id >= (int32_t) num_cols ) {
            continue;
        }
        bool pred_ok =
            evaluate_cell_predicate_on_multi_column_data( data, pred );
        if( pred_ok ) {
            or_pred_ok = true;
            break;
        }
    }

    bool ret = or_pred_ok;
    if( predicate.is_and_join_predicates ) {
        ret = and_pred_ok and or_pred_ok;
    } else if( !predicate.and_predicates.empty() ) {
        ret = and_pred_ok or or_pred_ok;
    }

    return ret;
}

#define TEMPL_PRED_EVAL_OP( _data, _pred_data, _op )                  \
    auto _ret = ( _data _op _pred_data );                             \
    DVLOG( 40 ) << "Eval PRED: _data:" << _data << ", op:" << #_op    \
                << ", _pred_data:" << _pred_data << ", ret:" << _ret; \
    return _ret;

#define TEMPL_PRED_EVAL( _data, _pred_data, _pred_type )    \
    switch( _pred_type ) {                                  \
        case predicate_type::type::EQUALITY: {              \
            TEMPL_PRED_EVAL_OP( _data, _pred_data, == );    \
            break;                                          \
        }                                                   \
        case predicate_type::type::INEQUALITY: {            \
            TEMPL_PRED_EVAL_OP( _data, _pred_data, != );    \
            break;                                          \
        }                                                   \
        case predicate_type::type::LESS_THAN: {             \
            TEMPL_PRED_EVAL_OP( _data, _pred_data, < );     \
            break;                                          \
        }                                                   \
        case predicate_type::type::GREATER_THAN: {          \
            TEMPL_PRED_EVAL_OP( _data, _pred_data, > );     \
            break;                                          \
        }                                                   \
        case predicate_type::type::LESS_THAN_OR_EQUAL: {    \
            TEMPL_PRED_EVAL_OP( _data, _pred_data, <= );    \
            break;                                          \
        }                                                   \
        case predicate_type::type::GREATER_THAN_OR_EQUAL: { \
            TEMPL_PRED_EVAL_OP( _data, _pred_data, >= );    \
            break;                                          \
        }                                                   \
    }                                                       \
    return false;

bool evaluate_predicate_on_uint64_data( uint64_t data, uint64_t pred_data,
                                        const predicate_type::type& p_type ) {
    TEMPL_PRED_EVAL( data, pred_data, p_type );
}
bool evaluate_predicate_on_int64_data( int64_t data, int64_t pred_data,
                                       const predicate_type::type& p_type ) {
    TEMPL_PRED_EVAL( data, pred_data, p_type );
}
bool evaluate_predicate_on_double_data( double data, double pred_data,
                                        const predicate_type::type& p_type ) {
    TEMPL_PRED_EVAL( data, pred_data, p_type );
}

#define TEMPL_PRED_EVAL_STR( _data, _pred_data, _op )                 \
    auto _ret = ( _data.compare( _pred_data ) _op 0 );                \
    DVLOG( 40 ) << "Eval PRED: _data:" << _data << #_op               \
                << ", _pred_data:" << _pred_data << ", ret:" << _ret; \
    return _ret;

bool evaluate_predicate_on_string_data( const std::string&          data,
                                        const std::string&          pred_data,
                                        const predicate_type::type& p_type ) {
    switch( p_type ) {
        case predicate_type::type::EQUALITY: {
            TEMPL_PRED_EVAL_STR( data, pred_data, == );
            break;
        }
        case predicate_type::type::INEQUALITY: {
            TEMPL_PRED_EVAL_STR( data, pred_data, != );
            break;
        }
        case predicate_type::type::LESS_THAN: {
            TEMPL_PRED_EVAL_STR( data, pred_data, < );
            break;
        }
        case predicate_type::type::GREATER_THAN: {
            TEMPL_PRED_EVAL_STR( data, pred_data, > );
            break;
        }
        case predicate_type::type::LESS_THAN_OR_EQUAL: {
            TEMPL_PRED_EVAL_STR( data, pred_data, <= );
            break;
        }
        case predicate_type::type::GREATER_THAN_OR_EQUAL: {
            TEMPL_PRED_EVAL_STR( data, pred_data, >= );
            break;
        }
    }
    return false;
}

std::vector<uint32_t> get_col_ids_to_scan_from_partition_id(
    uint64_t low_key, uint64_t high_key,
    const std::vector<uint32_t>&       project_cols,
    const partition_column_identifier& pid ) {
    std::vector<uint32_t> filtered_down_cols;
    DCHECK_LE( low_key, high_key );
    if( low_key > (uint64_t) pid.partition_end ) {
        return filtered_down_cols;
    }
    if( high_key < (uint64_t) pid.partition_start ) {
        return filtered_down_cols;
    }
    DCHECK( std::is_sorted( std::begin( project_cols ),
                            std::end( project_cols ) ) );
    for( uint32_t col : project_cols ) {
        if( ( col >= (uint32_t) pid.column_start ) and
            ( col <= (uint32_t) pid.column_end ) ) {
            filtered_down_cols.emplace_back( normalize_column_id( col, pid ) );
        }
    }
    return filtered_down_cols;
}

predicate_chain translate_predicate_to_partition_id(
    const predicate_chain& predicate, const partition_column_identifier& pid,
    const std::vector<cell_data_type>& col_types ) {

    predicate_chain translated_predicate;
    translated_predicate.is_and_join_predicates =
        predicate.is_and_join_predicates;

    for( const auto& pred : predicate.and_predicates ) {
        if( ( pred.table_id == pid.table_id ) and
            ( pred.col_id >= pid.column_start ) and
            ( pred.col_id <= pid.column_end ) ) {

            cell_predicate translated_pred = pred;
            translated_pred.col_id = normalize_column_id( pred.col_id, pid );

            DCHECK_EQ( cell_data_type_to_data_type(
                           col_types.at( translated_pred.col_id ) ),
                       translated_pred.type );

            translated_predicate.and_predicates.emplace_back( translated_pred );
        }
    }
    for( const auto& pred : predicate.or_predicates ) {
        if( ( pred.table_id == pid.table_id ) and
            ( pred.col_id >= pid.column_start ) and
            ( pred.col_id <= pid.column_end ) ) {

            cell_predicate translated_pred = pred;
            translated_pred.col_id = normalize_column_id( pred.col_id, pid );

            DCHECK_EQ( cell_data_type_to_data_type(
                           col_types.at( translated_pred.col_id ) ),
                       translated_pred.type );

            translated_predicate.or_predicates.emplace_back( translated_pred );
        }
    }
    return translated_predicate;
}

