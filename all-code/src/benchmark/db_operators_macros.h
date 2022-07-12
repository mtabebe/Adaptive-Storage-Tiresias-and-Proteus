#pragma once

#define DO_DB_OP( _db_ops, _method, _cast_type, _data, _class_var, _cid ) \
    _db_ops->_method( _cid, (_cast_type) _data->_class_var );

#define DO_DB_READ_OP( _ret, _db_ops, _lookup_method, _lookup_nullable_method, \
                       _lookup_latest_method, _lookup_latest_nullable_method,  \
                       _cast_type, _data, _class_var, _cid, _is_latest,        \
                       _is_nullable )                                          \
    _ret = false;                                                              \
    if( _is_latest ) {                                                         \
        if( _is_nullable ) {                                                   \
            auto _found = _db_ops->_lookup_latest_nullable_method( _cid );     \
            _data->_class_var = (_cast_type) std::get<1>( _found );            \
            _ret = std::get<0>( _found );                                      \
        } else {                                                               \
            _data->_class_var =                                                \
                (_cast_type) _db_ops->_lookup_latest_method( _cid );           \
            _ret = true;                                                       \
        }                                                                      \
    } else {                                                                   \
        if( _is_nullable ) {                                                   \
            auto _found = _db_ops->_lookup_nullable_method( _cid );            \
            _data->_class_var = (_cast_type) std::get<1>( _found );            \
            _ret = std::get<0>( _found );                                      \
        } else {                                                               \
            _data->_class_var = (_cast_type) _db_ops->_lookup_method( _cid );  \
            _ret = true;                                                       \
        }                                                                      \
    }

#define DO_UINT64_READ_OP( _ret, _db_ops, _cast_type, _data, _class_var, _cid, \
                           _is_latest, _is_nullable )                          \
    DO_DB_READ_OP( _ret, _db_ops, lookup_uint64, lookup_nullable_uint64,       \
                   lookup_latest_uint64, lookup_latest_nullable_uint64,        \
                   _cast_type, _data, _class_var, _cid, _is_latest,            \
                   _is_nullable )
#define DO_INT64_READ_OP( _ret, _db_ops, _cast_type, _data, _class_var, _cid, \
                          _is_latest, _is_nullable )                          \
    DO_DB_READ_OP( _ret, _db_ops, lookup_int64, lookup_nullable_int64,        \
                   lookup_latest_int64, lookup_latest_nullable_int64,         \
                   _cast_type, _data, _class_var, _cid, _is_latest,           \
                   _is_nullable )
#define DO_DOUBLE_READ_OP( _ret, _db_ops, _cast_type, _data, _class_var, _cid, \
                           _is_latest, _is_nullable )                          \
    DO_DB_READ_OP( _ret, _db_ops, lookup_double, lookup_nullable_double,       \
                   lookup_latest_double, lookup_latest_nullable_double,        \
                   _cast_type, _data, _class_var, _cid, _is_latest,            \
                   _is_nullable )
#define DO_STRING_READ_OP( _ret, _db_ops, _cast_type, _data, _class_var, _cid, \
                           _is_latest, _is_nullable )                          \
    DO_DB_READ_OP( _ret, _db_ops, lookup_string, lookup_nullable_string,       \
                   lookup_latest_string, lookup_latest_nullable_string,        \
                   _cast_type, _data, _class_var, _cid, _is_latest,            \
                   _is_nullable )

#define DO_SCAN_OP( _ret, _conv_method, _cast_type, _cell, _data, _class_var ) \
    _ret = false;                                                              \
    if( _cell.present ) {                                                      \
        _data->_class_var = (_cast_type) _conv_method( _cell.data );           \
        _ret = true;                                                           \
    }

