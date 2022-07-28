import sys

typemap = {
        'int64_t' : 'int64_t',
        'int32_t' : 'int64_t',
        'uint64_t' : 'uint64_t',
        'uint32_t' : 'uint64_t',
        'double' : 'double',
        'float' : 'double',
        'std::string' : 'std::string',
}

shorttypemap = {
        'int64_t' : 'int64',
        'int32_t' : 'int64',
        'uint64_t' : 'uint64',
        'uint32_t' : 'uint64',
        'double' : 'double',
        'float' : 'double',
        'std::string' : 'string',
}
readtypemap = {
        'int64_t' : 'INT64',
        'int32_t' : 'INT64',
        'uint64_t' : 'UINT64',
        'uint32_t' : 'UINT64',
        'double' : 'DOUBLE',
        'float' : 'DOUBLE',
        'std::string' : 'STRING',
}

def name_to_col( name ):
    new_name = name
    new_name = new_name.replace( '.', '_' )
    return new_name


class Table():
    def __init__(self):
        self.var_types = list()
        self.var_names = list()
        self.table_name = ""

    def output( self ):
        print( "Table: {}".format( self.table_name ) )
        for i in range(len(self.var_names ) ):
            print( "\t var: {}, type:{}, conv_type:{}".format( self.var_names[ i ], self.var_types[ i ], typemap[ self.var_types[ i ] ] ) )

    def buildStruct( self, outfile ):
        with open( outfile, "w") as f:
#            f.write("#pragma once\n")
            f.write("\n")
            f.write("struct {} {{\n".format( self.table_name ) )
            f.write("\tpublic:\n\n")
            for i in range(len(self.var_names ) ):
                f.write("\t\t{} {};\n".format( self.var_types[ i ], self.var_names[ i ] ) )
            f.write("};\n\n")
            f.write("struct {}_cols {{\n".format( self.table_name ) )
            f.write("\tpublic:\n\n")
            for i in range(len(self.var_names ) ):
                f.write("\t\t static constexpr uint32_t {} = {};\n".format( name_to_col( self.var_names[ i ] ) , i ) )
            f.write("};")

    def buildJava( self, outfile ):
        with open( outfile, "w") as f:
#            f.write("#pragma once\n")
            f.write("\n")
            for i in range(len(self.var_names ) ):
                f.write("\tpublic static final int COL_{} = {};\n".format( name_to_col( self.var_names[ i ] ).upper(), i ) )
            f.write("\n")


    def buildTypes( self, outfile ):
        with open( outfile, "w") as f:
#            f.write("#pragma once\n")
            f.write("\n")
            f.write("static constexpr uint32_t k_tpcc_{}_num_columns = {};\n".format( self.table_name, len( self.var_names ) ) )
            f.write("static const std::vector<cell_data_type> k_tpcc_{}_col_types = {{\n".format( self.table_name ) )
            for i in range(len(self.var_names ) ):
                f.write("\tcell_data_type::{} /* {} */,\n".format( readtypemap[ self.var_types[ i ] ], self.var_names[ i ] ) )
            f.write("};\n")

    def buildConstDef( self, outfile ):
        with open( outfile, "w") as f:
#            f.write("#pragma once\n")
            f.write("\n")
            f.write("DECLARE_uint32( tpcc_{}_column_partition_size );\n".format( self.table_name ) )
            f.write("DECLARE_string( tpcc_{}_part_type );\n".format( self.table_name ) )
            f.write("DECLARE_string( tpcc_{}_storage_type );\n".format( self.table_name ) )
            f.write("\n" )
            f.write("extern uint32_t k_tpcc_{}_column_partition_size;\n".format( self.table_name ) )
            f.write("extern partition_type::type k_tpcc_{}_part_type;\n".format( self.table_name ) )
            f.write("extern storage_tier_type::type k_tpcc_{}_storage_type;\n".format( self.table_name ) )

    def buildConstImpl( self, outfile ):
        with open( outfile, "w") as f:
            f.write("\n")
            f.write("DEFINE_uint32( tpcc_{}_column_partition_size, k_tpcc_{}_num_columns, \"The number of columns in a {} partition\" );\n".format( self.table_name, self.table_name, self.table_name ) )
            f.write("DEFINE_string( tpcc_{}_part_type, \"ROW\", \"The partition type of {}\");\n".format( self.table_name, self.table_name ) )
            f.write("DEFINE_string( tpcc_{}_storage_type, \"MEMORY\", \"The storage tier type of {}\" );\n".format( self.table_name, self.table_name ) )
            f.write("\n" )
            f.write("uint32_t k_tpcc_{}_column_partition_size = FLAGS_tpcc_{}_column_partition_size;\n".format( self.table_name, self.table_name ) )
            f.write("partition_type::type k_tpcc_{}_part_type = partition_type::type::ROW;\n".format( self.table_name ) )
            f.write("storage_tier_type::type k_tpcc_{}_storage_type = storage_tier_type::type::MEMORY;\n".format( self.table_name ) )
            f.write("\n" )
            f.write("\tk_tpcc_{}_column_partition_size = FLAGS_tpcc_{}_column_partition_size;\n".format( self.table_name, self.table_name ) )
            f.write("\tVLOG( 0 ) << \"k_tpcc_{}_column_partition_size:\" << k_tpcc_{}_column_partition_size;\n".format( self.table_name, self.table_name ) )
            f.write("\tk_tpcc_{}_part_type = string_to_partition_type( FLAGS_tpcc_{}_part_type );\n".format( self.table_name, self.table_name ) )
            f.write("\tVLOG( 0 ) << \"k_tpcc_{}_part_type:\" << k_tpcc_{}_part_type;\n".format( self.table_name, self.table_name ) )
            f.write("\tk_tpcc_{}_storage_type = string_to_storage_type( FLAGS_tpcc_{}_storage_type );\n".format( self.table_name, self.table_name ) )
            f.write("\tVLOG( 0 ) << \"k_tpcc_{}_storage_type:\" << k_tpcc_{}_storage_type;\n".format( self.table_name, self.table_name ) )



    def buildDef( self, outfile ):
        with open( outfile, "w") as f:
#            f.write("#pragma once\n")
            f.write("\n")
            f.write("void insert_{}(\n".format( self.table_name ) )
            f.write( "\tbenchmark_db_operators* db_ops, const {}* var,\n".format( self.table_name ) )
            f.write( "\tuint64_t row_id, const cell_key_ranges& ckr );\n\n" )

            f.write("void update_{}(\n".format( self.table_name ) )
            f.write( "\tbenchmark_db_operators* db_ops, const {}* var,\n".format( self.table_name ) )
            f.write( "\tuint64_t row_id, const cell_key_ranges& ckr, bool do_propagate );\n\n" )

            f.write("bool lookup_{}(\n".format( self.table_name ) )
            f.write( "\tbenchmark_db_operators* db_ops, {}* var,\n".format( self.table_name ) )
            f.write( "\tuint64_t row_id, const cell_key_ranges& ckr,\n" )
            f.write( "\tbool is_lates, bool is_nullable );\n" )

            f.write( "std::unordered_set<uint32_t> read_from_scan_{}(\n".format( self.table_name ) )
            f.write( "\tconst result_tuple& res, {}* var);\n".format( self.table_name ) )


    def buildImpl( self, outfile ):
        with open( outfile, "w") as f:
            f.write("void insert_{}(\n".format( self.table_name ) )
            f.write( "\tbenchmark_db_operators* db_ops, const {}* var,\n".format( self.table_name ) )
            f.write( "\tuint64_t row_id, const cell_key_ranges& ckr ) {\n" )
            f.write( "\tDCHECK( db_ops );\n" )
            f.write( "\tDCHECK( var );\n" )
            f.write( "\tauto cid = create_cell_identifier( ckr.table_id, ckr.col_id_start, row_id );\n\n" )
            f.write( "\tfor( int32_t col = ckr.col_id_start; col <= ckr.col_id_end; col++) {\n")
            f.write( "\t\tcid.col_id_ = col;\n")
            f.write( "\t\tswitch( col ) {\n")
            for i in range(len(self.var_names ) ):
                f.write( "\t\t\tcase {}_cols::{}: {{\n".format( self.table_name, name_to_col(  self.var_names[ i ] ) ) )
                mapped_type = typemap[ self.var_types[ i ] ]
                short_type = shorttypemap[ self.var_types[ i ] ]
                f.write("\t\t\t\tDO_DB_OP( db_ops, insert_{}, {}, var, {}, cid );\n".format( short_type, mapped_type, self.var_names[ i ] ) )
                f.write( "\t\t\t\tbreak;\n\t\t\t}\n")
            f.write( "\t\t}\n") # switch
            f.write( "\t}\n") # for
            f.write( "}\n\n" ) # insert

            f.write("void update_{}(\n".format( self.table_name ) )
            f.write( "\tbenchmark_db_operators* db_ops, const {}* var,\n".format( self.table_name ) )
            f.write( "\tuint64_t row_id, const cell_key_ranges& ckr, bool do_propagate ) {\n" )
            f.write( "\tDCHECK( db_ops );\n" )
            f.write( "\tDCHECK( var );\n" )
            f.write( "\tauto cid = create_cell_identifier( ckr.table_id, ckr.col_id_start, row_id );\n\n" )
            f.write( "\tfor( int32_t col = ckr.col_id_start; col <= ckr.col_id_end; col++) {\n")
            f.write( "\t\tcid.col_id_ = col;\n")
            f.write( "\t\tswitch( col ) {\n")
            for i in range(len(self.var_names ) ):
                f.write( "\t\t\tcase {}_cols::{}: {{\n".format( self.table_name, name_to_col( self.var_names[ i ] ) ) )
                mapped_type = typemap[ self.var_types[ i ] ]
                short_type = shorttypemap[ self.var_types[ i ] ]
                f.write("\t\t\t\tDO_DB_OP( db_ops, write_{}, {}, var, {}, cid );\n".format( short_type, mapped_type, self.var_names[ i ] ) )
                f.write( "\t\t\t\tbreak;\n\t\t\t}\n")
            f.write( "\t\t}\n") # switch
            f.write( "\t}\n") # for
            f.write( "}\n\n" ) # write

            f.write("bool lookup_{}(\n".format( self.table_name ) )
            f.write( "\tbenchmark_db_operators* db_ops, {}* var,\n".format( self.table_name ) )
            f.write( "\tuint64_t row_id, const cell_key_ranges& ckr,\n")
            f.write( "\tbool is_latest, bool is_nullable) {\n" )
            f.write( "\tDCHECK( db_ops );\n" )
            f.write( "\tDCHECK( var );\n" )
            f.write( "\tauto cid = create_cell_identifier( ckr.table_id, ckr.col_id_start, row_id );\n" )
            f.write( "\tbool ret = true;\n\n" )
            f.write( "\tfor( int32_t col = ckr.col_id_start; col <= ckr.col_id_end; col++) {\n")
            f.write( "\t\tbool loc_ret = true;\n")
            f.write( "\t\tcid.col_id_ = col;\n")
            f.write( "\t\tswitch( col ) {\n")
            for i in range(len(self.var_names ) ):
                f.write( "\t\t\tcase {}_cols::{}: {{\n".format( self.table_name, name_to_col( self.var_names[ i ] ) ) )
                read_type = readtypemap[ self.var_types[ i ] ]
                f.write("\t\t\t\tDO_{}_READ_OP( loc_ret, db_ops, {}, var, {}, cid, is_latest, is_nullable );\n".format( read_type, self.var_types[ i ], self.var_names[ i ] ) )
                f.write( "\t\t\t\tbreak;\n\t\t\t}\n")
            f.write( "\t\t}\n") # switch
            f.write( "\t\tret = ret and loc_ret;\n") # switch
            f.write( "\t}\n") # for
            f.write( "\treturn ret;\n") #
            f.write( "}\n\n" ) # lookup

            f.write( "std::unordered_set<uint32_t> read_from_scan_{}(\n".format( self.table_name ) )
            f.write( "\tconst result_tuple& res, {}* var) {{\n".format( self.table_name ) )
            f.write( "\tstd::unordered_set<uint32_t> ret;\n\n" )
            f.write( "\tfor( const auto& cell : res.cells ) {\n")
            f.write( "\t\tbool loc_ret = false;\n")
            f.write( "\t\tswitch( cell.col_id ) {\n")
            for i in range(len(self.var_names ) ):
                f.write( "\t\t\tcase {}_cols::{}: {{\n".format( self.table_name, name_to_col( self.var_names[ i ] ) ) )
                string_method = "string_to_{}".format( shorttypemap[ self.var_types[ i ] ] )
                f.write("\t\t\t\tDO_SCAN_OP( loc_ret, {}, {}, cell, var, {} );\n".format( string_method, self.var_types[ i ], self.var_names[ i ] ) )
                f.write( "\t\t\tif ( loc_ret ) {\n") # if
                f.write( "\t\t\t\t ret.emplace( {}_cols::{} );\n".format( self.table_name, name_to_col( self.var_names[ i ] ) ) ) # if
                f.write( "\t\t\t}\n") # if
                f.write( "\t\t\t\tbreak;\n\t\t\t}\n")
            f.write( "\t\t}\n") # switch
            f.write( "\t}\n") # for
            f.write( "\treturn ret;\n") #
            f.write( "}\n\n" ) # read_from_scan



inputFile = sys.argv[ 1 ]
outputDefFile = sys.argv[ 2 ]
outputImplFile = sys.argv[ 3 ]
outputStructFile = sys.argv[ 4 ]
outputTypeFile = sys.argv[ 5 ]
outputConstDefFile = sys.argv[ 6 ]
outputConstImplFile = sys.argv[ 7 ]
outputJavaFile = sys.argv[ 8 ]

with open( inputFile) as f:
    content = f.readlines()
content = [x.strip() for x in content ]
# first line is the class name
# other lines are the types and labels
table = Table()
table.table_name = content[ 0 ]
for line in content[ 1: ]:
    if line is '':
        continue
    split_line = line.split()
    table.var_types.append( split_line[ 0 ] )
    table.var_names.append( split_line[ 1 ] )

table.output()

table.buildStruct( outputStructFile )
table.buildJava( outputJavaFile )
table.buildDef( outputDefFile )
table.buildImpl( outputImplFile )
table.buildTypes( outputTypeFile )
table.buildConstDef( outputConstDefFile )
table.buildConstImpl( outputConstImplFile )
