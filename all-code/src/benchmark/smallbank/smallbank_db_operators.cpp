#include "smallbank_db_operators.h"

#include <glog/logging.h>

void smallbank_create_tables( db_abstraction*          db,
                              const smallbank_configs& configs ) {
    DVLOG( 10 ) << "Creating smallbank tables";
    auto table_infos =
        create_smallbank_table_metadata( configs, db->get_site_location() );
    for( auto m : table_infos ) {
        db->create_table( m );
    }
    DVLOG( 10 ) << "Creating smallbank tables okay!";
}
void smallbank_create_tables( db* database, const smallbank_configs& configs ) {
    DVLOG( 10 ) << "Creating smallbank tables";
    auto table_infos = create_smallbank_table_metadata(
        configs, database->get_site_location() );
    for( auto m : table_infos ) {
        database->get_tables()->create_table( m );
    }
    DVLOG( 10 ) << "Creating smallbank tables okay!";
}
