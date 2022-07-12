#pragma once

#include "../../common/hw.h"
#include "../../data-site/db/db.h"
#include "../../data-site/single-site-db/db_abstraction.h"
#include "../../data-site/single-site-db/db_abstraction_types.h"
#include "../benchmark_db_operators.h"
#include "smallbank_record_types.h"
#include "smallbank_table_ids.h"
#include "smallbank_table_sizes.h"
#include "smallbank_workload_generator.h"

void smallbank_create_tables( db_abstraction*          db,
                              const smallbank_configs& configs );
void smallbank_create_tables( db* database, const smallbank_configs& configs );

