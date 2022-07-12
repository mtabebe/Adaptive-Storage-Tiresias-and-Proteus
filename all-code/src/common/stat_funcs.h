#pragma once

#include "../gen-cpp/gen-cpp/proto_types.h"

cell_widths_stats create_cell_widths_stats( int64_t count,
                                            double  running_sizes );
selectivity_stats create_selectivity_stats( int64_t count,
                                            double  running_selectivity );

