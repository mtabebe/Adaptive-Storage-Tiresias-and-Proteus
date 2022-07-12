#include "stat_funcs.h"

cell_widths_stats create_cell_widths_stats( int64_t count,
                                            double  running_sizes ) {
    cell_widths_stats cws;

    cws.avg_width = 0;
    cws.num_observations = count;
    if( count != 0 ) {
        cws.avg_width = running_sizes / (double) count;
    }

    return cws;
}
selectivity_stats create_selectivity_stats( int64_t count,
                                            double  running_selectivity ) {

    selectivity_stats ss;

    ss.avg_selectivity = 0;
    ss.num_observations = count;
    if( count != 0 ) {
        ss.avg_selectivity = running_selectivity / (double) count;
    }

    return ss;
}

