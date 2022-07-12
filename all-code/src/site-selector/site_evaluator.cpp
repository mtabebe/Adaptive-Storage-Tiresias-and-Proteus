#include "site_evaluator.h"

#include "heuristic_site_evaluator.h"

abstract_site_evaluator* construct_site_evaluator(
    const heuristic_site_evaluator_configs&        configs,
    std::shared_ptr<cost_modeller2>                cost_model2,
    std::shared_ptr<partition_data_location_table> data_loc_tab,
    std::shared_ptr<sites_partition_version_information>
                                                    site_partition_version_info,
    std::shared_ptr<query_arrival_predictor>        query_predictor,
    std::shared_ptr<stat_tracked_enumerator_holder> enumerator_holder,
    std::shared_ptr<partition_tier_tracking>        tier_tracking ) {
    return new heuristic_site_evaluator(
        configs, cost_model2, data_loc_tab, site_partition_version_info,
        query_predictor, enumerator_holder, tier_tracking );
}

