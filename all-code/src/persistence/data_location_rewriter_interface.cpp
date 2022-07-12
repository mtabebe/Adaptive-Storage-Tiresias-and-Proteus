#include "data_location_rewriter_interface.h"

data_location_rewriter_interface::data_location_rewriter_interface(
    const std::vector<table_metadata>& tables_metadata, int num_sites,
    const std::vector<std::vector<propagation_configuration>>& prop_configs,
    const persistence_configs&                                 per_configs,
    std::shared_ptr<update_destination_generator>              update_generator,
    std::shared_ptr<update_enqueuers>                          enqueuers )
    : tables_metadata_( tables_metadata ),
      num_sites_( num_sites ),
      prop_configs_( prop_configs ),
      per_configs_( per_configs ),
      update_generator_( update_generator ),
      enqueuers_( enqueuers ) {}

std::vector<table_metadata>
    data_location_rewriter_interface::get_tables_metadata() const {
    return tables_metadata_;
}
int data_location_rewriter_interface::get_num_sites() const {
    return num_sites_;
}
std::vector<std::vector<propagation_configuration>>
    data_location_rewriter_interface::get_propagation_configurations() const {
    return prop_configs_;
}
std::shared_ptr<update_destination_generator>
    data_location_rewriter_interface::get_update_generator() const {
    return update_generator_;
}
std::shared_ptr<update_enqueuers>
    data_location_rewriter_interface::get_update_enqueuers() const {
    return enqueuers_;
}

persistence_configs data_location_rewriter_interface::get_persistence_configs()
    const {
    return per_configs_;
}
