#pragma once

#include "../common/partition_funcs.h"
#include "../data-site/db/partition_metadata.h"
#include "../data-site/update-propagation/update_destination_generator.h"
#include "../data-site/update-propagation/update_enqueuers.h"
#include "../site-selector/partition_payload.h"
#include "../site-selector/ss_types.h"
#include "persistence_configs.h"

typedef std::unordered_map<
    int32_t, partition_column_identifier_map_t<partition_location_information>>
    partition_locations_map;

class data_location_rewriter_interface {
   public:
    data_location_rewriter_interface(
        const std::vector<table_metadata>& tables_metadata, int num_sites,
        const std::vector<std::vector<propagation_configuration>>& prop_configs,
        const persistence_configs&                                 per_configs,
        std::shared_ptr<update_destination_generator> update_generator,
        std::shared_ptr<update_enqueuers>             enqueuers );
    virtual ~data_location_rewriter_interface() {}

    virtual void rewrite_partition_locations(
        partition_locations_map& partition_locations ) = 0;

    std::vector<table_metadata> get_tables_metadata() const;
    int                         get_num_sites() const;
    std::vector<std::vector<propagation_configuration>>
                                                  get_propagation_configurations() const;
    std::shared_ptr<update_destination_generator> get_update_generator() const;
    std::shared_ptr<update_enqueuers>             get_update_enqueuers() const;

    persistence_configs get_persistence_configs() const;

   protected:
    std::vector<table_metadata> tables_metadata_;
    int                         num_sites_;

    std::vector<std::vector<propagation_configuration>> prop_configs_;
    persistence_configs                                 per_configs_;
    std::shared_ptr<update_destination_generator>       update_generator_;
    std::shared_ptr<update_enqueuers>                   enqueuers_;
};
