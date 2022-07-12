#include "smallbank_workload_generator.h"

smallbank_workload_generator::smallbank_workload_generator(
    zipf_distribution_cdf*             z_cdf,
    const workload_operation_selector& op_selector, int32_t client_id,
    const smallbank_configs& configs )
    : dist_( z_cdf ),
      configs_( configs ),
      customer_ids_( {0, 0} ),
      op_selector_( op_selector ),
      client_id_( client_id ) {}

smallbank_workload_generator::smallbank_workload_generator(
    zipf_distribution_cdf* z_cdf, int32_t client_id,
    const smallbank_configs& configs )
    : dist_( z_cdf ),
      configs_( configs ),
      customer_ids_( {0, 0} ),
      op_selector_(),
      client_id_( client_id ) {}

smallbank_workload_generator::~smallbank_workload_generator() {}
