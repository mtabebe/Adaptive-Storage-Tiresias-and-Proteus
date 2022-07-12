#include "ycsb_workload_generator.h"

ycsb_workload_generator::ycsb_workload_generator(
    zipf_distribution_cdf*             z_cdf,
    const workload_operation_selector& op_selector, const ycsb_configs& configs,
    uint32_t table_id )
    : dist_( z_cdf ),
      op_selector_( op_selector ),
      configs_( configs ),
      table_id_( table_id ) {}
ycsb_workload_generator::~ycsb_workload_generator() {}

predicate_chain ycsb_workload_generator::get_scan_predicate( uint32_t col ) {
    cell_predicate c_pred;
    c_pred.table_id = 0;
    c_pred.col_id = col;
    c_pred.type = data_type::type::STRING;
    c_pred.data = dist_.generate_string_for_selectivity(
        configs_.scan_selectivity_, configs_.value_size_, k_all_chars );
    c_pred.predicate = predicate_type::type::LESS_THAN_OR_EQUAL;

    predicate_chain pred;
    pred.and_predicates.emplace_back( c_pred );
    return pred;
}

