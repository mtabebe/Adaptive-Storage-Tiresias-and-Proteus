#pragma once

inline const std::string& workload_operation_string(
    const workload_operation_enum& op ) {
    return k_workload_operations_to_strings.at( op );
}

inline const std::string& workload_operation_outcome_string(
    const workload_operation_outcome_enum& op ) {
    return k_workload_operation_outcomes_to_strings.at( op );
}
