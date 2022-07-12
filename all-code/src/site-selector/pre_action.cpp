#include "pre_action.h"

#include <glog/logging.h>

#include "../data-site/db/partition.h"
std::string pre_action_type_to_string( const pre_action_type& type ) {
    switch( type ) {
        case NONE:
            return "NONE";
        case REMASTER:
            return "REMASTER";
        case ADD_PARTITIONS:
            return "ADD_PARTITIONS";
        case ADD_REPLICA_PARTITIONS:
            return "ADD_REPLICA_PARTITIONS";
        case REMOVE_PARTITIONS:
            return "REMOVE_PARTITIONS";
        case SPLIT_PARTITION:
            return "SPLIT_PARTITION";
        case MERGE_PARTITION:
            return "MERGE_PARTITION";
        case CHANGE_OUTPUT_DESTINATION:
            return "CHANGE_OUTPUT_DESTINATION";
        case TRANSFER_REMASTER:
            return "TRANSFER_REMASTER";
        default:
            return "UNKNOWN ACTION TYPE";
    }
    return "UNKNOWN ACTION TYPE";
}

pre_action::pre_action(
    /*
    int site, pre_action_type type, void* args,
    const std::vector<std::shared_ptr<pre_action>>& dependencies */ )
    : id_( 0 ),
      site_( -1 ),
      type_( pre_action_type::NONE ),
      args_( nullptr ),
      cost_( 0 ),
      model_prediction_(),
      relevant_pids_(),
      dependencies_(),
      /*
        site_( site ),
        type_( type ),
        args_( args ),
        dependencies_( dependencies ),
        */
      mutex_(),
      cv_(),
      complete_( false ),
      outcome_( action_outcome::OK ) {}
pre_action::~pre_action() {
    if( args_ == nullptr ) {
        return;
    }
    switch( type_ ) {
        case NONE: {
            break;
        }
        case REMASTER: {
            remaster_partition_pre_action* remaster_args =
                (remaster_partition_pre_action*) args_;
            delete remaster_args;
            break;
        }
        case ADD_PARTITIONS: {
            add_partition_pre_action* add_args =
                (add_partition_pre_action*) args_;
            delete add_args;
            break;
        }
        case ADD_REPLICA_PARTITIONS: {
            add_replica_partition_pre_action* add_args =
                (add_replica_partition_pre_action*) args_;
            delete add_args;
            break;
        }
        case REMOVE_PARTITIONS: {
            break;
        }
        case SPLIT_PARTITION: {
            split_partition_pre_action* split_args =
                (split_partition_pre_action*) args_;
            delete split_args;
            break;
        }
        case MERGE_PARTITION: {
            merge_partition_pre_action* merge_args =
                (merge_partition_pre_action*) args_;
            delete merge_args;
            break;
        }
        case CHANGE_OUTPUT_DESTINATION: {
            change_output_destination_action* change_args =
                (change_output_destination_action*) args_;
            delete change_args;
            break;
        }
        case CHANGE_PARTITION_TYPE: {
            change_partition_type_action* change_args =
                (change_partition_type_action*) args_;
            delete change_args;
            break;
        }
        case TRANSFER_REMASTER: {
            transfer_remaster_action* transfer_args =
                (transfer_remaster_action*) args_;
            delete transfer_args;
            break;
        }
    }
    args_ = nullptr;
}

void pre_action::init(
    int site, pre_action_type type, void* args, double cost,
    const cost_model_prediction_holder&             model_prediction,
    const std::vector<partition_column_identifier>& relevant_pids,
    const std::vector<std::shared_ptr<pre_action>>& dependencies ) {

    DVLOG( 40 ) << "Initializing action:" << pre_action_type_to_string( type )
                << ", site:" << site << ", cost:" << cost
                << ", upfront cost prediction:"
                << model_prediction.get_prediction();

    site_ = site;
    type_ = type;
    args_ = args;
    cost_ = cost;
    model_prediction_ = model_prediction;
    relevant_pids_ = relevant_pids;
    dependencies_ = dependencies;
}

void pre_action::mark_complete( const action_outcome& outcome ) {
    {
        std::lock_guard<std::mutex> guard( mutex_ );
        if( !complete_ ) {
            DVLOG( 25 ) << "Marking:" << id_
                        << " complete, outcome:" << outcome;
            outcome_ = outcome;
            complete_ = true;
        }
    }
    cv_.notify_all();
}

action_outcome pre_action::wait_to_satisfy_dependencies() {
    DVLOG( 25 ) << "Waiting to satisfy dependencies:" << id_;
    action_outcome outcome = action_outcome::OK;
    for( auto dep : dependencies_ ) {
        outcome = dep->wait_for_completion_or_failure();
        if( outcome == action_outcome::FAILED ) {
            break;
        }
    }
    if( outcome == action_outcome::FAILED ) {
        // cooperatively set
        if( outcome_ != outcome ) {
            mark_complete( outcome );
        }
    }
    return outcome;
}

action_outcome pre_action::wait_for_completion_or_failure() {
    DVLOG( 25 ) << "Waiting for completion of:" << id_;
    if( complete_ ) {
        return outcome_;
    }
    {
        std::unique_lock<std::mutex> guard( mutex_ );
        while( !complete_ ) {
            cv_.wait( guard );
        }
    }
    return outcome_;
}

add_partition_pre_action::add_partition_pre_action(
    int                                                    master_site,
    const std::vector<std::shared_ptr<partition_payload>>& insert_partitions,
    const std::vector<partition_type::type>&               partition_types,
    const std::vector<storage_tier_type::type>&            storage_types,
    uint32_t update_destination_slot )
    : master_site_( master_site ),
      partitions_( insert_partitions ),
      partition_ids_( payloads_to_identifiers( partitions_ ) ),
      partition_types_( partition_types ),
      storage_types_( storage_types ),
      update_destination_slot_( update_destination_slot ) {}
add_partition_pre_action::add_partition_pre_action(
    int master_site, const std::vector<partition_column_identifier>& pids,
    const std::vector<partition_type::type>&    partition_types,
    const std::vector<storage_tier_type::type>& storage_types,
    uint32_t                                    update_destination_slot )
    : master_site_( master_site ),
      partitions_(),
      partition_ids_( pids ),
      partition_types_( partition_types ),
      storage_types_( storage_types ),
      update_destination_slot_( update_destination_slot ) {}

add_replica_partition_pre_action::add_replica_partition_pre_action(
    int                                                    master_site,
    const std::vector<std::shared_ptr<partition_payload>>& insert_partitions,
    const std::vector<partition_type::type>&               partition_types,
    const std::vector<storage_tier_type::type>&            storage_types )
    : master_site_( master_site ),
      partitions_( insert_partitions ),
      partition_ids_( payloads_to_identifiers( insert_partitions ) ),
      partition_types_( partition_types ),
      storage_types_( storage_types ) {}
add_replica_partition_pre_action::add_replica_partition_pre_action(
    int master_site, const std::vector<partition_column_identifier>& pids,
    const std::vector<partition_type::type>&    partition_types,
    const std::vector<storage_tier_type::type>& storage_types )
    : master_site_( master_site ),
      partitions_(),
      partition_ids_( pids ),
      partition_types_( partition_types ),
      storage_types_( storage_types ) {}

remaster_partition_pre_action::remaster_partition_pre_action(
    int old_master, int new_master,
    const std::vector<std::shared_ptr<partition_payload>>& remaster_partitions,
    const std::vector<uint32_t>& update_destination_slots )
    : old_master_( old_master ),
      new_master_( new_master ),
      partitions_( remaster_partitions ),
      partition_ids_( payloads_to_identifiers( remaster_partitions ) ),
      update_destination_slots_( update_destination_slots ) {}
remaster_partition_pre_action::remaster_partition_pre_action(
    int old_master, int new_master,
    const std::vector<partition_column_identifier>& pids,
    const std::vector<uint32_t>&                    update_destination_slots )
    : old_master_( old_master ),
      new_master_( new_master ),
      partitions_(),
      partition_ids_( pids ),
      update_destination_slots_( update_destination_slots ) {}

split_partition_pre_action::split_partition_pre_action(
    std::shared_ptr<partition_payload> partition, const cell_key& split_point,
    const partition_type::type& low_type, const partition_type::type& high_type,
    const storage_tier_type::type& low_storage_type,
    const storage_tier_type::type& high_storage_type,
    uint32_t                       low_update_destination_slot,
    uint32_t                       high_update_destination_slot )
    : partition_( partition ),
      partition_id_( partition->identifier_ ),
      split_point_( split_point ),
      low_type_( low_type ),
      high_type_( high_type ),
      low_storage_type_( low_storage_type ),
      high_storage_type_( high_storage_type ),
      low_update_destination_slot_( low_update_destination_slot ),
      high_update_destination_slot_( high_update_destination_slot ) {}
split_partition_pre_action::split_partition_pre_action(
    const partition_column_identifier& partition_id,
    const cell_key& split_point, const partition_type::type& low_type,
    const partition_type::type&    high_type,
    const storage_tier_type::type& low_storage_type,
    const storage_tier_type::type& high_storage_type,
    uint32_t                       low_update_destination_slot,
    uint32_t                       high_update_destination_slot )
    : partition_( nullptr ),
      partition_id_( partition_id ),
      split_point_( split_point ),
      low_type_( low_type ),
      high_type_( high_type ),
      low_storage_type_( low_storage_type ),
      high_storage_type_( high_storage_type ),
      low_update_destination_slot_( low_update_destination_slot ),
      high_update_destination_slot_( high_update_destination_slot ) {}

merge_partition_pre_action::merge_partition_pre_action(
    const partition_column_identifier& left_pid,
    const partition_column_identifier& right_pid,
    const partition_type::type&        merge_type,
    const storage_tier_type::type&     merge_storage_type,
    uint32_t                           update_destination_slot )
    : left_pid_( left_pid ),
      right_pid_( right_pid ),
      merge_type_( merge_type ),
      merge_storage_type_( merge_storage_type ),
      update_destination_slot_( update_destination_slot ) {}

change_output_destination_action::change_output_destination_action(
    const std::vector<partition_column_identifier>& pids,
    uint32_t                                        update_destination_slot )
    : partitions_(),
      partition_ids_( pids ),
      update_destination_slot_( update_destination_slot ) {}
change_output_destination_action::change_output_destination_action(
    std::vector<std::shared_ptr<partition_payload>> partitions,
    uint32_t                                        update_destination_slot )
    : partitions_( partitions ),
      partition_ids_( payloads_to_identifiers( partitions_ ) ),
      update_destination_slot_( update_destination_slot ) {}

remove_partition_action::remove_partition_action(
    const std::vector<partition_column_identifier>& pids )
    : partitions_(), partition_ids_( pids ) {}
remove_partition_action::remove_partition_action(
    std::vector<std::shared_ptr<partition_payload>> partitions )
    : partitions_( partitions ),
      partition_ids_( payloads_to_identifiers( partitions ) ) {}

change_partition_type_action::change_partition_type_action(
    const std::vector<partition_column_identifier>& pids,
    const std::vector<partition_type::type>&        part_types,
    const std::vector<storage_tier_type::type>&     storage_types )
    : partitions_(),
      partition_ids_( pids ),
      partition_types_( part_types ),
      storage_types_( storage_types ) {}
change_partition_type_action::change_partition_type_action(
    const std::vector<std::shared_ptr<partition_payload>>& payloads,
    const std::vector<partition_type::type>&               part_types,
    const std::vector<storage_tier_type::type>&            storage_types )
    : partitions_( payloads ),
      partition_ids_( payloads_to_identifiers( payloads ) ),
      partition_types_( part_types ),
      storage_types_( storage_types ) {}

transfer_remaster_action::transfer_remaster_action(
    int old_master, int new_master,
    const std::vector<partition_column_identifier>&        pids,
    const std::vector<std::shared_ptr<partition_payload>>& partitions,
    const std::vector<partition_type::type>&               partition_types,
    const std::vector<storage_tier_type::type>&            storage_types )
    : old_master_( old_master ),
      new_master_( new_master ),
      partitions_( partitions ),
      partition_ids_( pids ),
      partition_types_( partition_types ),
      storage_types_( storage_types ) {}

std::shared_ptr<pre_action> create_add_partition_pre_action(
    int destination, int master_site,
    const std::vector<std::shared_ptr<partition_payload>>& insert_partitions,
    const std::vector<partition_type::type>&               partition_types,
    const std::vector<storage_tier_type::type>&            storage_types,
    uint32_t update_destination_slot ) {
    DCHECK_EQ( destination, master_site );
    add_partition_pre_action* add_action = new add_partition_pre_action(
        master_site, insert_partitions, partition_types, storage_types,
        update_destination_slot );
    cost_model_prediction_holder model_prediction;
    auto                         action = std::make_shared<pre_action>();
    action->init( destination, pre_action_type::ADD_PARTITIONS,
                  (void*) add_action, 0, model_prediction,
                  payloads_to_identifiers( insert_partitions ), {} );
    return action;
}

std::shared_ptr<pre_action> create_add_partition_pre_action(
    int destination, int master_site,
    const std::vector<partition_column_identifier>& add_pids,
    const std::vector<partition_type::type>&        partition_types,
    const std::vector<storage_tier_type::type>&     storage_types,
    const std::vector<std::shared_ptr<pre_action>>& deps, double cost,
    cost_model_prediction_holder& model_prediction,
    uint32_t                      update_destination_slot ) {
    add_partition_pre_action* add_action =
        new add_partition_pre_action( master_site, add_pids, partition_types,
                                      storage_types, update_destination_slot );
    auto action = std::make_shared<pre_action>();
    action->init( destination, pre_action_type::ADD_PARTITIONS,
                  (void*) add_action, cost, model_prediction, add_pids, deps );

    return action;
}

std::shared_ptr<pre_action> create_add_replica_partition_pre_action(
    int destination, int master_site,
    const std::vector<std::shared_ptr<partition_payload>>& insert_partitions,
    const std::vector<partition_type::type>&               partition_types,
    const std::vector<storage_tier_type::type>& storage_types, double cost,
    const cost_model_prediction_holder& model_prediction ) {
    add_replica_partition_pre_action* add_action =
        new add_replica_partition_pre_action( master_site, insert_partitions,
                                              partition_types, storage_types );
    auto action = std::make_shared<pre_action>();
    action->init( destination, pre_action_type::ADD_REPLICA_PARTITIONS,
                  (void*) add_action, cost, model_prediction,
                  payloads_to_identifiers( insert_partitions ), {} );
    return action;
}

std::shared_ptr<pre_action> create_add_replica_partition_pre_action(
    int destination, int master_site,
    const std::vector<partition_column_identifier>& add_pids,
    const std::vector<partition_type::type>&        partition_types,
    const std::vector<storage_tier_type::type>&     storage_types,
    const std::vector<std::shared_ptr<pre_action>>& deps, double cost,
    const cost_model_prediction_holder& model_prediction ) {
    add_replica_partition_pre_action* add_action =
        new add_replica_partition_pre_action( master_site, add_pids,
                                              partition_types, storage_types );
    auto action = std::make_shared<pre_action>();
    action->init( destination, pre_action_type::ADD_REPLICA_PARTITIONS,
                  (void*) add_action, cost, model_prediction, add_pids, deps );

    return action;
}

std::shared_ptr<pre_action> create_remaster_partition_pre_action(
    int old_master, int new_master,
    const std::vector<std::shared_ptr<partition_payload>>& remaster_partitions,
    double cost, const cost_model_prediction_holder& model_prediction,
    const std::vector<uint32_t> update_destination_slots ) {
    DCHECK_EQ( remaster_partitions.size(), update_destination_slots.size() );

    remaster_partition_pre_action* remaster_action =
        new remaster_partition_pre_action( old_master, new_master,
                                           remaster_partitions,
                                           update_destination_slots );
    auto action = std::make_shared<pre_action>();
    action->init( new_master, pre_action_type::REMASTER,
                  (void*) remaster_action, cost, model_prediction,
                  payloads_to_identifiers( remaster_partitions ), {} );

    return action;
}
std::shared_ptr<pre_action> create_remaster_partition_pre_action(
    int old_master, int new_master,
    const std::vector<partition_column_identifier>& remaster_pids,
    const std::vector<std::shared_ptr<pre_action>>& deps, double op_score,
    const cost_model_prediction_holder& model_prediction,
    const std::vector<uint32_t>         update_destination_slots ) {
    DCHECK_EQ( remaster_pids.size(), update_destination_slots.size() );

    remaster_partition_pre_action* remaster_action =
        new remaster_partition_pre_action(
            old_master, new_master, remaster_pids, update_destination_slots );
    auto action = std::make_shared<pre_action>();
    action->init( new_master, pre_action_type::REMASTER,
                  (void*) remaster_action, op_score, model_prediction,
                  remaster_pids, deps );

    return action;
}

std::shared_ptr<pre_action> create_split_partition_pre_action(
    int master_site, std::shared_ptr<partition_payload> partition,
    const cell_key& split_point, double cost,
    const cost_model_prediction_holder& model_prediction,
    const partition_type::type& low_type, const partition_type::type& high_type,
    const storage_tier_type::type& low_storage_type,
    const storage_tier_type::type& high_storage_type,
    uint32_t                       low_update_destination_slot,
    uint32_t                       high_update_destination_slot ) {
    split_partition_pre_action* split_action = new split_partition_pre_action(
        partition, split_point, low_type, high_type, low_storage_type,
        high_storage_type, low_update_destination_slot,
        high_update_destination_slot );
    auto action = std::make_shared<pre_action>();

    std::vector<partition_column_identifier> pids;
    auto split_pids = construct_split_partition_column_identifiers(
        partition->identifier_, split_point.row_id, split_point.col_id );
    pids.emplace_back( partition->identifier_ );
    pids.emplace_back( std::get<0>( split_pids ) );
    pids.emplace_back( std::get<1>( split_pids ) );

    action->init( master_site, pre_action_type::SPLIT_PARTITION,
                  (void*) split_action, cost, model_prediction, pids, {} );

    return action;
}
std::shared_ptr<pre_action> create_split_partition_pre_action(
    int master_site, const partition_column_identifier& pid,
    const cell_key& split_point, const partition_type::type& low_type,
    const partition_type::type&                     high_type,
    const storage_tier_type::type&                  low_storage_type,
    const storage_tier_type::type&                  high_storage_type,
    const std::vector<std::shared_ptr<pre_action>>& deps, double cost,
    const cost_model_prediction_holder& model_prediction,
    uint32_t                            low_update_destination_slot,
    uint32_t                            high_update_destination_slot ) {
    split_partition_pre_action* split_action = new split_partition_pre_action(
        pid, split_point, low_type, high_type, low_storage_type,
        high_storage_type, low_update_destination_slot,
        high_update_destination_slot );
    auto action = std::make_shared<pre_action>();

    std::vector<partition_column_identifier> pids;
    auto split_pids = construct_split_partition_column_identifiers(
        pid, split_point.row_id, split_point.col_id );
    pids.emplace_back( pid );
    pids.emplace_back( std::get<0>( split_pids ) );
    pids.emplace_back( std::get<1>( split_pids ) );

    action->init( master_site, pre_action_type::SPLIT_PARTITION,
                  (void*) split_action, cost, model_prediction, pids, deps );

    return action;
}

std::shared_ptr<pre_action> create_merge_partition_pre_action(
    int master_site, const partition_column_identifier& left_pid,
    const partition_column_identifier&              right_pid,
    const partition_column_identifier&              merged_pid,
    const partition_type::type&                     merge_type,
    const storage_tier_type::type&                  merge_storage_type,
    const std::vector<std::shared_ptr<pre_action>>& deps, double cost,
    const cost_model_prediction_holder& model_prediction,
    uint32_t                            update_destination_slot ) {
    auto action = std::make_shared<pre_action>();
    std::vector<partition_column_identifier> pids;
    pids.emplace_back( left_pid );
    pids.emplace_back( right_pid );
    pids.emplace_back( merged_pid );
    merge_partition_pre_action* merge_action = new merge_partition_pre_action(
        left_pid, right_pid, merge_type, merge_storage_type,
        update_destination_slot );

    action->init( master_site, pre_action_type::MERGE_PARTITION,
                  (void*) merge_action, cost, model_prediction, pids, deps );

    return action;
}

std::shared_ptr<pre_action> create_change_output_destination_action(
    int master_site, const std::vector<partition_column_identifier>& pids,
    const std::vector<std::shared_ptr<pre_action>>& deps, double cost,
    cost_model_prediction_holder& model_prediction,
    uint32_t                      update_destination_slot ) {
    auto                              action = std::make_shared<pre_action>();
    change_output_destination_action* change_action =
        new change_output_destination_action( pids, update_destination_slot );
    action->init( master_site, pre_action_type::CHANGE_OUTPUT_DESTINATION,
                  (void*) change_action, cost, model_prediction, pids, deps );

    return action;
}
std::shared_ptr<pre_action> create_change_output_destination_action(
    int                                                    master_site,
    const std::vector<std::shared_ptr<partition_payload>>& partitions,
    const std::vector<std::shared_ptr<pre_action>>& deps, double cost,
    const cost_model_prediction_holder& model_prediction,
    uint32_t                            update_destination_slot ) {
    change_output_destination_action* change_action =
        new change_output_destination_action( partitions,
                                              update_destination_slot );
    auto action = std::make_shared<pre_action>();
    action->init( master_site, pre_action_type::CHANGE_OUTPUT_DESTINATION,
                  (void*) change_action, cost, model_prediction,
                  payloads_to_identifiers( partitions ), deps );

    return action;
}

std::shared_ptr<pre_action> create_remove_partition_action(
    int site, const std::vector<std::shared_ptr<partition_payload>>& partitions,
    const std::vector<std::shared_ptr<pre_action>>& deps, double cost,
    const cost_model_prediction_holder& model_prediction ) {
    remove_partition_action* remove_action =
        new remove_partition_action( partitions );
    auto action = std::make_shared<pre_action>();
    action->init( site, pre_action_type::REMOVE_PARTITIONS,
                  (void*) remove_action, cost, model_prediction,
                  payloads_to_identifiers( partitions ), deps );

    return action;
}
std::shared_ptr<pre_action> create_remove_partition_action(
    int site, const std::vector<partition_column_identifier>& pids,
    const std::vector<std::shared_ptr<pre_action>>& deps, double cost,
    const cost_model_prediction_holder& model_prediction ) {
    remove_partition_action* remove_action =
        new remove_partition_action( pids );
    auto action = std::make_shared<pre_action>();
    action->init( site, pre_action_type::REMOVE_PARTITIONS,
                  (void*) remove_action, cost, model_prediction, pids, deps );

    return action;
}

std::shared_ptr<pre_action> create_transfer_remaster_partition_action(
    int old_master, int new_master, const partition_column_identifier& pid,
    std::shared_ptr<partition_payload>              payload,
    const std::vector<partition_type::type>&        partition_types,
    const std::vector<storage_tier_type::type>&     storage_types,
    const std::vector<std::shared_ptr<pre_action>>& deps ) {
    std::vector<partition_column_identifier> pids;
    pids.push_back( pid );
    std::vector<std::shared_ptr<partition_payload>> payloads;
    payloads.push_back( payload );

    return create_transfer_remaster_partitions_action(
        old_master, new_master, pids, payloads, partition_types, storage_types,
        deps );
}
std::shared_ptr<pre_action> create_transfer_remaster_partitions_action(
    int old_master, int new_master,
    const std::vector<partition_column_identifier>&        pids,
    const std::vector<std::shared_ptr<partition_payload>>& payloads,
    const std::vector<partition_type::type>&               partition_types,
    const std::vector<storage_tier_type::type>&            storage_types,
    const std::vector<std::shared_ptr<pre_action>>&        deps ) {
    transfer_remaster_action* transfer_action =
        new transfer_remaster_action( old_master, new_master, pids, payloads,
                                      partition_types, storage_types );
    auto action = std::make_shared<pre_action>();

    cost_model_prediction_holder model_prediction;

    action->init( new_master, pre_action_type::TRANSFER_REMASTER,
                  (void*) transfer_action, 0 /*cost*/, model_prediction, pids,
                  deps );

    return action;
}

std::shared_ptr<pre_action> create_change_partition_type_action(
    int site, const std::vector<partition_column_identifier>& pids,
    const std::vector<partition_type::type>&    part_types,
    const std::vector<storage_tier_type::type>& storage_types, double cost,
    const cost_model_prediction_holder&             model_prediction,
    const std::vector<std::shared_ptr<pre_action>>& deps ) {

    change_partition_type_action* change_action =
        new change_partition_type_action( pids, part_types, storage_types );
    auto action = std::make_shared<pre_action>();


    action->init( site, pre_action_type::CHANGE_PARTITION_TYPE,
                  (void*) change_action, cost, model_prediction, pids, deps );

    return action;
}
std::shared_ptr<pre_action> create_change_partition_type_action(
    int site, const std::vector<std::shared_ptr<partition_payload>>& payloads,
    const std::vector<partition_type::type>& part_types,
    const std::vector<storage_tier_type::type>& storage_types,
    double cost,
    const cost_model_prediction_holder&             model_prediction,
    const std::vector<std::shared_ptr<pre_action>>& deps ) {

    change_partition_type_action* change_action =
        new change_partition_type_action( payloads, part_types, storage_types );
    auto action = std::make_shared<pre_action>();

    action->init( site, pre_action_type::CHANGE_PARTITION_TYPE,
                  (void*) change_action, cost, model_prediction,
                  payloads_to_identifiers( payloads ), deps );

    return action;
}


