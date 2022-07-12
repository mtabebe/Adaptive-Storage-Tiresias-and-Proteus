#include "pre_transaction_plan.h"

#include <glog/logging.h>
#include <numeric>

#include "../common/partition_funcs.h"

pre_transaction_plan::pre_transaction_plan(
    std::shared_ptr<stat_tracked_enumerator_decision_holder>
        stat_decision_holder )
    : destination_( -1 ),
      write_pids_(),
      read_pids_(),
      inflight_pids_(),
      svv_(),
      estimated_number_of_updates_to_wait_for_( 0 ),
      txn_stats_(),
      mq_plan_( nullptr ),
      stat_decision_holder_( stat_decision_holder ),
      cost_( 0 ),
      actions_(),
      work_queue_(),
      background_work_items_(),
      plan_outcome_( action_outcome::OK ),
      edges_(),
      edge_counts_(),
      actions_with_no_edges_(),
      partition_column_identifier_to_payload_(),
      mapping_mutex_(),
      mutex_() {}

void pre_transaction_plan::build_work_queue() {
    // read and write sets have the latest unlock time
    for( auto pid : write_pids_ ) {
        DVLOG( 40 ) << "Setting partition_column_identifier_to_unlock_state_["
                    << pid << "] to :" << (int32_t) actions_.size();
        partition_column_identifier_to_unlock_state_[pid] =
            (int32_t) actions_.size();
    }
    for( auto pid : read_pids_ ) {
        DVLOG( 40 ) << "Setting partition_column_identifier_to_unlock_state_["
                    << pid << "] to :" << (int32_t) actions_.size();
        partition_column_identifier_to_unlock_state_[pid] =
            (int32_t) actions_.size();
    }

    if( actions_.size() == 0 ) {
        return;
    }

    for( auto action : actions_ ) {
        edges_.emplace_back();
    }
    for( auto action : actions_ ) {
        for( auto dep : action->dependencies_ ) {
            edges_.at( dep->id_ ).emplace( action->id_ );
        }
    }

    // topological sort
    DCHECK_NE( actions_with_no_edges_.size(), 0 );

    while( !actions_with_no_edges_.empty() ) {
        uint32_t id = actions_with_no_edges_.front();
        actions_with_no_edges_.pop();
        work_queue_.push( id );

        std::unordered_set<uint32_t> children = edges_.at( id );
        for( uint32_t child : children ) {
            // edge count > 0 --> still have edges
            if( edge_counts_[child] > 0 ) {
                edge_counts_[child] = edge_counts_[child] - 1;
                // if edge count is 0, then that means no more incoming edges
                if( edge_counts_[child] == 0 ) {
                    actions_with_no_edges_.push( child );
                }
            }
        }
    }
    DCHECK_EQ( 0,
               std::accumulate( edge_counts_.begin(), edge_counts_.end(), 0 ) );
    DCHECK_EQ( work_queue_.size(), actions_.size() );
}

void pre_transaction_plan::unlock_background_partitions() {
    if( background_work_items_.size() == 0 ) {
        return;
    }
    for( auto& entry : partition_column_identifier_to_unlock_state_ ) {
        if( entry.second == -2 ) {
            auto pid = entry.first;
            DVLOG( 40 ) << "Background partition:" << pid
                        << ", will try to unlock";
            auto part = get_payload( pid, false /* don't assert it exists */ );
            if( part ) {
                DVLOG( 40 ) << "Unlocking background partition:" << pid;
                part->unlock();
            }
            entry.second = -1;
        }
    }
}

std::shared_ptr<pre_action>
    pre_transaction_plan::get_next_pre_action_work_item() {
    std::shared_ptr<pre_action> action = nullptr;
    {
        std::lock_guard<std::mutex> guard( mutex_ );
        if( !work_queue_.empty() ) {
            uint32_t id = work_queue_.front();
            work_queue_.pop();
            action = actions_[id];
        }
    }
    return action;
}

void pre_transaction_plan::add_pre_action_work_item(
    std::shared_ptr<pre_action> action ) {
    cost_ = cost_ + action->cost_;

    action->id_ = actions_.size();
    actions_.push_back( action );

    if( action->dependencies_.empty() ) {
        actions_with_no_edges_.push( action->id_ );
    }

    DVLOG( 20 ) << "Adding pre_action:" << action->id_
                << ", action:" << action->type_
                << ", relevant pids:" << action->relevant_pids_;

    std::unordered_set<uint32_t> dep_ids;
    for( auto dep : action->dependencies_ ) {
        dep_ids.emplace( dep->id_ );
        DVLOG( 40 ) << "Action:" << action->id_ << ", depends on:" << dep->id_;
    }

    edge_counts_.push_back( action->dependencies_.size() );

    for( auto pid : action->relevant_pids_ ) {
        auto found = partition_column_identifier_to_unlock_state_.find( pid );
        if( found != partition_column_identifier_to_unlock_state_.end() ) {
            if( found->second >= 0 ) {
                DVLOG( 40 ) << "Dep pid:" << pid
                            << " cur action:" << action->id_
                            << ", prev action:" << found->second;
                if( dep_ids.count( found->second ) != 1 ) {
                    DLOG( ERROR ) << "Didn't find dependency for pid:" << pid
                                  << ", id:" << found->second
                                  << ", dep_ids:" << dep_ids;
                }
                DCHECK_EQ( 1, dep_ids.count( found->second ) );
            }
        } else {
            DVLOG( 40 ) << "Dep pid:" << pid << " cur action:" << action->id_;
        }
        DVLOG( 40 ) << "Setting partition_column_identifier_to_unlock_state_["
                    << pid << "] to :" << action->id_;
        partition_column_identifier_to_unlock_state_[pid] =
            (int32_t) action->id_;
    }
}

void pre_transaction_plan::add_background_work_item(
    std::shared_ptr<pre_action> action ) {
    background_work_items_.push_back( action );

    DVLOG( 20 ) << "Adding background work item:" << action->id_
                << ", action:" << action->type_
                << ", relevant pids:" << action->relevant_pids_;

    for( auto pid : action->relevant_pids_ ) {
        auto found = partition_column_identifier_to_unlock_state_.find( pid );
        if( found == partition_column_identifier_to_unlock_state_.end() ) {
            // if it's not already in there, unlock it early
            // -2 means unlock at first action
            DVLOG( 40 )
                << "Setting partition_column_identifier_to_unlock_state_["
                << pid << "] to :" << -2;
            partition_column_identifier_to_unlock_state_[pid] = -2;
        }
    }
}

const std::vector<std::shared_ptr<pre_action>>&
    pre_transaction_plan::get_background_work_items() const {
    return background_work_items_;
}

uint32_t pre_transaction_plan::get_num_work_items() const {
    return work_queue_.size();
}

std::shared_ptr<partition_payload> pre_transaction_plan::get_partition(
    std::shared_ptr<partition_payload> payload,
    const partition_column_identifier& pid, bool assert_exists ) {
    if( payload != nullptr ) {
        return payload;
    }

    std::shared_ptr<partition_payload> found_payload = nullptr;
    {
        std::shared_lock<std::shared_timed_mutex> guard( mapping_mutex_ );
        found_payload = get_payload( pid, assert_exists );
    }
    return found_payload;
}
std::vector<std::shared_ptr<partition_payload>>
    pre_transaction_plan::get_partitions(
        const std::vector<std::shared_ptr<partition_payload>>& payloads,
        const std::vector<partition_column_identifier>&        pids ) {
    if( ( payloads.size() > 0 ) or ( pids.size() == 0 ) ) {
        return payloads;
    }
    std::vector<std::shared_ptr<partition_payload>> found_payloads;
    {
        std::shared_lock<std::shared_timed_mutex> guard( mapping_mutex_ );
        for( const auto& pid : pids ) {
            found_payloads.push_back( get_payload( pid, true ) );
        }
    }
    return found_payloads;
}

void pre_transaction_plan::add_to_partition_mappings(
    const std::vector<std::shared_ptr<partition_payload>>& payloads ) {
    {
        std::unique_lock<std::shared_timed_mutex> guard( mapping_mutex_ );
        for( auto& payload : payloads ) {
            add_payload( payload );
        }
    }
}
void pre_transaction_plan::add_to_partition_mappings(
    std::shared_ptr<partition_payload> payload ) {
    {
        std::unique_lock<std::shared_timed_mutex> guard( mapping_mutex_ );
        add_payload( payload );
    }
}

void pre_transaction_plan::add_to_location_information_mappings(
    partition_column_identifier_map_t<std::shared_ptr<
        partition_location_information>>&& partition_location_informations ) {
    (void) partition_location_informations;  // MV-TODO
}

void pre_transaction_plan::sort_and_finalize_pids() {
    std::sort( read_pids_.begin(), read_pids_.end() );
    std::sort( write_pids_.begin(), write_pids_.end() );

    auto last = std::unique( read_pids_.begin(), read_pids_.end() );
    read_pids_.erase( last, read_pids_.end() );

    last = std::unique( write_pids_.begin(), write_pids_.end() );
    write_pids_.erase( last, write_pids_.end() );
}

void pre_transaction_plan::unlock_payloads_if_able(
    uint32_t                                         completed_id,
    std::vector<std::shared_ptr<partition_payload>>& payloads,
    partition_lock_mode                              mode ) {
    for( auto payload : payloads ) {
        unlock_payload_if_able( completed_id, payload, mode );
    }
}
void pre_transaction_plan::unlock_payload_if_able(
    uint32_t completed_id, std::shared_ptr<partition_payload> payload,
    partition_lock_mode mode ) {
    DVLOG( 30 ) << "Unlock payload:" << *payload
                << " if able, completed id:" << completed_id;
    auto found = partition_column_identifier_to_unlock_state_.find(
        payload->identifier_ );
    DCHECK( found != partition_column_identifier_to_unlock_state_.end() );
    int32_t unlock_id = found->second;
    DVLOG( 40 ) << "Unlock id:" << unlock_id;
    if( unlock_id == (int32_t) completed_id ) {
        DVLOG( 40 ) << "Able to unlock payload:" << *payload
                    << ", after action id:" << completed_id;
        payload->unlock( mode );

        // store -1 to indicate not to unlock again
        // This is safe even though it is not locked because
        // 1) it is an overwrite, not an insert so there will not be rehashing
        // 2) only 1 thread will have the completed_id that matches it so there
        // are not two concurrent writers
        partition_column_identifier_to_unlock_state_[payload->identifier_] = -1;
    }
}

void pre_transaction_plan::unlock_remaining_payloads(
    partition_lock_mode mode ) {

    DVLOG( 30 ) << "Unlock remaining payloads";

    for( auto& entry : partition_column_identifier_to_unlock_state_ ) {
        auto&   pid = entry.first;
        int32_t finish_time = entry.second;

        DVLOG( 40 ) << "Unlock payload:" << pid
                    << ", finish time:" << finish_time;

        if( finish_time == (int32_t) actions_.size() ) {
            auto payload = get_partition( nullptr, pid );
            DVLOG( 40 ) << "Able to unlock:" << *payload
                        << ", as the plan is complete";
            DCHECK( payload );
            payload->unlock( mode );

            // store -1 to indicate not to unlock again
            partition_column_identifier_to_unlock_state_[pid] = -1;
        }
    }
}

void pre_transaction_plan::unlock_payloads_because_of_failure(
    partition_lock_mode mode ) {

    DVLOG( 30 ) << "Unlock payloads because of failure";

    for( auto& entry : partition_column_identifier_to_unlock_state_ ) {
        auto&   pid = entry.first;
        int32_t finish_time = entry.second;

        DVLOG( 40 ) << "Unlock payload:" << pid
                    << ", finish time:" << finish_time;

        if( finish_time != -1 ) {
            auto payload = get_partition( nullptr, pid, false );
            if( payload ) {
                DVLOG( 40 ) << "Able to unlock:" << *payload
                            << ", as the plan failed";
                DCHECK( payload );
                payload->unlock( mode );
            }

            // store -1 to indicate not to unlock again
            partition_column_identifier_to_unlock_state_[pid] = -1;
        }
    }
}

std::shared_ptr<partition_payload> pre_transaction_plan::get_payload(
    const partition_column_identifier& pid, bool assert_exists ) {
    std::shared_ptr<partition_payload> payload = nullptr;
    auto found = partition_column_identifier_to_payload_.find( pid );
    if( found != partition_column_identifier_to_payload_.end() ) {
        payload = found->second;
    } else if( assert_exists ) {
        for( const auto& entry : partition_column_identifier_to_payload_ ) {
            DLOG( WARNING ) << "Cannot find pid:" << pid
                            << ", in map:" << entry.first;
        }
        LOG( FATAL ) << "Unable to find payload, pid:" << pid;
    }
    return payload;
}
void pre_transaction_plan::add_payload(
    std::shared_ptr<partition_payload> payload ) {
    DVLOG( 40 ) << "Adding payload to mapping:" << payload->identifier_;
    partition_column_identifier_to_payload_.emplace( payload->identifier_,
                                                     payload );
}

void pre_transaction_plan::mark_plan_as_failed() {
    plan_outcome_ = action_outcome::FAILED;
    for( auto& action : actions_ ) {
        action->mark_complete( action_outcome::FAILED );
    }
}
action_outcome pre_transaction_plan::get_plan_outcome() {
    return plan_outcome_;
}

double pre_transaction_plan::get_write_set_contention() {
    double accum_contention = 0.0;
    for( auto w_pid : write_pids_ ) {
        accum_contention += get_partition_contention( w_pid );
    }

    return accum_contention;
}

double pre_transaction_plan::get_partition_contention(
    const partition_column_identifier& pid ) {
    auto part = get_partition( nullptr, pid );
    return part->get_contention();
}

std::vector<transaction_prediction_stats>
    pre_transaction_plan::build_and_get_transaction_prediction_stats(
        const std::vector<::cell_key_ranges>& sorted_ckr_write_set,
        const std::vector<::cell_key_ranges>& sorted_ckr_read_set,
        const site_selector_query_stats*      query_stats,
        std::shared_ptr<cost_modeller2>&      cost_model2 ) {
    txn_stats_.clear();

    DCHECK( std::is_sorted( sorted_ckr_write_set.begin(),
                            sorted_ckr_write_set.end() ) );
    DCHECK( std::is_sorted( sorted_ckr_read_set.begin(),
                            sorted_ckr_read_set.end() ) );

    std::vector<::partition_column_identifier> pids;
    pids.insert( pids.begin(), write_pids_.begin(), write_pids_.end() );
    pids.insert( pids.begin(), read_pids_.begin(), read_pids_.end() );
    pids.insert( pids.begin(), inflight_pids_.begin(), inflight_pids_.end() );

    std::sort( pids.begin(), pids.end() );

    uint32_t write_pos = 0;
    uint32_t read_pos = 0;

    uint32_t min_pid_pos =0;

    partition_column_identifier_map_t<transaction_prediction_stats>
        pid_to_transaction_pred_stats;
    cached_ss_stat_holder cached_stats;

    while( ( write_pos < sorted_ckr_write_set.size() ) or
           ( read_pos < sorted_ckr_read_set.size() ) ) {

        bool              is_write = false;
        bool              is_read = false;
        ::cell_key_ranges ckr;

        // find the ckr

        if( ( write_pos < sorted_ckr_write_set.size() ) and
            ( read_pos < sorted_ckr_read_set.size() ) ) {
            if( sorted_ckr_write_set.at( write_pos ) <
                sorted_ckr_read_set.at( read_pos ) ) {
                ckr = sorted_ckr_write_set.at( write_pos );
                is_write = true;
                write_pos += 1;
            } else if( sorted_ckr_write_set.at( write_pos ) ==
                       sorted_ckr_read_set.at( read_pos ) ) {
                is_read = true;
                read_pos += 1;

                is_write = true;
                write_pos += 1;
            } else {
                ckr = sorted_ckr_read_set.at( read_pos );
                is_read = true;
                read_pos += 1;
            }
        } else if( write_pos < sorted_ckr_write_set.size() ) {
            ckr = sorted_ckr_write_set.at( write_pos );
            write_pos += 1;
            is_write = true;
        } else {
            is_read = true;
            ckr = sorted_ckr_read_set.at( read_pos );
            read_pos += 1;
        }

        for( uint32_t pid_pos = min_pid_pos; pid_pos < pids.size();
             pid_pos++ ) {
            if( is_ckr_greater_than_pcid( ckr, pids.at( pid_pos ) ) ) {
                // nothing is less than that
                min_pid_pos = pid_pos;
            } else if( is_ckr_partially_within_pcid( ckr,
                                                     pids.at( pid_pos ) ) ) {
                update_transaction_stats(
                    pids.at( pid_pos ), ckr, is_write, is_read,
                    pid_to_transaction_pred_stats, cached_stats, query_stats,
                    cost_model2 );
            } else {
                break;
            }
        }
    }

    for( const auto& entry : pid_to_transaction_pred_stats ) {
        txn_stats_.emplace_back( entry.second );
    }

    return txn_stats_;
}
void pre_transaction_plan::update_transaction_stats(
    const partition_column_identifier& pid, const cell_key_ranges& ckr,
    bool is_write, bool is_read,
    partition_column_identifier_map_t<transaction_prediction_stats>&
                                     pid_to_transaction_pred_stats,
    cached_ss_stat_holder&           cached_stats,
    const site_selector_query_stats* query_stats,
    std::shared_ptr<cost_modeller2>& cost_model2 ) {

    auto part = get_partition( nullptr, pid, false );
    if( !part ) {
        return;
    }

    auto shaped_ckr = shape_ckr_to_pcid( ckr, pid );
    uint32_t num_cells = get_number_of_cells( shaped_ckr );

    auto found_stats = pid_to_transaction_pred_stats.find( pid );
    if (found_stats != pid_to_transaction_pred_stats.end()) {
        if( is_read ) {
            found_stats->second.is_point_read_ = true;
            found_stats->second.num_point_reads_ += num_cells;
        }
        if( is_write ) {
            found_stats->second.is_point_update_ = true;
            found_stats->second.num_point_updates_ += num_cells;
        }
    } else {
        std::vector<double> widths =
            query_stats->get_and_cache_cell_widths( pid, cached_stats );
        auto part_type = part->get_partition_type( destination_ );
        auto storage_type = part->get_storage_type( destination_ );

        double scan_selectivity = 0;
        double num_updates_needed = 0;

        transaction_prediction_stats txn_stat(
            std::get<1>( part_type ), std::get<1>( storage_type ),
            cost_model2->normalize_contention_by_time( part->get_contention() ),
            num_updates_needed, widths, part->get_num_rows(),
            false /* is scan */, scan_selectivity, is_read, num_cells, is_write,
            num_cells );
        pid_to_transaction_pred_stats[pid] = txn_stat;
    }
}

multi_site_pre_transaction_plan::multi_site_pre_transaction_plan(
    std::shared_ptr<stat_tracked_enumerator_decision_holder>
        stat_decision_holder )
    : scan_arg_sites_(),
      read_pids_(),
      inflight_pids_(),
      per_site_parts_(),
      per_site_part_locs_(),
      plan_( std::make_shared<pre_transaction_plan>( stat_decision_holder ) ),
      txn_stats_(),
      part_costs_(),
      per_part_costs_() {}

