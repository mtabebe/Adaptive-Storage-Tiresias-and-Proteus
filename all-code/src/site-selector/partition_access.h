#pragma once

#include "../common/partition_funcs.h"
#include <iostream>
#include <unordered_set>
#include <vector>

struct partition_access {

    partition_access( partition_column_identifier &p_id, bool is_write,
                      int site )
        : partition_id_( p_id ), is_write_( is_write ), site_( site ) {}

    partition_access() {}

    partition_column_identifier partition_id_;
    bool                        is_write_;
    int                         site_;

    bool operator==( const partition_access &o ) const {
        if( is_write_ != o.is_write_ ) {
            return false;
        }

        return partition_id_.table_id == o.partition_id_.table_id &&
               partition_id_.partition_start ==
                   o.partition_id_.partition_start &&
               partition_id_.partition_end == o.partition_id_.partition_end;
    }

    friend std::ostream &operator<<( std::ostream &          out,
                                     const partition_access &part_access ) {
        out << "{ pid:";
        out << part_access.partition_id_ << ", site:";
        out << part_access.site_ << ", " << part_access.is_write_ << " }";
        return out;
    }
};

struct partition_access_hasher {
    std::size_t operator()( const partition_access &access ) const {
        std::size_t seed = 0;
        seed = folly::hash::hash_combine( seed, access.is_write_ );
        partition_column_identifier_key_hasher pkh;
        seed = folly::hash::hash_combine( seed, pkh( access.partition_id_ ) );
        return seed;
    }
};

struct transaction_partition_accesses {

    transaction_partition_accesses(
        const std::vector<partition_access> & txn_part_accesses,
        const std::vector<::cell_key_ranges> &write_cids,
        const std::vector<::cell_key_ranges> &read_cids )
        : partition_accesses_( txn_part_accesses ),
          write_cids_( write_cids ),
          read_cids_( read_cids ) {}

    transaction_partition_accesses() {}

    void drop_partition_access( const partition_access &part_access ) {
        for( auto it = partition_accesses_.begin();
             it != partition_accesses_.end(); it++ ) {
            if( *it == part_access ) {
                partition_accesses_.erase( it );
                return;
            }
        }
        // Should never happen
        DCHECK( false );
    }

    bool contains_read_access( const partition_column_identifier &pid ) const {
        for( const auto &part_access : partition_accesses_ ) {
            if( part_access.partition_id_ == pid and !part_access.is_write_ ) {
                return true;
            }
        }
        return false;
    }

    friend std::ostream &operator<<(
        std::ostream &                        out,
        const transaction_partition_accesses &txn_partition_accesses ) {
        out << "[ ";
        for( const auto &part_access :
             txn_partition_accesses.partition_accesses_ ) {
            out << part_access << ", ";
        }
        out << "]";
        return out;
    }

    std::vector<partition_access> partition_accesses_;
    std::vector<::cell_key_ranges> write_cids_;
    std::vector<::cell_key_ranges> read_cids_;
};

struct across_transaction_access_correlations {

    across_transaction_access_correlations(
        transaction_partition_accesses &            origin_accesses,
        std::vector<transaction_partition_accesses> subsequent_txn_accesses )
        : origin_accesses_( origin_accesses ),
          subsequent_txn_accesses_( subsequent_txn_accesses ) {}

    across_transaction_access_correlations() {}

    transaction_partition_accesses get_unique_subsequent_txn_accesses() {
        std::unordered_set<partition_access, partition_access_hasher>
            unique_partition_accesses;
        for( const auto &txn_part_accesses : subsequent_txn_accesses_ ) {
            for( const auto &part_access :
                 txn_part_accesses.partition_accesses_ ) {
                unique_partition_accesses.insert( part_access );
            }
        }
        std::vector<partition_access> unique_part_accesses_as_vec;
        unique_part_accesses_as_vec.reserve( unique_partition_accesses.size() );
        for( const auto &part_access : unique_partition_accesses ) {
            unique_part_accesses_as_vec.push_back( part_access );
        }
        transaction_partition_accesses subsequent_partition_accesses;
        subsequent_partition_accesses.partition_accesses_ =
            std::move( unique_part_accesses_as_vec );
        return subsequent_partition_accesses;
    }

    transaction_partition_accesses              origin_accesses_;
    std::vector<transaction_partition_accesses> subsequent_txn_accesses_;
};
