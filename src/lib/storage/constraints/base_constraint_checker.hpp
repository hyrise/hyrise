#pragma once

#include <optional>
#include <string>
#include <tuple>

#include "storage/chunk.hpp"
#include "storage/table.hpp"

namespace opossum {

static constexpr std::tuple<bool, ChunkID> CO_TUPLE {true, MAX_CHUNK_ID};

/**
 * Base class for a constraint checker. Takes a table and a constraint definition and
 * allows to check if the constraint is valid / will be still valid with certain values inserted.
 */
class BaseConstraintChecker {
 public:
  BaseConstraintChecker(const Table& table, const TableConstraintDefinition& constraint)
      : _table(table), _constraint(constraint) {}
  virtual ~BaseConstraintChecker() = default;

  /**
   * Checks if the table entries on a given MVCC snapshot satisfy the constraint.
   */
  virtual bool is_valid(const CommitID snapshot_commit_id, const TransactionID our_tid) = 0;

  /**
   * Checks if a constraint is still satisfied when some values are inserted into the table.
   * Snapshot commit ID and transaction ID are required to identify the MVCC snapshot.
   * To skip compressed chunks on incremental unique checkings during the commit phase,
   * only chunks after the `since` ChunkID are checked.
   */
  virtual std::tuple<bool, ChunkID> is_valid_for_inserted_values(std::shared_ptr<const Table> table_to_insert,
                                                                 const CommitID snapshot_commit_id,
                                                                 const TransactionID our_tid, const ChunkID since) = 0;

 protected:
  const Table& _table;
  const TableConstraintDefinition& _constraint;
};

}  // namespace opossum
