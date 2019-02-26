#pragma once

#include <optional>
#include <string>
#include <tuple>

#include "storage/chunk.hpp"
#include "storage/table.hpp"

namespace opossum {

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
   * Checks if a constraint is satisfied on a table.
   */
  virtual std::tuple<bool, ChunkID> is_valid(const CommitID snapshot_commit_id, const TransactionID our_tid) = 0;

  /**
   * Checks if a constraint is still satisfied when some values are inserted into the table.
   * Snapshot commit ID and transaction ID are required, also a start chunk ID can be passed to skip
   * compressed chunks on incremental unique checkings during the commit phase.
   */
  virtual std::tuple<bool, ChunkID> is_valid_for_inserted_values(std::shared_ptr<const Table> table_to_insert,
                                                                 const CommitID snapshot_commit_id,
                                                                 const TransactionID our_tid, const ChunkID since) = 0;

 protected:
  const Table& _table;
  const TableConstraintDefinition& _constraint;
};

}  // namespace opossum
