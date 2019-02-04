#pragma once

#include <string>
#include <tuple>

#include "storage/table.hpp"

namespace opossum {

class BaseConstraintChecker {
 public:
  BaseConstraintChecker(const Table& table, const TableConstraintDefinition& constraint)
      : _table(table), _constraint(constraint) {}
  virtual ~BaseConstraintChecker() = default;

  virtual std::tuple<bool, ChunkID> isValid(const CommitID snapshot_commit_id, const TransactionID our_tid) = 0;
  virtual std::tuple<bool, ChunkID> isValidForInsertedValues(std::shared_ptr<const Table> table_to_insert,
                                                             const CommitID snapshot_commit_id,
                                                             const TransactionID our_tid, const ChunkID since) = 0;

 protected:
  const Table& _table;
  const TableConstraintDefinition& _constraint;
};

}  // namespace opossum
