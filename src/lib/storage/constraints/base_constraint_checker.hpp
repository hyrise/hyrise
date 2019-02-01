#pragma once

#include <string>

namespace opossum {

class BaseConstraintChecker {
public:
  BaseConstraintChecker(const Table& table, const TableConstraintDefinition& constraint)
    : _table(table), _constraint(constraint) {
  }
  virtual ~BaseConstraintChecker() = default;

  virtual bool isValid(const CommitID snapshot_commit_id, const TransactionID our_tid) = 0;
  virtual bool isValidForInsertedValues(std::shared_ptr<const Table> table_to_insert, const CommitID snapshot_commit_id, const TransactionID our_tid) = 0;

protected:
  const Table& _table;
  const TableConstraintDefinition& _constraint;
};

}  // namespace opossum
