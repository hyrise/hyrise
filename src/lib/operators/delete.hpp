#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_read_write_operator.hpp"
#include "utils/assert.hpp"

namespace opossum {

/**
 * Operator that deletes a number of rows from one table.
 * Expects a table with one chunk referencing only one table which
 * is passed via the AbstractOperator in the constructor.
 *
 * Assumption: The input has been validated before.
 */
class Delete : public AbstractReadWriteOperator {
 public:
  explicit Delete(const std::string& table_name, const AbstractOperatorCSPtr& values_to_delete);

  const std::string name() const override;

 protected:
  TableCSPtr _on_execute(TransactionContextSPtr context) override;
  AbstractOperatorSPtr _on_recreate(
      const std::vector<AllParameterVariant>& args, const AbstractOperatorSPtr& recreated_input_left,
      const AbstractOperatorSPtr& recreated_input_right) const override;
  void _on_commit_records(const CommitID cid) override;
  void _finish_commit() override;
  void _on_rollback_records() override;

 private:
  /**
   * Validates the context and the input table
   */
  bool _execution_input_valid(const TransactionContextSPtr& context) const;

 private:
  const std::string _table_name;
  TableSPtr _table;
  TransactionID _transaction_id;
  std::vector<PosListCSPtr> _pos_lists;
};
}  // namespace opossum
