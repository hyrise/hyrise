#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_read_write_operator.hpp"
#include "storage/pos_lists/row_id_pos_list.hpp"
#include "utils/assert.hpp"

namespace opossum {

/**
 * Operator that marks the rows referenced by its input table as MVCC-expired.
 * Assumption: The input has been validated before.
 */
class Delete : public AbstractReadWriteOperator {
 public:
  explicit Delete(const std::shared_ptr<const AbstractOperator>& referencing_table_op);

  const std::string& name() const override;

 protected:
  std::shared_ptr<const Table> _on_execute(std::shared_ptr<TransactionContext> context) override;
  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_left_input,
      const std::shared_ptr<AbstractOperator>& copied_right_input,
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const override;
  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;
  void _on_commit_records(const CommitID commit_id) override;
  void _on_rollback_records() override;

 private:
  TransactionID _transaction_id;
  std::shared_ptr<const Table> _referencing_table;
};
}  // namespace opossum
