#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_read_write_operator.hpp"
#include "storage/pos_list.hpp"
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
  explicit Delete(const std::string& table_name, const std::shared_ptr<const AbstractOperator>& values_to_delete);

  const std::string name() const override;

 protected:
  std::shared_ptr<const Table> _on_execute(std::shared_ptr<TransactionContext> context) override;
  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_input_left,
      const std::shared_ptr<AbstractOperator>& copied_input_right) const override;
  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;
  void _on_commit_records(const CommitID cid) override;
  void _finish_commit() override;
  void _on_rollback_records() override;

 private:
  /**
   * Validates the context and the input table
   */
  bool _execution_input_valid(const std::shared_ptr<TransactionContext>& context) const;

 private:
  const std::string _table_name;
  std::shared_ptr<Table> _table;
  TransactionID _transaction_id;
  std::vector<std::shared_ptr<const PosList>> _pos_lists;
  uint64_t _num_rows_deleted;
};
}  // namespace opossum
