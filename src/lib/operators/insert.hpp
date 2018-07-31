#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_read_write_operator.hpp"
#include "utils/assert.hpp"

namespace opossum {

class TransactionContext;

/**
 * Operator that inserts a number of rows from one table into another.
 * Expects the table name of the table to insert into as a string and
 * the values to insert in a separate table using the same column layout.
 *
 * Assumption: The input has been validated before.
 * Note: Insert does not support null values at the moment
 */
class Insert : public AbstractReadWriteOperator {
 public:
  explicit Insert(const std::string& target_table_name, const std::shared_ptr<AbstractOperator>& values_to_insert);

  const std::string name() const override;

 protected:
  std::shared_ptr<const Table> _on_execute(std::shared_ptr<TransactionContext> context) override;
  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_input_left,
      const std::shared_ptr<AbstractOperator>& copied_input_right) const override;
  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;
  void _on_commit_records(const CommitID cid) override;
  void _on_rollback_records() override;

 private:
  const std::string _target_table_name;
  std::shared_ptr<Table> _target_table;

  PosList _inserted_rows;
};

}  // namespace opossum
