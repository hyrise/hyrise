#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_read_write_operator.hpp"

namespace opossum {

class TransactionContext;

/**
 * Operator that modifies meta tables, e.g. calls insert on the plugins meta table to load a plugin.
 * It expects the table name of the table to modify as a string.
 *
 * The input tables are like the inputs of the Update operator.
 * If used, they must have the exact same column layout as the meta table (otherwise, it can be dummy tables).
 * The first input table specifies which rows and columns of the meta table
 * should be updated or deleted.
 * For inserts, the second input table contains the rows to insert.
 * For updates, it must have the same number of rows as the first table and contains the
 * data that is used to update the rows specified by the first table.
 *
 * Modifying meta tables is not MVCC safe, so we do nothing on commit or rollback.
 * This is why the operator throws an exception if not used in auto-commit queries.
 */
class ChangeMetaTable : public AbstractReadWriteOperator {
 public:
  explicit ChangeMetaTable(const std::string& table_name, const MetaTableChangeType& change_type,
                           const std::shared_ptr<const AbstractOperator>& values_to_modify,
                           const std::shared_ptr<const AbstractOperator>& modification_values);

  const std::string& name() const override;

 protected:
  std::shared_ptr<const Table> _on_execute(std::shared_ptr<TransactionContext> context) override;
  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_input_left,
      const std::shared_ptr<AbstractOperator>& copied_input_right) const override;
  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;
  void _on_commit_records(const CommitID cid) override {}
  void _on_rollback_records() override {}

 private:
  const std::string _meta_table_name;
  const MetaTableChangeType _change_type;
};

}  // namespace opossum
