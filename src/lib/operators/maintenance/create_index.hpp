#pragma once

#include "operators/abstract_read_write_operator.hpp"
#include "operators/insert.hpp"
#include "storage/table_column_definition.hpp"

namespace opossum {

// maintenance operator for the "CREATE TABLE" sql statement
class CreateIndex : public AbstractReadWriteOperator {
 public:
  CreateIndex(const std::string& init_index_name,
              const bool init_if_not_exists,
              const std::string& init_table_name,
              const std::shared_ptr<const std::vector<ColumnID>>& init_column_ids);

  const std::string& name() const override;
  std::string description(DescriptionMode description_mode) const override;

  const std::string index_name;
  const bool if_not_exists;
  const std::string table_name;
  const std::shared_ptr<const std::vector<ColumnID>> column_ids;

 protected:
  std::shared_ptr<const Table> _on_execute(std::shared_ptr<TransactionContext> context) override;

  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_left_input,
      const std::shared_ptr<AbstractOperator>& copied_right_input,
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const override;

  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  // Commit happens in Insert operator
  void _on_commit_records(const CommitID cid) override {}

  // Rollback happens in Insert operator
  void _on_rollback_records() override {}

  void _check_if_index_already_exists(std::string new_index_name, std::shared_ptr<Table> table);
};
}  // namespace opossum
