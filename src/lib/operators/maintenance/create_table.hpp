#pragma once

#include "operators/abstract_read_write_operator.hpp"
#include "operators/insert.hpp"
#include "storage/table_column_definition.hpp"

namespace opossum {

// maintenance operator for the "CREATE TABLE" sql statement
class CreateTable : public AbstractReadWriteOperator {
 public:
  CreateTable(const std::string& table_name, bool if_not_exists, const std::shared_ptr<const AbstractOperator>& in);

  const std::string& name() const override;
  std::string description(DescriptionMode description_mode) const override;
  const TableColumnDefinitions& column_definitions() const;

  const std::string table_name;
  const bool if_not_exists;

 protected:
  std::shared_ptr<const Table> _on_execute(std::shared_ptr<TransactionContext> context) override;

  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_input_left,
      const std::shared_ptr<AbstractOperator>& copied_input_right) const override;

  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  // Commit happens in Insert operator
  void _on_commit_records(const CommitID cid) override {}

  // Rollback happens in Insert operator
  void _on_rollback_records() override {}

  std::shared_ptr<Insert> _insert;
};
}  // namespace opossum
