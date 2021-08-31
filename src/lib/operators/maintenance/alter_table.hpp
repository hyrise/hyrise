#pragma once

#include "operators/abstract_read_write_operator.hpp"
#include "operators/insert.hpp"
#include "storage/table_column_definition.hpp"
#include "logical_query_plan/abstract_alter_table_action.hpp"
#include "abstract_alter_table_impl.hpp"

namespace opossum {

class AlterTable : public AbstractReadWriteOperator {
 public:
  AlterTable(const std::string& init_table_name, const std::shared_ptr<AbstractAlterTableAction>& init_alter_action);

  const std::string& name() const override;
  std::string description(DescriptionMode description_mode) const override;

  const std::string target_table_name;
  const std::shared_ptr<AbstractAlterTableAction> action;

 protected:
  std::shared_ptr<AbstractAlterTableImpl> _impl;
  std::shared_ptr<const Table> _on_execute(std::shared_ptr<TransactionContext> context) override;

  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_left_input,
      const std::shared_ptr<AbstractOperator>& copied_right_input,
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const override;

  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  void _on_commit_records(const CommitID cid) override {}

  void _on_rollback_records() override {}
  std::shared_ptr<AbstractAlterTableImpl> _create_impl();
};
}  // namespace opossum
