#pragma once

#include "operators/abstract_read_write_operator.hpp"
#include "storage/table_key_constraint.hpp"

namespace opossum {

class CreateTableKeyConstraint : public AbstractReadWriteOperator {
 public:
  CreateTableKeyConstraint(const std::unordered_set<ColumnID> init_columns, const KeyConstraintType init_key_type, const std::shared_ptr<Table> init_table, const bool init_if_not_exists);

  const std::unordered_set<ColumnID> columns();
  const KeyConstraintType key_type();
  const std::shared_ptr<Table> table();


  const bool if_not_exists;

 protected:
  std::shared_ptr<TableKeyConstraint> _on_execute(std::shared_ptr<TransactionContext> context) override;

  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_left_input,
      const std::shared_ptr<AbstractOperator>& copied_right_input,
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const override;

  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  const std::unordered_set<ColumnID> _columns;
  const KeyConstraintType _key_type;
  const std::shared_ptr<Table> _table;
};
}
