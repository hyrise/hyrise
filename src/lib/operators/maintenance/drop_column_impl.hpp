#pragma once

#include "abstract_alter_table_impl.hpp"
#include "logical_query_plan/drop_column_action.hpp"

namespace opossum {

class DropColumnImpl : public AbstractAlterTableImpl {
 public:
  DropColumnImpl(std::string init_table_name, const std::shared_ptr<AbstractAlterTableAction> init_alter_action);
  std::string description() override;
  void on_execute(std::shared_ptr<TransactionContext> context) override;

 protected:
  std::shared_ptr<DropColumnAction> drop_column_action;
  static bool _column_exists_on_table(const std::string& table_name, std::string& column_name);
};
}  // namespace opossum
