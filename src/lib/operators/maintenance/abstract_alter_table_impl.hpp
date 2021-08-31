#pragma once

#include "logical_query_plan/abstract_alter_table_action.hpp"
#include "operators/abstract_read_write_operator.hpp"
#include "operators/insert.hpp"
#include "storage/table_column_definition.hpp"

namespace opossum {

class AbstractAlterTableImpl {
 public:
  AbstractAlterTableImpl(std::string init_table_name,
                         const std::shared_ptr<AbstractAlterTableAction>& init_alter_action);
  virtual ~AbstractAlterTableImpl() = default;
  virtual std::string description() = 0;
  virtual void on_execute(std::shared_ptr<TransactionContext> context) = 0;

 protected:
  const std::string _target_table_name;
  const std::shared_ptr<AbstractAlterTableAction> _action;
};
}  // namespace opossum
