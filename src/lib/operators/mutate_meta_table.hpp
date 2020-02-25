#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_read_only_operator.hpp"

namespace opossum {

class TransactionContext;

/**
 * Operator that inserts a number of rows from one table into another.
 * Expects the table name of the table to insert into as a string and
 * the values to insert in a separate table using the same column layout.
 *
 * Assumption: The input has been validated before.
 */
class MutateMetaTable : public AbstractReadOnlyOperator {
 public:
  explicit MutateMetaTable(const std::string& table_name, const MetaTableMutation& mutation_type,
                           const std::shared_ptr<const AbstractOperator>& modification_values);

  const std::string& name() const override;

 protected:
  std::shared_ptr<const Table> _on_execute() override;
  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_input_left,
      const std::shared_ptr<AbstractOperator>& copied_input_right) const override;
  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

 private:
  const std::string _table_name;
  const MetaTableMutation _mutation_type;
};

}  // namespace opossum
