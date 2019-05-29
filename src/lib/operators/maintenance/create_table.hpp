#pragma once

#include "operators/abstract_read_only_operator.hpp"
#include "storage/table_column_definition.hpp"

namespace opossum {

// maintenance operator for the "CREATE TABLE" sql statement
class CreateTable : public AbstractReadOnlyOperator {
 public:
  CreateTable(const std::string& table_name, bool if_not_exists, std::shared_ptr<const AbstractOperator> in = nullptr);

  const std::string name() const override;
  const std::string description(DescriptionMode description_mode) const override;
  const TableColumnDefinitions column_definitions() const;

  const std::string table_name;
  const bool if_not_exists;

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_input_left,
      const std::shared_ptr<AbstractOperator>& copied_input_right) const override;

  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;
};
}  // namespace opossum
