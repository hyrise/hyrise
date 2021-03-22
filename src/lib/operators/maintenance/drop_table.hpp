#pragma once

#include "operators/abstract_read_only_operator.hpp"

namespace opossum {

// maintenance operator for the "DROP TABLE" sql statement
class DropTable : public AbstractReadOnlyOperator {
 public:
  DropTable(const std::string& init_table_name, bool init_if_exists);

  const std::string& name() const override;
  std::string description(DescriptionMode description_mode) const override;

  const std::string table_name;
  const bool if_exists;

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_left_input,
      const std::shared_ptr<AbstractOperator>& copied_right_input,
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const override;

  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;
};
}  // namespace opossum
