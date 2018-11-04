#pragma once

#include "operators/abstract_read_only_operator.hpp"

namespace opossum {

// maintenance operator for the "DROP TABLE" sql statement
class DropTable : public AbstractReadOnlyOperator {
 public:
  explicit DropTable(const std::string& table_name);

  const std::string name() const override;
  const std::string description(DescriptionMode description_mode) const override;

  const std::string table_name;

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_input_left,
      const std::shared_ptr<AbstractOperator>& copied_input_right) const override;

  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;
};
}  // namespace opossum
