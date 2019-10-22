#pragma once

#include "abstract_operator.hpp"
#include "abstract_read_only_operator.hpp"

namespace opossum {

/**
 * Forward the input columns in the specified order with updated column names
 */
class AliasOperator : public AbstractReadOnlyOperator {
 public:
  AliasOperator(const std::shared_ptr<const AbstractOperator>& input, const std::vector<ColumnID>& column_ids,
                const std::vector<std::string>& aliases);

  const std::string& name() const override;
  std::string description(DescriptionMode description_mode) const override;

 protected:
  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_input_left,
      const std::shared_ptr<AbstractOperator>& copied_input_right) const override;
  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  std::shared_ptr<const Table> _on_execute() override;

 private:
  const std::vector<ColumnID> _column_ids;
  const std::vector<std::string> _aliases;
};

}  // namespace opossum
