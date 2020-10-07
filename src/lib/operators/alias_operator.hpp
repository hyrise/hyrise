#pragma once

#include "abstract_operator.hpp"
#include "abstract_read_only_operator.hpp"
#include "property.hpp"

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
      const std::shared_ptr<AbstractOperator>& copied_left_input,
      const std::shared_ptr<AbstractOperator>& copied_right_input) const override;
  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  std::shared_ptr<const Table> _on_execute() override;

 private:
  const std::vector<ColumnID> _column_ids;
  const std::vector<std::string> _aliases;

 public:
  // TODO(CAJan93): Support all relevant members, including parent members. Done?
  inline constexpr static auto properties = std::make_tuple(
      property(&AliasOperator::_left_input, "_left_input"), property(&AliasOperator::_column_ids, "_column_ids"),
      property(&AliasOperator::_aliases, "_aliases"),
      // From AbstractReadOnlyOperator
      // TODO(CAJan93): performance_data not supported
      // TODO(CAJan93): _output not supported
      // TODO(CAJan93): _transaction_context not supported
      // // No support for lqp_node
      /*, property(&AliasOperator::_right_input, "_right_input")*/ property(&AliasOperator::_type, "_type"));
};

}  // namespace opossum
