#pragma once

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "abstract_rule.hpp"
#include "all_type_variant.hpp"
#include "types.hpp"

namespace opossum {

class AbstractLQPNode;
class ProjectionNode;
class LQPExpression;
class LQPColumnReference;

/**
 * This optimizer rule looks for Expressions in ProjectionNodes that are calculable at planning time
 * and replaces them with a constant.
 */
class ConstantCalculationRule : public AbstractRule {
 public:
  std::string name() const override;
  bool apply_to(const std::shared_ptr<AbstractLQPNode>& node) override;

 private:
  void _calculate_expressions_in_tree(const std::shared_ptr<AbstractLQPNode>& node,
                                      std::map<LQPColumnReference, AllTypeVariant>& column_reference_to_value_map);
  bool _replace_column_references_in_tree(
      const std::shared_ptr<AbstractLQPNode>& node,
      const std::map<LQPColumnReference, AllTypeVariant>& column_reference_to_value_map);
  std::shared_ptr<LQPExpression> _replace_column_references_in_expression(
      const std::shared_ptr<LQPExpression>& expression,
      const std::map<LQPColumnReference, AllTypeVariant>& column_reference_to_value_map);
  bool _remove_columns_from_projections(
      const std::shared_ptr<AbstractLQPNode>& node,
      const std::map<LQPColumnReference, AllTypeVariant>& column_reference_to_value_map);

  std::optional<DataType> _get_type_of_expression(const std::shared_ptr<LQPExpression>& expression) const;

  template <typename T>
  std::optional<AllTypeVariant> _calculate_expression(boost::hana::basic_type<T> type,
                                                      const std::shared_ptr<LQPExpression>& expression) const;
};

}  // namespace opossum
