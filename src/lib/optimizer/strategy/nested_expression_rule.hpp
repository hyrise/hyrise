#pragma once

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
 * This optimizer rule looks for ProjectionNodes that contain calculable Expressions.
 * It then calculates the Expressions, and replaces all LQPColumnReferences to these columns
 * in any PredicateNode of the parent tree with the Expression result.
 * The column containin the Expression is then removed from the ProjectionNode.
 */
class NestedExpressionRule : public AbstractRule {
 public:
  std::string name() const override;
  bool apply_to(const std::shared_ptr<AbstractLQPNode>& node) override;

 private:
  bool _replace_expression_in_parents(const std::shared_ptr<AbstractLQPNode>& node,
                                      const LQPColumnReference& expression_column, const AllTypeVariant& value);
  void _remove_column_from_projection(const std::shared_ptr<ProjectionNode>& node, ColumnID column_id);

  std::optional<DataType> _get_type_of_expression(const std::shared_ptr<LQPExpression>& expression) const;

  template <typename T>
  AllTypeVariant _evaluate_expression(boost::hana::basic_type<T> type,
                                      const std::shared_ptr<LQPExpression>& expression) const;
};

}  // namespace opossum
