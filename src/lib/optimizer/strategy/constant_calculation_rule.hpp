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
 * This optimizer rule looks for Expressions in ProjectionNodes that are calculable at planning time
 * and replaces them with a constant.
 */
class ConstantCalculationRule : public AbstractRule {
 public:
  std::string name() const override;
  bool apply_to(const AbstractLQPNodeSPtr& node) override;

 private:
  bool _replace_expression_in_outputs(const AbstractLQPNodeSPtr& node,
                                      const LQPColumnReference& expression_column, const AllTypeVariant& value);
  void _remove_column_from_projection(const ProjectionNodeSPtr& node, ColumnID column_id);

  std::optional<DataType> _get_type_of_expression(const LQPExpressionSPtr& expression) const;

  template <typename T>
  std::optional<AllTypeVariant> _calculate_expression(boost::hana::basic_type<T> type,
                                                      const LQPExpressionSPtr& expression) const;
};

}  // namespace opossum
