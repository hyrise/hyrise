#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_rule.hpp"
#include "expression/expression_functional.hpp"

#include "types.hpp"

namespace opossum {

class AbstractLQPNode;
class PredicateNode;

enum ColumnBoundaryType {
  None,
  LowerBoundaryInclusive,
  LowerBoundaryExclusive,
  UpperBoundaryInclusive,
  UpperBoundaryExclusive,
};

struct ColumnBoundary {
  std::shared_ptr<PredicateNode> node;
  std::shared_ptr<LQPColumnExpression> column_expression;
  std::shared_ptr<ValueExpression> value_expression;
  ColumnBoundaryType type;
};
/**
 * This rule determines which chunks can be excluded from table scans based on
 * the predicates present in the LQP and stores that information in the stored
 * table nodes.
 */
class BetweenCompositionRule : public AbstractRule {
 public:
  std::string name() const override;
  void apply_to(const std::shared_ptr<AbstractLQPNode>& node) const override;

 private:
  void _replace_predicates(std::vector<std::shared_ptr<AbstractLQPNode>>& predicates) const;

  const ColumnBoundary _get_boundary(const std::shared_ptr<BinaryPredicateExpression>& expression,
                                     const std::shared_ptr<PredicateNode>& node) const;
};

}  // namespace opossum
