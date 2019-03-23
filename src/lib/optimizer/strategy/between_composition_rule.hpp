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

/**
 * Rewrite "column_a >= x AND column_a <= y" to "column_a BETWEEN x AND y" for a performance boost.
 *
 * The BetweenCompositionRule searches for a chain of predicate nodes and within this chain substitutes
 * BinaryPredicateConditions with BetweenExpressions. The algorithm checks whether two or more BinaryPredicateConditions
 * represent a range on one column. The highest lower bound and the lowest upper bound are substituted by a
 * corresponding (exclusive or inclusive) BetweenExpression. All obsolete BinaryPredicateConditions are removed
 * after the substitution.
**/
class BetweenCompositionRule : public AbstractRule {
 public:
  std::string name() const override;
  void apply_to(const std::shared_ptr<AbstractLQPNode>& node) const override;

 private:
  /**
 * The ColumnBoundaryType defines whether a value represents a boundary for a column or not (NONE) and if it is a
 * boundary it also defines which kind of boundary it is including the inclusive and exclusive property.
**/
  enum class ColumnBoundaryType {
    None,
    LowerBoundaryInclusive,
    LowerBoundaryExclusive,
    UpperBoundaryInclusive,
    UpperBoundaryExclusive,
  };

  /**
 * A column boundary is a normalized format for further computation that allows us to store a column and a value
 * expression of a predicate node. The value represents a boundary for the column, if the ColumnBoundaryType does not
 * equal None.
**/
  struct ColumnBoundary {
    std::shared_ptr<LQPColumnExpression> column_expression;
    std::shared_ptr<ValueExpression> value_expression;
    ColumnBoundaryType type;
  };

  void _replace_predicates(std::vector<std::shared_ptr<AbstractLQPNode>>& predicates) const;

  const ColumnBoundary _get_boundary(const std::shared_ptr<BinaryPredicateExpression>& expression) const;
};

}  // namespace opossum
