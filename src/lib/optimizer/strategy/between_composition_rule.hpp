#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_rule.hpp"
#include "expression/expression_functional.hpp"

#include "types.hpp"

namespace hyrise {

class AbstractLQPNode;
class PredicateNode;

/**
 * Rewrite "column_a >= x AND column_a <= y" with x and y being values to "column_a BETWEEN x AND y" for a performance
 * boost. The case where x and y are columns can largely be handled by this rule as well. As no scan can handle that
 * combination yet, we still emit two separate predicates in this case.
 *
 * The BetweenCompositionRule searches for a chain of PredicateNodes and within this chain substitutes
 * BinaryPredicateConditions with BetweenExpressions. The algorithm checks whether two or more BinaryPredicateConditions
 * represent a range on one column. The highest lower bound and the lowest upper bound are substituted by a
 * corresponding (exclusive or inclusive) BetweenExpression. All obsolete BinaryPredicateConditions are removed
 * after the substitution.
 */
class BetweenCompositionRule : public AbstractRule {
 public:
  std::string name() const override;

 protected:
  void _apply_to_plan_without_subqueries(const std::shared_ptr<AbstractLQPNode>& lqp_root) const override;

 private:
  using PredicateChain = std::vector<std::shared_ptr<PredicateNode>>;
  static void _substitute_predicates_with_between_expressions(const PredicateChain& predicate_chain);

  /**
   * The ColumnBoundaryType defines whether a value represents a boundary for a column or not (NONE) and if it is a
   * boundary it also defines which kind of boundary it is including the inclusive and exclusive property.
   */
  enum class ColumnBoundaryType {
    None,
    LowerBoundaryInclusive,
    LowerBoundaryExclusive,
    UpperBoundaryInclusive,
    UpperBoundaryExclusive,
  };

  /**
   * A column boundary is a normalized format that allows us to store a column and a value
   * expression of a PredicateNode. The value represents a boundary for the column if the ColumnBoundaryType does not
   * equal None. To create the ColumnBoundary for the other column, if both expressions are LQPColumnExpressions, the
   * boundary_is_column_expression flag has been added.
   */
  struct ColumnBoundary {
    std::shared_ptr<LQPColumnExpression> column_expression;
    std::shared_ptr<AbstractExpression> border_expression;
    BetweenCompositionRule::ColumnBoundaryType type;
    bool boundary_is_column_expression;
    size_t id;
  };

  static ColumnBoundary _create_inverse_boundary(const std::shared_ptr<ColumnBoundary>& column_boundary);

  static ColumnBoundary _get_boundary(const std::shared_ptr<BinaryPredicateExpression>& expression, const size_t id);
};

}  // namespace hyrise
