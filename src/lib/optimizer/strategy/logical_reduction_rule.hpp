#pragma once

#include <unordered_map>

#include "abstract_rule.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/logical_expression.hpp"

namespace opossum {

class AbstractLQPNode;

/**
 * This rule tries to simpifly logical expressions. Currently, it only looks at disjunctive chains, i.e., chains of
 * ORs: `(a AND b) OR (a AND c) OR (a AND d)`. In this example, `a` can be extracted: `a AND (b OR c OR d)`.
 * Furthermore, if the expression is used by a predicate, this rule extracts `a` into its own PredicateNode, which
 * can then be independently pushed down or used for detecting joins. A good example can be seen in TPC-H query 19.
 * Without this rule, `p_partkey = l_partkey` would not be available as a join predicate and the predicates on
 * `l_shipmode` and `l_shipinstruct` could not be pulled below the join.
 * Conjunctive chains, i.e., `(a OR b) AND (a OR c) -> a OR (b AND c)` are currently ignored, as they don't allow
 * us to fully extract predicates.
 */
class LogicalReductionRule : public AbstractRule {
 public:
  std::string name() const override;
  bool apply_to(const std::shared_ptr<AbstractLQPNode>& node) const override;

  static std::shared_ptr<AbstractExpression> reduce_distributivity(
      const std::shared_ptr<AbstractExpression>& expression);

 protected:
  using MapType = ExpressionUnorderedMap<std::shared_ptr<AbstractExpression>>;

  // Applies the rule to a node, specifically Predicate- or ProjectionNodes, and recurses.
  // `previously_reduced_expressions` is used as a cache of expressions that have previously been modified. This is
  // not only to avoid doing the work twice, but to make sure that two nodes that share one expression will use
  // the same expression after optimization, too.
  bool _apply_to_node(const std::shared_ptr<AbstractLQPNode>& node, MapType& previously_reduced_expressions) const;

  // Recurses into the expression, tries to identify chains, and extracts common predicates. The order of predicates
  // may be changed, e.g.: (a AND b) OR (a AND c) -> a AND (c OR b)
  bool _apply_to_expressions(std::vector<std::shared_ptr<AbstractExpression>>& expressions,
                             MapType& previously_reduced_expressions) const;

  // Given an expression and a LogicalOperator (i.e., AND or OR), sets `result` to a list of all top-level expressions
  // connected with that LogicalOperator. Example: When calling this method with expression =
  // `(((a AND b) AND c) AND (d OR e)` and `logical_operator == And`, a chaing with four expressions would be
  // identified: (1) a, (2) b, (3) c, (4) d or e. `top_level` is used to identify the top-level call of this function.
  // If the top-level call would result in a single expression being the result, the result is cleared, as we cannot
  // call a single expression a "chain".
  static void _flatten_logical_expressions(const std::shared_ptr<AbstractExpression>& expression,
                                           LogicalOperator logical_operator, ExpressionUnorderedSet& flattened);

  // Given a chain of expressions (see _collect_chained_logical_expressions), remove all expressions that are in
  // `expressions_to_remove`. Example: When called on `(((a AND b) AND c) AND (d OR e)` with
  // `logical_operator == And` and `expressions_to_remove` containing `b` and `c`, the result would be
  // `(a AND (d OR e)`.
  static void _remove_expressions_from_chain(std::shared_ptr<AbstractExpression>& chain,
                                             LogicalOperator logical_operator,
                                             const ExpressionUnorderedSet& expressions_to_remove);
};

}  // namespace opossum
