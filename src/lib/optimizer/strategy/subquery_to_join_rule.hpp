#pragma once

#include "abstract_rule.hpp"
#include "expression/abstract_expression.hpp"
#include "types.hpp"

namespace opossum {

class AbstractExpression;
class AbstractLQPNode;
class AliasNode;
class AggregateNode;
class BinaryPredicateExpression;
class LQPSubqueryExpression;
class PredicateNode;
class ProjectionNode;

/**
 * Optimizes:
 *    - (NOT) IN predicates with a subquery as the right operand
 *    - (NOT) EXISTS predicates
 *    - comparison (<,>,<=,>=,=,<>) predicates with subquery as the right operand
 * Does not currently optimize:
 *    - (NOT) IN expressions where
 *        - the left value is not a column expression.
 *    - NOT IN with a correlated subquery
 *    - Correlated subqueries where the correlated parameter
 *        - is used outside predicates
 *        - is used in predicates at a point where it cannot be pulled up into a join predicate (below limits, etc.)
 */
class SubqueryToJoinRule : public AbstractRule {
 public:
  struct PredicateNodeInfo {
    /**
     * Join predicate to achieve the semantic of the input expression type (IN, comparison, ...) in the created join.
     *
     * This can be nullptr (for (NOT) EXISTS), in this case only the join predicates from correlated predicates in the
     * subquery will be used in the created join.
     */
    std::shared_ptr<BinaryPredicateExpression> join_predicate;
    JoinMode join_mode;

    std::shared_ptr<LQPSubqueryExpression> subquery;
  };

  /**
   * Result of pulling up correlated predicates from an LQP.
   */
  struct PredicatePullUpResult {
    std::shared_ptr<AbstractLQPNode> adapted_lqp;

    std::vector<std::shared_ptr<BinaryPredicateExpression>> join_predicates;

    size_t pulled_predicate_node_count = 0;

    /**
     * Column expressions from the subquery required by the extracted join predicates.
     *
     * This list contains every column expression only once, even if it is used required by multiple join predicates.
     * This is a vector instead of an unordered_set so that tests are reproducible. Since correlation is usually very
     * low there shouldn't be much of a performance difference.
     */
    std::vector<std::shared_ptr<AbstractExpression>> required_column_expressions = {};
  };

  /**
   * Extract information about the input LQP into a general format.
   *
   * Returns nullopt if the LQP does not match one of the supported formats.
   */
  static std::optional<PredicateNodeInfo> is_predicate_node_join_candidate(const PredicateNode& predicate_node);

  /**
   * Searches for usages of correlated parameters.
   *
   * The first boolean is false when a correlated parameter is used outside of predicate nodes (for example in joins).
   * In this case we can never optimize this LQP. If it is true, the size_t contains the number of predicate nodes in
   * the LQP that use correlated parameters.
   */
  static std::pair<bool, size_t> assess_correlated_parameter_usage(
      const std::shared_ptr<AbstractLQPNode>& lqp,
      const std::map<ParameterID, std::shared_ptr<AbstractExpression>>& parameter_mapping);

  /**
   * Tries to safely extract new join predicates from a predicate node.
   *
   * Returns a binary predicate expressions where the left operand is always the expression associated with the
   * correlated parameter (and thus a column from the left subtree) and the right operand a column from the subqueries
   * LQP. Also returns a new expression containing all non-correlated parts of the original nodes expression. If
   * every part of the original nodes expression was turned into join predicates, nullptr is returned instead.
   */
  static std::pair<std::vector<std::shared_ptr<BinaryPredicateExpression>>, std::shared_ptr<AbstractExpression>>
  try_to_extract_join_predicates(const std::shared_ptr<PredicateNode>& predicate_node,
                                 const std::map<ParameterID, std::shared_ptr<AbstractExpression>>& parameter_mapping,
                                 bool is_below_aggregate);

  /**
   * Copy an aggregate node and adapt it to group by all required columns.
   */
  static std::shared_ptr<AggregateNode> adapt_aggregate_node(
      const std::shared_ptr<AggregateNode>& node,
      const std::vector<std::shared_ptr<AbstractExpression>>& required_column_expressions);

  /**
   * Copy an alias node and adapt it to keep all required columns.
   */
  static std::shared_ptr<AliasNode> adapt_alias_node(
      const std::shared_ptr<AliasNode>& node,
      const std::vector<std::shared_ptr<AbstractExpression>>& required_column_expressions);

  /**
   * Copy a projection node and adapt it to keep all required columns.
   */
  static std::shared_ptr<ProjectionNode> adapt_projection_node(
      const std::shared_ptr<ProjectionNode>& node,
      const std::vector<std::shared_ptr<AbstractExpression>>& required_column_expressions);

  /**
   * Walk the subquery LQP, removing all correlated predicate nodes and adapting other nodes as necessary.
   */
  static PredicatePullUpResult pull_up_correlated_predicates(
      const std::shared_ptr<AbstractLQPNode>& node,
      const std::map<ParameterID, std::shared_ptr<AbstractExpression>>& parameter_mapping);

  void apply_to(const std::shared_ptr<AbstractLQPNode>& node) const override;
};

}  // namespace opossum
