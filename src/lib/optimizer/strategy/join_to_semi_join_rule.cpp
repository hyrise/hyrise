#include "join_to_semi_join_rule.hpp"

#include <magic_enum.hpp>

#include "expression/abstract_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_column_expression.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

std::string JoinToSemiJoinRule::name() const {
  static const auto name = std::string{"JoinToSemiJoinRule"};
  return name;
}

void JoinToSemiJoinRule::_apply_to_plan_without_subqueries(const std::shared_ptr<AbstractLQPNode>& lqp_root) const {
  visit_lqp(lqp_root, [&](const auto& node) {
    // Sometimes, joins are not actually used to combine tables but only to check the existence of a tuple in a second
    // table. Example: SELECT c_name FROM customer, nation WHERE c_nationkey = n_nationkey AND n_name = 'GERMANY'
    // If the join is on a unique/primary key column, we can rewrite these joins into semi joins. If, however, the
    // uniqueness is not guaranteed, we cannot perform the rewrite as non-unique joins could possibly emit a matching
    // line more than once.

    if (node->type == LQPNodeType::Join) {
      auto join_node = std::dynamic_pointer_cast<JoinNode>(node);

      // We don't rewrite semi- and anti-joins.
      if (join_node->join_mode != JoinMode::Inner) {
        return LQPVisitation::VisitInputs;
      }

      /**
       * We can only rewrite an inner join to a semi join when it has a join cardinality of 1:1 or n:1, which we check * as follows:
       * (1) From all predicates of type Equals, we collect the operand expressions by input node.
       * (2) We determine the input node that should be used for filtering.
       * (3) We check the input node from (2) for a matching single- or multi-expression unique constraint.
       *     a) Found match -> Rewrite to semi join
       *     b) No match    -> Do no rewrite to semi join because we might end up with duplicated input records.
       */
      const auto& join_predicates = join_node->join_predicates();
      auto equals_predicate_expressions_left = ExpressionUnorderedSet{};
      auto equals_predicate_expressions_right = ExpressionUnorderedSet{};

      for (const auto& join_predicate : join_predicates) {
        const auto& predicate = std::dynamic_pointer_cast<BinaryPredicateExpression>(join_predicate);
        // Skip predicates that are not of type Equals (because we need n:1 or 1:1 join cardinality)
        if (predicate->predicate_condition != PredicateCondition::Equals) {
          continue;
        }

        // Collect operand expressions table-wise
        for (const auto& operand_expression : {predicate->left_operand(), predicate->right_operand()}) {
          if (join_node->left_input()->has_output_expressions({operand_expression})) {
            equals_predicate_expressions_left.insert(operand_expression);
          } else if (join_node->right_input()->has_output_expressions({operand_expression})) {
            equals_predicate_expressions_right.insert(operand_expression);
          }
        }
      }

      // Early out, if we did not see any Equals-predicates.
      if (equals_predicate_expressions_left.empty() || equals_predicate_expressions_right.empty()) {
        return LQPVisitation::VisitInputs;
      }

      if (!join_node->get_unused_input().has_value()) {
        return LQPVisitation::VisitInputs;
      }

      // Determine, which node to use for Semi-Join-filtering and check for the required uniqueness guarantees
      if (join_node->get_unused_input().value() == LQPInputSide::Left &&
          join_node->left_input()->has_matching_unique_constraint(equals_predicate_expressions_left)) {
        join_node->join_mode = JoinMode::Semi;
        const auto temp = join_node->left_input();
        join_node->set_left_input(join_node->right_input());
        join_node->set_right_input(temp);
      } else if (join_node->get_unused_input().value() == LQPInputSide::Right &&
          join_node->right_input()->has_matching_unique_constraint(equals_predicate_expressions_right)) {
        join_node->join_mode = JoinMode::Semi;
      }
    }

    return LQPVisitation::VisitInputs;
  });
}

}  // namespace opossum
