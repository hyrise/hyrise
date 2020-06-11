#include "join_algorithm_rule.hpp"

#include <limits>
//#include <memory>
//#include <optional>
//#include <string>
//#include <utility>
//#include <vector>
//
//#include "expression/binary_predicate_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "operators/operator_join_predicate.hpp"
//#include "logical_query_plan/lqp_utils.hpp"
//#include "logical_query_plan/predicate_node.hpp"
//#include "logical_query_plan/stored_table_node.hpp"
//#include "types.hpp"
//#include "utils/assert.hpp"

using namespace std::string_literals;  // NOLINT

namespace opossum {

JoinAlgorithmRule::JoinAlgorithmRule(const std::shared_ptr<AbstractCostEstimator>& init_cost_estimator)
    : _cost_estimator(init_cost_estimator) {}

void JoinAlgorithmRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) const {
  if (node->type == LQPNodeType::Join) {
    // ... "potential"_cross_join_node until this if below
    auto join_node = std::dynamic_pointer_cast<JoinNode>(node);
    if (join_node->join_mode != JoinMode::Cross && !join_node->join_type) {
      const auto valid_join_types = _valid_join_types(join_node);

      // SortMerge can handle everything
      JoinType minimal_costs_join_type = JoinType::SortMerge;
      Cost minimal_costs{std::numeric_limits<float>::max()};

      for (const auto& join_type : valid_join_types) {
        join_node->join_type = join_type;
        const auto predicted_costs = _cost_estimator->estimate_plan_cost(join_node);
        if (predicted_costs < minimal_costs) {
          minimal_costs_join_type = join_type;
          minimal_costs = predicted_costs;
        }
      }

      join_node->join_type = minimal_costs_join_type;
    }
  }

  _apply_to_inputs(node);
}

const std::vector<JoinType> JoinAlgorithmRule::_valid_join_types(const std::shared_ptr<JoinNode>& node) const {
  // TODO(Sven): Add IndexJoin

  /**
        * Assert that the Join Predicate is simple, e.g. of the form <column_a> <predicate> <column_b>.
        * We do not require <column_a> to be in the left input though.
        */

  const auto operator_join_predicate = OperatorJoinPredicate::from_expression(
      *node->join_predicates().front(), *node->left_input(), *node->right_input());
  Assert(operator_join_predicate,
         "Couldn't translate join predicate: "s + node->join_predicates().front()->as_column_name());

  const auto predicate_condition = operator_join_predicate->predicate_condition;

  if (predicate_condition == PredicateCondition::Equals && node->join_mode != JoinMode::FullOuter) {
    return {JoinType::Hash, JoinType::NestedLoop, JoinType::SortMerge};
  }

  return {JoinType::NestedLoop, JoinType::SortMerge};
}

}  // namespace opossum
