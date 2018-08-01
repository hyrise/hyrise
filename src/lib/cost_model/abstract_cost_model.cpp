#include "abstract_cost_model.hpp"

#include "cost_feature_lqp_node_proxy.hpp"
#include "cost_feature_operator_proxy.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "operators/join_hash.hpp"
#include "operators/operator_join_predicate.hpp"
#include "operators/operator_scan_predicate.hpp"
#include "operators/product.hpp"
#include "operators/table_scan.hpp"
#include "operators/union_positions.hpp"

namespace opossum {

Cost AbstractCostModel::estimate_operator_cost(const std::shared_ptr<AbstractOperator>& op) const {
  CostFeatureOperatorProxy feature_proxy(op);
  return _cost_model_impl(op->type(), feature_proxy);
}

Cost AbstractCostModel::estimate_lqp_node_cost(const std::shared_ptr<AbstractLQPNode>& node) const {
  auto operator_type = OperatorType::Mock;  // Use Mock just to have a valid initialisation

  /**
   * The following switch makes assumptions about the concrete Operator that the LQPTranslator will choose.
   * TODO(anybody) somehow ask the LQPTranslator about this instead of making assumptions.
   */

  switch (node->type) {
    case LQPNodeType::Predicate:
      operator_type = OperatorType::TableScan;
      break;

    case LQPNodeType::Join: {
      const auto join_node = std::static_pointer_cast<JoinNode>(node);
      const auto operator_predicate = OperatorJoinPredicate::from_expression(
          *join_node->join_predicate, *join_node->left_input(), *join_node->right_input());
      Assert(operator_predicate, "Expected Join predicate to be OperatorScanPredicate compatible");

      if (join_node->join_mode == JoinMode::Cross) {
        operator_type = OperatorType::Product;
      } else if (join_node->join_mode == JoinMode::Inner &&
                 operator_predicate->predicate_condition == PredicateCondition::Equals) {
        operator_type = OperatorType::JoinHash;
      } else {
        operator_type = OperatorType::JoinSortMerge;
      }
    } break;

    case LQPNodeType::Union: {
      operator_type = OperatorType::UnionPositions;
    } break;

    default:
      // TODO(anybody) we're not costing this OperatorType yet (since it is not involved in JoinOrdering and all
      //               costing is currently done for JoinOrdering only)
      return 0.0f;
  }

  CostFeatureLQPNodeProxy feature_proxy(node);
  return _cost_model_impl(operator_type, feature_proxy);
}
}  // namespace opossum
