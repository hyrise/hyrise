#include "cost_model_logical.hpp"

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "statistics/table_statistics.hpp"
#include "utils/assert.hpp"

namespace opossum {

Cost CostModelLogical::_estimate_node_cost(const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto output_row_count = node->get_statistics()->row_count();
  const auto left_row_count = node->left_input()->get_statistics()->row_count();
  const auto right_row_count = node->right_input() ? node->right_input()->get_statistics()->row_count() : 0.0f;

  switch (node->type) {
    case LQPNodeType::Join:
      return left_row_count + right_row_count + output_row_count;

    case LQPNodeType::Sort:
      return left_row_count * std::log(left_row_count);

    case LQPNodeType::Union: {
      const auto union_node = std::static_pointer_cast<UnionNode>(node);

      switch(union_node->union_mode) {
        case UnionMode::Positions:
          return left_row_count * std::log(left_row_count) + right_row_count * std::log(right_row_count);
      }
    }

    default:
      return left_row_count + output_row_count;
  }
}

}  // namespace opossum
