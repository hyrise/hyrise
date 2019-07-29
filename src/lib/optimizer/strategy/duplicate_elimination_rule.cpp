#include "duplicate_elimination_rule.hpp"

#include <map>
#include <string>

#include "boost/algorithm/string.hpp"
#include "boost/functional/hash.hpp"
#include "expression/lqp_column_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"

namespace opossum {

std::string DuplicateEliminationRule::name() const { return "Duplicate Elimination Rule"; }

void DuplicateEliminationRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) const {
  _eliminate_sub_plan_duplicates_traversal(node);

  for (const auto& [original, replacement] : _original_replacement_pairs) {
    for (const auto& output : original->outputs()) {
      const auto& input_side = original->get_input_side(output);
      output->set_input(input_side, replacement);
    }
  }
}

void DuplicateEliminationRule::_eliminate_sub_plan_duplicates_traversal(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto left_node = node->left_input();
  const auto right_node = node->right_input();

  if (left_node) {
    _eliminate_sub_plan_duplicates_traversal(left_node);
  }
  if (right_node) {
    _eliminate_sub_plan_duplicates_traversal(right_node);
  }
  const auto it = std::find_if(_sub_plans.cbegin(), _sub_plans.cend(),
                               [&node](const auto& sub_plan) { return *node == *sub_plan; });
  if (it == _sub_plans.end()) {
    std::cout << node->description() << "\n";
    _sub_plans.emplace_back(node);
  } else {
    std::cout << node->description() << "\n";
    _original_replacement_pairs.emplace_back(node, *it);
  }
}

}  // namespace opossum
