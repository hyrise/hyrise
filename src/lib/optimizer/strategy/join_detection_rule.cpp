#include "join_detection_rule.hpp"

#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

std::string JoinDetectionRule::name() const { return "Join Detection Rule"; }

bool JoinDetectionRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) {
  std::unordered_set<std::shared_ptr<PredicateNode>> predicate_nodes;

  _traverse(node, predicate_nodes);
}

void JoinDetectionRule::_traverse(const std::shared_ptr<AbstractLQPNode>& node, std::unordered_set<std::shared_ptr<AbstractLQPNode>>& predicate_nodes) {

  if (node->type == LQPNodeType::Predicate) {
    predicate_nodes.emplace(std::static_pointer_cast<PredicateNode>(node));
    _traverse(node->left_input(), predicate_nodes);
  }

  if (node->type == LQPNodeType::Join) {
    const auto join_node =  std::static_pointer_cast<JoinNode>(node);
    if (join_node->join_mode == JoinMode::Cross) {

    }
  }

  if (node->input_count() == 1) {
    _traverse(node->left_input(), predicate_nodes);
  } else if (node->input_count() == 2) {
    auto predicate_nodes_for_left = predicate_nodes;
    auto predicate_nodes_for_right = predicate_nodes;

    _traverse(node->left_input(), predicate_nodes_for_left);
    _traverse(node->right_input(), predicate_nodes_for_right);
  }
}

}  // namespace opossum
