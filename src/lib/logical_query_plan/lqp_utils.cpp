#include "lqp_utils.hpp"

#include "utils/assert.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"

namespace {

using namespace opossum;  // NOLINT

void lqp_create_node_mapping_impl(LQPNodeMapping & mapping,
const std::shared_ptr<AbstractLQPNode>& lhs, const std::shared_ptr<AbstractLQPNode>& rhs
) {
  if (!lhs && !rhs) return;

  Assert(lhs && rhs, "LQPs aren't equally structured, can't create mapping");
  Assert(lhs->type == rhs->type, "LQPs aren't equally structured, can't create mapping");

  // To avoid traversing subgraphs of ORs twice, check whether we've been here already
  const auto mapping_iter = mapping.find(lhs);
  if (mapping_iter != mapping.end()) return;

  mapping[lhs] = rhs;

  lqp_create_node_mapping_impl(mapping, lhs->left_input(), rhs->left_input());
  lqp_create_node_mapping_impl(mapping, lhs->right_input(), rhs->right_input());
}

std::optional<LQPMismatch>
lqp_find_structure_mismatch(const std::shared_ptr<const AbstractLQPNode>& lhs, const std::shared_ptr<const AbstractLQPNode>& rhs) {
  if (!lhs && !rhs) return std::nullopt;
  if (!(lhs && rhs) || lhs->type != rhs->type) return LQPMismatch(lhs, rhs);

  const auto mismatch_left = lqp_find_structure_mismatch(lhs->left_input(), rhs->left_input());
  if (mismatch_left) return mismatch_left;

  return lqp_find_structure_mismatch(lhs->right_input(), rhs->right_input());
}

std::optional<LQPMismatch>
lqp_find_subplan_mismatch_impl(const LQPNodeMapping& node_mapping, const std::shared_ptr<const AbstractLQPNode>& lhs, const std::shared_ptr<const AbstractLQPNode>& rhs) {
  if (!lhs && !rhs) return std::nullopt;
  if (!lhs->shallow_equals(*rhs, node_mapping)) return LQPMismatch(lhs, rhs);

  const auto mismatch_left = lqp_find_subplan_mismatch_impl(node_mapping, lhs->left_input(), rhs->left_input());
  if (mismatch_left) return mismatch_left;

  return lqp_find_subplan_mismatch_impl(node_mapping, lhs->right_input(), rhs->right_input());
}

}  // namespace

namespace opossum {

LQPNodeMapping lqp_create_node_mapping(
const std::shared_ptr<AbstractLQPNode>& lhs, const std::shared_ptr<AbstractLQPNode>& rhs
) {
  LQPNodeMapping mapping;
  lqp_create_node_mapping_impl(mapping, lhs, rhs);
  return mapping;
}

std::optional<LQPMismatch>
lqp_find_subplan_mismatch(const std::shared_ptr<AbstractLQPNode>& lhs, const std::shared_ptr<AbstractLQPNode>& rhs) {
  // Check for type/structural mismatched
  auto mismatch = lqp_find_structure_mismatch(lhs, rhs);
  if (mismatch) return mismatch;

  const auto node_mapping = lqp_create_node_mapping(lhs, rhs);

  return lqp_find_subplan_mismatch_impl(node_mapping, lhs, rhs);
}

void lqp_replace_node(const std::shared_ptr<AbstractLQPNode>& original_node,
                      const std::shared_ptr<AbstractLQPNode>& replacement_node) {
  DebugAssert(replacement_node->outputs().empty(), "Node can't have outputs");
  DebugAssert(!replacement_node->left_input() && !replacement_node->right_input(), "Node can't have inputs");

  const auto outputs = original_node->outputs();
  const auto input_sides = original_node->get_input_sides();

  /**
   * Tie the replacement_node with this nodes inputs
   */
  replacement_node->set_left_input(original_node->left_input());
  replacement_node->set_right_input(original_node->right_input());

  /**
   * Tie the replacement_node with this nodes outputs. This will effectively perform clear_outputs() on this node.
   */
  for (size_t output_idx = 0; output_idx < outputs.size(); ++output_idx) {
    outputs[output_idx]->set_input(input_sides[output_idx], replacement_node);
  }

  /**
   * Untie this node from the LQP
   */
  original_node->set_left_input(nullptr);
  original_node->set_right_input(nullptr);
}

void lqp_remove_node(const std::shared_ptr<AbstractLQPNode>& node) {
  Assert(!node->right_input(), "Can only remove nodes that only have a left input or no inputs");

  /**
   * Back up outputs and in which input side they hold this node
   */
  auto outputs = node->outputs();
  auto input_sides = node->get_input_sides();

  /**
   * Hold left_input ptr in extra variable to keep the ref count up and untie it from this node.
   * left_input might be nullptr
   */
  auto left_input = node->left_input();
  node->set_left_input(nullptr);

  /**
   * Tie this node's previous outputs with this nodes previous left input
   * If left_input is nullptr, still call set_input so this node will get untied from the LQP.
   */
  for (size_t output_idx = 0; output_idx < outputs.size(); ++output_idx) {
    outputs[output_idx]->set_input(input_sides[output_idx], left_input);
  }
}

bool lqp_is_validated(const std::shared_ptr<AbstractLQPNode>& lqp) {
  if (!lqp) return true;
  if (lqp->type == LQPNodeType::Validate) return true;

  if (!lqp->left_input() && !lqp->right_input()) return false;

  return lqp_is_validated(lqp->left_input()) && lqp_is_validated(lqp->right_input());
}

}  // namespace opossum
