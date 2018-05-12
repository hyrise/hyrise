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

}  // namespace opossum
