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
  Assert(lhs->type() == rhs->type(), "LQPs aren't equally structured, can't create mapping");

  // To avoid traversing subgraphs of ORs twice, check whether we've been here already
  const auto mapping_iter = mapping.find(lhs);
  if (mapping_iter != mapping.end()) return;

  mapping[lhs] = rhs;

  lqp_create_node_mapping_impl(mapping, lhs->left_input(), rhs->left_input());
  lqp_create_node_mapping_impl(mapping, lhs->right_input(), rhs->right_input());
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

}  // namespace opossum
