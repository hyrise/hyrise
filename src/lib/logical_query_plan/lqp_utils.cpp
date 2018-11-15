#include "lqp_utils.hpp"

#include <set>

#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/delete_node.hpp"
#include "logical_query_plan/insert_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "logical_query_plan/update_node.hpp"
#include "utils/assert.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace {

using namespace opossum;  // NOLINT

void lqp_create_node_mapping_impl(LQPNodeMapping& mapping, const std::shared_ptr<AbstractLQPNode>& lhs,
                                  const std::shared_ptr<AbstractLQPNode>& rhs) {
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

std::optional<LQPMismatch> lqp_find_structure_mismatch(const std::shared_ptr<const AbstractLQPNode>& lhs,
                                                       const std::shared_ptr<const AbstractLQPNode>& rhs) {
  if (!lhs && !rhs) return std::nullopt;
  if (!(lhs && rhs) || lhs->type != rhs->type) return LQPMismatch(lhs, rhs);

  const auto mismatch_left = lqp_find_structure_mismatch(lhs->left_input(), rhs->left_input());
  if (mismatch_left) return mismatch_left;

  return lqp_find_structure_mismatch(lhs->right_input(), rhs->right_input());
}

std::optional<LQPMismatch> lqp_find_subplan_mismatch_impl(const LQPNodeMapping& node_mapping,
                                                          const std::shared_ptr<const AbstractLQPNode>& lhs,
                                                          const std::shared_ptr<const AbstractLQPNode>& rhs) {
  if (!lhs && !rhs) return std::nullopt;
  if (!lhs->shallow_equals(*rhs, node_mapping)) return LQPMismatch(lhs, rhs);

  const auto mismatch_left = lqp_find_subplan_mismatch_impl(node_mapping, lhs->left_input(), rhs->left_input());
  if (mismatch_left) return mismatch_left;

  return lqp_find_subplan_mismatch_impl(node_mapping, lhs->right_input(), rhs->right_input());
}

void lqp_find_subplan_roots_impl(std::vector<std::shared_ptr<AbstractLQPNode>>& root_nodes,
                                 std::unordered_set<std::shared_ptr<AbstractLQPNode>>& visited_nodes,
                                 const std::shared_ptr<AbstractLQPNode>& lqp) {
  root_nodes.emplace_back(lqp);

  visit_lqp(lqp, [&](const auto& sub_node) {
    if (!visited_nodes.emplace(sub_node).second) return LQPVisitation::DoNotVisitInputs;

    for (const auto& expression : sub_node->node_expressions()) {
      visit_expression(expression, [&](const auto sub_expression) {
        if (const auto select_expression = std::dynamic_pointer_cast<LQPSelectExpression>(sub_expression)) {
          lqp_find_subplan_roots_impl(root_nodes, visited_nodes, select_expression->lqp);
        }

        return ExpressionVisitation::VisitArguments;
      });
    }

    return LQPVisitation::VisitInputs;
  });
}

}  // namespace

namespace opossum {

LQPNodeMapping lqp_create_node_mapping(const std::shared_ptr<AbstractLQPNode>& lhs,
                                       const std::shared_ptr<AbstractLQPNode>& rhs) {
  LQPNodeMapping mapping;
  lqp_create_node_mapping_impl(mapping, lhs, rhs);
  return mapping;
}

std::optional<LQPMismatch> lqp_find_subplan_mismatch(const std::shared_ptr<const AbstractLQPNode>& lhs,
                                                     const std::shared_ptr<const AbstractLQPNode>& rhs) {
  // Check for type/structural mismatched
  auto mismatch = lqp_find_structure_mismatch(lhs, rhs);
  if (mismatch) return mismatch;

  // For lqp_create_node_mapping() we need mutable pointers - but won't use them to manipulate, promised.
  // It's just that NodeMapping has takes a mutable ptr in as the value type
  const auto mutable_lhs = std::const_pointer_cast<AbstractLQPNode>(lhs);
  const auto mutable_rhs = std::const_pointer_cast<AbstractLQPNode>(rhs);
  const auto node_mapping = lqp_create_node_mapping(mutable_lhs, mutable_rhs);

  return lqp_find_subplan_mismatch_impl(node_mapping, lhs, rhs);
}

void lqp_replace_node(const std::shared_ptr<AbstractLQPNode>& original_node,
                      const std::shared_ptr<AbstractLQPNode>& replacement_node) {
  DebugAssert(replacement_node->outputs().empty(), "Node can't have outputs");
  DebugAssert(!replacement_node->left_input() && !replacement_node->right_input(),
              "Replacement node can't have inputs");

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

void lqp_insert_node(const std::shared_ptr<AbstractLQPNode>& parent_node, const LQPInputSide input_side,
                     const std::shared_ptr<AbstractLQPNode>& node) {
  DebugAssert(node->input_count() == 0 && node->output_count() == 0, "Expected node without inputs and outputs");

  const auto old_input = parent_node->input(input_side);
  parent_node->set_input(input_side, node);
  node->set_left_input(old_input);
}

bool lqp_is_validated(const std::shared_ptr<AbstractLQPNode>& lqp) {
  if (!lqp) return true;
  if (lqp->type == LQPNodeType::Validate) return true;

  if (!lqp->left_input() && !lqp->right_input()) return false;

  return lqp_is_validated(lqp->left_input()) && lqp_is_validated(lqp->right_input());
}

std::set<std::string> lqp_find_modified_tables(const std::shared_ptr<AbstractLQPNode>& lqp) {
  std::set<std::string> modified_tables;

  visit_lqp(lqp, [&](const auto& node) {
    switch (node->type) {
      case LQPNodeType::Insert:
        modified_tables.insert(std::static_pointer_cast<InsertNode>(node)->table_name);
        break;
      case LQPNodeType::Update:
        modified_tables.insert(std::static_pointer_cast<UpdateNode>(node)->table_name);
        break;
      case LQPNodeType::Delete:
        modified_tables.insert(std::static_pointer_cast<DeleteNode>(node)->table_name);
        break;
      case LQPNodeType::CreateTable:
      case LQPNodeType::DropTable:
      case LQPNodeType::Validate:
      case LQPNodeType::Aggregate:
      case LQPNodeType::Alias:
      case LQPNodeType::CreateView:
      case LQPNodeType::DropView:
      case LQPNodeType::DummyTable:
      case LQPNodeType::Join:
      case LQPNodeType::Limit:
      case LQPNodeType::Predicate:
      case LQPNodeType::Projection:
      case LQPNodeType::Root:
      case LQPNodeType::ShowColumns:
      case LQPNodeType::ShowTables:
      case LQPNodeType::Sort:
      case LQPNodeType::StoredTable:
      case LQPNodeType::Union:
      case LQPNodeType::Mock:
        return LQPVisitation::VisitInputs;
    }
    return LQPVisitation::VisitInputs;
  });

  return modified_tables;
}

std::shared_ptr<AbstractExpression> lqp_subplan_to_boolean_expression(const std::shared_ptr<AbstractLQPNode>& lqp) {
  static const auto whitelist = std::set<LQPNodeType>{LQPNodeType::Projection, LQPNodeType::Sort};

  if (whitelist.count(lqp->type)) return lqp_subplan_to_boolean_expression(lqp->left_input());

  switch (lqp->type) {
    case LQPNodeType::Predicate: {
      const auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(lqp);
      const auto left_input_expression = lqp_subplan_to_boolean_expression(lqp->left_input());
      if (left_input_expression) {
        return and_(predicate_node->predicate, left_input_expression);
      } else {
        return predicate_node->predicate;
      }
    }

    case LQPNodeType::Union: {
      const auto union_node = std::dynamic_pointer_cast<UnionNode>(lqp);
      const auto left_input_expression = lqp_subplan_to_boolean_expression(lqp->left_input());
      const auto right_input_expression = lqp_subplan_to_boolean_expression(lqp->right_input());
      if (left_input_expression && right_input_expression) {
        return or_(left_input_expression, right_input_expression);
      } else {
        return nullptr;
      }
    }

    case LQPNodeType::Projection:
    case LQPNodeType::Sort:
      return lqp_subplan_to_boolean_expression(lqp->left_input());

    default:
      return nullptr;
  }
}

std::vector<std::shared_ptr<AbstractLQPNode>> lqp_find_subplan_roots(const std::shared_ptr<AbstractLQPNode>& lqp) {
  auto root_nodes = std::vector<std::shared_ptr<AbstractLQPNode>>{};
  auto visited_nodes = std::unordered_set<std::shared_ptr<AbstractLQPNode>>{};
  lqp_find_subplan_roots_impl(root_nodes, visited_nodes, lqp);
  return root_nodes;
}

}  // namespace opossum
