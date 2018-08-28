#include "cxlumn_pruning_rule.hpp"

#include <unordered_map>

#include "expression/abstract_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/dummy_table_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/sort_node.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

std::string CxlumnPruningRule::name() const { return "Cxlumn Pruning Rule"; }

bool CxlumnPruningRule::apply_to(const std::shared_ptr<AbstractLQPNode>& lqp) const {
  // Collect the cxlumns that are used in expressions somewhere in the LQP.
  // This EXCLUDES cxlumns that are merely forwarded by Projections throughout the LQP
  auto actually_used_cxlumns = _collect_actually_used_cxlumns(lqp);

  // The output cxlumns of the plan are always considered to be referenced (i.e., we cannot prune them)
  const auto output_cxlumns = lqp->cxlumn_expressions();
  actually_used_cxlumns.insert(output_cxlumns.begin(), output_cxlumns.end());

  // Search for ProjectionNodes that forward the unused cxlumns and prune them accordingly
  _prune_cxlumns_in_projections(lqp, actually_used_cxlumns);

  // Search the plan for leaf nodes and prune all cxlumns from them that are not referenced
  return _prune_cxlumns_from_leafs(lqp, actually_used_cxlumns);
}

ExpressionUnorderedSet CxlumnPruningRule::_collect_actually_used_cxlumns(const std::shared_ptr<AbstractLQPNode>& lqp) {
  auto consumed_cxlumns = ExpressionUnorderedSet{};

  // Search an expression for referenced cxlumns
  const auto collect_consumed_cxlumns_from_expression = [&](const auto& expression) {
    visit_expression(expression, [&](const auto& sub_expression) {
      if (sub_expression->type == ExpressionType::LQPCxlumn) {
        consumed_cxlumns.emplace(sub_expression);
      }
      return ExpressionVisitation::VisitArguments;
    });
  };

  // Search the entire LQP for cxlumns used in AbstractLQPNode::node_expressions(), i.e. cxlumns that are necessary for
  // the "functioning" of the LQP.
  // For ProjectionNodes, ignore forwarded cxlumns (since they would include all cxlumns and we wouldn't be able to
  // prune) by only searching the arguments of expression.
  visit_lqp(lqp, [&](const auto& node) {
    for (const auto& expression : node->node_expressions()) {
      if (node->type == LQPNodeType::Projection) {
        for (const auto& argument : expression->arguments) {
          collect_consumed_cxlumns_from_expression(argument);
        }
      } else {
        collect_consumed_cxlumns_from_expression(expression);
      }
    }
    return LQPVisitation::VisitInputs;
  });

  return consumed_cxlumns;
}

bool CxlumnPruningRule::_prune_cxlumns_from_leafs(const std::shared_ptr<AbstractLQPNode>& lqp,
                                                  const ExpressionUnorderedSet& referenced_cxlumns) {
  auto lqp_changed = false;

  // Collect all parents of leafs and on which input side their leave is (if a node has two leafs as inputs, it will be
  // collected twice)
  auto leaf_parents = std::vector<std::pair<std::shared_ptr<AbstractLQPNode>, LQPInputSide>>{};
  visit_lqp(lqp, [&](auto& node) {
    for (const auto input_side : {LQPInputSide::Left, LQPInputSide::Right}) {
      const auto input = node->input(input_side);
      if (input && input->input_count() == 0) leaf_parents.emplace_back(node, input_side);
    }

    // Do not visit the ProjectionNode we may have just inserted, that would lead to infinite recursion
    return LQPVisitation::VisitInputs;
  });

  // Insert ProjectionNodes that prune unused cxlumns between the leafs and their parents
  for (const auto& parent_and_leaf_input_side : leaf_parents) {
    const auto& parent = parent_and_leaf_input_side.first;
    const auto& leaf_input_side = parent_and_leaf_input_side.second;
    const auto leaf = parent->input(leaf_input_side);

    // Collect all cxlumns from the leaf that are actually referenced
    auto referenced_leaf_cxlumns = std::vector<std::shared_ptr<AbstractExpression>>{};
    for (const auto& expression : leaf->cxlumn_expressions()) {
      if (referenced_cxlumns.find(expression) != referenced_cxlumns.end()) {
        referenced_leaf_cxlumns.emplace_back(expression);
      }
    }

    if (leaf->cxlumn_expressions().size() == referenced_leaf_cxlumns.size()) continue;

    // We cannot have a ProjectionNode that outputs no cxlumns, so let's avoid that
    if (referenced_leaf_cxlumns.empty()) continue;

    // If a leaf outputs cxlumns that are never used, prune those cxlumns by inserting a ProjectionNode that only
    // contains the used cxlumns
    lqp_insert_node(parent, leaf_input_side, ProjectionNode::make(referenced_leaf_cxlumns));
    lqp_changed = true;
  }

  return lqp_changed;
}

void CxlumnPruningRule::_prune_cxlumns_in_projections(const std::shared_ptr<AbstractLQPNode>& lqp,
                                                      const ExpressionUnorderedSet& referenced_cxlumns) {
  /**
   * Prune otherwise unused cxlumns that are forwarded by ProjectionNodes
   */

  // First collect all the ProjectionNodes. Don't prune while visiting because visit_lqp() can't deal with nodes being
  // replaced
  auto projection_nodes = std::vector<std::shared_ptr<ProjectionNode>>{};
  visit_lqp(lqp, [&](auto& node) {
    if (node->type != LQPNodeType::Projection) return LQPVisitation::VisitInputs;
    projection_nodes.emplace_back(std::static_pointer_cast<ProjectionNode>(node));
    return LQPVisitation::VisitInputs;
  });

  // Replace ProjectionNodes with pruned ProjectionNodes if necessary
  for (const auto& projection_node : projection_nodes) {
    auto referenced_projection_expressions = std::vector<std::shared_ptr<AbstractExpression>>{};
    for (const auto& expression : projection_node->node_expressions()) {
      // We keep all non-cxlumn expressions
      if (expression->type != ExpressionType::LQPCxlumn) {
        referenced_projection_expressions.emplace_back(expression);
      } else if (referenced_cxlumns.find(expression) != referenced_cxlumns.end()) {
        referenced_projection_expressions.emplace_back(expression);
      }
    }

    if (projection_node->node_expressions().size() == referenced_projection_expressions.size()) {
      // No cxlumns to prune
      continue;
    }

    // We cannot have a ProjectionNode that outputs no cxlumns
    if (referenced_projection_expressions.empty()) {
      lqp_remove_node(projection_node);
    } else {
      projection_node->expressions = referenced_projection_expressions;
    }
  }
}

}  // namespace opossum
