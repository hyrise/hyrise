#include "predicate_reordering_rule.hpp"

#include <algorithm>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "constant_mappings.hpp"
#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"
#include "optimizer/abstract_syntax_tree/predicate_node.hpp"
#include "optimizer/table_statistics.hpp"
#include "utils/assert.hpp"

namespace opossum {

std::string PredicateReorderingRule::name() const { return "Predicate Reordering Rule"; }

bool PredicateReorderingRule::apply_to(const std::shared_ptr<AbstractASTNode>& node) {
  auto reordered = false;

  if (node->type() == ASTNodeType::Predicate) {
    std::vector<std::shared_ptr<PredicateNode>> predicate_nodes;

    // Gather adjacent PredicateNodes
    auto current_node = node;
    while (current_node->type() == ASTNodeType::Predicate) {
      // Once a node has multiple parents, we're not talking about a Predicate chain anymore
      if (current_node->parents().size() > 1) {
        break;
      }

      predicate_nodes.emplace_back(std::dynamic_pointer_cast<PredicateNode>(current_node));
      current_node = current_node->left_child();
    }

    /**
     * A chain of predicates was found.
     * Sort PredicateNodes in descending order with regards to the expected row_count
     * Continue rule in deepest child
     */
    if (predicate_nodes.size() > 1) {
      reordered = _reorder_predicates(predicate_nodes);
      reordered |= _apply_to_children(predicate_nodes.back());
    } else {
      // No chain was found, continue with the current nodes children.
      reordered = _apply_to_children(node);
    }
  } else {
    reordered = _apply_to_children(node);
  }

  return reordered;
}

bool PredicateReorderingRule::_reorder_predicates(std::vector<std::shared_ptr<PredicateNode>>& predicates) const {
  // Store original child and parent
  auto child = predicates.back()->left_child();
  const auto parents = predicates.front()->parents();
  const auto child_sides = predicates.front()->get_child_sides();

  const auto sort_predicate = [&](auto& left, auto& right) {
    return left->derive_statistics_from(child)->row_count() > right->derive_statistics_from(child)->row_count();
  };

  if (std::is_sorted(predicates.begin(), predicates.end(), sort_predicate)) {
    return false;
  }

  // Untie predicates from AST, so we can freely retie them
  for (auto& predicate : predicates) {
    predicate->remove_from_tree();
  }

  // Sort in descending order
  std::sort(predicates.begin(), predicates.end(), sort_predicate);

  // Ensure that nodes are chained correctly
  predicates.back()->set_left_child(child);

  for (size_t parent_idx = 0; parent_idx < parents.size(); ++parent_idx) {
    parents[parent_idx]->set_child(child_sides[parent_idx], predicates.front());
  }

  for (size_t predicate_index = 0; predicate_index < predicates.size() - 1; predicate_index++) {
    predicates[predicate_index]->set_left_child(predicates[predicate_index + 1]);
  }

  return true;
}

}  // namespace opossum
