#include "predicate_reordering_rule.hpp"

#include <algorithm>
#include <iostream>
#include <memory>
#include <vector>

#include "constant_mappings.hpp"
#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"
#include "optimizer/abstract_syntax_tree/predicate_node.hpp"
#include "optimizer/table_statistics.hpp"
#include "utils/assert.hpp"

namespace opossum {

std::string PredicateReorderingRule::name() const {
  return "Predicate Reordering Rule";
}

bool PredicateReorderingRule::apply_to(const std::shared_ptr<AbstractASTNode>& node) {
  auto reordered = false;

  if (node->type() == ASTNodeType::Predicate) {
    std::vector<std::shared_ptr<PredicateNode>> predicate_nodes;

    // Gather adjacent PredicateNodes
    auto current_node = node;
    while (current_node->type() == ASTNodeType::Predicate) {
      predicate_nodes.emplace_back(std::dynamic_pointer_cast<PredicateNode>(current_node));
      current_node = current_node->left_child();
    }

    // Sort PredicateNodes in descending order with regards to the expected row_count
    if (predicate_nodes.size() > 1) {
      reordered = _reorder_predicates(predicate_nodes);
    }
    reordered |= _apply_to_children(predicate_nodes.back());
  } else {
    reordered = _apply_to_children(node);
  }

  return reordered;
}

bool PredicateReorderingRule::_reorder_predicates(std::vector<std::shared_ptr<PredicateNode>>& predicates) const {
  // Store original child and parent
  auto child = predicates.back()->left_child();
  auto parent = predicates.front()->parent();

  const auto sort_predicate = [&](auto& l, auto& r) {
    return l->derive_statistics_from(child)->row_count() > r->derive_statistics_from(child)->row_count();
  };

  if (std::is_sorted(predicates.begin(), predicates.end(), sort_predicate)) {
    return false;
  }

  // Sort in descending order
  std::sort(predicates.begin(), predicates.end(), sort_predicate);

  // Ensure that nodes are chained correctly
  predicates.back()->set_left_child(child);
  parent->set_left_child(predicates.front());

  for (size_t predicate_index = 0; predicate_index < predicates.size() - 1; predicate_index++) {
    predicates[predicate_index]->set_left_child(predicates[predicate_index + 1]);
  }

  return true;
}

}  // namespace opossum
