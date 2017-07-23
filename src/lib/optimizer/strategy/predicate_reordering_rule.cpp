#include "predicate_reordering_rule.hpp"

#include <algorithm>
#include <iostream>
#include <memory>
#include <vector>

#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"

namespace opossum {

std::shared_ptr<AbstractASTNode> PredicateReorderingRule::apply_rule(std::shared_ptr<AbstractASTNode> node) {
  if (node->type() == ASTNodeType::Predicate) {
    std::vector<std::shared_ptr<PredicateNode>> predicate_nodes;

    auto current_node = node;

    while (current_node->type() == ASTNodeType::Predicate) {
      predicate_nodes.emplace_back(std::dynamic_pointer_cast<PredicateNode>(current_node));
      current_node = current_node->left_child();
    }

    reorder_predicates(predicate_nodes);

    apply_rule(current_node);
    return predicate_nodes.back();
  } else {
    if (node->left_child()) apply_rule(node->left_child());
    if (node->right_child()) apply_rule(node->right_child());
  }

  return node;
}

void PredicateReorderingRule::reorder_predicates(std::vector<std::shared_ptr<PredicateNode>>& predicates) {
  auto parent = predicates.front()->parent();
  auto child = predicates.back()->left_child();
  auto is_left = parent && parent->left_child() == predicates.front();

  std::sort(predicates.begin(), predicates.end(),
            [](auto& l, auto& r) { return l->statistics()->row_count() < r->statistics()->row_count(); });

  if (parent) {
    if (is_left)
      parent->set_left_child(predicates.back());
    else
      parent->set_right_child(predicates.back());
  }

  predicates.front()->set_left_child(child);

  for (size_t table_scan_idx = 1; table_scan_idx < predicates.size(); table_scan_idx++) {
    predicates[table_scan_idx]->set_left_child(predicates[table_scan_idx - 1]);
  }
}

}  // namespace opossum
