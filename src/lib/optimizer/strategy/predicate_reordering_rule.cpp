#include "predicate_reordering_rule.hpp"

#include <algorithm>
#include <iostream>
#include <memory>
#include <vector>

#include "constant_mappings.hpp"
#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"
#include "optimizer/abstract_syntax_tree/predicate_node.hpp"
#include "optimizer/table_statistics.hpp"

namespace opossum {

std::shared_ptr<AbstractASTNode> PredicateReorderingRule::apply_rule(std::shared_ptr<AbstractASTNode> node) {
  if (node->type() == ASTNodeType::Predicate) {
    std::vector<std::shared_ptr<PredicateNode>> predicate_nodes;

    while (node->type() == ASTNodeType::Predicate) {
      predicate_nodes.emplace_back(std::dynamic_pointer_cast<PredicateNode>(node));
      node = node->left_child();
    }

    reorder_predicates(predicate_nodes);

    apply_rule(node);
    return predicate_nodes.back();
  } else {
    if (node->left_child()) apply_rule(node->left_child());
    if (node->right_child()) apply_rule(node->right_child());
  }

  return node;
}

const void PredicateReorderingRule::reorder_predicates(std::vector<std::shared_ptr<PredicateNode>>& predicates) const {
  auto parent = predicates.front()->parent();
  auto child = predicates.back()->left_child();
  auto is_left = parent && parent->left_child() == predicates.front();

  std::sort(predicates.begin(), predicates.end(), [&](auto& l, auto& r) {
    return l->create_statistics_from(child)->row_count() < r->create_statistics_from(child)->row_count();
  });

  // Embed in AST hierarchy
  if (parent) {
    if (is_left) {
      parent->set_left_child(predicates.back());
    } else {
      parent->set_right_child(predicates.back());
    }
  }

  predicates.front()->set_left_child(child);

  for (size_t predicate_index = 1; predicate_index < predicates.size(); predicate_index++) {
    predicates[predicate_index]->set_left_child(predicates[predicate_index - 1]);
  }
}

}  // namespace opossum
