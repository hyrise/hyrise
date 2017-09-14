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

const std::shared_ptr<AbstractASTNode> PredicateReorderingRule::apply_to(const std::shared_ptr<AbstractASTNode> & node) {
  auto result_node = node;

  if (node->type() == ASTNodeType::Predicate) {
    std::vector<std::shared_ptr<PredicateNode>> predicate_nodes;

    // Gather adjacent PredicateNodes
    auto current_node = node;
    while (current_node->type() == ASTNodeType::Predicate) {
      predicate_nodes.emplace_back(std::dynamic_pointer_cast<PredicateNode>(current_node));
      current_node = current_node->left_child();
    }

    // Sort PredicateNodes in descending order with regards to the expected row_count
    _reorder_predicates(predicate_nodes);

    result_node = predicate_nodes.front();
  }

  apply_to_children(result_node);
  return result_node;
}

void PredicateReorderingRule::_reorder_predicates(std::vector<std::shared_ptr<PredicateNode>>& predicates) const {
  // Store original child
  auto child = predicates.back()->left_child();

  // Sort in descending order
  std::sort(predicates.begin(), predicates.end(), [&](auto& l, auto& r) {
    return l->get_statistics_from(child)->row_count() > r->get_statistics_from(child)->row_count();
  });

  // Ensure that nodes are chained correctly
  predicates.back()->set_left_child(child);

  for (size_t predicate_index = 0; predicate_index < predicates.size() - 1; predicate_index++) {
    predicates[predicate_index]->set_left_child(predicates[predicate_index + 1]);
  }
}

}  // namespace opossum
