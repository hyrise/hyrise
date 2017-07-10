#include "predicate_reordering_rule.hpp"

#include <iostream>
#include <memory>
#include <vector>

#include "optimizer/abstract_syntax_tree/query_plan_helper.hpp"

namespace opossum {

std::vector<std::shared_ptr<PredicateNode>> PredicateReorderingRule::find_all_predicates_in_scope(
    std::shared_ptr<AbstractAstNode> node) {
  std::vector<std::shared_ptr<PredicateNode>> nodes;

  QueryPlanHelper::filter<PredicateNode>(
      node, nodes, [&](std::shared_ptr<AbstractAstNode> item) { return item->type() == AstNodeType::Predicate; });

  return nodes;
}

std::shared_ptr<AbstractAstNode> PredicateReorderingRule::apply_rule(std::shared_ptr<AbstractAstNode> node) {
  auto nodes = find_all_predicates_in_scope(node);
  std::cout << "Found " << nodes.size() << " nodes" << std::endl;

  // Reorder predicates

  return node;
}

}  // namespace opossum
