#include "predicate_reordering_rule.hpp"

#include <iostream>
#include <memory>
#include <vector>

#include "optimizer/abstract_syntax_tree/query_plan_helper.hpp"

namespace opossum {

std::vector<std::shared_ptr<TableScanNode>> PredicateReorderingRule::find_all_predicates_in_scope(
    std::shared_ptr<AbstractNode> node) {
  std::vector<std::shared_ptr<TableScanNode>> nodes;

  QueryPlanHelper::filter<TableScanNode>(
      node, nodes, [&](std::shared_ptr<AbstractNode> item) { return item->type() == NodeType::TableScan; });

  return nodes;
}

std::shared_ptr<AbstractNode> PredicateReorderingRule::apply_rule(std::shared_ptr<AbstractNode> node) {
  auto nodes = find_all_predicates_in_scope(node);
  std::cout << "Found " << nodes.size() << " nodes" << std::endl;

  // Reorder predicates

  return node;
}

}  // namespace opossum
