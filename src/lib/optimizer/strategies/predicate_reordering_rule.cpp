#include "predicate_reordering_rule.hpp"

#include <iostream>
#include <memory>
#include <vector>

#include "optimizer/abstract_syntax_tree/query_plan_helper.hpp"

namespace opossum {


std::shared_ptr<AbstractNode> PredicateReorderingRule::apply_rule(std::shared_ptr<AbstractNode> node) {

  if (node->type() == NodeType::TableScanNodeType) {
    std::vector<std::shared_ptr<TableScanNode>> table_scan_nodes;

    auto current_node = node;

    while (current_node->type() == NodeType::TableScanNodeType) {
      table_scan_nodes.emplace_back(std::dynamic_pointer_cast<TableScanNode>(current_node));
      current_node = current_node->left();
    }

    reorder_table_scans(table_scan_nodes);

    apply_rule(current_node);
    return table_scan_nodes.back();
  } else {
    if (node->left()) apply_rule(node->left());
    if (node->right()) apply_rule(node->right());
  }

  return node;
}

void PredicateReorderingRule::reorder_table_scans(std::vector<std::shared_ptr<TableScanNode>> & table_scans) {
  auto parent = table_scans.front()->parent().lock();
  auto child = table_scans.back()->left();
  auto is_left = parent && parent->left() == table_scans.front();

  std::sort(table_scans.begin(), table_scans.end(), [] (auto & l, auto & r) {
    return l->statistics()->row_count() < r->statistics()->row_count();
  });

  if (parent) {
    if (is_left) parent->set_left(table_scans.back());
    else parent->set_right(table_scans.back());
  }

  table_scans.front()->set_left(child);

  for (size_t table_scan_idx = 1; table_scan_idx < table_scans.size(); table_scan_idx++) {
    table_scans[table_scan_idx]->set_left(table_scans[table_scan_idx - 1]);
  }

}

}  // namespace opossum
