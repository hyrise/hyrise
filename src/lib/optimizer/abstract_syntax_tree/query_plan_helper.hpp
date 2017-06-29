#pragma once

#include <iostream>
#include <memory>
#include <vector>

#include "optimizer/table_statistics.hpp"

namespace opossum {

class QueryPlanHelper {
 public:
  template <typename T>
  static void filter(std::shared_ptr<AbstractNode> node, std::vector<std::shared_ptr<T>>& filtered,
                     const std::function<bool(std::shared_ptr<AbstractNode>)>& filter_condition) {
    if (filter_condition(node)) {
      filtered.push_back(std::dynamic_pointer_cast<T>(node));
    }

    if (node->get_left()) {
      filter<T>(node->get_left(), filtered, filter_condition);
    }

    if (node->get_right()) {
      filter<T>(node->get_right(), filtered, filter_condition);
    }
  }

  static void traverse(std::shared_ptr<AbstractNode> node, std::function<void(std::shared_ptr<AbstractNode>)> function) {
    function(node);

    if (node->get_left()) {
      traverse(node->get_left(), function);
    }

    if (node->get_right()) {
      traverse(node->get_right(), function);
    }
  }

  static std::shared_ptr<TableStatistics> generate_statistics(std::shared_ptr<AbstractNode> node) {
    if (node->get_statistics()) {
      return node->get_statistics();
    }

    if (node->get_type() == NodeType::TableScanNodeType) {
      auto statistics = generate_statistics(node->get_left());
      auto tableScan = std::dynamic_pointer_cast<TableScanNode>(node);
      return statistics->predicate_statistics(tableScan->column_name(), tableScan->op(), tableScan->value(), tableScan->value2());
    }

    if (node->get_left()) {
      return generate_statistics(node->get_left());
    }

    Assert(node->get_type() == NodeType::TableNodeType, "Last node is not a Table node");
    Assert(static_cast<bool>(node->get_statistics()), "TableNode must contain table statistic");
    return node->get_statistics();
  }
};

}  // namespace opossum
