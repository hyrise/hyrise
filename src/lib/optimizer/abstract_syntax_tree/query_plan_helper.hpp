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

    //    if (node->get_left()) {
    //      filter<T>(node->get_left(), filtered, filter_condition);
    //    }
    //
    //    if (node->get_right()) {
    //      filter<T>(node->get_right(), filtered, filter_condition);
    //    }
  }

  static void traverse(std::shared_ptr<AbstractNode> node,
                       std::function<void(std::shared_ptr<AbstractNode>)> function) {
    function(node);

    //    if (node->get_left()) {
    //      traverse(node->get_left(), function);
    //    }
    //
    //    if (node->get_right()) {
    //      traverse(node->get_right(), function);
    //    }
  }
};

}  // namespace opossum
