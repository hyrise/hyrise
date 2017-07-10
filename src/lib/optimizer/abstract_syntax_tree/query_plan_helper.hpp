#pragma once

#include <iostream>
#include <memory>
#include <vector>

namespace opossum {

class QueryPlanHelper {
 public:
  template <typename T>
  static void filter(std::shared_ptr<AbstractAstNode> node, std::vector<std::shared_ptr<T>>& filtered,
                     const std::function<bool(std::shared_ptr<AbstractAstNode>)>& filter_condition) {
    if (filter_condition(node)) {
      filtered.push_back(std::dynamic_pointer_cast<T>(node));
    }

    if (node->left()) {
      filter<T>(node->left(), filtered, filter_condition);
    }

    if (node->right()) {
      filter<T>(node->right(), filtered, filter_condition);
    }
  }
};

}  // namespace opossum
