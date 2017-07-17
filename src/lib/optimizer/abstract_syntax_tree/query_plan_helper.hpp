#pragma once

#include <iostream>
#include <memory>
#include <vector>

#include "abstract_ast_node.hpp"

namespace opossum {

class QueryPlanHelper {
 public:
  template <typename T>
  static void filter(std::shared_ptr<AbstractASTNode> node, std::vector<std::shared_ptr<T>>& filtered,
                     const std::function<bool(std::shared_ptr<AbstractASTNode>)>& filter_condition) {
    if (filter_condition(node)) {
      filtered.push_back(std::dynamic_pointer_cast<T>(node));
    }

    if (node->left_child()) {
      filter<T>(node->left_child(), filtered, filter_condition);
    }

    if (node->right_child()) {
      filter<T>(node->right_child(), filtered, filter_condition);
    }
  }
};

}  // namespace opossum
