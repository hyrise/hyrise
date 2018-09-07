#include "base_non_query_node.hpp"

namespace opossum {

const std::vector<std::shared_ptr<AbstractExpression>>& BaseNonQueryNode::column_expressions() const {
  return _column_expressions_dummy;
}

}  // namespace opossum
