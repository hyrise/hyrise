#include "base_non_query_node.hpp"

namespace opossum {

const std::vector<std::shared_ptr<AbstractExpression>>& BaseNonQueryNode::cxlumn_expressions() const {
  return _cxlumn_expressions_dummy;
}

}  // namespace opossum
