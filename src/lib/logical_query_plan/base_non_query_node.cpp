#include "base_non_query_node.hpp"

#include "utils/assert.hpp"

namespace opossum {

const std::vector<std::shared_ptr<AbstractExpression>>& BaseNonQueryNode::column_expressions() const {
  return _column_expressions_dummy;
}

bool BaseNonQueryNode::is_column_nullable(const ColumnID column_id) const {
  // Default behaviour for non-query nodes is to output no column (CreateTable, DropTable, ...)
  Fail("Node does not return any column");
}

}  // namespace opossum
