#include "base_non_query_node.hpp"

#include "utils/assert.hpp"

namespace opossum {

const std::vector<std::shared_ptr<AbstractExpression>>& BaseNonQueryNode::column_expressions() const {
  return _column_expressions_dummy;
}

bool BaseNonQueryNode::is_column_nullable(const ColumnID column_id) const {
  // The majority of non-query nodes output no column (CreateTable, DropTable, ...)
  // Non-query nodes that do return columns (ShowColumns, ...) need to override this function
  Fail("Node does not return any column");
}

}  // namespace opossum
