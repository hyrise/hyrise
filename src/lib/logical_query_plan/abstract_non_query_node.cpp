#include "abstract_non_query_node.hpp"

#include "utils/assert.hpp"

namespace hyrise {

std::vector<std::shared_ptr<AbstractExpression>> AbstractNonQueryNode::output_expressions() const {
  return {};
}

bool AbstractNonQueryNode::is_column_nullable(const ColumnID column_id) const {
  // The majority of non-query nodes output no column (CreateTable, DropTable, ...)
  // Non-query nodes that do return columns (ShowColumns, ...) need to override this function
  Fail("Node does not return any column");
}

std::shared_ptr<UniqueColumnCombinations> AbstractNonQueryNode::unique_column_combinations() const {
  Fail("Node does not support unique constraints.");
}

std::vector<FunctionalDependency> AbstractNonQueryNode::non_trivial_functional_dependencies() const {
  Fail("Node does not support functional dependencies.");
}

}  // namespace hyrise
