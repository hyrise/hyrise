#include "column_origin.hpp"

#include "abstract_lqp_node.hpp"
#include "utils/assert.hpp"

namespace opossum {

ColumnOrigin::ColumnOrigin(const std::shared_ptr<const AbstractLQPNode>& node, ColumnID column_id):
  node(node), column_id(column_id) {}

bool ColumnOrigin::operator==(const ColumnOrigin& rhs) const {
  return node == rhs.node && column_id == rhs.column_id;
}

std::string ColumnOrigin::get_verbose_name() const {
  DebugAssert(node && column_id != INVALID_COLUMN_ID, "ColumnOrigin state not sufficient to retrieve column name");
  return node->get_verbose_column_name(column_id);
}

}