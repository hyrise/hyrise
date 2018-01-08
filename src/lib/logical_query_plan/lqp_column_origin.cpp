#include "lqp_column_origin.hpp"

#include "abstract_lqp_node.hpp"
#include "utils/assert.hpp"

namespace opossum {

LQPColumnOrigin::LQPColumnOrigin(const std::shared_ptr<const AbstractLQPNode>& node, ColumnID column_id)
    : _node(node), _column_id(column_id) {}

std::shared_ptr<const AbstractLQPNode> LQPColumnOrigin::node() const { return _node.lock(); }

ColumnID LQPColumnOrigin::column_id() const { return _column_id; }

std::string LQPColumnOrigin::description() const {
  const auto node = this->node();

  DebugAssert(node, "LQPColumnOrigin state not sufficient to retrieve column name");
  return node->get_verbose_column_name(_column_id);
}

bool LQPColumnOrigin::operator==(const LQPColumnOrigin& rhs) const {
  return node() == rhs.node() && _column_id == rhs._column_id;
}

std::ostream& operator<<(std::ostream& os, const LQPColumnOrigin& column_origin) {
  const auto node = column_origin.node();

  if (column_origin.node()) {
    os << column_origin.description();
  } else {
    os << "[Invalid LQPColumnOrigin, ColumnID:" << column_origin.column_id() << "]";
  }
  return os;
}
}  // namespace opossum
