#include "column_origin.hpp"

#include "abstract_lqp_node.hpp"
#include "utils/assert.hpp"

namespace opossum {

ColumnOrigin::ColumnOrigin(const std::shared_ptr<const AbstractLQPNode>& node, ColumnID column_id)
    : _node(node), _column_id(column_id) {}

std::shared_ptr<const AbstractLQPNode> ColumnOrigin::node() const { return _node.lock(); }

ColumnID ColumnOrigin::column_id() const { return _column_id; }

std::string ColumnOrigin::get_verbose_name() const {
  const auto node = this->node();

  DebugAssert(node, "ColumnOrigin state not sufficient to retrieve column name");
  return node->get_verbose_column_name(_column_id);
}

bool ColumnOrigin::operator==(const ColumnOrigin& rhs) const {
  return node() == rhs.node() && _column_id == rhs._column_id;
}

std::ostream& operator<<(std::ostream& os, const ColumnOrigin& column_origin) {
  const auto node = column_origin.node();

  if (column_origin.node()) {
    os << column_origin.get_verbose_name();
  } else {
    os << "[Invalid ColumnOrigin, ColumnID:" << column_origin.column_id() << "]";
  }
  return os;
}
}

opossum::ColumnID foo() {
  return opossum::ColumnID{0};
}