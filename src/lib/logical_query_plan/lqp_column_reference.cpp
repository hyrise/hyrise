#include "lqp_column_reference.hpp"

#include "abstract_lqp_node.hpp"
#include "utils/assert.hpp"

namespace opossum {

LQPColumnReference::LQPColumnReference(const std::shared_ptr<const AbstractLQPNode>& original_node,
                                       ColumnID original_column_id)
    : _original_node(original_node), _original_column_id(original_column_id) {}

std::shared_ptr<const AbstractLQPNode> LQPColumnReference::original_node() const { return _original_node.lock(); }

ColumnID LQPColumnReference::original_column_id() const { return _original_column_id; }

std::string LQPColumnReference::description() const {
  const auto node = this->original_node();

  DebugAssert(node, "LQPColumnReference state not sufficient to retrieve column name");
  return node->get_verbose_column_name(_original_column_id);
}

bool LQPColumnReference::operator==(const LQPColumnReference& rhs) const {
  return original_node() == rhs.original_node() && _original_column_id == rhs._original_column_id;
}

std::ostream& operator<<(std::ostream& os, const LQPColumnReference& column_reference) {
  const auto node = column_reference.original_node();

  if (column_reference.original_node()) {
    os << column_reference.description();
  } else {
    os << "[Invalid LQPColumnReference, ColumnID:" << column_reference.original_column_id() << "]";
  }
  return os;
}
}  // namespace opossum
