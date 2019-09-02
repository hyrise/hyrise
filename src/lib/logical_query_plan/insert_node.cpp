#include "insert_node.hpp"

#include <algorithm>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "utils/assert.hpp"

namespace opossum {

InsertNode::InsertNode(const std::string& table_name) : AbstractLQPNode(LQPNodeType::Insert), table_name(table_name) {}

std::string InsertNode::description() const {
  std::ostringstream desc;

  desc << "[Insert] Into table '" << table_name << "'";

  return desc.str();
}

bool InsertNode::is_column_nullable(const ColumnID column_id) const { Fail("Insert returns no columns"); }

const std::vector<std::shared_ptr<AbstractExpression>>& InsertNode::column_expressions() const {
  static std::vector<std::shared_ptr<AbstractExpression>> empty_vector;
  return empty_vector;
}

size_t InsertNode::_shallow_hash() const { return boost::hash_value(table_name); }

std::shared_ptr<AbstractLQPNode> InsertNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return InsertNode::make(table_name);
}

bool InsertNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& insert_node_rhs = static_cast<const InsertNode&>(rhs);
  return table_name == insert_node_rhs.table_name;
}

}  // namespace opossum
