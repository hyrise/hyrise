#include "update_node.hpp"

#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "expression/expression_utils.hpp"
#include "utils/assert.hpp"

namespace opossum {

UpdateNode::UpdateNode(const std::string& table_name) : BaseNonQueryNode(LQPNodeType::Update), table_name(table_name) {}

std::string UpdateNode::description() const {
  std::ostringstream desc;

  desc << "[Update] Table: '" << table_name << "'";

  return desc.str();
}

std::shared_ptr<AbstractLQPNode> UpdateNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return UpdateNode::make(table_name);
}

bool UpdateNode::is_column_nullable(const ColumnID column_id) const { Fail("Update does not output any colums"); }

const std::vector<std::shared_ptr<AbstractExpression>>& UpdateNode::column_expressions() const {
  static std::vector<std::shared_ptr<AbstractExpression>> empty_vector;
  return empty_vector;
}

bool UpdateNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& update_node_rhs = static_cast<const UpdateNode&>(rhs);
  return table_name == update_node_rhs.table_name;
}

}  // namespace opossum
