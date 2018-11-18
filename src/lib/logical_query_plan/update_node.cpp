#include "update_node.hpp"

#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "expression/expression_utils.hpp"
#include "utils/assert.hpp"

namespace opossum {

UpdateNode::UpdateNode(const std::string& table_name,
                       const std::vector<std::shared_ptr<AbstractExpression>>& update_column_expressions)
    : AbstractLQPNode(LQPNodeType::Update, update_column_expressions), table_name(table_name) {}

std::string UpdateNode::description() const {
  std::ostringstream desc;

  desc << "[Update] Table: '" << table_name << "'";
  desc << " Columns: " << expression_column_names(node_expressions);

  return desc.str();
}

std::shared_ptr<AbstractLQPNode> UpdateNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return UpdateNode::make(table_name, expressions_copy_and_adapt_to_different_lqp(node_expressions, node_mapping));
}

bool UpdateNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& update_node_rhs = static_cast<const UpdateNode&>(rhs);
  return table_name == update_node_rhs.table_name &&
         expressions_equal_to_expressions_in_different_lqp(node_expressions, update_node_rhs.node_expressions,
                                                           node_mapping);
}

}  // namespace opossum
