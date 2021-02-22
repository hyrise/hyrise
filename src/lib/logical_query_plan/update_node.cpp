#include "update_node.hpp"

#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "expression/expression_utils.hpp"
#include "utils/assert.hpp"

namespace opossum {

UpdateNode::UpdateNode(const std::shared_ptr<HyriseEnvironmentRef>& init_hyrise_env, const std::string& init_table_name)
    : AbstractNonQueryNode(LQPNodeType::Update), hyrise_env(init_hyrise_env), table_name(init_table_name) {}

std::string UpdateNode::description(const DescriptionMode mode) const {
  std::ostringstream desc;

  desc << "[Update] Table: '" << table_name << "'";

  return desc.str();
}

std::shared_ptr<AbstractLQPNode> UpdateNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return UpdateNode::make(hyrise_env, table_name);
}

bool UpdateNode::is_column_nullable(const ColumnID column_id) const { Fail("Update does not output any colums"); }

std::vector<std::shared_ptr<AbstractExpression>> UpdateNode::output_expressions() const { return {}; }

size_t UpdateNode::_on_shallow_hash() const {
  auto hash = boost::hash_value(hyrise_env);
  boost::hash_combine(hash, table_name);
  return hash;
}

bool UpdateNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& update_node_rhs = static_cast<const UpdateNode&>(rhs);
  return hyrise_env == update_node_rhs.hyrise_env && table_name == update_node_rhs.table_name;
}

}  // namespace opossum
