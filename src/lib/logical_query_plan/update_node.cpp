#include "update_node.hpp"

#include <cstddef>
#include <functional>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include <boost/container_hash/hash.hpp>

#include "expression/expression_utils.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/abstract_non_query_node.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

UpdateNode::UpdateNode(const ObjectID init_table_id)
    : AbstractNonQueryNode(LQPNodeType::Update), table_id(init_table_id) {}

std::string UpdateNode::description(const DescriptionMode /*mode*/) const {
  const auto& table_name = Hyrise::get().catalog.table_name(table_id);
  auto desc = std::ostringstream{};
  desc << "[Update] Table: '" << table_name << "'";
  return desc.str();
}

std::shared_ptr<AbstractLQPNode> UpdateNode::_on_shallow_copy(LQPNodeMapping& /*node_mapping*/) const {
  return UpdateNode::make(table_id);
}

bool UpdateNode::is_column_nullable(const ColumnID /*column_id*/) const {
  Fail("Update does not output any columns.");
}

std::vector<std::shared_ptr<AbstractExpression>> UpdateNode::output_expressions() const {
  return {};
}

size_t UpdateNode::_on_shallow_hash() const {
  auto hash_value = size_t{0};
  boost::hash_combine(hash_value, table_id);
  return hash_value;
}

bool UpdateNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& /*node_mapping*/) const {
  const auto& update_node_rhs = static_cast<const UpdateNode&>(rhs);
  return table_id == update_node_rhs.table_id;
}

}  // namespace hyrise
