#include "insert_node.hpp"

#include <cstddef>
#include <functional>
#include <memory>
#include <sstream>
#include <string>

#include <boost/container_hash/hash.hpp>

#include "hyrise.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/abstract_non_query_node.hpp"
#include "types.hpp"

namespace hyrise {

InsertNode::InsertNode(const ObjectID init_table_id)
    : AbstractNonQueryNode(LQPNodeType::Insert), table_id{init_table_id} {}

std::string InsertNode::description(const DescriptionMode /*mode*/) const {
  auto desc = std::ostringstream{};
  desc << "[Insert] Into table '" << Hyrise::get().catalog.table_name(table_id) << "'";
  return desc.str();
}

size_t InsertNode::_on_shallow_hash() const {
  auto hash_value = size_t{0};
  boost::hash_combine(hash_value, table_id);
  return hash_value;
}

std::shared_ptr<AbstractLQPNode> InsertNode::_on_shallow_copy(LQPNodeMapping& /*node_mapping*/) const {
  return InsertNode::make(table_id);
}

bool InsertNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& /*node_mapping*/) const {
  const auto& insert_node_rhs = static_cast<const InsertNode&>(rhs);
  return table_id == insert_node_rhs.table_id;
}

}  // namespace hyrise
