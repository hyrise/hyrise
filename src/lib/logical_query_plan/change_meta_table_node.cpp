#include "change_meta_table_node.hpp"

namespace opossum {

ChangeMetaTableNode::ChangeMetaTableNode(const std::string& init_table_name,
                                         const MetaTableChangeType& init_change_type)
    : BaseNonQueryNode(LQPNodeType::ChangeMetaTable), table_name(init_table_name), change_type(init_change_type) {}

std::string ChangeMetaTableNode::description(const DescriptionMode mode) const {
  std::ostringstream desc;

  desc << "[Change] Meta Table: '" << table_name << "'";

  return desc.str();
}

std::shared_ptr<AbstractLQPNode> ChangeMetaTableNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return ChangeMetaTableNode::make(table_name, change_type);
}

size_t ChangeMetaTableNode::_on_shallow_hash() const {
  auto hash = boost::hash_value(table_name);
  boost::hash_combine(hash, change_type);
  return hash;
}

bool ChangeMetaTableNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& change_meta_table_node = static_cast<const ChangeMetaTableNode&>(rhs);
  return table_name == change_meta_table_node.table_name && change_type == change_meta_table_node.change_type;
}

}  // namespace opossum
