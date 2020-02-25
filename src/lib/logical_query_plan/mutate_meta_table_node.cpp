#include "mutate_meta_table_node.hpp"

#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "expression/expression_utils.hpp"
#include "utils/assert.hpp"

namespace opossum {

MutateMetaTableNode::MutateMetaTableNode(const std::string& init_table_name,
                                         const MetaTableMutation& init_mutation_type)
    : BaseNonQueryNode(LQPNodeType::MutateMetaTable), table_name(init_table_name), mutation_type(init_mutation_type) {}

std::string MutateMetaTableNode::description(const DescriptionMode mode) const {
  std::ostringstream desc;

  desc << "[Mutate] Meta Table: '" << table_name << "'";

  return desc.str();
}

std::shared_ptr<AbstractLQPNode> MutateMetaTableNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return MutateMetaTableNode::make(table_name, mutation_type);
}

size_t MutateMetaTableNode::_on_shallow_hash() const {
  auto hash = boost::hash_value(table_name);
  boost::hash_combine(hash, mutation_type);
  return hash;
}

bool MutateMetaTableNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& mutate_meta_table_node = static_cast<const MutateMetaTableNode&>(rhs);
  return table_name == mutate_meta_table_node.table_name && mutation_type == mutate_meta_table_node.mutation_type;
}

}  // namespace opossum
