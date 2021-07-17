#pragma once

#include <string>

#include "abstract_non_query_node.hpp"
#include "enable_make_for_lqp_node.hpp"
#include "storage/table_column_definition.hpp"
#include "stored_table_node.hpp"

namespace opossum {

/**
 * This node type represents the DROP INDEX management command.
 */
class DropIndexNode : public EnableMakeForLQPNode<DropIndexNode>, public AbstractNonQueryNode {
 public:
  DropIndexNode(const std::string& init_index_name, const bool init_if_exists, const std::string& init_table_name);

  std::string description(const DescriptionMode mode = DescriptionMode::Short) const override;

  const std::string index_name;
  const bool if_exists;
  const std::string table_name;


 protected:
  size_t _on_shallow_hash() const override;
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;
};

}  // namespace opossum
