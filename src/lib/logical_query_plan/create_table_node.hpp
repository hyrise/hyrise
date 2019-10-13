#pragma once

#include <string>

#include "base_non_query_node.hpp"
#include "enable_make_for_lqp_node.hpp"
#include "storage/table_column_definition.hpp"

namespace opossum {

/**
 * This node type represents the CREATE TABLE management command.
 */
class CreateTableNode : public EnableMakeForLQPNode<CreateTableNode>, public BaseNonQueryNode {
 public:
  CreateTableNode(const std::string& table_name, bool if_not_exists);

  std::string description() const override;

  const std::string table_name;
  const bool if_not_exists;

 protected:
  size_t _shallow_hash() const override;
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;
};

}  // namespace opossum
