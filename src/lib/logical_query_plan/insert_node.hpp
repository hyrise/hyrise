#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_lqp_node.hpp"

namespace opossum {

/**
 * Node type to represent insertion of rows into a table.
 */
class InsertNode : public EnableMakeForLQPNode<InsertNode>, public AbstractLQPNode {
 public:
  explicit InsertNode(const std::string& table_name);

  std::string description(const DescriptionMode mode = DescriptionMode::Short) const override;
  bool is_column_nullable(const ColumnID column_id) const override;
  const std::vector<std::shared_ptr<AbstractExpression>>& column_expressions() const override;

  const std::string table_name;

 protected:
  size_t _on_shallow_hash() const override;
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;
};

}  // namespace opossum
