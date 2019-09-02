#pragma once

#include <memory>
#include <string>
#include <vector>

#include "base_non_query_node.hpp"

namespace opossum {

class AbstractExpression;

/**
 * Node type to represent updates (i.e., invalidation and inserts) in a table.
 */
class UpdateNode : public EnableMakeForLQPNode<UpdateNode>, public BaseNonQueryNode {
 public:
  explicit UpdateNode(const std::string& table_name);

  std::string description() const override;
  bool is_column_nullable(const ColumnID column_id) const override;
  const std::vector<std::shared_ptr<AbstractExpression>>& column_expressions() const override;

  const std::string table_name;

 protected:
  size_t _shallow_hash() const override;
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;
};

}  // namespace opossum
