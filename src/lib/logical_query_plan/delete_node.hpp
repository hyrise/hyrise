#pragma once

#include <memory>
#include <string>

#include "abstract_non_query_node.hpp"

namespace opossum {

/**
 * Node type to represent deletion (more specifically, invalidation) in a table.
 */
class DeleteNode : public EnableMakeForLQPNode<DeleteNode>, public AbstractNonQueryNode {
 public:
  DeleteNode();

  std::string description(const DescriptionMode mode = DescriptionMode::Short) const override;
  bool is_column_nullable(const ColumnID column_id) const override;
  std::vector<std::shared_ptr<AbstractExpression>> output_expressions() const override;

 protected:
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;
};

}  // namespace opossum
