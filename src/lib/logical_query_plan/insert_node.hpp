#pragma once

#include <memory>
#include <string>

#include "abstract_non_query_node.hpp"
#include "types.hpp"

namespace hyrise {

/**
 * Node type to represent insertion of rows into a table.
 */
class InsertNode : public EnableMakeForLQPNode<InsertNode>, public AbstractNonQueryNode {
 public:
  explicit InsertNode(const ObjectID init_table_id);

  std::string description(const DescriptionMode mode = DescriptionMode::Short) const override;

  const ObjectID table_id;

 protected:
  size_t _on_shallow_hash() const override;
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& /*node_mapping*/) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& /*node_mapping*/) const override;
};

}  // namespace hyrise
