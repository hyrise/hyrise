#pragma once

#include <memory>
#include <string>

#include "abstract_non_query_node.hpp"

namespace hyrise {

class AbstractExpression;

/**
 * Node to represent modifications of a meta table. The parameters are the name of the modified meta table and the type
 * of the modification (insert, delete, update).
 *
 * Depending of the change type, this node needs one or two input nodes: one for inserts and deletes, two for updates.
 * Further documentation of the resulting operator's inputs can be found at `operators/change_meta_table.hpp`.
 */
class ChangeMetaTableNode : public EnableMakeForLQPNode<ChangeMetaTableNode>, public AbstractNonQueryNode {
 public:
  explicit ChangeMetaTableNode(const std::string& init_table_name, const MetaTableChangeType& init_change_type);

  std::string description(const DescriptionMode mode = DescriptionMode::Short) const override;

  const std::string table_name;
  const MetaTableChangeType change_type;

 protected:
  size_t _on_shallow_hash() const override;
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& /*node_mapping*/) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& /*node_mapping*/) const override;
};

}  // namespace hyrise
