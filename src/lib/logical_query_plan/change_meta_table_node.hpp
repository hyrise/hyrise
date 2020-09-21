#pragma once

#include <memory>
#include <string>
#include <vector>

#include "base_non_query_node.hpp"

namespace opossum {

class AbstractExpression;

/*
 * Node type to represent modifications of a meta table.
 * The parameters are the name of the modified meta table and the type of the modification (insert, delete, update).
 * This node needs two input nodes: one with the selected values (for update or delete)
 * and one with updated or inserted values.
 * Further documentation for the resulting operator's inputs can be found at operators/change_meta_table.hpp
 */
class ChangeMetaTableNode : public EnableMakeForLQPNode<ChangeMetaTableNode>, public BaseNonQueryNode {
 public:
  explicit ChangeMetaTableNode(const std::string& init_table_name, const MetaTableChangeType& init_change_type);

  std::string description(const DescriptionMode mode = DescriptionMode::Short) const override;

  const std::string table_name;
  const MetaTableChangeType change_type;

 protected:
  size_t _on_shallow_hash() const override;
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;
};

}  // namespace opossum
