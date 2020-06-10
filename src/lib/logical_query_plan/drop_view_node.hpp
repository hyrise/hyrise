#pragma once

#include <memory>
#include <string>

#include "abstract_non_query_node.hpp"

#include "enable_make_for_lqp_node.hpp"
#include "operators/abstract_operator.hpp"
#include "statistics/table_statistics.hpp"
#include "types.hpp"

namespace opossum {

/**
 * Node type to represent deleting a view from the StorageManager
 */
class DropViewNode : public EnableMakeForLQPNode<DropViewNode>, public AbstractNonQueryNode {
 public:
  DropViewNode(const std::string& init_view_name, bool init_if_exists);

  std::string description(const DescriptionMode mode = DescriptionMode::Short) const override;

  const std::string view_name;
  const bool if_exists;

  OperatorType operator_type() const override { return OperatorType::DropView; }

 protected:
  size_t _on_shallow_hash() const override;
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;
};

}  // namespace opossum
