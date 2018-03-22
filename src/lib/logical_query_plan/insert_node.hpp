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
  explicit InsertNode(const std::string table_name);

  std::string description() const override;
  bool subplan_is_read_only() const override;

  bool shallow_equals(const AbstractLQPNode& rhs) const override;

  const std::string& table_name() const;

 protected:
  std::shared_ptr<AbstractLQPNode> _deep_copy_impl(
      const std::shared_ptr<AbstractLQPNode>& copied_left_input,
      const std::shared_ptr<AbstractLQPNode>& copied_right_input) const override;
  const std::string _table_name;
};

}  // namespace opossum
