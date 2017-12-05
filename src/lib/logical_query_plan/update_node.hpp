#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_lqp_node.hpp"

namespace opossum {

/**
 * Node type to represent updates (i.e., invalidation and inserts) in a table.
 */
class UpdateNode : public AbstractLQPNode {
 public:
  explicit UpdateNode(const std::string& table_name,
                      const std::vector<std::shared_ptr<Expression>>& column_expressions);

  std::string description() const override;
  bool subtree_is_read_only() const override;

  const std::string& table_name() const;

  const std::vector<std::shared_ptr<Expression>>& column_expressions() const;

 protected:
  std::shared_ptr<AbstractLQPNode> _deep_copy_impl() const override;
  const std::string _table_name;
  std::vector<std::shared_ptr<Expression>> _column_expressions;
};

}  // namespace opossum
