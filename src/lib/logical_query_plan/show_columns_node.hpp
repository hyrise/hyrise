#pragma once

#include <string>

#include "abstract_non_optimizable_lqp_node.hpp"

namespace opossum {

/**
 * This node type represents the SHOW COLUMNS management command.
 */
class ShowColumnsNode : public AbstractNonOptimizableLQPNode {
 public:
  explicit ShowColumnsNode(const std::string& table_name);

  std::string description() const override;

  const std::string& table_name() const;

 private:
  const std::string _table_name;
};

}  // namespace opossum
