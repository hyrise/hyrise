#pragma once

#include <string>

#include "abstract_non_optimizable_lqp_node.hpp"

namespace opossum {

/**
 * This node type represents the SHOW TABLES management command.
 */
class ShowTablesNode : public AbstractNonOptimizableLQPNode {
 public:
  ShowTablesNode();

  std::string description() const override;
};

}  // namespace opossum
