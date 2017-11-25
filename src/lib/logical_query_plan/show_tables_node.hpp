#pragma once

#include <string>

#include "abstract_lqp_node.hpp"

namespace opossum {

/**
 * This node type represents the SHOW TABLES management command.
 */
class ShowTablesNode : public AbstractLQPNode {
 public:
  ShowTablesNode();

  std::string description() const override;
};

}  // namespace opossum
