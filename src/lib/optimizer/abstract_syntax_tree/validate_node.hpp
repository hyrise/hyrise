#pragma once

#include <string>

#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"

namespace opossum {

/**
 * This node type represents validating tables with the Validate operator.
 */
class ValidateNode : public AbstractLogicalPlanNode {
 public:
  ValidateNode();

  std::string description() const override;
};

}  // namespace opossum
