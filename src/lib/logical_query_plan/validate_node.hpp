#pragma once

#include <string>

#include "abstract_logical_query_plan_node.hpp"

namespace opossum {

/**
 * This node type represents validating tables with the Validate operator.
 */
class ValidateNode : public AbstractLogicalQueryPlanNode {
 public:
  ValidateNode();

  std::string description() const override;
};

}  // namespace opossum
