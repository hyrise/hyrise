#include "validate_node.hpp"

#include <string>

namespace opossum {

ValidateNode::ValidateNode() : AbstractLogicalQueryPlanNode(LQPNodeType::Validate) {}

std::string ValidateNode::description() const { return "[Validate]"; }

}  // namespace opossum
