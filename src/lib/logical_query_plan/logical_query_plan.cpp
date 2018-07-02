#include "logical_query_plan.hpp"

namespace opossum {

LogicalQueryPlan::LogicalQueryPlan(const std::shared_ptr<AbstractLQPNode>& root_node,
                                   const std::unordered_map<ValuePlaceholderID, ParameterID>& value_placeholders)
    : root_node(root_node), value_placeholders(value_placeholders) {}

}  // namespace opossum
