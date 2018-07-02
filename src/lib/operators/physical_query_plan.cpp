#include "physical_query_plan.hpp"

namespace opossum {

PhysicalQueryPlan::PhysicalQueryPlan(const std::shared_ptr<AbstractOperator>& root_op,
                                     const std::unordered_map<ValuePlaceholderID, ParameterID>& value_placeholders)
    : root_op(root_op), value_placeholders(value_placeholders) {}

}  // namespace opossum
