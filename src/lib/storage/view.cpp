#include "view.hpp"

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"

namespace opossum {

View::View(const std::shared_ptr<AbstractLQPNode>& lqp,
                       const std::unordered_map<ColumnID, std::string>& column_names):
  lqp(lqp),
  column_names(column_names) {}

std::shared_ptr<View> View::deep_copy() const {
  return std::make_shared<View>(lqp->deep_copy(), column_names);
}

bool View::deep_equals(const View& other) const {
  return !lqp_find_subplan_mismatch(lqp, other.lqp) && column_names == other.column_names;
}

}  // namespace opossum
