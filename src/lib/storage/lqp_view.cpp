#include "lqp_view.hpp"

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"

namespace opossum {

LQPView::LQPView(const std::shared_ptr<AbstractLQPNode>& lqp,
                 const std::unordered_map<ColumnID, std::string>& column_names)
    : lqp(lqp), column_names(column_names) {}

std::shared_ptr<LQPView> LQPView::deep_copy() const {
  return std::make_shared<LQPView>(lqp->deep_copy(), column_names);
}

bool LQPView::deep_equals(const LQPView& other) const {
  return *lqp == *other.lqp && column_names == other.column_names;
}

}  // namespace opossum
