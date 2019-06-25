#include "with_view.hpp"

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"

namespace opossum {

WithView::WithView(const std::shared_ptr<AbstractLQPNode>& lqp,
                   const std::unordered_multimap<ColumnID, std::string>& column_names)
    : lqp(lqp), column_names(column_names) {}

std::shared_ptr<WithView> WithView::deep_copy() const {
  return std::make_shared<WithView>(lqp->deep_copy(), column_names);
}

bool WithView::deep_equals(const WithView& other) const {
  return *lqp == *other.lqp && column_names == other.column_names;
}

}  // namespace opossum