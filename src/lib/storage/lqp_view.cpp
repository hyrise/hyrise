#include "lqp_view.hpp"

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"

namespace opossum {

LQPView::LQPView(const std::shared_ptr<AbstractLQPNode>& lqp,
                 const std::unordered_map<CxlumnID, std::string>& cxlumn_names)
    : lqp(lqp), cxlumn_names(cxlumn_names) {}

std::shared_ptr<LQPView> LQPView::deep_copy() const {
  return std::make_shared<LQPView>(lqp->deep_copy(), cxlumn_names);
}

bool LQPView::deep_equals(const LQPView& other) const {
  return *lqp == *other.lqp && cxlumn_names == other.cxlumn_names;
}

}  // namespace opossum
