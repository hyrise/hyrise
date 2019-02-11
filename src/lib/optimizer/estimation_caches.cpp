#include "estimation_caches.hpp"

#include "expression/abstract_expression.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_subquery_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"

namespace opossum {

void OptimizationContext::clear_caches() {
  join_statistics_cache.reset();
  plan_statistics_cache.reset();
  plan_cost_cache.reset();
}

}  // namespace opossum