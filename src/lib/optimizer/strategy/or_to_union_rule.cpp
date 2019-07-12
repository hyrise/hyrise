#include "or_to_union_rule.hpp"

//#include <algorithm>
//#include <iostream>
//#include <memory>
//#include <string>
//#include <vector>
//
//#include "constant_mappings.hpp"
//#include "cost_estimation/abstract_cost_estimator.hpp"
//#include "logical_query_plan/abstract_lqp_node.hpp"
//#include "logical_query_plan/lqp_utils.hpp"
//#include "logical_query_plan/predicate_node.hpp"
//#include "optimizer/join_ordering/join_graph.hpp"
//#include "statistics/cardinality_estimation_cache.hpp"
//#include "statistics/cardinality_estimator.hpp"
//#include "statistics/table_statistics.hpp"
//#include "utils/assert.hpp"

namespace opossum {

std::string OrToUnionRule::name() const { return "Or to Union Rule"; }

void OrToUnionRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) const {
  _apply_to_inputs(node);
}

}  // namespace opossum
