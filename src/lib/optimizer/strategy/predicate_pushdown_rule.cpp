#include "predicate_pushdown_rule.hpp"

#include "logical_query_plan/abstract_lqp_node.hpp"

namespace opossum {

std::string PredicatePushdownRule::name() const { return "Predicate Pushdown Rule"; }

bool PredicatePushdownRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) { return false; }
}  // namespace opossum
