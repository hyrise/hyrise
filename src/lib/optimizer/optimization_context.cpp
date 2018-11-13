#include "optimization_context.hpp"

#include "expression/abstract_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"

namespace opossum {

void OptimizationContext::print(std::ostream& stream) const {
  stream << "OptimizationContext {" << std::endl;
  stream << "Leafs:" << std::endl;
  for (const auto& [leaf, idx] : plan_leaf_indices) {
    stream << "  " << leaf->description() << ": " << idx << std::endl;
  }
  stream << "Predicates:" << std::endl;
  for (const auto& [predicate, idx] : predicate_indices) {
    stream << "  " << predicate->as_column_name() << ": " << (idx + plan_leaf_indices.size()) << std::endl;
  }

  stream << "}" << std::endl;
}

}  // namespace opossum