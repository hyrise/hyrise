#include "index_scan_rule.hpp"

#include <algorithm>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "constant_mappings.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "optimizer/table_statistics.hpp"
#include "utils/assert.hpp"

namespace opossum {

std::string IndexScanRule::name() const { return "Index Scan Rule"; }

bool IndexScanRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) {
  // auto reordered = false;

  if (node->type() == LQPNodeType::Predicate) {
    auto parent = node->parents()[0];

    if (parent->type() == LQPNodeType::StoredTable) {
      std::cout << "d" << std::endl;
    }
  }

  return true;
}

// bool IndexScanRule::_reorder_predicates(std::vector<std::shared_ptr<PredicateNode>>& predicates) const {
//   // Store original child and parent
//   auto child = predicates.back()->left_child();
//   const auto parents = predicates.front()->parents();
//   const auto child_sides = predicates.front()->get_child_sides();

//   const auto sort_predicate = [&](auto& left, auto& right) {
//     return left->derive_statistics_from(child)->row_count() > right->derive_statistics_from(child)->row_count();
//   };

//   if (std::is_sorted(predicates.begin(), predicates.end(), sort_predicate)) {
//     return false;
//   }

//   // Untie predicates from LQP, so we can freely retie them
//   for (auto& predicate : predicates) {
//     predicate->remove_from_tree();
//   }

//   // Sort in descending order
//   std::sort(predicates.begin(), predicates.end(), sort_predicate);

//   // Ensure that nodes are chained correctly
//   predicates.back()->set_left_child(child);

//   for (size_t parent_idx = 0; parent_idx < parents.size(); ++parent_idx) {
//     parents[parent_idx]->set_child(child_sides[parent_idx], predicates.front());
//   }

//   for (size_t predicate_index = 0; predicate_index < predicates.size() - 1; predicate_index++) {
//     predicates[predicate_index]->set_left_child(predicates[predicate_index + 1]);
//   }

//   return true;
// }

}  // namespace opossum
