#pragma once

#include <unordered_map>

#include "abstract_rule.hpp"
#include "expression/abstract_expression.hpp"

namespace opossum {

class AbstractLQPNode;

// Removes expressions (i.e., columns) that are never or no longer used from the plan
// - In StoredTableNodes, we can get rid of all columns that are not used anywhere in the LQP
// - In ProjectionNodes, we can get rid of columns that are not part of the result or not used anymore. Example:
//     SELECT SUM(a + 2) FROM (SELECT a, a + 1 FROM t1) t2
//   Here, `a + 1` is never actually used and should be pruned
// - Joins that emit columns that are never used can be rewritten to semi joins if (a) the unused side has a unique
//     constraint and (b) the join is an inner join. This is done in the ColumnPruningRule because it requires
//     information about which columns are needed and which ones are not. That information is gathered here and not
//     exported.
class ColumnPruningRule : public AbstractRule {
 public:
  void apply_to(const std::shared_ptr<AbstractLQPNode>& lqp) const override;
};

}  // namespace opossum
