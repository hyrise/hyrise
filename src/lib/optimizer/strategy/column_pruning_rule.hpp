#pragma once

#include <unordered_map>

#include "abstract_rule.hpp"
#include "expression/abstract_expression.hpp"

namespace opossum {

class AbstractLQPNode;

// TODO update comment, include join
// - In StoredTableNodes, we can get rid of all columns that are not used anywhere in the LQP
// - In ProjectionNodes, we can get rid of columns that were used before but are not used anymore. Example:
//     SELECT SUM(a + 2) FROM (SELECT a, a + 1 FROM t1) t2
//   Here, `a + 1` is never actually used and should be pruned  // TODO test
class ColumnPruningRule : public AbstractRule {
 public:
  void apply_to(const std::shared_ptr<AbstractLQPNode>& lqp) const override;
};

}  // namespace opossum
