#pragma once

#include "abstract_rule.hpp"
#include "expression/abstract_expression.hpp"

namespace opossum {

class AbstractLQPNode;

// Removes columns from list of group by column when they are functionally dependent from another column of the list.
// This is currently only the case, when both the primary key and other columns of the same table are grouped.
// This rule implements choke point 1.4 of "TPC-H Analyzed: Hidden Messages and Lessons Learned from an Influential
// Benchmark" (Boncz et al.). Due to the current lack of foreign key suppport, not all queries can be optimized.
class DependentGroupByReductionRule : public AbstractRule {
 public:
  void apply_to(const std::shared_ptr<AbstractLQPNode>& lqp) const override;
};

}  // namespace opossum
