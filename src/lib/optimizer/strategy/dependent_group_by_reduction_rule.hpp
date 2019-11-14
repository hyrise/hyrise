#pragma once

#include "abstract_rule.hpp"
#include "expression/abstract_expression.hpp"

namespace opossum {

class AbstractLQPNode;

// Removes columns from list of group by column when they are functionally dependent from another column of the list.
// This is currently only the case, when both the primary key and other columns of the same table are grouped.
// This rule implements choke point 1.4 of "TPC-H Analyzed: Hidden Messages and Lessons Learned from an Influential
// Benchmark" (Boncz et al.). Due to the current lack of foreign key suppport, not all queries can be optimized.
// 
// When only a single table is aggregated and the full primary key is grouped, one could theoretically replace
// aggregate functions such as SUM(dependent_column) to ANY(dependent_column) as the group will only include a single
// tuple. Currently, we do not do that as (i) we need the additional check if only a single table is reference
// (otherwise the group could be larger) and (ii) aggregate functions for single tuples should only be neglectably
// more expensive than ANY().
class DependentGroupByReductionRule : public AbstractRule {
 public:
  void apply_to(const std::shared_ptr<AbstractLQPNode>& lqp) const override;
};

}  // namespace opossum
