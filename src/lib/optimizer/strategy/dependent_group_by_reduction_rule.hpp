#pragma once

#include "abstract_rule.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/lqp_column_expression.hpp"

namespace opossum {

class AbstractLQPNode;

/**
 * Removes columns from list of group-by columns when they are functionally dependent from column(s) of the grouping.

 * Example: in TPC-H Q10, the result projects c_acctbal. To do so after a grouping on the customer table, c_acctbal
 * needs to be part of the group-by columns. But since the primary key of the customer table (c_custkey) is also
 * grouped, the additional grouping on c_acctbal does not have an effect. As we consequence, we do the following
 * reformulation (only shown for c_acctbal) which removed c_acctbal from the list of group-by columns and adds it to
 * the list of aggregates via the ANY() function:
 * 		Aggregate: Grouping = [ c_custkey, c_name, c_acctbal, c_phone,n_name, c_address, c_comment],
 * 				   Aggregates = [ sum(l_extendedprice * (1 - l_discount)) ]
 *		>>>
 * 		Aggregate: Grouping = [ c_custkey, c_name, c_phone,n_name, c_address, c_comment],
 * 				   Aggregates = [ SUM(l_extendedprice * (1 - l_discount)), ANY(c_acctbal) ]

 * ANY() selects "any" value from the list of values per group (since we group by the primary key in this case, the
 * group is ensured to be of size one). This rule implements choke point 1.4 of "TPC-H Analyzed: Hidden Messages and
 * Lessons Learned from an Influential Benchmark" (Boncz et al.). Due to the current lack of foreign key suppport, not
 * all queries that are listed in the paper can be optimized.
 * When the group by reduction changes the column order and this order is also the final column order of the plan, a
 * projection is appended as the new root to restore the column order.
 *
 * For this rule, we search the list of group-by columns for primary key columns as well as unique columns. Since 
 * unique columns might include NULLs, we only consider non-nullable unique columns.
 */
class DependentGroupByReductionRule : public AbstractRule {
 public:
  void apply_to(const std::shared_ptr<AbstractLQPNode>& lqp) const override;
};

}  // namespace opossum
