#pragma once

#include "abstract_rule.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/lqp_column_expression.hpp"

namespace opossum {

class AbstractLQPNode;
class AggregateNode;
struct TableConstraintDefinition;
class StoredTableNode;

/**
 * In SQL, the returned columns of an aggregate are either "group-by columns" or "aggregate columns", e.g., SUM(a). As
 * an example, in TPC-H Q10, the aggregate on customer includes both c_custkey as well as c_name (amongst others) in
 * the group-by columns.
 * Note how c_name is included as a group-by column, even though the groups are already exhaustively defined by
 * GROUP BY c_custkey. The reason for that is the functional dependency c_custkey => c_name, or, more practically
 * speaking, the fact that c_custkey is the primary key of the customer table.
 * This rule identifies cases where functionally dependent columns are given as group-by columns while the functional
 * dependencies's determinant column(s) is a/are also group-by column(s). It then replaces that column (e.g., c_name)
 * with a dummy aggregate function ANY(c_name). This reduces the grouping cost in the aggregate operator.
 * As the aggregate operators first produce all group-by columns, followed by all aggregate columns (including ANY),
 * the order might be changed. Unless a following operation redefines the order anyway (e.g., a projection or another
 * aggregate), a new projection is inserted to restore the original column order.

 * Example reformulation (only shown for c_name) which removes c_name from the list of group-by columns and adds it to
 * the list of aggregates via the ANY() function:
 * 		Aggregate: Grouping = [ c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment],
 * 				   Aggregates = [ sum(l_extendedprice * (1 - l_discount)) ]
 *		>>>
 * 		Aggregate: Grouping = [ c_custkey, c_acctbal, c_phone, n_name, c_address, c_comment],
 * 				   Aggregates = [ SUM(l_extendedprice * (1 - l_discount)), ANY(c_name) ]

 * ANY() selects "any" value from the list of values per group (since we group by the functional dependency's
 * determinant column(s) in this case the group is ensured to be of size one). This rule implements choke point 1.4 of
 * "TPC-H Analyzed: Hidden Messages and Lessons Learned from an Influential Benchmark" (Boncz et al.). Due to the
 * current lack of foreign key support, not all queries that are listed in the paper can be optimized.
 *
 * For this rule, we search the list of group-by columns for functional dependencies (as of now: primary key columns as
 * well as unique columns, given by soft constraints). Since unique columns might include NULLs (similar to previously
 * executed outer joins), we only consider non-nullable group-by columns.
 */
class DependentGroupByReductionRule : public AbstractRule {
 public:
  void apply_to(const std::shared_ptr<AbstractLQPNode>& lqp) const override;
};

}  // namespace opossum
