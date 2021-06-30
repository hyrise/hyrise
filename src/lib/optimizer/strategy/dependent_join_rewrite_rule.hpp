#pragma once

#include "abstract_rule.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/lqp_column_expression.hpp"

namespace opossum {

class AbstractLQPNode;
class AggregateNode;
class StoredTableNode;

/**
 * Given a scan followed by a join, e.g.:
 * SELECT ...
 *   FROM sales s, dates d 
 *   WHERE s.sold_date_sk = d.date_sk
 *   AND d.date BETWEEN ’20201104’
 *   AND ’20201204’
 * 
 * Imagine none of dates' columns are selected and there is an order dependency d.date_sk -> d.date.
 * We can turn the join into a between scan with the date_sk values matching the given dates.
 * This can also be done if the original scan runs on a unique column and is an equals predicate.
 */
class DependentJoinRewriteRule : public AbstractRule {
 protected:
  void _apply_to_plan_without_subqueries(const std::shared_ptr<AbstractLQPNode>& lqp_root) const override;
};

}  // namespace opossum
