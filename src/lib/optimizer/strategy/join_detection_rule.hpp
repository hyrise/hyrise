#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "abstract_rule.hpp"
#include "logical_query_plan/lqp_column_reference.hpp"
#include "types.hpp"

namespace opossum {

class AbstractExpression;
class AbstractLQPNode;
class JoinNode;
class PredicateNode;

struct ColumnID;

/**
 * This optimizer rule tries to find join conditions for cross join.
 * The rule tries to rewrite the corresponding LQPs for the following SQL statements to an equivalent LQP:
 *
 * SELECT * FROM a, b WHERE a.id = b.id;
 * =>
 * SELECT * FROM a INNER JOIN b ON a.id = b.id
 *
 *
 *
 * HOW THIS WORKS
 *
 * The rule traverses the LQP recursively searching for JoinNodes with JoinMode::Cross.
 * For each Cross Join Node it will look for an appropriate join condition
 * by searching the output nodes for PredicateNodes. Each PredicateNode is a potential candidate
 * but only those that compare two columns are interesting enough to check.
 * When such a PredicateNode is found, the rule will check whether each ColumnID comes from the left/right input.
 *
 * Note: Limited first iteration. This will only work on subtrees consisting of Joins and Predicates, so we don't
 * have to deal with ColumnID re-mappings for now. Projections, Aggregates, etc. amidst Joins and Predicates
 * should be rare anyway.
 */
class JoinDetectionRule : public AbstractRule {
 protected:
  std::string name() const override;

  bool apply_to(const std::shared_ptr<AbstractLQPNode>& node) const override;

 private:
  std::shared_ptr<PredicateNode> _find_predicate_for_cross_join(const std::shared_ptr<JoinNode>& cross_join) const;
};

}  // namespace opossum
