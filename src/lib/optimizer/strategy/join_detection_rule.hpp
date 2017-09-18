#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_rule.hpp"

namespace opossum {

class AbstractASTNode;
class JoinNode;
class PredicateNode;

struct ColumnID;

/**
 * This optimizer rule tries to find join conditions for cross join.
 * The rule tries to rewrite the corresponding ASTs for the following SQL statements to something that is equivalent :
 *
 * SELECT * FROM a, b WHERE a.id = b.id;
 * =>
 * SELECT * FROM a INNER JOIN b ON a.id = b.id
 *
 * ### HOW THIS WORKS ###
 *
 * The rule traverses the AST recursively untit it detects a JoinNode that has JoinMode::Cross.
 * For each Cross Join Node it will look for an appropriate join condition
 * by searching the parent nodes for PredicateNodes. Each PredicateNode is a potential candidate
 * but only those that compare two columns are interesting enough to check.
 * When such a PredicateNode is found, the rule will check whether each ColumnID comes from the left/right input.
 *
 * Note: Limited first iteration. Will only work on subtrees consisting of Joins and Predicates, so we don't
 * have to deal with ColumnID re-mappings for now. Projections, Aggregates, etc. amidst Joins and Predicates
 * should be rare anyway.
 */
class JoinConditionDetectionRule : public AbstractRule {
 protected:
  bool apply_to(const std::shared_ptr<AbstractASTNode> &node) override;

 private:
  const std::shared_ptr<PredicateNode> _find_predicate_for_cross_join(const std::shared_ptr<JoinNode> &cross_join);

  /**
   * Checks whether a predicate that operates on @param left and @param right is a join condition for a cross
   * join with @param left_num_cols number of columns in its left input and @param right_num_cols in its right input
   */
  bool _is_join_condition(ColumnID left, ColumnID right, size_t left_num_cols, size_t right_num_cols) const;
};

}  // namespace opossum
