#pragma once

#include <ostream>

#include "all_parameter_variant.hpp"
#include "logical_query_plan/lqp_column_reference.hpp"
#include "types.hpp"

namespace opossum {

/**
 * Classes to represent Predicates in the JoinGraph/JoinPlan. These can be nested to any depth, so even complex
 * predicates such as "(a > 5 AND b < 3) OR (a = b AND b = c AND c = d)" can be represented.
 */

enum class JoinPlanPredicateLogicalOperator { And, Or };

enum class JoinPlanPredicateType { Atomic, LogicalOperator };

class AbstractJoinPlanPredicate {
 public:
  explicit AbstractJoinPlanPredicate(const JoinPlanPredicateType type);
  virtual ~AbstractJoinPlanPredicate() = default;

  JoinPlanPredicateType type() const;

  virtual void print(std::ostream& stream = std::cout, const bool enclosing_braces = false) const = 0;

 private:
  const JoinPlanPredicateType _type;
};

/**
 * Represents a logical predicate of the form "<predicate_a> <logical_operator> <predicate_b>"
 */
class JoinPlanLogicalPredicate : public AbstractJoinPlanPredicate {
 public:
  JoinPlanLogicalPredicate(const std::shared_ptr<const AbstractJoinPlanPredicate>& left_operand,
                           JoinPlanPredicateLogicalOperator logical_operator,
                           const std::shared_ptr<const AbstractJoinPlanPredicate>& right_operand);

  void print(std::ostream& stream = std::cout, const bool enclosing_braces = false) const override;

  bool operator==(const JoinPlanLogicalPredicate& rhs) const;

  const std::shared_ptr<const AbstractJoinPlanPredicate> left_operand;
  const JoinPlanPredicateLogicalOperator logical_operator;
  const std::shared_ptr<const AbstractJoinPlanPredicate> right_operand;
};

/**
 * Represents a predicate of the format "<column> <predicate_condition> <column_or_value>", e.g. "a > 5"
 */
class JoinPlanAtomicPredicate : public AbstractJoinPlanPredicate {
 public:
  JoinPlanAtomicPredicate(const LQPColumnReference& left_operand, const PredicateCondition predicate_condition,
                          const AllParameterVariant& right_operand);

  void print(std::ostream& stream = std::cout, const bool enclosing_braces = false) const override;

  bool operator==(const JoinPlanAtomicPredicate& rhs) const;

  const LQPColumnReference left_operand;
  const PredicateCondition predicate_condition;
  const AllParameterVariant right_operand;
};

}  // namespace opossum
