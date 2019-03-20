#pragma once

#include <unordered_map>

#include "abstract_rule.hpp"
#include "expression/abstract_expression.hpp"

namespace opossum {

class AbstractLQPNode;

// Optimizes:
//    - (NOT) IN predicates with a subquery as the right operand
//    - (NOT) EXISTS predicates
//    - comparison (<,>,<=,>=,=,<>) predicates with subquery as the right operand
// Does not currently optimize:
//    - (NOT) IN expressions where
//        - the left value is not a column expression.
//    - NOT IN with a correlated subquery
//    - Correlated subqueries where the correlated parameter
//        - is used outside predicates
//        - is used in predicates at a point where it cannot be pulled up into a join predicate (e.g., below joins,
//          limits, etc.)

class SubqueryToJoinRule : public AbstractRule {
 public:
  std::string name() const override;
  void apply_to(const std::shared_ptr<AbstractLQPNode>& node) const override;
};

}  // namespace opossum
