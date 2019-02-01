#pragma once

#include <unordered_map>

#include "abstract_rule.hpp"
#include "expression/abstract_expression.hpp"

namespace opossum {

class AbstractLQPNode;

// Optimizes (NOT) IN and (NOT) EXISTS expressions into semi/anti joins.
// Does not currently optimize:
//    - (NOT) IN expressions where
//        - the in value is not a column reference.
//        - the sub-select produces something other than a column reference
//    - Correlated sub-selects where the correlated parameter
//        - is used outside predicates
//        - is used in predicates at a point where it cannot be pulled up into a join predicate (e.g., below joins,
//          limits, etc.)
//
// Due to missing support for multi-predicate joins, it also
//    - does not optimize NOT IN and NOT EXISTS expression
//    - does not work correctly when the left input of the predicate node has duplicate values

class SubselectToJoinReformulationRule : public AbstractRule {
 public:
  std::string name() const override;
  bool apply_to(const std::shared_ptr<AbstractLQPNode>& node) const override;
};

}  // namespace opossum
