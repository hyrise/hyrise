#pragma once

#include <unordered_map>

#include "abstract_rule.hpp"
#include "expression/abstract_expression.hpp"

namespace opossum {

class AbstractLQPNode;

// Optimizes (NOT) IN expressions where the set is a sub-select by translating them into semi/anti joins.
// Does not currently optimize:
//    - In values that are not column references.
//    - Sub-selects, which produce anything other than a column reference
//    - Correlated sub-selects where the correlated parameter:
//        - is used outside predicates
//        - is used in predicates at a point where it cannot be pulled up to the top of the sub-select (e.g., below
//          joins, limits, etc.)

class InReformulationRule : public AbstractRule {
 public:
  std::string name() const override;
  bool apply_to(const std::shared_ptr<AbstractLQPNode>& node) const override;
};

}  // namespace opossum
