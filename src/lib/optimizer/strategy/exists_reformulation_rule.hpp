#pragma once

#include <unordered_map>

#include "abstract_rule.hpp"
#include "expression/abstract_expression.hpp"

namespace opossum {

class AbstractLQPNode;

// Checks for constructs like `SELECT * FROM t1 WHERE EXISTS (SELECT a FROM t2 WHERE t1.a = t2.a)
// Does not cover - cases where the subselect is not correlated (because we cannot use a semi join without a predicate)
//                - cases where the predicate is not `=` (because only the hash join can do semi/anti)
//                - cases where multiple predicates exists (because our joins can only handle single predicates)
//                - cases where the subselect uses multiple external parameters, or uses one twice
//                    (because that makes things more complicated; it is sometimes possible though)
//                - other complex subselects (because we have not thought about all eventualities yet)

class ExistsReformulationRule : public AbstractRule {
 public:
  std::string name() const override;
  bool apply_to(const std::shared_ptr<AbstractLQPNode>& node) const override;
};

}  // namespace opossum
