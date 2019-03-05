#pragma once

#include <memory>
#include <string>

#include "abstract_rule.hpp"
#include "expression/expression_functional.hpp"

#include "types.hpp"

namespace opossum {

/**
 * This optimizer rule replaces PredicateNodes with a like condition by BinaryPredicateExpressions for some special cases.
 * For example, column LIKE "abc%" can be replaced by column >= "abc" AND column < "abd" to be executed in a more efficient way.
 * String comparisons are expensive and BinaryPredicateExpressions can benefit from Hyrise's encodings since these operations are executed on ValueIDs and string comparisons are avoided.
 */
class LikeReplacementRule : public AbstractRule {
 public:
  std::string name() const override;
  void apply_to(const std::shared_ptr<AbstractLQPNode>& node) const override;
};

}  // namespace opossum
