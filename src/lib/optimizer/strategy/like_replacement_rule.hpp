#pragma once

#include <memory>
#include <set>
#include <string>
#include <vector>

#include "abstract_rule.hpp"
#include "expression/expression_functional.hpp"

#include "types.hpp"

namespace opossum {

class AbstractLQPNode;
class ChunkStatistics;
class PredicateNode;

/**
 * This rule determines special cases of like ("abc%") which can be rewritten into two BinaryPredicateExpressions (column >= "abc" and column < "abd")
 * in order to gain greater performance.
 */
class LikeReplacementRule : public AbstractRule {
 public:
  std::string name() const override;
  void apply_to(const std::shared_ptr<AbstractLQPNode>& node) const override;
};

}  // namespace opossum
