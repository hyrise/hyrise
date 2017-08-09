#pragma once

#include <memory>
#include <vector>

#include "abstract_rule.hpp"

namespace opossum {

class AbstractASTNode;
class PredicateNode;

/**
 * This optimizer rule finds chains of PredicateNodes and sorts them based on the expected cardinality.
 * By that predicates with a low selectivity are executed first to (hopefully) reduce the size of intermediate results.
 */
class PredicateReorderingRule : public AbstractRule {
 public:
  const std::shared_ptr<AbstractASTNode> apply_to(const std::shared_ptr<AbstractASTNode> node) override;

 private:
  void _reorder_predicates(std::vector<std::shared_ptr<PredicateNode>> &predicates) const;
};

}  // namespace opossum
