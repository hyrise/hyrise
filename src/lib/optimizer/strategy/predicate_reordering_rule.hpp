#pragma once

#include <memory>
#include <vector>

#include "abstract_rule.hpp"

namespace opossum {

class AbstractASTNode;
class PredicateNode;

/**
 * This optimizer rule finds chains of adjacent PredicateNodes and sorts them based on the expected cardinality.
 * By that predicates with a low selectivity are executed first to (hopefully) reduce the size of intermediate results.
 *
 * Note:
 * For now this rule only finds adjacent PredicateNodes, meaning that if there is another node, e.g. a ProjectionNode,
 * between two
 * chains of PredicateNodes we won't order all of them, but only each chain separately.
 * A potential optimization would be to ignore certain intermediate nodes, such as ProjectionNode or SortNode, but
 * respect
 * others, such as JoinNode or UnionNode.
 */
class PredicateReorderingRule : public AbstractRule {
 protected:
  void _apply_to_impl(const std::shared_ptr<AbstractASTNode> &node) override;

 private:
  void _reorder_predicates(std::vector<std::shared_ptr<PredicateNode>> &predicates) const;
};

}  // namespace opossum
