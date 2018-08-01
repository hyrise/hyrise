#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_rule.hpp"

namespace opossum {

class AbstractLQPNode;
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
 public:
  std::string name() const override;
  bool apply_to(const std::shared_ptr<AbstractLQPNode>& node) const override;

 private:
  bool _reorder_predicates(std::vector<std::shared_ptr<PredicateNode>>& predicates) const;
};

}  // namespace opossum
