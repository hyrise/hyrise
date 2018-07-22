#pragma once

#include <memory>
#include <set>
#include <string>
#include <vector>

#include "abstract_rule.hpp"

#include "types.hpp"

namespace opossum {

class AbstractLQPNode;
class ChunkStatistics;
class PredicateNode;

/**
 * This rule determines which chunks can be excluded from table scans based on
 * the predicates present in the LQP and stores that information in the stored
 * table nodes.
 */
class ChunkPruningRule : public AbstractRule {
 public:
  std::string name() const override;
  bool apply_to(const std::shared_ptr<AbstractLQPNode>& node) const override;

 protected:
  std::set<ChunkID> _compute_exclude_list(const std::vector<std::shared_ptr<ChunkStatistics>>& stats,
                                          const std::shared_ptr<PredicateNode>& predicate) const;
};

}  // namespace opossum
