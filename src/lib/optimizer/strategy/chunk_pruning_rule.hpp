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

class ChunkPruningRule : public AbstractRule {
 public:
  std::string name() const override;
  bool apply_to(const std::shared_ptr<AbstractLQPNode>& node) override;

 protected:
  std::set<ChunkID> _calculate_exclude_list(
        const std::vector<std::shared_ptr<ChunkStatistics>>& stats,
        std::shared_ptr<PredicateNode> predicate);
};

}  // namespace opossum
