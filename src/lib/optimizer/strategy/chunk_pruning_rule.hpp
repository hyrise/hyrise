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
class AbstractExpression;
class StoredTableNode;

/**
 * This rule determines which chunks can be pruned from table scans based on
 * the predicates present in the LQP and stores that information in the stored
 * table nodes.
 */
class ChunkPruningRule : public AbstractRule {
 public:
  std::string name() const override;
  void apply_to(const std::shared_ptr<AbstractLQPNode>& node) const override;

 protected:
  std::set<ChunkID> _compute_exclude_list(const std::vector<std::shared_ptr<ChunkStatistics>>& statistics,
                                          const AbstractExpression& predicate,
                                          const std::shared_ptr<StoredTableNode>& stored_table_node) const;

  bool _is_non_filtering_node(const AbstractLQPNode& node) const;
};

}  // namespace opossum
