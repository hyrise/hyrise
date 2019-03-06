#pragma once

#include <memory>
#include <set>
#include <string>
#include <vector>

#include "abstract_rule.hpp"
#include "statistics/table_statistics.hpp"
#include "types.hpp"

namespace opossum {

class AbstractLQPNode;
class BaseColumnStatistics;
class PredicateNode;
class Table;

/**
 * This rule determines which chunks can be excluded from table scans based on
 * the predicates present in the LQP and stores that information in the stored
 * table nodes.
 */
class ChunkPruningRule : public AbstractRule {
 public:
  std::string name() const override;

  void apply_to(const std::shared_ptr<AbstractLQPNode>& node) const override;

 protected:
  std::set<ChunkID> _compute_exclude_list(const Table& table,
                                          const std::shared_ptr<PredicateNode>& predicate_node) const;

  // Check whether any of the statistics objects available for this Segment identify the predicate as prunable
  bool _can_prune(const BaseColumnStatistics& base_column_statistics, const PredicateCondition predicate_type,
                  const AllTypeVariant& variant_value, const std::optional<AllTypeVariant>& variant_value2) const;
};

}  // namespace opossum
