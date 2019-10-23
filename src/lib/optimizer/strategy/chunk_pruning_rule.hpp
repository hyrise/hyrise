#pragma once

#include <memory>
#include <set>
#include <string>
#include <vector>

#include "abstract_rule.hpp"
#include "operators/operator_scan_predicate.hpp"
#include "statistics/table_statistics.hpp"
#include "types.hpp"

namespace opossum {

class AbstractLQPNode;
class ChunkStatistics;
class AbstractExpression;
class StoredTableNode;
class PredicateNode;
class Table;

/**
 * This rule determines which chunks can be pruned from table scans based on
 * the predicates present in the LQP and stores that information in the stored
 * table nodes.
 */
class ChunkPruningRule : public AbstractRule {
 public:
  void apply_to(const std::shared_ptr<AbstractLQPNode>& node) const override;

 protected:
  static std::set<ChunkID> _compute_exclude_list(const Table& table, const AbstractExpression& predicate,
                                                 const std::shared_ptr<StoredTableNode>& stored_table_node);

  // Check whether any of the statistics objects available for this Segment identify the predicate as prunable
  static bool _can_prune(const BaseAttributeStatistics& base_segment_statistics,
                         const PredicateCondition predicate_condition, const AllTypeVariant& variant_value,
                         const std::optional<AllTypeVariant>& variant_value2);

  static bool _is_non_filtering_node(const AbstractLQPNode& node);

  static std::shared_ptr<TableStatistics> _prune_table_statistics(const TableStatistics& old_statistics,
                                                                  OperatorScanPredicate predicate,
                                                                  size_t num_rows_pruned);
};

}  // namespace opossum
