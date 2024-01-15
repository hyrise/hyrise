#pragma once

#include <memory>
#include <optional>

#include "all_type_variant.hpp"

namespace hyrise {

class AbstractStatisticsObject;
enum class PredicateCondition;

/**
 * Statistically represents
 * - a column, when used in TableStatistics, i.e., for cardinality estimation
 * - a segment, when used in ColumnPruningStatistics, i.e., for chunk pruning
 *
 * Contains any number of AbstractStatisticsObjects (histograms, filters, etc.).
 */
class BaseAttributeStatistics {
 public:
  explicit BaseAttributeStatistics(const DataType init_data_type);
  virtual ~BaseAttributeStatistics() = default;

  /**
   * Utility to assign a statistics object to this slice. Spares the caller from having to deduce the type of
   * `statistics_object` and picking the right member to assign it to.
   * @param statistics_object   can be a nullptr, which will just be dropped. This is to facilitate being able to call
   *                            `statistics->set_statistics_object(stat_obj->sliced(...))` without having to check
   *                            whether `sliced()` returned a nullptr.
   */
  virtual void set_statistics_object(const std::shared_ptr<const AbstractStatisticsObject>& statistics_object) = 0;

  /**
   * Creates a new AttributeStatistics with all members of this slice scaled as requested. We use this method to estima-
   * te how the statistics would change if we executed any predicate with the given selectivity on the column/segment
   * (e.g., another column was sliced by the predicate and we scale the current column down with the selectivity). When
   * we cannot be sure how the statistics will change, we assume the worst-case scenario (i.e., nothing changes).
   */
  virtual std::shared_ptr<const BaseAttributeStatistics> scaled(const Selectivity selectivity) const = 0;

  /**
   * Creates a new AttributeStatistics with all members of this slice sliced as requested. We use this method to estima-
   * te how the statistics would change if we executed the given predicate (i.e., a ColumnVsValue/ColumnBetween
   * TableScan) on the column/segment.
   */
  virtual std::shared_ptr<const BaseAttributeStatistics> sliced(
      const PredicateCondition predicate_condition, const AllTypeVariant& variant_value,
      const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const = 0;

  /*
   * Creates a new AttributeStatistics that reflects pruning on a given predicate where num_values_pruned have been
   * pruned. That is, remove num_values_pruned that DO NOT satisfy the predicate from the statistics, assuming
   * equidistribution.
   */
  virtual std::shared_ptr<const BaseAttributeStatistics> pruned(
      const size_t num_values_pruned, const PredicateCondition predicate_condition, const AllTypeVariant& variant_value,
      const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const;

  const DataType data_type;
};

}  // namespace hyrise
