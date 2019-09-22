#pragma once

#include <memory>
#include <optional>

#include "all_type_variant.hpp"

namespace opossum {

class AbstractStatisticsObject;
enum class PredicateCondition;

/**
 * Statistically represents
 * - a Column, when used in TableStatistics, i.e., for cardinality estimation
 * - a Segment, when used in ColumnPruningStatistics, i.e., for Chunk pruning
 *
 * Contains any number of AbstractStatisticsObjects (Histograms, Filters, etc.).
 */
class BaseAttributeStatistics {
 public:
  explicit BaseAttributeStatistics(const DataType data_type);
  virtual ~BaseAttributeStatistics() = default;

  /**
   * Utility to assign a statistics object to this slice. Spares the caller from having to deduce the type of
   * `statistics_object` and picking the right member to assign it to
   * @param statistics_object   can be a nullptr, which will just be dropped. This is to facilitate being able to call
   *                            `statistics->set_statistics_object(stat_obj->sliced(...))` without having to check
   *                            whether `sliced()` returned a nullptr
   */
  virtual void set_statistics_object(const std::shared_ptr<AbstractStatisticsObject>& statistics_object) = 0;

  /**
   * Creates a new AttributeStatistics with all members of this slice scaled as requested
   */
  virtual std::shared_ptr<BaseAttributeStatistics> scaled(const Selectivity selectivity) const = 0;

  /**
   * Creates a new AttributeStatistics with all members of this slice sliced as requested
   */
  virtual std::shared_ptr<BaseAttributeStatistics> sliced(
      const PredicateCondition predicate_condition, const AllTypeVariant& variant_value,
      const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const = 0;

  /*
   * Creates a new AttributeStatistics that reflects pruning on a given predicate where num_values_pruned have been
   * pruned. That is, remove num_values_pruned that DO NOT satisfy the predicate from the statistics, assuming
   * equidistribution.
   */
  virtual std::shared_ptr<BaseAttributeStatistics> pruned(
      const size_t num_values_pruned, const PredicateCondition predicate_condition, const AllTypeVariant& variant_value,
      const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const;

  const DataType data_type;
};

}  // namespace opossum
