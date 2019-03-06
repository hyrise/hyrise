#pragma once

#include <memory>
#include <optional>

#include "all_type_variant.hpp"

namespace opossum {

class AbstractStatisticsObject;
enum class PredicateCondition;

/**
 * Base class for ColumnStatistics<T>.
 * Statistically represents a Column (i.e. covers all Segments with the same ColumnID over all Chunks of a Table).
 *
 * Contains slots for AbstractStatisticsObjects (Histograms, Filters, etc.), see ColumnStatistics<T>.
 */
class BaseColumnStatistics {
 public:
  explicit BaseColumnStatistics(const DataType data_type);
  virtual ~BaseColumnStatistics() = default;

  /**
   * Utility to assign a statistics object to this slice. Spares the caller from having to deduce the type of
   * `statistics_object` and picking the right member to assign it to
   * @param statistics_object   can be a nullptr, which will just be dropped. This is to facilitate being able to call
   *                            `statistics->set_statistics_object(stat_obj->sliced(...))` without having to check
   *                            whether `sliced()` returned a nullptr
   */
  virtual void set_statistics_object(const std::shared_ptr<AbstractStatisticsObject>& statistics_object) = 0;

  /**
   * Creates a new ColumnStatistics with all members of this slice scaled as requested
   */
  virtual std::shared_ptr<BaseColumnStatistics> scaled(const Selectivity selectivity) const = 0;

  /**
   * Creates a new ColumnStatistics with all members of this slice sliced as requested
   */
  virtual std::shared_ptr<BaseColumnStatistics> sliced(
      const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
      const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const = 0;

  const DataType data_type;
};

}  // namespace opossum
