#pragma once

#include <memory>
#include <optional>
#include <utility>

#include "cardinality.hpp"
#include "selectivity.hpp"

#include "all_type_variant.hpp"
#include "types.hpp"

namespace opossum {

template <typename T>
class SingleBinHistogram;

enum class EstimateType { MatchesNone, MatchesExactly, MatchesApproximately, MatchesAll };

struct CardinalityEstimate {
  Cardinality cardinality;
  EstimateType type;
};

class AbstractStatisticsObject {
 public:
  explicit AbstractStatisticsObject(const DataType data_type);
  virtual ~AbstractStatisticsObject() = default;

  /**
   * @brief Estimate how many values match the predicate.
   * @returns the estimated cardinality and a bool indicating whether the statistics object is absolutely certain about
   *          that cardinality or not.
   */
  virtual CardinalityEstimate estimate_cardinality(
      const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
      const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const = 0;

  /**
   * @return Whether the predicate will return no result, based on the statistics object.
   */
  bool does_not_contain(const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
                        const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const;

  /**
   * @return a statistics object that represents the data after the predicate has been applied.
   */
  virtual std::shared_ptr<AbstractStatisticsObject> sliced_with_predicate(
      const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
      const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const = 0;

  /**
   * @return a statistics object that represents the data after a filter with the given selectivity has been applied.
   */
  virtual std::shared_ptr<AbstractStatisticsObject> scaled_with_selectivity(const Selectivity selectivity) const = 0;

  /**
   * @return this statistics object aggregated into a SingleBinHistogram (which is the most "minimal" conceivable
   *         statistics object that is still useful for, e.g. cardinality estimations).
   */
  template <typename T>
  std::shared_ptr<SingleBinHistogram<T>> reduce_to_single_bin_histogram() const;

  /**
   * DataType of the data that this statistics object represents
   */
  const DataType data_type;

  /**
   * Flag indicating that this statistics object is derived from an immutable, unsampled chunk.
   * This indicates whether this statistics object can be used for Chunk pruning.
   */
  bool is_derived_from_complete_chunk{false};

 protected:
  virtual std::shared_ptr<AbstractStatisticsObject> _reduce_to_single_bin_histogram_impl() const;
};

}  // namespace opossum

#include "abstract_statistics_object.ipp"
