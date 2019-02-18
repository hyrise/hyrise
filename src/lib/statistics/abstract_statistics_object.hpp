#pragma once

#include <memory>
#include <optional>
#include <utility>

#include "cardinality.hpp"
#include "selectivity.hpp"

#include "all_type_variant.hpp"
#include "types.hpp"

namespace opossum {

enum class EstimateType { MatchesNone, MatchesExactly, MatchesApproximately, MatchesAll };

struct CardinalityEstimate {
  CardinalityEstimate() = default;
  CardinalityEstimate(const Cardinality cardinality, const EstimateType type);

  bool operator==(const CardinalityEstimate& rhs) const;

  Cardinality cardinality{};
  EstimateType type{};
};

/**
 * Base class for types that hold statistical information about a column/segment of data.
 */
class AbstractStatisticsObject {
 public:
  explicit AbstractStatisticsObject(const DataType data_type);
  virtual ~AbstractStatisticsObject() = default;

  /**
   * @brief Estimate how many values match the predicate.
   */
  virtual CardinalityEstimate estimate_cardinality(
      const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
      const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const = 0;

  /**
   * @return Whether the predicate will return no result
   */
  bool does_not_contain(const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
                        const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const;

  /**
   * @return A statistics object that represents the data after the predicate has been applied.
   */
  virtual std::shared_ptr<AbstractStatisticsObject> sliced(
      const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
      const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const = 0;

  /**
   * @return a statistics object that represents the data after a filter with the given selectivity has been applied.
   */
  virtual std::shared_ptr<AbstractStatisticsObject> scaled(const Selectivity selectivity) const = 0;

  /**
   * DataType of the data that this statistics object represents
   */
  const DataType data_type;

  /**
   * Flag indicating that this statistics object is derived from an immutable, unsampled chunk.
   * This indicates whether this statistics object can be used for Chunk pruning.
   */
  bool is_derived_from_complete_chunk{false};
};

}  // namespace opossum
