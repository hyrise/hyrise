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
  Cardinality cardinality;
  EstimateType type;
};

class AbstractStatisticsObject {
 public:
  virtual ~AbstractStatisticsObject() = default;

  /**
   * Estimate how many values match the predicate.
   * Returns the estimated cardinality and a bool indicating whether the statistics object is absolutely certain about
   * that cardinality or not.
   */
  virtual CardinalityEstimate estimate_cardinality(
      const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
      const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const = 0;

  /**
   * Return a statistics object that represents the data after the predicate has been applied.
   */
  virtual std::shared_ptr<AbstractStatisticsObject> slice_with_predicate(
      const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
      const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const = 0;

  /**
   * Return a statistics object that represents the data after a filter with the given selectivity has been applied.
   */
  virtual std::shared_ptr<AbstractStatisticsObject> scale_with_selectivity(const Selectivity selectivity) const = 0;
};

}  // namespace opossum
