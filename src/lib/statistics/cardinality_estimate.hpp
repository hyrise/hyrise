#pragma once

#include "cardinality.hpp"

namespace opossum {

enum class EstimateType { MatchesNone, MatchesExactly, MatchesApproximately, MatchesAll };

struct CardinalityEstimate {
  CardinalityEstimate() = default;
  CardinalityEstimate(const Cardinality cardinality, const EstimateType type);

  // For gtest
  bool operator==(const CardinalityEstimate& rhs) const;

  Cardinality cardinality{};
  EstimateType type{};
};

}  // namespace opossum
