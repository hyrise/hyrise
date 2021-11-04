#pragma once

namespace opossum {

class DependencyUsageConfig {
 public:
  constexpr static bool ENABLE_GROUPBY_REDUCTION = true;
  constexpr static bool ENABLE_JOIN_TO_SEMI = false;
  constexpr static bool ENABLE_JOIN_TO_PREDICATE = false;
  constexpr static bool ENABLE_JOIN_ELIMINATION = false;

  constexpr static bool ALLOW_PRESET_CONSTRAINTS = false;
};

}  // namespace opossum
