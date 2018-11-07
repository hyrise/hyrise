#include "statistics_utils.hpp"

#include <algorithm>

namespace opossum {

Cardinality scale_distinct_count(Selectivity selectivity, Cardinality value_count, Cardinality distinct_count) {
  return std::min(distinct_count, value_count * selectivity);
}

}  // namespace