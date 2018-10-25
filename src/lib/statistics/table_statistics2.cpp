#include "table_statistics2.hpp"

#include <numeric>

#include "chunk_statistics2.hpp"

namespace opossum {

Cardinality TableStatistics2::row_count() const {
  return std::accumulate(chunk_statistics.begin(), chunk_statistics.end(), Cardinality{0},
                         [](const auto& a, const auto& chunk_statistics2) { return a + chunk_statistics2->row_count; });
}

}  // namespace opossum
