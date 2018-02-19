#include "chunk_statistics.hpp"

#include <iterator>
#include <sstream>

#include "all_parameter_variant.hpp"
#include "utils/assert.hpp"

namespace opossum {

bool ChunkStatistics::can_prune(const ColumnID column_id, const AllTypeVariant& value,
                                const PredicateCondition scan_type) const {
  DebugAssert(column_id < _statistics.size(), "the passed column id should fit in the bounds of the statistics");
  return _statistics[column_id]->can_prune(value, scan_type);
}

// std::string ChunkStatistics::to_string() const {
//   std::stringstream s;
//   s << "ChunkStatistics:" << std::endl;
//   int i = 0;
//   for (const auto& column_stats : _statistics) {
//     const auto min = AllParameterVariant(column_stats->min());
//     const auto max = AllParameterVariant(column_stats->max());
//     s << "\t" << std::to_string(i++) << ": min: " << opossum::to_string(min) << " max: " << opossum::to_string(max)
//       << std::endl;
//   }
//   return s.str();
// }

}  // namespace opossum
