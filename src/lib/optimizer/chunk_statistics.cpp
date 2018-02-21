#include "chunk_statistics.hpp"

#include <iterator>
#include <sstream>

#include "all_parameter_variant.hpp"
#include "resolve_type.hpp"
#include "utils/assert.hpp"

namespace opossum {

namespace {
template<typename T>
std::shared_ptr<ChunkColumnStatistics> build_stats(const DictionaryColumn<T>& column) {
    auto statistics = std::make_shared<ChunkColumnStatistics>();
    const auto & dictionary = *column.dictionary();
    // only create statistics when the compressed dictionary is not empty
    if(!dictionary.empty()) {
      auto min_max_filter = std::make_shared<MinMaxFilter<T>>(dictionary.front(), dictionary.back());
      statistics->add_filter(min_max_filter);

      auto range_filter = RangeFilter<T>::build_filter(dictionary);
      if (range_filter) { // no range filter for strings
          statistics->add_filter(range_filter);
      }
    }
    return statistics;
}

template <typename T>
auto build_stats(const ValueColumn<T>& column) {
  DebugAssert(false, "Chunk statistics should only be computed for compressed columns!");
  return std::make_shared<ChunkColumnStatistics>();
}

template <typename T>
auto build_stats(const DeprecatedDictionaryColumn<T>& column) {
  auto statistics = std::make_shared<ChunkColumnStatistics>();
    const auto & dictionary = *column.dictionary();
    // only create statistics when the compressed dictionary is not empty
    if(!dictionary.empty()) {
      auto min_max_filter = std::make_shared<MinMaxFilter<T>>(dictionary.front(), dictionary.back());
      statistics->add_filter(min_max_filter);

      auto range_filter = RangeFilter<T>::build_filter(dictionary);
      if (range_filter) { // no range filter for strings
          statistics->add_filter(range_filter);
      }
    }
    return statistics;
}

template <typename T>
auto build_stats(const ReferenceColumn& column) {
  DebugAssert(false, "Chunk statistics should only be computed for compressed columns!");
  return std::make_shared<ChunkColumnStatistics>();
}

template <typename T>
auto build_stats(const RunLengthColumn<T>& column) {
  DebugAssert(false, "Not Implemented!");
  return std::make_shared<ChunkColumnStatistics>();
}
}

std::shared_ptr<ChunkColumnStatistics>
ChunkColumnStatistics::build_statistics(DataType data_type, std::shared_ptr<BaseColumn> column)
{
    std::shared_ptr<ChunkColumnStatistics> statistics;
    resolve_data_and_column_type(data_type, *column, [&statistics](auto type, auto& typed_column) {
        using ColumnDataType = typename decltype(type)::type;
        statistics = build_stats<ColumnDataType>(typed_column);
    });
    return statistics;
}

bool ChunkStatistics::can_prune(const ColumnID column_id, const AllTypeVariant& value,
                                const PredicateCondition scan_type) const {
  DebugAssert(column_id < _statistics.size(), "the passed column id should fit in the bounds of the statistics");
  DebugAssert(_statistics[column_id], "the statistics should contain no empty shared_ptrs");
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
