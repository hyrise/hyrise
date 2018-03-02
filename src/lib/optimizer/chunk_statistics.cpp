#include "chunk_statistics.hpp"

#include <iterator>

#include "all_parameter_variant.hpp"
#include "resolve_type.hpp"
#include "utils/assert.hpp"

namespace opossum {

template <typename T>
static std::shared_ptr<ChunkColumnStatistics> build_statistics_from_dictionary(const pmr_vector<T>& dictionary) {
  auto statistics = std::make_shared<ChunkColumnStatistics>();
  // only create statistics when the compressed dictionary is not empty
  if (!dictionary.empty()) {
    auto min_max_filter = std::make_unique<MinMaxFilter<T>>(dictionary.front(), dictionary.back());
    statistics->add_filter(std::move(min_max_filter));

    // no range filter for strings
    // clang-format off
    if constexpr(std::is_arithmetic_v<T>) {
      auto range_filter = RangeFilter<T>::build_filter(dictionary);
      statistics->add_filter(std::move(range_filter));
    }
    // clang-format on
  }
  return statistics;
}

template <typename T>
static std::shared_ptr<ChunkColumnStatistics> build_statistics_from_concrete_column(const DictionaryColumn<T>& column) {
  const auto& dictionary = *column.dictionary();
  return build_statistics_from_dictionary(dictionary);
}

template <typename T>
static std::shared_ptr<ChunkColumnStatistics> build_statistics_from_concrete_column(const ValueColumn<T>& column) {
  DebugAssert(false, "Chunk statistics should only be computed for compressed columns!");
  return std::make_shared<ChunkColumnStatistics>();
}

template <typename T>
static std::shared_ptr<ChunkColumnStatistics> build_statistics_from_concrete_column(
    const DeprecatedDictionaryColumn<T>& column) {
  const auto& dictionary = *column.dictionary();
  return build_statistics_from_dictionary(dictionary);
}

template <typename T>
static std::shared_ptr<ChunkColumnStatistics> build_statistics_from_concrete_column(const ReferenceColumn& column) {
  DebugAssert(false, "Chunk statistics should only be computed for compressed columns!");
  return std::make_shared<ChunkColumnStatistics>();
}

template <typename T>
static std::shared_ptr<ChunkColumnStatistics> build_statistics_from_concrete_column(const RunLengthColumn<T>& column) {
  // DebugAssert(false, "Not Implemented!");
  return std::make_shared<ChunkColumnStatistics>();
}

std::shared_ptr<ChunkColumnStatistics> ChunkColumnStatistics::build_statistics(DataType data_type,
                                                                               std::shared_ptr<BaseColumn> column) {
  std::shared_ptr<ChunkColumnStatistics> statistics;
  resolve_data_and_column_type(data_type, *column, [&statistics](auto type, auto& typed_column) {
    using ColumnDataType = typename decltype(type)::type;
    statistics = build_statistics_from_concrete_column<ColumnDataType>(typed_column);
  });
  return statistics;
}

bool ChunkStatistics::can_prune(const ColumnID column_id, const AllTypeVariant& value,
                                const PredicateCondition scan_type) const {
  DebugAssert(column_id < _statistics.size(), "The passed column ID should fit in the bounds of the statistics.");
  DebugAssert(_statistics[column_id], "The statistics should not contain any empty shared_ptrs.");
  return _statistics[column_id]->can_prune(value, scan_type);
}

}  // namespace opossum
