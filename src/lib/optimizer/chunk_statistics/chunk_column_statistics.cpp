#include "chunk_column_statistics.hpp"

#include <iterator>

#include "resolve_type.hpp"

#include "optimizer/chunk_statistics/abstract_filter.hpp"
#include "optimizer/chunk_statistics/min_max_filter.hpp"
#include "optimizer/chunk_statistics/range_filter.hpp"
#include "storage/base_encoded_column.hpp"
#include "storage/deprecated_dictionary_column.hpp"
#include "storage/dictionary_column.hpp"
#include "storage/reference_column.hpp"
#include "storage/run_length_column.hpp"
#include "storage/value_column.hpp"

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
  // TODO(tbjoern): implement build_statistics_from_concrete_column
  return std::make_shared<ChunkColumnStatistics>();
}

static std::shared_ptr<ChunkColumnStatistics> build_statistics_from_concrete_column(const BaseColumn& column) {
  return std::make_shared<ChunkColumnStatistics>();
}

std::shared_ptr<ChunkColumnStatistics> ChunkColumnStatistics::build_statistics(DataType data_type,
                                                                               std::shared_ptr<BaseColumn> column) {
  std::shared_ptr<ChunkColumnStatistics> statistics;
  resolve_data_and_column_type(data_type, *column, [&statistics](auto type, auto& typed_column) {
    statistics = build_statistics_from_concrete_column(typed_column);
  });
  return statistics;
}
void ChunkColumnStatistics::add_filter(std::shared_ptr<AbstractFilter> filter) { _filters.emplace_back(filter); }

bool ChunkColumnStatistics::can_prune(const AllTypeVariant& value, const PredicateCondition predicate_type) const {
  for (const auto& filter : _filters) {
    if (filter->can_prune(value, predicate_type)) {
      return true;
    }
  }
  return false;
}
}  // namespace opossum
