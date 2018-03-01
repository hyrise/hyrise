#pragma once

#include <exception>
#include <memory>
#include <type_traits>
#include <vector>

#include "all_type_variant.hpp"
#include "type_cast.hpp"
#include "types.hpp"

#include "storage/base_encoded_column.hpp"
#include "storage/deprecated_dictionary_column.hpp"
#include "storage/dictionary_column.hpp"
#include "storage/reference_column.hpp"
#include "storage/run_length_column.hpp"
#include "storage/value_column.hpp"

namespace opossum {

class BaseFilter : public std::enable_shared_from_this<BaseFilter> {
 public:
  virtual ~BaseFilter() = default;

  virtual bool can_prune(const AllTypeVariant& value, const PredicateCondition predicate_type) const = 0;
};

class ChunkColumnStatistics {
 public:
  static std::shared_ptr<ChunkColumnStatistics> build_statistics(DataType data_type, std::shared_ptr<BaseColumn> column);

  void add_filter(std::shared_ptr<BaseFilter> filter) {
    _filters.emplace_back(filter);
  }

  bool can_prune(const AllTypeVariant& value, const PredicateCondition predicate_type) const {
    for(const auto& filter : _filters) {
      if(filter->can_prune(value, predicate_type)) {
        return true;
      }
    }
    return false;
  }

 protected:
  template<typename T>
  static std::shared_ptr<ChunkColumnStatistics> build_statistics_from_dictionary(const pmr_vector<T>& dictionary);

  template<typename T>
  static std::shared_ptr<ChunkColumnStatistics> build_statistics_from_concrete_column(const DictionaryColumn<T>& column) {
    const auto & dictionary = *column.dictionary();
    return build_statistics_from_dictionary(dictionary);
  }

  template<typename T>
  static std::shared_ptr<ChunkColumnStatistics> build_statistics_from_concrete_column(const ValueColumn<T>& column) {
  DebugAssert(false, "Chunk statistics should only be computed for compressed columns!");
  return std::make_shared<ChunkColumnStatistics>();
  }

  template<typename T>
  static std::shared_ptr<ChunkColumnStatistics> build_statistics_from_concrete_column(const DeprecatedDictionaryColumn<T>& column) {
    const auto & dictionary = *column.dictionary();
    return build_statistics_from_dictionary(dictionary);
  }

  template<typename T>
  static std::shared_ptr<ChunkColumnStatistics> build_statistics_from_concrete_column(const ReferenceColumn& column) {
  DebugAssert(false, "Chunk statistics should only be computed for compressed columns!");
  return std::make_shared<ChunkColumnStatistics>();
  }

  template<typename T>
  static std::shared_ptr<ChunkColumnStatistics> build_statistics_from_concrete_column(const RunLengthColumn<T>& column) {
  //DebugAssert(false, "Not Implemented!");
  return std::make_shared<ChunkColumnStatistics>();
  }

 protected:
  std::vector<std::shared_ptr<BaseFilter>> _filters;
};

template <typename T>
class MinMaxFilter : public BaseFilter {
 public:

  MinMaxFilter(T min, T max) : _min(min), _max(max) {};
  virtual ~MinMaxFilter() = default;

  bool can_prune(const AllTypeVariant& value, const PredicateCondition predicate_type) const override {
    T t_value = type_cast<T>(value);
    // Operators work as follows: value_from_table <operator> t_value
    // e.g. OpGreaterThan: value_from_table > t_value
    // thus we can exclude chunk if t_value >= _max since then no value from the table can be greater than t_value
    switch (predicate_type) {
      case PredicateCondition::GreaterThan:
        return t_value >= _max;
      case PredicateCondition::GreaterThanEquals:
        return t_value > _max;
      case PredicateCondition::LessThan:
        return t_value <= _min;
      case PredicateCondition::LessThanEquals:
        return t_value < _min;
      case PredicateCondition::Equals:
        return t_value < _min || t_value > _max;
      default:
        return false;
    }
  }
 protected:
  T _min;
  T _max;
};

template <typename T>
class RangeFilter : public BaseFilter {
 public:
  static_assert(std::is_arithmetic_v<T>, "RangeFilter should not be instantiated for strings.");

  RangeFilter(std::vector<std::pair<T,T>> ranges) : _ranges(ranges) {};
  virtual ~RangeFilter() = default;

  static std::unique_ptr<RangeFilter<T>> build_filter(const pmr_vector<T>& dictionary);

  bool can_prune(const AllTypeVariant& value, const PredicateCondition predicate_type) const override {
    T t_value = type_cast<T>(value);
    switch (predicate_type) {
      case PredicateCondition::Equals: {
        bool prunable = false;
        for(const auto& [min, max] : _ranges) {
          prunable |=  min < t_value && t_value < max;
        }
        return prunable;
      }
      default:
        return false;
    }
  }

 protected:
  std::vector<std::pair<T,T>> _ranges;
};

template<typename T>
std::unique_ptr<RangeFilter<T>> RangeFilter<T>::build_filter(const pmr_vector<T>& dictionary) {
  // calculate distances by taking the difference between two neighbouring elements
  std::vector<std::pair<T, size_t>> distances;
  distances.reserve(dictionary.size());
  for (auto dict_it = dictionary.cbegin(); dict_it + 1 != dictionary.cend(); ++dict_it) {
    auto dict_it_next = dict_it + 1;
    distances.emplace_back(*dict_it_next - *dict_it, std::distance(dictionary.cbegin(), dict_it));
  }

  std::sort(distances.begin(), distances.end(),
            [](const auto& pair1, const auto& pair2){ return pair1.first > pair2.first; });

  // select how many ranges we want in the filter
  // make this customizable?
  const size_t max_ranges_count = 10;

  if ((max_ranges_count - 1) < distances.size()) {
    distances.erase(distances.cbegin() + (max_ranges_count - 1), distances.cend());
  }

  std::sort(distances.begin(), distances.end(),
            [](const auto& pair1, const auto& pair2){ return pair1.second < pair2.second; });

  // derive intervals where items don't exist from distances
  //
  //         index  index + 1
  //         v      v
  // 1 2 3 4 5      10 11     15 16
  //         ^
  //       distance 5, index 4

  std::vector<std::pair<T,T>> ranges;
  for(const auto& distance_index_pair : distances) {
    // `index + 1` is ok because we check `dict_it + 1 != dictionary.cend()` above
    auto index = std::get<1>(distance_index_pair);
    ranges.push_back(std::make_pair(dictionary[index], dictionary[index + 1]));
  }

  return std::make_unique<RangeFilter<T>>(std::move(ranges));
}

template<typename T>
std::shared_ptr<ChunkColumnStatistics> ChunkColumnStatistics::build_statistics_from_dictionary(const pmr_vector<T>& dictionary) {
  auto statistics = std::make_shared<ChunkColumnStatistics>();
  // only create statistics when the compressed dictionary is not empty
  if(!dictionary.empty()) {
   auto min_max_filter = std::make_unique<MinMaxFilter<T>>(dictionary.front(), dictionary.back());
   statistics->add_filter(std::move(min_max_filter));

   // no range filter for strings
   if constexpr (std::is_arithmetic_v<T>) {
     auto range_filter = RangeFilter<T>::build_filter(dictionary);
     statistics->add_filter(std::move(range_filter));
   }
  }
  return statistics;
}

class ChunkStatistics : public std::enable_shared_from_this<ChunkStatistics> {
 public:
  explicit ChunkStatistics(std::vector<std::shared_ptr<ChunkColumnStatistics>> statistics) : _statistics(statistics) {}

  const std::vector<std::shared_ptr<ChunkColumnStatistics>>& statistics() const { return _statistics; }

  bool can_prune(const ColumnID column_id, const AllTypeVariant& value, const PredicateCondition predicate_type) const;

 protected:
  std::vector<std::shared_ptr<ChunkColumnStatistics>> _statistics;
};
}  // namespace opossum
