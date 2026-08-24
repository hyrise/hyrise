#pragma once

#include <algorithm>
#include <cstddef>
#include <type_traits>
#include <unordered_set>
#include <vector>

#include <boost/unordered/unordered_flat_set.hpp>

#include "operators/abstract_aggregate_operator.hpp"
#include "types.hpp"
#include "window_function_traits.hpp"

namespace hyrise {

// Holds the (intermediate) results for a single aggregate.
class AbstractAggregateVector {
 public:
  virtual ~AbstractAggregateVector() = default;
  virtual void grow_if_necessary(const size_t size) = 0;
  virtual void merge(AbstractAggregateVector& other) = 0;

  size_t count(const GroupID group_id) const {
    return _counts[group_id];
  }

  void increment_count(const GroupID group_id) {
    _counts[group_id]++;
  }

  const std::vector<size_t>& counts() const {
    return _counts;
  }

 protected:
  // Stores the number of rows that have been aggregated for each group.
  std::vector<size_t> _counts;
};

template <typename ColumnDataType, WindowFunction aggregate_function>
class TypedAggregateVector : public AbstractAggregateVector {
 protected:
  using AggregateDataType = typename WindowFunctionTraits<ColumnDataType, aggregate_function>::ReturnType;
  using AccumulatorDataType = std::conditional_t<aggregate_function == WindowFunction::CountDistinct,
                                                 boost::unordered_flat_set<ColumnDataType>, AggregateDataType>;

 public:
  // The mutable accessor is needed because aggregator functions mutate values directly.
  AccumulatorDataType& accumulator(const GroupID group_id) {
    return _accumulators[group_id];
  }

  const std::vector<AccumulatorDataType>& accumulators() const {
    return _accumulators;
  }

  void grow_if_necessary(const size_t size) override {
    if (_counts.size() < size) {
      _counts.resize(size);
      _accumulators.resize(size);
    }
  }

  void merge(AbstractAggregateVector& other) override {
    auto& typed_other = static_cast<TypedAggregateVector<ColumnDataType, aggregate_function>&>(other);
    _merge(typed_other);
  }

 protected:
  // Stores intermediate aggregate results for each group. This may not be the actual result of the
  // aggregation. For example, for AVG, the accumulators store the sum per group. The average is
  // only computed when the output is written.
  std::vector<AccumulatorDataType> _accumulators;

  void _merge(TypedAggregateVector<ColumnDataType, aggregate_function>& other) {
    const auto new_size = std::max(_accumulators.size(), other._accumulators.size());

    _accumulators.resize(new_size);
    _counts.resize(new_size);

    if constexpr (aggregate_function == WindowFunction::CountDistinct) {
      // For COUNT DISTINCT, the accumulators are sets of distinct values. Merge the other set into ours.
      auto& other_accumulators = other._accumulators;
      const auto other_size = other_accumulators.size();

      for (auto index = size_t{0}; index < new_size; ++index) {
        if (index < other_size) {
          _accumulators[index].merge(other_accumulators[index]);
        }
      }
    } else if constexpr (aggregate_function == WindowFunction::Count) {
      const auto& other_counts = other._counts;
      const auto other_size = other_counts.size();

      for (auto index = size_t{0}; index < new_size; ++index) {
        if (index < other_size && other_counts[index] > 0) {
          _counts[index] += other_counts[index];
        }
      }
    } else if constexpr (aggregate_function == WindowFunction::Any) {
      const auto& other_accumulators = other._accumulators;
      const auto& other_counts = other._counts;
      const auto other_size = other_accumulators.size();

      for (auto index = size_t{0}; index < new_size; ++index) {
        if (_counts[index] == 0 && index < other_size && other_counts[index] > 0) {
          _accumulators[index] = other_accumulators[index];
          _counts[index] += other_counts[index];
        }
      }
    } else {
      const auto& other_accumulators = other._accumulators;
      const auto& other_counts = other._counts;
      const auto other_size = other_accumulators.size();

      const auto aggregator =
          WindowFunctionBuilder<AggregateDataType, AggregateDataType, aggregate_function>().get_aggregate_function();

      for (auto index = size_t{0}; index < new_size; ++index) {
        if (index < other_size && other_counts[index] > 0) {
          aggregator(other_accumulators[index], _counts[index], _accumulators[index]);
          _counts[index] += other_counts[index];
        }
      }
    }
  }
};

}  // namespace hyrise
