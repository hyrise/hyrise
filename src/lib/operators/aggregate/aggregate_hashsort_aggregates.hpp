#pragma once

#include <memory>
#include <vector>

#include "operators/aggregate/aggregate_traits.hpp"
#include "storage/column_iterable.hpp"

namespace opossum {

namespace aggregate_hashsort {

// Data isn't copied/aggregated directly. Instead copy/aggregation operations are gathered and then executed as one.
struct AggregationBufferEntry {
  size_t target_offset;
  size_t source_offset;
};

struct BaseAggregateRun {
  virtual ~BaseAggregateRun() = default;

  virtual void resize(const size_t size) = 0;

  virtual void initialize(ColumnIterable& column_iterable, const RowID& begin_row_id, const size_t row_count) = 0;

  virtual std::unique_ptr<BaseAggregateRun> new_instance(const size_t size) const = 0;

  virtual void flush_append_buffer(size_t target_offset, const std::vector<size_t>& buffer,
                                   const BaseAggregateRun& base_aggregate_run) = 0;
  virtual void flush_aggregation_buffer(const std::vector<AggregationBufferEntry>& buffer,
                                        const BaseAggregateRun& base_aggregate_run) = 0;

  virtual std::shared_ptr<BaseSegment> materialize_output(const size_t end) = 0;
};

template <typename SourceColumnDataType, AggregateFunction aggregate_function, typename Derived>
struct BaseDistributiveAggregateRun : public BaseAggregateRun {
 public:
  using AggregateType = typename AggregateTraits<SourceColumnDataType, aggregate_function>::AggregateType;

  explicit BaseDistributiveAggregateRun(const size_t size) { resize(size); }

  void initialize(ColumnIterable& column_iterable, const RowID& begin_row_id, const size_t row_count) override {
    auto offset = size_t{0};
    column_iterable.for_each<SourceColumnDataType>(
    [&](const auto& segment_position, const RowID& row_id) {
      if (segment_position.is_null()) {
        null_values[offset] = true;
      } else {
        values[offset] = segment_position.value();
        null_values[offset] = false;
      }
      ++offset;

      return offset == row_count ? ColumnIteration::Break : ColumnIteration::Continue;
    },
    begin_row_id);
  }

  void resize(const size_t size) override {
    DebugAssert(size >= values.size(), "Cannot shrink aggregate runs");

    values.resize(size);
    null_values.resize(size, true);
  }

  void flush_append_buffer(size_t target_offset, const std::vector<size_t>& append_buffer,
                           const BaseAggregateRun& base_aggregate_run) override {
    DebugAssert(target_offset + append_buffer.size() <= values.size(), "Invalid append buffer length");

    const auto& source_run =
        static_cast<const BaseDistributiveAggregateRun<SourceColumnDataType, aggregate_function, Derived>&>(
            base_aggregate_run);

    for (const auto& source_offset : append_buffer) {
      values[target_offset] = source_run.values[source_offset];
      null_values[target_offset] = source_run.null_values[source_offset];
      ++target_offset;
    }
  }

  void flush_aggregation_buffer(const std::vector<AggregationBufferEntry>& buffer,
                                const BaseAggregateRun& base_source_run) override {
    const auto& source_run =
        static_cast<const BaseDistributiveAggregateRun<SourceColumnDataType, aggregate_function, Derived>&>(
            base_source_run);

    for (const auto& entry : buffer) {
      DebugAssert(entry.source_offset < source_run.values.size(), "Invalid aggregation buffer entry");
      DebugAssert(entry.target_offset < values.size(), "Invalid aggregation buffer entry");

      const auto& source_value = source_run.values[entry.source_offset];
      auto& target_value = values[entry.target_offset];

      if (source_run.null_values[entry.source_offset]) continue;

      if (null_values[entry.target_offset]) {
        target_value = source_value;
      } else {
        target_value = static_cast<Derived*>(this)->combine(target_value, source_value);
      }

      null_values[entry.target_offset] = false;
    }
  }

  std::shared_ptr<BaseSegment> materialize_output(const size_t end) override {
    values.resize(end);
    null_values.resize(end);
    return std::make_shared<ValueSegment<AggregateType>>(std::move(values), std::move(null_values));
  }

  std::vector<AggregateType> values;
  std::vector<bool> null_values;
};

template <typename SourceColumnDataType>
struct SumAggregateRun : public BaseDistributiveAggregateRun<SourceColumnDataType, AggregateFunction::Sum,
                                                             SumAggregateRun<SourceColumnDataType>> {
  using AggregateType = typename AggregateTraits<SourceColumnDataType, AggregateFunction::Sum>::AggregateType;
  using BaseDistributiveAggregateRun<SourceColumnDataType, AggregateFunction::Sum,
                                     SumAggregateRun<SourceColumnDataType>>::BaseDistributiveAggregateRun;

  AggregateType combine(const AggregateType& lhs, const AggregateType& rhs) const { return lhs + rhs; }

  std::unique_ptr<BaseAggregateRun> new_instance(const size_t size) const override {
    return std::make_unique<SumAggregateRun<SourceColumnDataType>>(size);
  }
};

template <typename SourceColumnDataType>
struct MinAggregateRun : public BaseDistributiveAggregateRun<SourceColumnDataType, AggregateFunction::Min,
                                                             MinAggregateRun<SourceColumnDataType>> {
  using AggregateType = typename AggregateTraits<SourceColumnDataType, AggregateFunction::Min>::AggregateType;
  using BaseDistributiveAggregateRun<SourceColumnDataType, AggregateFunction::Min,
                                     MinAggregateRun<SourceColumnDataType>>::BaseDistributiveAggregateRun;

  AggregateType combine(const AggregateType& lhs, const AggregateType& rhs) const { return std::min(lhs, rhs); }

  std::unique_ptr<BaseAggregateRun> new_instance(const size_t size) const override {
    return std::make_unique<MinAggregateRun<SourceColumnDataType>>(size);
  }
};

template <typename SourceColumnDataType>
struct MaxAggregateRun : public BaseDistributiveAggregateRun<SourceColumnDataType, AggregateFunction::Max,
                                                             MaxAggregateRun<SourceColumnDataType>> {
  using AggregateType = typename AggregateTraits<SourceColumnDataType, AggregateFunction::Max>::AggregateType;
  using BaseDistributiveAggregateRun<SourceColumnDataType, AggregateFunction::Max,
                                     MaxAggregateRun<SourceColumnDataType>>::BaseDistributiveAggregateRun;

  AggregateType combine(const AggregateType& lhs, const AggregateType& rhs) const { return std::max(lhs, rhs); }

  std::unique_ptr<BaseAggregateRun> new_instance(const size_t size) const override {
    return std::make_unique<MaxAggregateRun<SourceColumnDataType>>(size);
  }
};

struct CountRowsAggregateRun : public BaseAggregateRun {
 public:
  using AggregateType = typename AggregateTraits<void, AggregateFunction::CountRows>::AggregateType;

  explicit CountRowsAggregateRun(const size_t row_count) { values.resize(row_count, 1); }

  void initialize(ColumnIterable& column_iterable, const RowID& begin_row_id, const size_t row_count) override {
    Fail("Should not be called, CountRowsAggregateRun is already initialized in constructor");
  }

  void resize(const size_t size) override { values.resize(size, 0); }

  void flush_append_buffer(size_t target_offset, const std::vector<size_t>& buffer,
                           const BaseAggregateRun& base_aggregate_run) override {
    const auto& source_run = static_cast<const CountRowsAggregateRun&>(base_aggregate_run);
    for (const auto& source_offset : buffer) {
      values[target_offset] = source_run.values[source_offset];
      ++target_offset;
    }
  }

  void flush_aggregation_buffer(const std::vector<AggregationBufferEntry>& buffer,
                                const BaseAggregateRun& base_source_run) override {
    const auto& source_run = static_cast<const CountRowsAggregateRun&>(base_source_run);

    for (const auto& [target_offset, source_offset] : buffer) {
      values[target_offset] += source_run.values[source_offset];
    }
  }

  std::shared_ptr<BaseSegment> materialize_output(const size_t end) override {
    values.resize(end);
    return std::make_shared<ValueSegment<AggregateType>>(std::move(values));
  }

  std::unique_ptr<BaseAggregateRun> new_instance(const size_t size) const override {
    return std::make_unique<CountRowsAggregateRun>(size);
  }

  std::vector<AggregateType> values;
};

template <typename SourceColumnDataType>
struct CountNonNullAggregateRun : public BaseAggregateRun {
 public:
  using AggregateType = typename AggregateTraits<SourceColumnDataType, AggregateFunction::CountNonNull>::AggregateType;

  explicit CountNonNullAggregateRun(const size_t size) { resize(size); }

  void initialize(ColumnIterable& column_iterable, const RowID& begin_row_id, const size_t row_count) override {
    auto offset = size_t{0};
    column_iterable.for_each<SourceColumnDataType>(
    [&](const auto& segment_position, const RowID& row_id) {
      if (!segment_position.is_null()) {
        values[offset] = 1;
      }
      ++offset;
      return offset == row_count ? ColumnIteration::Break : ColumnIteration::Continue;
    },
    begin_row_id);
  }

  void resize(const size_t size) override { values.resize(size, 0); }

  void flush_append_buffer(size_t target_offset, const std::vector<size_t>& buffer,
                           const BaseAggregateRun& base_aggregate_run) override {
    const auto& source_run = static_cast<const CountNonNullAggregateRun&>(base_aggregate_run);
    for (const auto& source_offset : buffer) {
      values[target_offset] = source_run.values[source_offset];
      ++target_offset;
    }
  }

  void flush_aggregation_buffer(const std::vector<AggregationBufferEntry>& buffer,
                                const BaseAggregateRun& base_source_run) override {
    const auto& source_run = static_cast<const CountNonNullAggregateRun&>(base_source_run);

    for (const auto& [target_offset, source_offset] : buffer) {
      values[target_offset] += source_run.values[source_offset];
    }
  }

  std::shared_ptr<BaseSegment> materialize_output(const size_t end) override {
    values.resize(end);
    return std::make_shared<ValueSegment<AggregateType>>(std::move(values));
  }

  std::unique_ptr<BaseAggregateRun> new_instance(const size_t size) const override {
    return std::make_unique<CountNonNullAggregateRun>(size);
  }

  std::vector<AggregateType> values;
};

template <typename SourceColumnDataType>
struct CountDistinctAggregateRun : public BaseAggregateRun {
 public:
  using AggregateType = typename AggregateTraits<void, AggregateFunction::CountDistinct>::AggregateType;

  explicit CountDistinctAggregateRun(const size_t size) { resize(size); }

  void initialize(ColumnIterable& column_iterable, const RowID& begin_row_id, const size_t row_count) override {
    auto offset = size_t{0};
    column_iterable.for_each<SourceColumnDataType>(
    [&](const auto& segment_position, const RowID& row_id) {
      if (!segment_position.is_null()) {
        sets[offset].insert(segment_position.value());
      }
      ++offset;

      return offset == row_count ? ColumnIteration::Break : ColumnIteration::Continue;
    },
    begin_row_id);
  }

  void resize(const size_t size) override { sets.resize(size); }

  void flush_append_buffer(size_t target_offset, const std::vector<size_t>& buffer,
                           const BaseAggregateRun& base_aggregate_run) override {
    const auto& source_run = static_cast<const CountDistinctAggregateRun&>(base_aggregate_run);
    for (const auto& source_offset : buffer) {
      sets[target_offset] = source_run.sets[source_offset];
      ++target_offset;
    }
  }

  void flush_aggregation_buffer(const std::vector<AggregationBufferEntry>& buffer,
                                const BaseAggregateRun& base_source_run) override {
    const auto& source_run = static_cast<const CountDistinctAggregateRun&>(base_source_run);

    for (const auto& [target_offset, source_offset] : buffer) {
      const auto& source_set = source_run.sets[source_offset];
      sets[target_offset].insert(source_set.begin(), source_set.end());
    }
  }

  std::shared_ptr<BaseSegment> materialize_output(const size_t end) override {
    auto values = std::vector<AggregateType>(end);
    for (auto set_idx = size_t{0}; set_idx < end; ++set_idx) {
      values[set_idx] = sets[set_idx].size();
    }

    return std::make_shared<ValueSegment<AggregateType>>(std::move(values));
  }

  std::unique_ptr<BaseAggregateRun> new_instance(const size_t size) const override {
    return std::make_unique<CountDistinctAggregateRun>(size);
  }

  std::vector<std::unordered_set<SourceColumnDataType>> sets;
};

template <typename SourceColumnDataType>
struct AvgAggregateRun : public BaseAggregateRun {
 public:
  using AggregateType = typename AggregateTraits<SourceColumnDataType, AggregateFunction::Avg>::AggregateType;

  explicit AvgAggregateRun(const size_t size) { resize(size); }

  void initialize(ColumnIterable& column_iterable, const RowID& begin_row_id, const size_t row_count) override {
    auto offset = size_t{0};
    column_iterable.for_each<SourceColumnDataType>(
    [&](const auto& segment_position, const RowID& row_id) {
      if (!segment_position.is_null()) {
        pairs[offset].first += segment_position.value();
        ++pairs[offset].second;
      }
      ++offset;

      return offset == row_count ? ColumnIteration::Break : ColumnIteration::Continue;
    },
    begin_row_id);
  }

  void resize(const size_t size) override { pairs.resize(size); }

  void flush_append_buffer(size_t target_offset, const std::vector<size_t>& buffer,
                           const BaseAggregateRun& base_aggregate_run) override {
    const auto& source_run = static_cast<const AvgAggregateRun&>(base_aggregate_run);
    for (const auto& source_offset : buffer) {
      pairs[target_offset].first = source_run.pairs[source_offset].first;
      pairs[target_offset].second = source_run.pairs[source_offset].second;
      ++target_offset;
    }
  }

  void flush_aggregation_buffer(const std::vector<AggregationBufferEntry>& buffer,
                                const BaseAggregateRun& base_source_run) override {
    const auto& source_run = static_cast<const AvgAggregateRun<SourceColumnDataType>&>(base_source_run);

    for (const auto& [target_offset, source_offset] : buffer) {
      pairs[target_offset].first += source_run.pairs[source_offset].first;
      pairs[target_offset].second += source_run.pairs[source_offset].second;
    }
  }

  std::shared_ptr<BaseSegment> materialize_output(const size_t end) override {
    auto values = std::vector<AggregateType>(end);
    auto null_values = std::vector<bool>(end);

    auto target_offset = size_t{0};
    for (auto source_offset = size_t{0}; source_offset < end; ++source_offset, ++target_offset) {
      const auto& pair = pairs[source_offset];
      if (pair.second == 0) {
        null_values[target_offset] = true;
      } else {
        values[target_offset] = pair.first / pair.second;
      }
    }

    return std::make_shared<ValueSegment<AggregateType>>(std::move(values), std::move(null_values));
  }

  std::unique_ptr<BaseAggregateRun> new_instance(const size_t size) const override {
    return std::make_unique<AvgAggregateRun>(size);
  }

  std::vector<std::pair<AggregateType, size_t>> pairs;
};

}  // namespace aggregate_hashsort

}  // namespace opossum