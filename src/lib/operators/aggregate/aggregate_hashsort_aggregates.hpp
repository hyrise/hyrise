#pragma once

#include <memory>
#include <vector>

#include "operators/aggregate/aggregate_traits.hpp"
#include "storage/segment_iterate.hpp"

namespace opossum {

namespace aggregate_hashsort {

// Data isn't copied/aggregated directly. Instead copy/aggregation operations are gathered and then executed as one.
struct AggregationBufferEntry {
  size_t target_offset;
  size_t source_offset;
};

struct ColumnIterable {
  const Table& table;
  const ColumnID column_id;

  template <typename T, typename F>
  void for_each(const F& f) const {
    auto offset = size_t{0};

    for (const auto& chunk : table.chunks()) {
      const auto& segment = *chunk->get_segment(column_id);

      segment_with_iterators<T>(segment, [&](auto begin, auto end) {
        std::for_each(begin, end, [&](const auto& segment_position) {
          f(segment_position, offset);
          ++offset;
        });
      });
    }
  }
};

struct BaseColumnMaterialization {
  virtual ~BaseColumnMaterialization() = default;
};

template <typename T>
struct ColumnMaterialization : public BaseColumnMaterialization {
  std::vector<T> values;
  std::vector<bool> null_values;
};

struct BaseAggregateRun {
  virtual ~BaseAggregateRun() = default;

  virtual void resize(const size_t size) = 0;

  virtual std::unique_ptr<BaseAggregateRun> new_instance() const = 0;

  virtual void flush_append_buffer(const std::vector<size_t>& buffer, const BaseAggregateRun& base_aggregate_run) = 0;
  virtual void flush_aggregation_buffer(const std::vector<AggregationBufferEntry>& buffer,
                                        const BaseAggregateRun& base_aggregate_run) = 0;

  virtual std::shared_ptr<BaseSegment> materialize_output() const = 0;
};

template <typename SourceColumnDataType, AggregateFunction aggregate_function>
struct BaseDistributiveAggregateRun : public BaseAggregateRun {
 public:
  using AggregateType = typename AggregateTraits<SourceColumnDataType, aggregate_function>::AggregateType;

  BaseDistributiveAggregateRun() = default;

  BaseDistributiveAggregateRun(const ColumnIterable& column_iterable) {
    resize(column_iterable.table.row_count());

    column_iterable.for_each<SourceColumnDataType>([&](const auto& segment_position, const auto offset) {
      if (segment_position.is_null()) {
        null_values[offset] = true;
      } else {
        values[offset] = segment_position.value();
      }
    });
  }

  virtual AggregateType combine(const AggregateType& lhs, const AggregateType& rhs) const = 0;

  void resize(const size_t size) override {
    values.resize(size);
    null_values.resize(size);
  }

  void flush_append_buffer(const std::vector<size_t>& buffer, const BaseAggregateRun& base_aggregate_run) override {
    const auto& source_run =
    static_cast<const BaseDistributiveAggregateRun<SourceColumnDataType, aggregate_function>&>(base_aggregate_run);

    for (const auto& source_offset : buffer) {
      values.emplace_back(source_run.values[source_offset]);
      null_values.emplace_back(source_run.null_values[source_offset]);
    }
  }

  void flush_aggregation_buffer(const std::vector<AggregationBufferEntry>& buffer,
                                const BaseAggregateRun& base_source_run) override {
    const auto& source_run =
        static_cast<const BaseDistributiveAggregateRun<SourceColumnDataType, aggregate_function>&>(base_source_run);

    for (const auto& entry : buffer) {
      const auto& source_value = source_run.values[entry.source_offset];
      auto& target_value = values[entry.target_offset];

      if (source_run.null_values[entry.source_offset]) continue;

      if (null_values[entry.target_offset]) {
        target_value = source_value;
        continue;
      }

      target_value = combine(target_value, source_value);
    }
  }

  std::shared_ptr<BaseSegment> materialize_output() const override {
    return std::make_shared<ValueSegment<AggregateType>>(std::move(values), std::move(null_values));
  }

  std::vector<AggregateType> values;
  std::vector<bool> null_values;
};

template <typename SourceColumnDataType>
struct SumAggregateRun : public BaseDistributiveAggregateRun<SourceColumnDataType, AggregateFunction::Sum> {
  using AggregateType = typename AggregateTraits<SourceColumnDataType, AggregateFunction::Sum>::AggregateType;
  using BaseDistributiveAggregateRun<SourceColumnDataType, AggregateFunction::Sum>::BaseDistributiveAggregateRun;

  AggregateType combine(const AggregateType& lhs, const AggregateType& rhs) const override { return lhs + rhs; }

  std::unique_ptr<BaseAggregateRun> new_instance() const override {
    return std::make_unique<SumAggregateRun<SourceColumnDataType>>();
  }
};

template <typename SourceColumnDataType>
struct MinAggregateRun : public BaseDistributiveAggregateRun<SourceColumnDataType, AggregateFunction::Min> {
  using AggregateType = typename AggregateTraits<SourceColumnDataType, AggregateFunction::Min>::AggregateType;
  using BaseDistributiveAggregateRun<SourceColumnDataType, AggregateFunction::Min>::BaseDistributiveAggregateRun;

  AggregateType combine(const AggregateType& lhs, const AggregateType& rhs) const override {
    return std::min(lhs, rhs);
  }

  std::unique_ptr<BaseAggregateRun> new_instance() const override {
    return std::make_unique<MinAggregateRun<SourceColumnDataType>>();
  }
};

template <typename SourceColumnDataType>
struct MaxAggregateRun : public BaseDistributiveAggregateRun<SourceColumnDataType, AggregateFunction::Max> {
  using AggregateType = typename AggregateTraits<SourceColumnDataType, AggregateFunction::Max>::AggregateType;
  using BaseDistributiveAggregateRun<SourceColumnDataType, AggregateFunction::Max>::BaseDistributiveAggregateRun;

  AggregateType combine(const AggregateType& lhs, const AggregateType& rhs) const override {
    return std::max(lhs, rhs);
  }

  std::unique_ptr<BaseAggregateRun> new_instance() const override {
    return std::make_unique<MaxAggregateRun<SourceColumnDataType>>();
  }
};

struct CountRowsAggregateRun : public BaseAggregateRun {
 public:
  using AggregateType = typename AggregateTraits<void, AggregateFunction::CountRows>::AggregateType;

  explicit CountRowsAggregateRun(const size_t row_count) { values.resize(row_count, 1); }

  void resize(const size_t size) override { values.resize(size, 0); }

  void flush_append_buffer(const std::vector<size_t>& buffer, const BaseAggregateRun& base_aggregate_run) override {
    const auto& source_run = static_cast<const CountRowsAggregateRun&>(base_aggregate_run);
    for (const auto& source_offset : buffer) {
      values.emplace_back(source_run.values[source_offset]);
    }
  }

  void flush_aggregation_buffer(const std::vector<AggregationBufferEntry>& buffer,
                                const BaseAggregateRun& base_source_run) override {
    const auto& source_run = static_cast<const CountRowsAggregateRun&>(base_source_run);

    for (const auto& [source_offset, target_offset] : buffer) {
      values[target_offset] += source_run.values[source_offset];
    }
  }

  std::shared_ptr<BaseSegment> materialize_output() const override {
    return std::make_shared<ValueSegment<AggregateType>>(std::move(values));
  }

  std::unique_ptr<BaseAggregateRun> new_instance() const override {
    return std::make_unique<CountRowsAggregateRun>(0);
  }

  std::vector<AggregateType> values;
};

template <typename SourceColumnDataType>
struct CountNonNullAggregateRun : public BaseAggregateRun {
 public:
  using AggregateType = typename AggregateTraits<SourceColumnDataType, AggregateFunction::CountNonNull>::AggregateType;

  CountNonNullAggregateRun() = default;

  explicit CountNonNullAggregateRun(const ColumnIterable& column_iterable) {
    resize(column_iterable.table.row_count());

    column_iterable.for_each<SourceColumnDataType>([&](const auto& segment_position, const auto offset) {
      if (!segment_position.is_null()) {
        values[offset] = 1;
      }
    });
  }

  void resize(const size_t size) override { values.resize(size, 0); }

  void flush_append_buffer(const std::vector<size_t>& buffer, const BaseAggregateRun& base_aggregate_run) override {
    const auto& source_run = static_cast<const CountNonNullAggregateRun&>(base_aggregate_run);
    for (const auto& source_offset : buffer) {
      values.emplace_back(source_run.values[source_offset]);
    }
  }

  void flush_aggregation_buffer(const std::vector<AggregationBufferEntry>& buffer,
                                const BaseAggregateRun& base_source_run) override {
    const auto& source_run = static_cast<const CountNonNullAggregateRun&>(base_source_run);

    for (const auto& [source_offset, target_offset] : buffer) {
      values[target_offset] += source_run.values[source_offset];
    }
  }

  std::shared_ptr<BaseSegment> materialize_output() const override {
    return std::make_shared<ValueSegment<AggregateType>>(std::move(values));
  }

  std::unique_ptr<BaseAggregateRun> new_instance() const override {
    return std::make_unique<CountNonNullAggregateRun>();
  }

  std::vector<AggregateType> values;
};

template <typename SourceColumnDataType>
struct CountDistinctAggregateRun : public BaseAggregateRun {
 public:
  using AggregateType = typename AggregateTraits<void, AggregateFunction::CountDistinct>::AggregateType;

  CountDistinctAggregateRun() = default;

  explicit CountDistinctAggregateRun(const ColumnIterable& column_iterable) {
    resize(column_iterable.table.row_count());

    column_iterable.for_each<SourceColumnDataType>([&](const auto& segment_position, const auto offset) {
      if (!segment_position.is_null()) {
        sets[offset].insert(segment_position.value());
      }
    });
  }

  void resize(const size_t size) override { sets.resize(size); }

  void flush_append_buffer(const std::vector<size_t>& buffer, const BaseAggregateRun& base_aggregate_run) override {
    const auto& source_run = static_cast<const CountDistinctAggregateRun&>(base_aggregate_run);
    for (const auto& source_offset : buffer) {
      sets.emplace_back(source_run.sets[source_offset]);
    }
  }

  void flush_aggregation_buffer(const std::vector<AggregationBufferEntry>& buffer,
                                const BaseAggregateRun& base_source_run) override {
    const auto& source_run = static_cast<const CountDistinctAggregateRun&>(base_source_run);

    for (const auto& [source_offset, target_offset] : buffer) {
      const auto& source_set = source_run.sets[source_offset];
      sets[target_offset].insert(source_set.begin(), source_set.end());
    }
  }

  std::shared_ptr<BaseSegment> materialize_output() const override {
    auto values = std::vector<AggregateType>(sets.size());
    for (auto set_idx = size_t{0}; set_idx < sets.size(); ++set_idx) {
      values[set_idx] = sets[set_idx].size();
    }

    return std::make_shared<ValueSegment<AggregateType>>(std::move(values));
  }

  std::unique_ptr<BaseAggregateRun> new_instance() const override {
    return std::make_unique<CountDistinctAggregateRun>();
  }

  std::vector<std::unordered_set<SourceColumnDataType>> sets;
};

template <typename SourceColumnDataType>
struct AvgAggregateRun : public BaseAggregateRun {
 public:
  using AggregateType = typename AggregateTraits<SourceColumnDataType, AggregateFunction::Avg>::AggregateType;

  AvgAggregateRun() = default;

  explicit AvgAggregateRun(const ColumnIterable& column_iterable) {
    resize(column_iterable.table.row_count());

    column_iterable.for_each<SourceColumnDataType>([&](const auto& segment_position, const auto offset) {
      if (!segment_position.is_null()) {
        sets[offset].emplace_back(segment_position.value());
      }
    });
  }

  void resize(const size_t size) override { sets.resize(size); }

  void flush_append_buffer(const std::vector<size_t>& buffer, const BaseAggregateRun& base_aggregate_run) override {
    const auto& source_run = static_cast<const AvgAggregateRun&>(base_aggregate_run);
    for (const auto& source_offset : buffer) {
      sets.emplace_back(source_run.sets[source_offset]);
    }
  }

  void flush_aggregation_buffer(const std::vector<AggregationBufferEntry>& buffer,
                                const BaseAggregateRun& base_source_run) override {
    const auto& source_run = static_cast<const AvgAggregateRun<SourceColumnDataType>&>(base_source_run);

    for (const auto& [source_offset, target_offset] : buffer) {
      const auto& source_set = source_run.sets[source_offset];
      sets[target_offset].insert(sets[target_offset].end(), source_set.begin(), source_set.end());
    }
  }

  std::shared_ptr<BaseSegment> materialize_output() const override {
    auto values = std::vector<AggregateType>(sets.size());
    auto null_values = std::vector<bool>(sets.size());

    auto target_offset = size_t{0};
    for (auto source_offset = size_t{0}; source_offset < sets.size(); ++source_offset, ++target_offset) {
      const auto& source_values = sets[source_offset];
      if (source_values.empty()) {
        null_values[target_offset] = true;
      } else {
        values[target_offset] =
            std::accumulate(source_values.begin(), source_values.end(), AggregateType{0}) / source_values.size();
      }
    }

    return std::make_shared<ValueSegment<AggregateType>>(std::move(values), std::move(null_values));
  }

  std::unique_ptr<BaseAggregateRun> new_instance() const override {
    return std::make_unique<AvgAggregateRun>();
  }

  std::vector<std::vector<AggregateType>> sets;
};

inline std::vector<std::unique_ptr<BaseAggregateRun>> produce_initial_aggregates(
    const Table& table, const std::vector<AggregateColumnDefinition>& aggregate_column_definitions) {
  auto aggregates = std::vector<std::unique_ptr<BaseAggregateRun>>(aggregate_column_definitions.size());
  for (auto aggregate_idx = size_t{0}; aggregate_idx < aggregates.size(); ++aggregate_idx) {
    const auto& aggregate_column_definition = aggregate_column_definitions[aggregate_idx];

    if (aggregate_column_definition.function == AggregateFunction::CountRows) {
      aggregates[aggregate_idx] = std::make_unique<CountRowsAggregateRun>(table.row_count());
      continue;
    }

    const auto source_column_id = *aggregate_column_definition.column;

    resolve_data_type(table.column_data_type(*aggregate_column_definition.column), [&](const auto data_type_t) {
      using SourceColumnDataType = typename decltype(data_type_t)::type;

      ColumnIterable column_iterable{table, source_column_id};

      switch (aggregate_column_definition.function) {
        case AggregateFunction::Min:
          aggregates[aggregate_idx] = std::make_unique<MinAggregateRun<SourceColumnDataType>>(column_iterable);
          break;
        case AggregateFunction::Max:
          aggregates[aggregate_idx] = std::make_unique<MaxAggregateRun<SourceColumnDataType>>(column_iterable);
          break;
        case AggregateFunction::Sum:
          if constexpr (!std::is_same_v<SourceColumnDataType, pmr_string>) {
            aggregates[aggregate_idx] = std::make_unique<SumAggregateRun<SourceColumnDataType>>(column_iterable);
          } else {
            Fail("Cannot compute SUM() on string column");
          }
          break;
        case AggregateFunction::Avg:
          if constexpr (!std::is_same_v<SourceColumnDataType, pmr_string>) {
            aggregates[aggregate_idx] = std::make_unique<AvgAggregateRun<SourceColumnDataType>>(column_iterable);
          } else {
            Fail("Cannot compute AVG() on string column");
          }
          break;
        case AggregateFunction::CountRows:
          Fail("Handled above");
          break;
        case AggregateFunction::CountNonNull:
          aggregates[aggregate_idx] = std::make_unique<CountNonNullAggregateRun<SourceColumnDataType>>(column_iterable);
          break;
        case AggregateFunction::CountDistinct:
          aggregates[aggregate_idx] =
              std::make_unique<CountDistinctAggregateRun<SourceColumnDataType>>(column_iterable);
          break;
      }
    });
  }

  return aggregates;
}

}  // namespace aggregate_hashsort

}  // namespace opossum