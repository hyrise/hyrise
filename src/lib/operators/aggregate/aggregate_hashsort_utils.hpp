#pragma once

#include <condition_variable>
#include <unordered_map>
#include <vector>

#include "murmur_hash.hpp"
#include "sparsehash/dense_hash_map"
#include "uninitialized_vector.hpp"

#include "all_type_variant.hpp"
#include "operators/abstract_aggregate_operator.hpp"
#include "operators/aggregate/aggregate_hashsort_aggregates.hpp"
#include "operators/aggregate/aggregate_hashsort_config.hpp"
#include "operators/aggregate/aggregate_traits.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "utils/timer.hpp"

#define VERBOSE 0

// #define USE_UNORDERED_MAP
#define USE_DENSE_HASH_MAP
//#define USE_STATIC_HASH_MAP

namespace opossum {

namespace aggregate_hashsort {

using Hash = size_t;

template <typename T>
T divide_and_ceil(const T& a, const T& b) {
  return a / b + (a % b > 0 ? 1 : 0);
}

/**
 * Data type used for blob storage of group data
 */
using GroupRunElementType = uint32_t;
constexpr auto GROUP_RUN_ELEMENT_SIZE = sizeof(GroupRunElementType);

const auto data_type_sizes = std::unordered_map<DataType, size_t>{{DataType::Int, sizeof(int32_t)},
                                                                  {DataType::Long, sizeof(int64_t)},
                                                                  {DataType::Float, sizeof(float)},
                                                                  {DataType::Double, sizeof(double)}};

/**
 * Prototype for an aggregate run
 */
struct AggregateHashSortAggregateDefinition {
  AggregateFunction aggregate_function{};
  std::optional<DataType> data_type;
  std::optional<ColumnID> column_id;

  explicit AggregateHashSortAggregateDefinition(const AggregateFunction aggregate_function)
      : aggregate_function(aggregate_function) {}
  AggregateHashSortAggregateDefinition(const AggregateFunction aggregate_function, const DataType data_type,
                                       const ColumnID column_id)
      : aggregate_function(aggregate_function), data_type(data_type), column_id(column_id) {}
};

// For gtest
inline bool operator==(const AggregateHashSortAggregateDefinition& lhs,
                       const AggregateHashSortAggregateDefinition& rhs) {
  return std::tie(lhs.aggregate_function, lhs.data_type, lhs.column_id) ==
         std::tie(rhs.aggregate_function, rhs.data_type, rhs.column_id);
}

struct AggregateHashSortEnvironment {
  AggregateHashSortConfig config;

  TableColumnDefinitions output_column_definitions;

  /**
   * The data of a group in its opaque blob is physically layouted as below. Meta info is at the
   * end because it is expected to have the least entropy and we want to early-out as soon as possible when comparing
   * groups from the beginning of their blobs.
   *
   * -- Fixed size values
   * [0] fixed_size_value_0_is_null (bool)
   * [1] fixed_size_value_0 (int32_t)
   * [5] fixed_size_value_1 (double)
   *
   * -- Variably sized values
   * [13] variably_sized_value_0 (string of length 7)
   * ...
   * [20] variably_sized_value_1_is_null (bool)
   * [21] variably_sized_value_1 (string of length 3)
   * ...
   *
   * -- Meta info about the variably sized values
   * [24] variably_sized_value_0_end_offset (size_t, in this case: 20)
   * [32] variably_sized_value_1_end_offset (size_t, in this case: 24)
   */

  // One entry for each group-by column
  // For fixed size values:     the offset in bytes in the group blob at which the value starts
  // For variably sized values: n, for the n-th variably sized column among the group by columns.
  std::vector<size_t> offsets;

  // One entry for each variably-sized group-by column
  std::vector<ColumnID> variably_sized_column_ids;

  // Offset in bytes in the group blob at which the variably sized values start
  size_t variably_sized_values_begin_offset{};

  // One entry for each fixed-size group-by column
  std::vector<ColumnID> fixed_size_column_ids;

  // Offsets in bytes from the beginning of the group blob where the n-th fixed-size value is stored.
  std::vector<size_t> fixed_size_value_offsets;

  // Accumulated size in GroupRunElements of the fixed-size values in a group
  size_t fixed_group_size{};

  // Prototypes for the aggregates
  std::vector<AggregateHashSortAggregateDefinition> aggregate_definitions;

  // Number of tasks working on the Aggregation. Once this reached zero, the aggregation is complete
  std::mutex global_task_counter_mutex;
  size_t global_task_counter{0};
  std::condition_variable done_condition;

  // Output data and associated mutex
  std::mutex output_chunks_mutex;
  std::vector<std::shared_ptr<Chunk>> output_chunks;

  void increment_global_task_counter() {
    // global_task_counter is used as a condition variable, thus it has to be modified under mutex lock
    auto lock = std::unique_lock{global_task_counter_mutex};
    ++global_task_counter;
  }

  void decrement_global_task_counter() {
    auto lock = std::unique_lock{global_task_counter_mutex};
    --global_task_counter;

    /**
     * Check if this was the last task in the entire Aggregation that completed. If that's the case, notify the main
     * thread
     */
    if (global_task_counter == 0) {
      done_condition.notify_all();
    }
  }

  static std::shared_ptr<AggregateHashSortEnvironment> create(const AggregateHashSortConfig& config, const std::shared_ptr<const Table>& table,
                                       const std::vector<AggregateColumnDefinition>& aggregate_column_definitions,
                                       const std::vector<ColumnID>& group_by_column_ids, const TableColumnDefinitions& table_column_definitions) {
    Assert(!group_by_column_ids.empty(),
           "AggregateHashSort cannot operate without group by columns, use a different aggregate operator");

    auto setup = std::make_shared<AggregateHashSortEnvironment>();

    setup->config = config;
    setup->offsets.resize(group_by_column_ids.size());
    setup->table_column_definitions = table_column_definitions;

    for (auto column_id = ColumnID{0}; column_id < group_by_column_ids.size(); ++column_id) {
      const auto group_by_column_id = group_by_column_ids[column_id];
      const auto data_type = table->column_data_type(group_by_column_id);

      if (data_type == DataType::String) {
        setup->offsets[column_id] = setup->variably_sized_column_ids.size();
        setup->variably_sized_column_ids.emplace_back(group_by_column_id);
      } else {
        setup->offsets[column_id] = setup->variably_sized_values_begin_offset;
        setup->fixed_size_column_ids.emplace_back(group_by_column_id);
        setup->fixed_size_value_offsets.emplace_back(setup->variably_sized_values_begin_offset);

        if (table->column_is_nullable(group_by_column_id)) {
          ++setup->variably_sized_values_begin_offset;  // one extra byte for the is_null flag
        }
        setup->variably_sized_values_begin_offset += data_type_sizes.at(data_type);
      }
    }

    setup->fixed_group_size = divide_and_ceil(setup->variably_sized_values_begin_offset, GROUP_RUN_ELEMENT_SIZE);

    for (auto& aggregate_column_definition : aggregate_column_definitions) {
      if (aggregate_column_definition.column) {
        setup->aggregate_definitions.emplace_back(aggregate_column_definition.function,
                                                 table->column_data_type(*aggregate_column_definition.column),
                                                 *aggregate_column_definition.column);
      } else {
        setup->aggregate_definitions.emplace_back(aggregate_column_definition.function);
      }
    }

    return setup;
  }
};

/**
 * Data structure to represent a run of groups and aggregates
 */
template <typename GroupSizePolicy>
struct BasicRun : public GroupSizePolicy {
  using HashTableKey = typename GroupSizePolicy::HashTableKey;
  using HashTableCompare = typename GroupSizePolicy::HashTableCompare;
  using GroupSizePolicyType = GroupSizePolicy;

  // `group_data` is an opaque blob storage, logically partitioned into groups by `GroupSizePolicy`.
  uninitialized_vector<GroupRunElementType> group_data;
  uninitialized_vector<Hash> hashes;

  std::vector<std::unique_ptr<BaseAggregateRun>> aggregates;

  std::vector<size_t> append_buffer;
  std::vector<AggregationBufferEntry> aggregation_buffer;

  bool is_aggregated{false};
  size_t size{};

  BasicRun() = default;

  BasicRun(const GroupSizePolicy& group_size_policy, std::vector<std::unique_ptr<BaseAggregateRun>>&& aggregates)
      : BasicRun(group_size_policy, std::move(aggregates)) {}

  BasicRun(const GroupSizePolicy& group_size_policy, std::vector<std::unique_ptr<BaseAggregateRun>>&& aggregates,
           const size_t group_capacity, const size_t group_data_capacity)
      : GroupSizePolicy(group_size_policy), aggregates(std::move(aggregates)) {
    group_data.resize(group_data_capacity);
    hashes.resize(group_capacity);
  }

  /**
   * @return    A hash table key for the group at `group_idx`
   */
  HashTableKey make_key(const size_t group_idx) const { return GroupSizePolicy::make_key(*this, group_idx); }

  /**
   * Schedule the append of a group to this run. Append operations are first gathered and executed only when
   * `flush_append_buffer()` is called
   */
  void append(const BasicRun& source, const size_t source_group_idx, const size_t flush_threshold) {
    append_buffer.emplace_back(source_group_idx);
    GroupSizePolicy::schedule_append(source, source_group_idx);

    if (append_buffer.size() > flush_threshold) {
      flush_buffers(source);
    }
  }

  /**
   * Schedule the aggregation of a group from the `source_run` with a group in this run
   */
  void aggregate(const size_t target_offset, const BasicRun& source_run, const size_t source_offset,
                 const size_t flush_threshold) {
    if (aggregates.empty()) {
      return;
    }

    aggregation_buffer.emplace_back(AggregationBufferEntry{target_offset, source_offset});

    if (aggregation_buffer.size() > flush_threshold) {
      flush_buffers(source_run);
    }
  }

  /**
   * Append the groups at indices `append_buffer` from `source` to this run.
   * Aggregate according to `aggregation_buffer`
   */
  void flush_buffers(const BasicRun& source) {
    const auto target_offset = size;

    /**
     * Flush append_buffer
     */
    const auto new_group_data_size = std::max(group_data.size(), GroupSizePolicy::get_required_group_data_size(*this));
    const auto new_group_count = std::max(hashes.size(), size + append_buffer.size());
    if (new_group_data_size > group_data.size() || new_group_count >= hashes.size()) {
      // Growing group_data should happen infrequently and is a safeguard against data skew
      group_data.resize(new_group_data_size);
      hashes.resize(new_group_count);
      GroupSizePolicy::resize(new_group_count, new_group_data_size);

      for (auto& aggregate : aggregates) {
        aggregate->resize(new_group_count);
      }
    }

    auto target_data_iter = GroupSizePolicy::get_append_iterator(*this);

    for (const auto source_group_idx : append_buffer) {
      const auto [group_begin_offset, group_length] = source.get_group_range(source_group_idx);

      const auto source_data_iter = source.group_data.begin() + group_begin_offset;
      std::uninitialized_copy(source_data_iter, source_data_iter + group_length, target_data_iter);

      hashes[size] = source.hashes[source_group_idx];

      GroupSizePolicy::append(*this, source, source_group_idx);

      target_data_iter += group_length;
      ++size;
    }

    /**
     * Flush aggregates
     */
    for (auto aggregate_idx = size_t{0}; aggregate_idx < aggregates.size(); ++aggregate_idx) {
      auto& target_aggregate_run = aggregates[aggregate_idx];
      const auto& input_aggregate_run = source.aggregates[aggregate_idx];
      target_aggregate_run->flush_append_buffer(target_offset, append_buffer, *input_aggregate_run);
      target_aggregate_run->flush_aggregation_buffer(aggregation_buffer, *input_aggregate_run);
    }

    aggregation_buffer.clear();
    append_buffer.clear();
  }

  /**
   * Utility for tests, not functionally necessary, but allows for `EXPECT_EQ(run.hashes, expected_hashes)`-like tests
   * without having to account for the possibility that `run.hashes` may contain unused/uninitialized elements at the
   * end.
   */
  void shrink_to_size() {
    DebugAssert(append_buffer.empty() && aggregation_buffer.empty(),
                "Cannot shrink_to_size run when operations are pending");

    if (size == 0) {
      group_data.resize(0);
    } else {
      const auto [last_group_begin, last_group_length] = GroupSizePolicy::get_group_range(size - 1);
      group_data.resize(last_group_begin + last_group_length);
    }

    hashes.resize(size);
    GroupSizePolicy::shrink_to_size(size);

    for (auto& aggregate : aggregates) {
      aggregate->resize(size);
    }
  }

  /**
   * @return     Begin byte and length in bytes of the variably sized value `value_idx` in group `group_idx`
   */
  std::pair<size_t, size_t> get_variably_sized_value_range(const AggregateHashSortEnvironment& setup, const size_t group_idx,
                                                           const size_t value_idx) const {
    const auto [group_begin_offset, group_length] = GroupSizePolicy::get_group_range(group_idx);

    const auto variably_sized_column_count = setup.variably_sized_column_ids.size();
    DebugAssert(value_idx < variably_sized_column_count, "value_idx out of range");

    const auto* group_end = reinterpret_cast<const char*>(&group_data[group_begin_offset] + group_length);
    const auto* group_value_end_offsets = group_end - sizeof(size_t) * variably_sized_column_count;

    auto value_end_offset = size_t{};
    memcpy(&value_end_offset, group_value_end_offsets + sizeof(size_t) * value_idx, sizeof(size_t));

    auto value_begin_offset = size_t{};
    if (value_idx == 0) {
      value_begin_offset = setup.variably_sized_values_begin_offset;
    } else {
      memcpy(&value_begin_offset, group_value_end_offsets + sizeof(size_t) * (value_idx - 1), sizeof(size_t));
    }

    return {group_begin_offset * sizeof(GroupRunElementType) + value_begin_offset,
            value_end_offset - value_begin_offset};
  }

  /**
   * Materialize a Segment from this run
   */
  template <typename T>
  std::shared_ptr<BaseSegment> materialize_group_column(const AggregateHashSortEnvironment& setup, const ColumnID column_id,
                                                        const bool column_is_nullable) const {
    auto output_values = std::vector<T>(size);

    auto output_null_values = std::vector<bool>();
    if (column_is_nullable) {
      output_null_values.resize(size);
    }

    auto output_values_iter = output_values.begin();
    auto output_null_values_iter = output_null_values.begin();

    if constexpr (std::is_same_v<T, pmr_string>) {
      const auto variably_sized_value_idx = setup.offsets[column_id];

      for (auto group_idx = size_t{0}; group_idx < size; ++group_idx) {
        const auto [value_begin_offset, value_length] =
            get_variably_sized_value_range(setup, group_idx, variably_sized_value_idx);

        auto* source = &reinterpret_cast<const char*>(group_data.data())[value_begin_offset];

        if (column_is_nullable) {
          if (*source != 0) {
            *output_null_values_iter = true;
          } else {
            *output_null_values_iter = false;
            *output_values_iter = pmr_string{source + 1, value_length - 1};
          }
          ++output_null_values_iter;
        } else {
          *output_values_iter = pmr_string{source, value_length};
        }
        ++output_values_iter;
      }
    } else if constexpr (std::is_arithmetic_v<T>) {
      const auto offset_in_group = setup.offsets[column_id];

      for (auto group_idx = size_t{0}; group_idx < size; ++group_idx) {
        const auto [group_begin_offset, group_length] = GroupSizePolicy::get_group_range(group_idx);

        auto* source = reinterpret_cast<const char*>(&group_data[group_begin_offset]) + offset_in_group;

        if (column_is_nullable) {
          if (*source != 0) {
            *output_null_values_iter = true;
          } else {
            *output_null_values_iter = false;
            memcpy(&(*output_values_iter), source + 1, sizeof(T));
          }
          ++output_null_values_iter;
        } else {
          memcpy(&(*output_values_iter), source, sizeof(T));
        }
        ++output_values_iter;
      }
    } else {
      Fail("Unsupported type");
    }

    if (column_is_nullable) {
      return std::make_shared<ValueSegment<T>>(std::move(output_values), std::move(output_null_values));
    } else {
      return std::make_shared<ValueSegment<T>>(std::move(output_values));
    }
  }
};

template <typename Run>
Run create_run(const AggregateHashSortEnvironment& setup, const size_t group_capacity = {},
               const size_t group_data_capacity = {}) {
  auto group_size_policy = typename Run::GroupSizePolicyType{setup, group_capacity};

  auto aggregates = std::vector<std::unique_ptr<BaseAggregateRun>>(setup.aggregate_definitions.size());

  for (auto aggregate_idx = size_t{0}; aggregate_idx < aggregates.size(); ++aggregate_idx) {
    const auto& aggregate_definition = setup.aggregate_definitions[aggregate_idx];

    if (aggregate_definition.aggregate_function == AggregateFunction::CountRows) {
      aggregates[aggregate_idx] = std::make_unique<CountRowsAggregateRun>(group_capacity);
      continue;
    }

    resolve_data_type(*aggregate_definition.data_type, [&](const auto data_type_t) {
      using SourceColumnDataType = typename decltype(data_type_t)::type;

      switch (aggregate_definition.aggregate_function) {
        case AggregateFunction::Min:
          aggregates[aggregate_idx] = std::make_unique<MinAggregateRun<SourceColumnDataType>>(group_capacity);
          break;
        case AggregateFunction::Max:
          aggregates[aggregate_idx] = std::make_unique<MaxAggregateRun<SourceColumnDataType>>(group_capacity);
          break;
        case AggregateFunction::Sum:
          if constexpr (!std::is_same_v<SourceColumnDataType, pmr_string>) {
            aggregates[aggregate_idx] = std::make_unique<SumAggregateRun<SourceColumnDataType>>(group_capacity);
          } else {
            Fail("Cannot compute SUM() on string column");
          }
          break;
        case AggregateFunction::Avg:
          if constexpr (!std::is_same_v<SourceColumnDataType, pmr_string>) {
            aggregates[aggregate_idx] = std::make_unique<AvgAggregateRun<SourceColumnDataType>>(group_capacity);
          } else {
            Fail("Cannot compute AVG() on string column");
          }
          break;
        case AggregateFunction::CountRows:
          Fail("Handled above");
          break;
        case AggregateFunction::CountNonNull:
          aggregates[aggregate_idx] = std::make_unique<CountNonNullAggregateRun<SourceColumnDataType>>(group_capacity);
          break;
        case AggregateFunction::CountDistinct:
          aggregates[aggregate_idx] = std::make_unique<CountDistinctAggregateRun<SourceColumnDataType>>(group_capacity);
          break;
        case AggregateFunction::StandardDeviationSample:
          Fail("Not yet implemented");
      }
    });
  }

  return {group_size_policy, std::move(aggregates), group_capacity, group_data_capacity};
}

template <size_t group_size>
struct FixedSizeInPlaceKey {
  static FixedSizeInPlaceKey EMPTY_KEY;

  bool empty{};
  size_t hash{};
  std::array<GroupRunElementType, group_size> group{};
};

template <size_t group_size>
FixedSizeInPlaceKey<group_size> FixedSizeInPlaceKey<group_size>::EMPTY_KEY{true, {}, {}};

#if VERBOSE
template <size_t group_size>
inline std::ostream& operator<<(std::ostream& stream, const FixedSizeInPlaceKey<group_size>& key) {
  stream << "{" << key.empty << " " << key.hash << " ";
  for (auto v : key.group) stream << v << " ";
  stream << "}";
  return stream;
}
#endif

struct FixedSizeInPlaceCompare {
#if VERBOSE
  mutable size_t counter{};
#endif
  explicit FixedSizeInPlaceCompare(const AggregateHashSortEnvironment& setup) {}

  template <size_t group_size>
  bool operator()(const FixedSizeInPlaceKey<group_size>& lhs, const FixedSizeInPlaceKey<group_size>& rhs) const {
#if VERBOSE
    ++counter;
#endif
    if (lhs.empty && rhs.empty) return true;
    if (lhs.empty ^ rhs.empty) return false;

    return lhs.group == rhs.group;
  }
};

/**
 * Group size policy used for small groups of fixed size (i.e. groups comprised of non-strings)
 */
template <size_t group_size>
struct StaticFixedGroupSizePolicy {
  using HashTableKey = FixedSizeInPlaceKey<group_size>;
  using HashTableCompare = FixedSizeInPlaceCompare;

  StaticFixedGroupSizePolicy(const AggregateHashSortEnvironment& setup, const size_t group_capacity) {}

  template <typename Run>
  auto get_append_iterator(Run& run) const {
    return run.group_data.begin() + group_size * run.size;
  }

  template <typename Run>
  size_t get_required_group_data_size(const Run& run) {
    return (run.size + run.append_buffer.size()) * run.get_group_size();
  }

  void resize(const size_t group_count, const size_t group_data_size) {}

  template <typename Run>
  void schedule_append(const Run& source_run, const size_t source_group_idx) {}

  template <typename Run>
  void append(Run& target_run, const Run& source_run, const size_t source_group_idx) const {}

  std::pair<size_t, size_t> get_group_range(const size_t group_idx) const {
    return {group_size * group_idx, group_size};
  }

  size_t get_group_size(const size_t group_idx = {}) const { return group_size; }

  template <typename Run>
  HashTableKey make_key(const Run& run, const size_t group_idx) const {
    HashTableKey key;
    key.hash = run.hashes[group_idx];
    const auto iter = run.group_data.begin() + group_idx * get_group_size();
    std::copy(iter, iter + get_group_size(), key.group.begin());
    return key;
  }

  void shrink_to_size(const size_t size) {}
};

struct FixedSizeRemoteKey {
  static FixedSizeRemoteKey EMPTY_KEY;

  size_t hash{};
  const GroupRunElementType* group{};
};

inline FixedSizeRemoteKey FixedSizeRemoteKey::EMPTY_KEY{0, nullptr};

#if VERBOSE
inline std::ostream& operator<<(std::ostream& stream, const FixedSizeRemoteKey& key) { return stream; }
#endif

struct FixedSizeRemoteCompare {
  size_t group_size{};

#if VERBOSE
  mutable size_t counter{};
#endif

  explicit FixedSizeRemoteCompare(const AggregateHashSortEnvironment& setup) : group_size(setup.fixed_group_size) {}

  bool operator()(const FixedSizeRemoteKey& lhs, const FixedSizeRemoteKey& rhs) const {
#if VERBOSE
    ++counter;
#endif
    if (!lhs.group && !rhs.group) return true;
    if (!lhs.group ^ !rhs.group) return false;

    return std::equal(lhs.group, lhs.group + group_size, rhs.group);
  }
};

/**
 * Group size policy used for groups of fixed size (i.e. groups comprised of non-strings)
 */
struct DynamicFixedGroupSizePolicy {
  using HashTableKey = FixedSizeRemoteKey;
  using HashTableCompare = FixedSizeRemoteCompare;

  size_t group_size{};

  DynamicFixedGroupSizePolicy(const AggregateHashSortEnvironment& setup, const size_t group_capacity)
      : group_size(setup.fixed_group_size) {}

  template <typename Run>
  size_t get_required_group_data_size(const Run& run) {
    return (run.size + run.append_buffer.size()) * run.get_group_size();
  }

  template <typename Run>
  auto get_append_iterator(Run& run) const {
    return run.group_data.begin() + group_size * run.size;
  }

  void resize(const size_t group_count, const size_t group_data_size) {}

  template <typename Run>
  void schedule_append(const Run& source_run, const size_t source_group_idx) {}

  template <typename Run>
  void append(Run& target_run, const Run& source_run, const size_t source_group_idx) const {}

  std::pair<size_t, size_t> get_group_range(const size_t group_idx) const {
    return {group_size * group_idx, group_size};
  }

  size_t get_group_size(const size_t group_idx = {}) const { return group_size; }

  template <typename Run>
  HashTableKey make_key(const Run& run, const size_t group_idx) const {
    return {run.hashes[group_idx], &run.group_data[group_idx * group_size]};
  }

  void shrink_to_size(const size_t size) {}
};

struct VariablySizedKey {
  static VariablySizedKey EMPTY_KEY;

  size_t hash;
  const GroupRunElementType* group;
  size_t size;
};

inline VariablySizedKey VariablySizedKey::EMPTY_KEY{{}, nullptr, {}};

#if VERBOSE
inline std::ostream& operator<<(std::ostream& stream, const VariablySizedKey& key) { return stream; }
#endif

struct VariablySizedCompare {
#if VERBOSE
  mutable size_t counter{};
#endif
  explicit VariablySizedCompare(const AggregateHashSortEnvironment& setup) {}

  bool operator()(const VariablySizedKey& lhs, const VariablySizedKey& rhs) const {
#if VERBOSE
    ++counter;
#endif
    return std::equal(lhs.group, lhs.group + lhs.size, rhs.group, rhs.group + rhs.size);
  }
};

/**
 * Group size policy used for groups of variable size (i.e. groups containing one or more strings)
 */
struct VariableGroupSizePolicy {
  using HashTableKey = VariablySizedKey;
  using HashTableCompare = VariablySizedCompare;

  uninitialized_vector<size_t> group_end_offsets;

  // Number of `data` elements occupied after `append_buffer` is flushed into `data`
  size_t data_watermark{0};

  VariableGroupSizePolicy(const AggregateHashSortEnvironment& setup, const size_t group_capacity)
      : group_end_offsets(group_capacity) {}

  template <typename Run>
  auto get_append_iterator(Run& run) const {
    return run.group_data.begin() + (run.size == 0 ? 0 : group_end_offsets[run.size - 1]);
  }

  template <typename Run>
  size_t get_required_group_data_size(const Run& run) {
    return run.data_watermark;
  }

  void resize(const size_t group_count, const size_t group_data_size) { group_end_offsets.resize(group_count); }

  template <typename Run>
  void schedule_append(const Run& source_run, const size_t source_group_idx) {
    const auto group_size = source_run.get_group_size(source_group_idx);
    data_watermark += group_size;
  }

  template <typename Run>
  void append(Run& target_run, const Run& source_run, const size_t source_group_idx) {
    const auto group_size = source_run.get_group_size(source_group_idx);
    group_end_offsets[target_run.size] =
        (target_run.size == 0 ? 0 : group_end_offsets[target_run.size - 1]) + group_size;
    data_watermark += group_size;
  }

  std::pair<size_t, size_t> get_group_range(const size_t group_idx) const {
    const auto group_begin_offset = group_idx == 0 ? size_t{0} : group_end_offsets[group_idx - 1];
    return {group_begin_offset, group_end_offsets[group_idx] - group_begin_offset};
  }

  size_t get_group_size(const size_t group_idx) const {
    return group_idx == 0 ? group_end_offsets.front() : group_end_offsets[group_idx] - group_end_offsets[group_idx - 1];
  }

  template <typename Run>
  HashTableKey make_key(const Run& run, const size_t group_idx) const {
    const auto [group_begin_offset, group_size] = get_group_range(group_idx);
    return {run.hashes[group_idx], &run.group_data[group_begin_offset], group_size};
  }

  void shrink_to_size(const size_t size) { group_end_offsets.resize(size); }
};

#if VERBOSE
inline std::string indent(const size_t depth) { return std::string(depth * 2, ' '); }
#endif

/**
 * Describes the bits in the hash value used for partitioning.
 * E.g. a group with hash=0b010100101; hash_shift=2; hash_mask=0b1111, goes into partition 0b1001=9
 */
struct RadixFanOut {
  size_t partition_count;
  size_t hash_shift;
  size_t hash_mask;

  /**
   * At level 0, the `partition_bit_count` leftmost bits are used, for level 1, the `partition_bit_count` bits after
   * those, and so forth.
   */
  static RadixFanOut for_level(const size_t level, const size_t partition_bit_count = 4) {
    // E.g., 1 << 4 bits used for partitioning == 16
    const auto partition_count = 1u << partition_bit_count;

    // E.g., 16 - 1 == 0b1111
    const auto mask = partition_count - 1u;

    // Number of bits in size_t
    const auto hash_bit_count = sizeof(size_t) * CHAR_BIT;

    // E.g., 64 bits in size_t / 6 bits per partition - 1 = 9 levels max
    const auto max_level = hash_bit_count / partition_bit_count - 1;
    Assert(level <= max_level,
           "Recursion level too deep, all hash bits exhausted. This is very likely caused by a bad hash function.");

    const auto shift = (hash_bit_count - partition_bit_count) - level * partition_bit_count;

    return {partition_count, shift, mask};
  }

  RadixFanOut(const size_t partition_count, const size_t hash_shift, const size_t hash_mask)
      : partition_count(partition_count), hash_shift(hash_shift), hash_mask(hash_mask) {}

  size_t get_partition_for_hash(const size_t hash) const { return (hash >> hash_shift) & hash_mask; }
};

// For gtest
inline bool operator==(const RadixFanOut& lhs, const RadixFanOut& rhs) {
  return std::tie(lhs.partition_count, lhs.hash_shift, lhs.hash_mask) ==
         std::tie(rhs.partition_count, rhs.hash_shift, rhs.hash_mask);
}

/**
 * @return hash table size
 */
inline size_t configure_hash_table(const AggregateHashSortEnvironment& setup, const size_t row_count) {
#ifdef USE_DENSE_HASH_MAP
  return std::min(setup.config.hash_table_size,
                  static_cast<size_t>(std::ceil(row_count / setup.config.hash_table_max_load_factor) / 2));
#endif

#ifdef USE_UNORDERED_MAP
  return static_cast<size_t>(
      std::ceil(std::min(config.hash_table_size * config.hash_table_max_load_factor, static_cast<float>(row_count)) /
                config.hash_table_max_load_factor));
#endif

#ifdef USE_STATIC_HASH_MAP
  return static_cast<size_t>(
      std::ceil(std::min(static_cast<float>(setup.config.hash_table_size), static_cast<float>(row_count)) /
                setup.config.hash_table_max_load_factor));
#endif
}

template <typename T>
size_t hash(const T* key, const size_t size, const size_t seed) {
  DebugAssert(size <= std::numeric_limits<int>::max(), "MurmurHash only accepts ints as lengths");
  return MurmurHash64A(key, static_cast<int>(size), seed);
}

template <typename T>
size_t hash(const T& key, const size_t seed) {
  return MurmurHash64A(&key, sizeof(key), seed);
}

enum class HashSortMode { Hashing, Partition };

template<typename Run>
struct RunReader {
  std::vector<Run> runs;

  // Current position of the read cursor
  size_t run_idx{};
  size_t run_offset{};

  // Number of groups, fetched and unfetched, remaining from the read cursor onward
  size_t total_remaining_group_count{};

  // Amount of data not yet processed (i.e., partitioned/hashed) by the reader. Provides hint for run sizes
  // during partitioning
  size_t remaining_fetched_group_count{};
  size_t remaining_fetched_group_data_size{};

  explicit RunReader(std::vector<Run>&& runs) : runs(std::move(runs)) {
    for (const auto& run : this->runs) {
      total_remaining_group_count += run.size;
      remaining_fetched_group_count += run.size;
      remaining_fetched_group_data_size += run.group_data.size();
    }
  }

  bool end_of_reader() const { return end_of_run() && run_idx + 1 == this->runs.size(); }

  void next_run(const AggregateHashSortEnvironment& setup) {
    DebugAssert(end_of_run(), "Current run wasn't fully processed");
    DebugAssert(!end_of_reader(), "There is no next run");

    ++run_idx;
    run_offset = 0;
  }

  size_t size() const {
    return std::accumulate(runs.begin(), runs.end(), size_t{0},
                           [](const size_t count, const auto& run) { return count + run.size; });
  }

  /**
   * @return The Run that the read cursor points to
   */
  const Run& current_run() const {
    DebugAssert(run_idx < runs.size(), "No current run");
    return runs[run_idx];
  }

  /**
   * @return Whether the read cursor is at the end of the current run
   */
  bool end_of_run() const { return runs.empty() || run_offset >= current_run().size; }

  /**
   * @return  Increments the read cursor to the next group in the current run (or to the end of the run, if no more
   *          groups are available)
   */
  void next_group_in_run() {
    DebugAssert(this->total_remaining_group_count > 0, "No next group");
    DebugAssert(this->remaining_fetched_group_count > 0, "No next group");
    DebugAssert(this->remaining_fetched_group_data_size >= current_run().get_group_size(run_offset), "Bug detected");

    --this->total_remaining_group_count;
    --this->remaining_fetched_group_count;
    this->remaining_fetched_group_data_size -= current_run().get_group_size(run_offset);
    ++run_offset;
  }

};

/**
 * Abstraction over the fact that group and aggregate data can either stem from a Table or a previously created
 * Partition
 */
template <typename Run>
struct AbstractRunSource {
  virtual ~AbstractRunSource() = default;
  virtual std::vector<Run> fetch_runs() = 0;
};

/**
 * Determines the size of a run to be allocated, based on the remaining data in the data source and the partitioning
 * fan out and the assumption that partitioning distributes the data evenly
 *
 * @return {group_count, group_data_size}
 */
template <typename Run>
std::pair<size_t, size_t> next_run_size(const AbstractRunSource<Run>& run_source, const size_t group_count,
                                        const RadixFanOut& radix_fan_out) {
  const auto estimated_group_size =
      static_cast<float>(run_source.remaining_fetched_group_data_size) / run_source.remaining_fetched_group_count;

  auto group_count_per_partition =
      std::ceil(static_cast<float>(group_count) / std::min(radix_fan_out.partition_count, group_count));
  auto data_size_per_partition = estimated_group_size * group_count_per_partition;

  if (radix_fan_out.partition_count > 1) {
    // Add additional space to account for possible imperfect uniformity of the hash values
    group_count_per_partition *= 1.2f;
    data_size_per_partition *= 1.2f;
  }

  Assert(group_count_per_partition > 0 && data_size_per_partition > 0, "Values should be greater than zero");

  return {group_count_per_partition, 0};
}

/**
 * A set of Runs
 */
template <typename Run>
struct Partition {
  std::vector<Run> runs;

  size_t size() const {
    return std::accumulate(runs.begin(), runs.end(), size_t{0},
                           [&](const auto size, const auto& run) { return size + run.size; });
  }
};

// Just forwards a number of runs
template <typename Run>
struct RunSource : public AbstractRunSource<Run> {
  explicit RunSource(std::vector<Run>&& runs):
      runs{std::move(runs)} {}

  std::vector<Run> fetch_runs() override { return std::move(runs); }

  std::vector<Run> runs;
};

// Wraps a Table from which the runs are materialized
template <typename Run>
struct ChunkRunSource : public AbstractRunSource<Run> {
  std::shared_ptr<const Chunk> chunk;

  ChunkRunSource(const AggregateHashSortEnvironment& setup, const std::shared_ptr<const Chunk>& chunk)
      : AbstractRunSource<Run>({}), chunk(chunk), unfetched_row_count(chunk->size()) {
    this->total_remaining_group_count = chunk->size();
  }

  std::vector<Run> fetch_runs() override { return std::move(runs); }

  bool end_of_source() const override {
    return this->run_idx + 1 >= this->runs.size() && this->end_of_run() && unfetched_row_count == 0;
  }

  void prefetch(const AggregateHashSortEnvironment& setup) override {
    while (this->remaining_fetched_group_count < setup.config.group_prefetch_threshold && unfetched_row_count > 0) {
      fetch_run(setup);
    }
  }

  void next_run(const AggregateHashSortEnvironment& setup) override {
    DebugAssert(this->end_of_run(), "Current run wasn't fully processed");
    DebugAssert(!this->end_of_source(), "There is no next run");

    if (this->run_idx + 1 >= this->runs.size()) {
      fetch_run(setup);
    }

    ++this->run_idx;
    this->run_offset = 0;
  }

  void fetch_run(const AggregateHashSortEnvironment& setup) {
    DebugAssert(!end_of_source(), "fetch_run() should not have been called");

    const auto row_count = std::min(unfetched_row_count, setup.config.initial_run_size);
#if VERBOSE
    std::cout << "fetch_run(): " << row_count << " of " << unfetched_row_count << " remaining" << std::endl;
#endif

    auto [run, end_row_id] = from_table_range(setup, table, begin_row_id, row_count);

    unfetched_row_count -= row_count;

    begin_row_id = end_row_id;

    this->remaining_fetched_group_count += run.size;
    this->remaining_fetched_group_data_size += run.group_data.size();

    this->runs.emplace_back(std::move(run));
  }

  size_t size() const override { return table->row_count(); }

  /**
   * Build a `VariablySizedGroupRun` from a range of `row_count` rows starting at `begin_row_id` of a Table.
   *
   * @returns   The VariablySizedGroupRun and the end RowID, i.e., the RowID the next run starts at
   */
  static std::pair<Run, RowID> from_table_range(const AggregateHashSortEnvironment& setup,
                                                const std::shared_ptr<const Table>& table, const RowID& begin_row_id,
                                                const size_t row_count) {
    using GroupSizePolicy = typename Run::GroupSizePolicyType;

    Assert(row_count > 0, "Cannot materialize zero rows");

    const auto variably_sized_column_count = setup.variably_sized_column_ids.size();
    const auto fixed_size_column_count = setup.fixed_size_column_ids.size();

    const auto meta_data_size = sizeof(size_t) * variably_sized_column_count;

    /**
     * Materialize the variably sized values separately first, then compute the size of the opaque `group_data` blob
     */
    const auto [data_per_column, value_end_offsets, variably_sized_end_row_id] =
        materialize_variably_sized_columns(setup, table, begin_row_id, row_count);

    auto run = create_run<Run>(setup, row_count, 0);

    if constexpr (std::is_same_v<GroupSizePolicy, VariableGroupSizePolicy>) {
      run.group_end_offsets =
          determine_group_end_offsets(setup.variably_sized_column_ids.size(), row_count, value_end_offsets);
      run.group_data.resize(run.group_end_offsets.back());
    } else {
      run.group_data.resize(row_count * run.get_group_size());
    }

    run.size = row_count;

    /**
     * Initialize the gaps between group data and meta data in the groups with zero
     */
    if constexpr (std::is_same_v<GroupSizePolicy, VariableGroupSizePolicy>) {
      auto value_end_offsets_iter = value_end_offsets.begin() + variably_sized_column_count - 1;
      for (auto group_idx = size_t{0}; group_idx < row_count; ++group_idx) {
        const auto [group_begin_offset, group_size] = run.get_group_range(group_idx);

        if (run.group_end_offsets[group_idx] > 0) {
          auto* target = reinterpret_cast<char*>(&run.group_data[group_begin_offset]) + *value_end_offsets_iter;
          const auto gap_size = group_size * GROUP_RUN_ELEMENT_SIZE - *value_end_offsets_iter - meta_data_size;

          memset(target, 0, gap_size);
        }

        value_end_offsets_iter += variably_sized_column_count;
      }
    } else {
      auto target_iter = run.group_data.begin() + run.get_group_size() - 1;
      for (auto group_idx = size_t{0}; group_idx < row_count; ++group_idx) {
        *target_iter = 0;
        target_iter += run.get_group_size();
      }
    }

    /**
     * Materialize the fixed size values into the opaque blob
     */
    auto fixed_size_end_row_id = RowID{};
    for (auto column_id = ColumnID{0}; column_id < fixed_size_column_count; ++column_id) {
      const auto group_by_column_id = setup.fixed_size_column_ids[column_id];
      const auto base_offset = setup.fixed_size_value_offsets[column_id];

      fixed_size_end_row_id =
          materialize_fixed_size_column(run, table, base_offset, group_by_column_id, begin_row_id, row_count);
    }

    /**
     * Copy the value end offsets into the interleaved blob
     */
    if constexpr (std::is_same_v<GroupSizePolicy, VariableGroupSizePolicy>) {
      auto* source = value_end_offsets.data();

      for (auto group_idx = size_t{0}; group_idx < row_count; ++group_idx) {
        const auto [group_begin_offset, group_size] = run.get_group_range(group_idx);

        auto* target =
            reinterpret_cast<char*>(run.group_data.data() + group_begin_offset + group_size) - meta_data_size;

        memcpy(target, source, meta_data_size);

        source += variably_sized_column_count;
      }
    }

    /**
     * Copy the variably-sized values from `data_per_column` into the interleaved blob
     */
    for (auto column_id = ColumnID{0}; column_id < variably_sized_column_count; ++column_id) {
      auto* source = reinterpret_cast<const char*>(data_per_column[column_id].data());

      for (auto row_idx = size_t{0}; row_idx < row_count; ++row_idx) {
        const auto [target_offset, value_size] = run.get_variably_sized_value_range(setup, row_idx, column_id);

        auto* target = &reinterpret_cast<char*>(run.group_data.data())[target_offset];

        memcpy(target, source, value_size);
        source += value_size;
      }
    }

    /**
     * Compute hashes
     */
    run.hashes.resize(row_count);
    for (auto group_idx = size_t{0}; group_idx < row_count; ++group_idx) {
      const auto [group_begin_offset, group_size] = run.get_group_range(group_idx);
      run.hashes[group_idx] = hash(&run.group_data[group_begin_offset], group_size * sizeof(GroupRunElementType), 0);
    }

    /**
     * Materialize the aggregates
     */
    for (auto aggregate_idx = size_t{0}; aggregate_idx < run.aggregates.size(); ++aggregate_idx) {
      if (!setup.aggregate_definitions[aggregate_idx].column_id) {
        continue;
      }

      auto column_iterable = ColumnIterable{table, *setup.aggregate_definitions[aggregate_idx].column_id};

      run.aggregates[aggregate_idx]->initialize(column_iterable, begin_row_id, row_count);
    }

    const auto end_row_id = variably_sized_column_count > 0 ? variably_sized_end_row_id : fixed_size_end_row_id;
    return {std::move(run), end_row_id};
  }

  /**
   * Subroutine of `from_table_range()`. Exposed for better testability.
   *
   * Materialize values from each variably-sized column into a one continuous blob per column (`data_per_column`).
   * (For strings, exclude the `\0`-terminator)
   *
   * While doing so, build `value_end_offsets` which stores the offset (in bytes) from the beginning of the final
   * group where each variably-sized value ends.
   *
   * @return {data_per_column, value_end_offsets, end_row_id}
   */
  static std::tuple<std::vector<uninitialized_vector<char>>, uninitialized_vector<size_t>, RowID>
  materialize_variably_sized_columns(const AggregateHashSortEnvironment& setup, const std::shared_ptr<const Table>& table,
                                     const RowID& begin_row_id, const size_t row_count) {
    const auto variably_sized_column_count = setup.variably_sized_column_ids.size();

    // Return values
    auto end_row_id = RowID{};
    auto value_end_offsets = uninitialized_vector<size_t>(row_count * variably_sized_column_count);
    auto data_per_column = std::vector<uninitialized_vector<char>>(variably_sized_column_count);

    for (auto column_id = ColumnID{0}; column_id < variably_sized_column_count; ++column_id) {
      // Initial budget is one byte per row. Grown aggressively below.
      auto& column_data = data_per_column[column_id];
      column_data.resize(row_count);

      const auto group_by_column_id = setup.variably_sized_column_ids[column_id];
      const auto column_is_nullable = table->column_is_nullable(group_by_column_id);

      // Number of values from this column already materialized
      auto column_row_count = size_t{0};

      // If this is exhausted, we need to grow `column_data`
      auto column_remaining_data_budget_bytes = column_data.size();

      auto* target = reinterpret_cast<char*>(column_data.data());

      auto value_end_offsets_iter = value_end_offsets.begin() + column_id;

      const auto column_iterable = ColumnIterable{table, group_by_column_id};
      end_row_id = column_iterable.for_each<pmr_string>(
          [&](const auto& segment_position, const RowID& row_id) {
            const auto& value = segment_position.value();

            // Determine `value_size`
            auto value_size = size_t{};
            if (column_is_nullable) {
              if (segment_position.is_null()) {
                value_size = 1;
              } else {
                value_size = 1 + value.size();
              }
            } else {
              value_size = value.size();
            }

            while (value_size > column_remaining_data_budget_bytes) {
              const auto target_idx = target - column_data.data();

              // Grow the buffer available for the column aggressively
              column_data.resize(column_data.size() * 4);

              column_remaining_data_budget_bytes = column_data.size() - target_idx;
              target = column_data.data() + target_idx;
            }

            // Set `value_end_offsets`
            const auto previous_value_end =
                column_id == 0 ? setup.variably_sized_values_begin_offset : *(value_end_offsets_iter - 1);
            *value_end_offsets_iter = previous_value_end + value_size;
            value_end_offsets_iter += variably_sized_column_count;

            // Write `column_data`
            if (column_is_nullable) {
              if (segment_position.is_null()) {
                *target = 1;
              } else {
                *target = 0;
                memcpy(target + 1, value.data(), value_size - 1);
              }
            } else {
              memcpy(target, value.data(), value_size);
            }

            target += value_size;
            column_remaining_data_budget_bytes -= value_size;

            ++column_row_count;

            return column_row_count == row_count ? ColumnIteration::Break : ColumnIteration::Continue;
          },
          begin_row_id);

      // Cut off unused trailing storage - not necessarily, but makes testing easier
      column_data.resize(target - column_data.data());

      Assert(column_row_count == row_count, "Invalid row_count passed");
    }

    return {std::move(data_per_column), std::move(value_end_offsets), end_row_id};
  }

  /**
   * Subroutine of `from_table_range()`. Exposed for better testability.
   * Determine the `group_end_offsets`, i.e. the offset in the blob where a group ends
   *
   * @return    `group_end_offsets`
   */
  static uninitialized_vector<size_t> determine_group_end_offsets(
      const size_t variably_sized_column_count, const size_t row_count,
      const uninitialized_vector<size_t>& value_end_offsets) {
    auto group_end_offsets = uninitialized_vector<size_t>(row_count);
    const auto meta_data_size = variably_sized_column_count * sizeof(size_t);

    auto previous_group_end_offset = size_t{0};
    auto value_end_offset_iter = value_end_offsets.begin() + (variably_sized_column_count - 1);
    for (auto row_idx = size_t{0}; row_idx < row_count; ++row_idx) {
      const auto group_size = *value_end_offset_iter + meta_data_size;

      // Round up to a full DATA_ELEMENT_SIZE for each group
      const auto group_element_count = divide_and_ceil(group_size, GROUP_RUN_ELEMENT_SIZE);

      previous_group_end_offset += group_element_count;

      group_end_offsets[row_idx] = previous_group_end_offset;
      value_end_offset_iter += variably_sized_column_count;
    }

    return group_end_offsets;
  }

  /**
   * Subroutine of `from_table_range()`. Exposed for better testability.
   * Materialize the fixed-size columns into the opaque data blob
   * @return    The end RowID
   */
  static RowID materialize_fixed_size_column(Run& run, const std::shared_ptr<const Table>& table,
                                             const size_t base_offset, const ColumnID column_id,
                                             const RowID& begin_row_id, const size_t row_count) {
    auto group_idx = size_t{0};

    const auto column_is_nullable = table->column_is_nullable(column_id);

    auto column_iterable = ColumnIterable{table, column_id};
    const auto end_row_id = column_iterable.for_each<ResolveDataTypeTag>(
        [&](const auto& segment_position, const RowID& row_id) {
          using ColumnDataType = typename std::decay_t<decltype(segment_position)>::Type;

          const auto [group_begin_offset, group_size] = run.get_group_range(group_idx);

          auto* target = reinterpret_cast<char*>(&run.group_data[group_begin_offset]) + base_offset;

          if (column_is_nullable) {
            if (segment_position.is_null()) {
              *target = 1;
              memset(target + 1, 0, sizeof(ColumnDataType));
            } else {
              *target = 0;
              memcpy(target + 1, &segment_position.value(), sizeof(ColumnDataType));
            }
          } else {
            memcpy(target, &segment_position.value(), sizeof(ColumnDataType));
          }

          ++group_idx;

          return group_idx == row_count ? ColumnIteration::Break : ColumnIteration::Continue;
        },
        begin_row_id);

    Assert(group_idx == row_count, "Illegal row_count parameter passed");

    return end_row_id;
  }
};

}  // namespace aggregate_hashsort

}  // namespace opossum
