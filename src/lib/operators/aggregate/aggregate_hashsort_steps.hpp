#include <unordered_map>
#include <vector>

#include "sparsehash/dense_hash_map"
#include "murmur_hash.hpp"
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

const auto data_type_sizes = std::unordered_map<DataType, size_t>{
    {DataType::Int, sizeof(int32_t)}, {DataType::Long, sizeof(int64_t)}, {DataType::Float, sizeof(float)}, {DataType::Double, sizeof(double)}};

/**
 * Prototype for an aggregate run
 */
struct AggregateHashSortAggregateDefinition {
  AggregateFunction aggregate_function{};
  std::optional<DataType> data_type;
  std::optional<ColumnID> column_id;

  explicit AggregateHashSortAggregateDefinition(const AggregateFunction aggregate_function): aggregate_function(aggregate_function) {}
  AggregateHashSortAggregateDefinition(const AggregateFunction aggregate_function, const DataType data_type, const ColumnID column_id): aggregate_function(aggregate_function), data_type(data_type), column_id(column_id) {}
};


// For gtest
inline bool operator==(const AggregateHashSortAggregateDefinition& lhs, const AggregateHashSortAggregateDefinition& rhs) {
  return std::tie(lhs.aggregate_function, lhs.data_type, lhs.column_id) == std::tie(rhs.aggregate_function, rhs.data_type, rhs.column_id);
}

struct AggregateHashSortDefinition {
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

  static AggregateHashSortDefinition create(const std::shared_ptr<const Table>& table,
                                            const std::vector<AggregateColumnDefinition>& aggregate_column_definitions,
                                            const std::vector<ColumnID>& group_by_column_ids) {
    Assert(!group_by_column_ids.empty(), "AggregateHashSort cannot operate without group by columns, use a different aggregate operator");

    auto definition = AggregateHashSortDefinition{};

    definition.offsets.resize(group_by_column_ids.size());

    for (auto column_id = ColumnID{0}; column_id < group_by_column_ids.size(); ++column_id) {
      const auto group_by_column_id = group_by_column_ids[column_id];
      const auto data_type = table->column_data_type(group_by_column_id);

      if (data_type == DataType::String) {
        definition.offsets[column_id] = definition.variably_sized_column_ids.size();
        definition.variably_sized_column_ids.emplace_back(group_by_column_id);
      } else {
        definition.offsets[column_id] = definition.variably_sized_values_begin_offset;
        definition.fixed_size_column_ids.emplace_back(group_by_column_id);
        definition.fixed_size_value_offsets.emplace_back(definition.variably_sized_values_begin_offset);

        if (table->column_is_nullable(group_by_column_id)) {
          ++definition.variably_sized_values_begin_offset; // one extra byte for the is_null flag
        }
        definition.variably_sized_values_begin_offset += data_type_sizes.at(data_type);
      }
    }

    definition.fixed_group_size =  divide_and_ceil(definition.variably_sized_values_begin_offset, GROUP_RUN_ELEMENT_SIZE);

    for (auto& aggregate_column_definition : aggregate_column_definitions) {
      if (aggregate_column_definition.column) {
        definition.aggregate_definitions.emplace_back(aggregate_column_definition.function, table->column_data_type(*aggregate_column_definition.column), *aggregate_column_definition.column);
      } else {
        definition.aggregate_definitions.emplace_back(aggregate_column_definition.function);
      }
    }

    return definition;
  }
};

/**
 * Data structure to represent a run of groups and aggregates
 */
template <typename GroupSizePolicy>
struct BasicRun : public GroupSizePolicy {
  using HashTableKey = typename GroupSizePolicy::HashTableKey;
  using HashTableCompare = typename GroupSizePolicy::HashTableCompare;
  using HashTableHasher = typename GroupSizePolicy::HashTableHasher;

  // `group_data` is an opaque blob storage, logically partitioned into groups by `GroupSizePolicy`.
  uninitialized_vector<GroupRunElementType> group_data;
  uninitialized_vector<Hash> hashes;

  std::vector<std::unique_ptr<BaseAggregateRun>> aggregates;

  std::vector<size_t> append_buffer;
  std::vector<AggregationBufferEntry> aggregation_buffer;

  bool is_aggregated{false};
  size_t size{};

  BasicRun() = default;

  BasicRun(const GroupSizePolicy& group_size_policy, std::vector<std::unique_ptr<BaseAggregateRun>>&& aggregates, const size_t group_capacity, const size_t group_data_capacity)
      : GroupSizePolicy(group_size_policy), aggregates(std::move(aggregates)) {
    group_data.resize(group_data_capacity);
    hashes.resize(group_capacity);
  }

  /**
   * @return    A hash table key for the group at `group_idx`
   */
  HashTableKey make_key(const size_t group_idx) const {
    return GroupSizePolicy::make_key(*this, group_idx);
  }

  /**
   * @return    Whether there is enough capacity in this run to append a group from another run
   */
  bool can_append(const BasicRun& source, const size_t source_group_idx) const {
    return GroupSizePolicy::can_append(*this, source, source_group_idx);
  }

  /**
   * Schedule the append of a group to this run. Append operations are first gathered and executed only when
   * `flush_append_buffer()` is called
   */
  void schedule_append(const BasicRun& source, const size_t source_group_idx) {
    DebugAssert(can_append(source, source_group_idx), "Invalid schedule_append() call");

    append_buffer.emplace_back(source_group_idx);
    GroupSizePolicy::schedule_append(source, source_group_idx);
  }

  /**
   * Append the groups at indices `append_buffer` from `source` to this run.
   */
  void flush_append_buffer(const BasicRun& source) {
    auto target_data_iter = GroupSizePolicy::get_append_iterator(*this);

    for (const auto source_group_idx : append_buffer) {
      const auto [group_begin_offset, group_length] = source.get_group_range(source_group_idx);

      const auto source_data_iter = source.data.begin() + group_begin_offset;
      std::uninitialized_copy(source_data_iter, source_data_iter + group_length, target_data_iter);

      hashes[size] = source.hashes[source_group_idx];

      GroupSizePolicy::append(*this, source, source_group_idx);

      target_data_iter += group_length;
      ++size;
    }

    append_buffer.clear();
  }

  void shrink_to_size() {
    DebugAssert(append_buffer.empty() && aggregation_buffer.empty(), "Cannot shrink_to_size run when operations are pending");

    if (size == 0) {
      group_data.resize(0);
    } else {
      const auto [last_group_begin, last_group_length] = GroupSizePolicy::get_group_range(size - 1);
      group_data.resize(last_group_begin + last_group_length);
    }
    
    hashes.resize(size);
    
    GroupSizePolicy::shrink_to_size(size);
  }

  /**
   * @return     Begin byte and length in bytes of the variably sized value `value_idx` in group `group_idx`
   */
  std::pair<size_t, size_t> get_variably_sized_value_range(const AggregateHashSortDefinition& definition, const size_t group_idx, const size_t value_idx) const {
    const auto [group_begin_offset, group_length] = GroupSizePolicy::get_group_range(group_idx);

    const auto variably_sized_column_count = definition.variably_sized_column_ids.size();
    DebugAssert(value_idx < variably_sized_column_count, "value_idx out of range");

    const auto* group_end = reinterpret_cast<const char*>(&group_data[group_begin_offset] + group_length);
    const auto* group_value_end_offsets = group_end - sizeof(size_t) * variably_sized_column_count;

    auto value_end_offset = size_t{};
    memcpy(&value_end_offset, group_value_end_offsets + sizeof(size_t) * value_idx, sizeof(size_t));

    auto value_begin_offset = size_t{};
    if (value_idx == 0) {
      value_begin_offset = definition.variably_sized_values_begin_offset;
    } else {
      memcpy(&value_begin_offset, group_value_end_offsets + sizeof(size_t) * (value_idx - 1), sizeof(size_t));
    }

    return {group_begin_offset * sizeof(GroupRunElementType) + value_begin_offset, value_end_offset - value_begin_offset};
  }
  
  /**
   * Materialize a Segment from this run
   */
  template <typename T>
  std::shared_ptr<BaseSegment> materialize_group_column(const AggregateHashSortDefinition& definition, const ColumnID column_id, const bool column_is_nullable) const {
    auto output_values = std::vector<T>(size);

    auto output_null_values = std::vector<bool>();
    if (column_is_nullable) {
      output_null_values.resize(size);
    }

    auto output_values_iter = output_values.begin();
    auto output_null_values_iter = output_null_values.begin();

    if constexpr (std::is_same_v<T, pmr_string>) {
      const auto variably_sized_value_idx = definition.offsets[column_id];

      for (auto group_idx = size_t{0}; group_idx < size; ++group_idx) {
        const auto [value_begin_offset, value_length] = get_variably_sized_value_range(group_idx, variably_sized_value_idx);

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
    } else if constexpr (std::is_arithmetic_v<T>){
      const auto offset_in_group = definition.offsets[column_id];

      for (auto group_idx = size_t{0}; group_idx < size; ++group_idx) {
        const auto [group_begin_offset, group_length] = GroupSizePolicy::get_group_range(group_idx);

        auto *source = reinterpret_cast<const char *>(&group_data[group_begin_offset]) + offset_in_group;

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

template<typename GroupSizePolicy>
BasicRun<GroupSizePolicy> create_run(const AggregateHashSortDefinition& definition, const size_t group_capacity, const size_t group_data_capacity) const {
  auto group_size_policy = GroupSizePolicy{definition, group_capacity};

  auto aggregates = std::vector<std::unique_ptr<BaseAggregateRun>>(definition.aggregate_definitions.size());

  for (auto aggregate_idx = size_t{0}; aggregate_idx < aggregates.size(); ++aggregate_idx) {
    const auto& aggregate_definition = definition.aggregate_definitions[aggregate_idx];

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

template<size_t group_size>
FixedSizeInPlaceKey<group_size> FixedSizeInPlaceKey<group_size>::EMPTY_KEY{true, {}, {}};

struct FixedSizeInPlaceCompare {
#if VERBOSE
  mutable size_t counter{};
#endif

  template <size_t group_size>
  bool operator()(const FixedSizeInPlaceKey<group_size>& lhs,
                  const FixedSizeInPlaceKey<group_size>& rhs) const {
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
template<size_t group_size>
struct StaticFixedGroupSizePolicy {
  using HashTableKey = FixedSizeInPlaceKey<group_size>;
  using HashTableCompare = FixedSizeInPlaceCompare;

  StaticFixedGroupSizePolicy(const AggregateHashSortDefinition& definition, const size_t group_capacity) {}

  template<typename Run>
  bool can_append(const Run& target_run, const Run& source_run, const size_t source_group_idx) const {
    return target_run.size + target_run.append_buffer.size() + 1 <= target_run.hashes.size();
  }

  template<typename Run>
  void schedule_append(const Run& source_run, const size_t source_group_idx) const {
  }

  template<typename Run>
  auto get_append_iterator(Run& run) const {
    return run.group_data.begin() + group_size * run.size;
  }

  template<typename Run>
  void append(Run& target_run, const Run& source_run, const size_t source_group_idx) const {
  }

  std::pair<size_t, size_t> get_group_range(const size_t group_idx) const {
    return {group_size * group_idx, group_size};
  }

  size_t get_group_size(const size_t group_idx) const { return group_size; }

  HashTableCompare make_compare() const {
    return {};
  }

  template<typename Run>
  HashTableKey make_key(const Run& run, const size_t group_idx) const {
    HashTableKey key;
    key.hash = run.hashes[group_idx];
    const auto iter = run.group_data.begin() + group_idx * get_group_size();
    std::copy(iter, iter + get_group_size(), key.group.begin());
    return key;
  }

  void shrink_to_size(const size_t size) {
  }
};

struct FixedSizeRemoteKey {
  static FixedSizeRemoteKey EMPTY_KEY;

  size_t hash{};
  const GroupRunElementType* group{};
};

inline FixedSizeRemoteKey FixedSizeRemoteKey::EMPTY_KEY{0, nullptr};

struct FixedSizeRemoteCompare {
  size_t group_size{};

#if VERBOSE
  mutable size_t counter{};
#endif

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

  DynamicFixedGroupSizePolicy(const AggregateHashSortDefinition& definition, const size_t group_capacity): group_size(definition.fixed_group_size) {}

  template<typename Run>
  bool can_append(const Run& target_run, const Run& source_run, const size_t source_group_idx) const {
    return target_run.size + target_run.append_buffer.size() + 1 <= target_run.hashes.size();
  }

  template<typename Run>
  void schedule_append(const Run& source_run, const size_t source_group_idx) const {
  }

  template<typename Run>
  auto get_append_iterator(Run& run) const {
    return run.group_data.begin() + group_size * run.size;
  }

  template<typename Run>
  void append(Run& target_run, const Run& source_run, const size_t source_group_idx) const {
  }

  std::pair<size_t, size_t> get_group_range(const size_t group_idx) const {
    return {group_size * group_idx, group_size};
  }

  size_t get_group_size(const size_t group_idx) const { return group_size; }

  HashTableCompare make_compare() const {
    return {group_size};
  }

  template<typename Run>
  HashTableKey make_key(const Run& run, const size_t group_idx) const {
    return {run.hashes[group_idx], &run.group_data[group_idx *group_size]};
  }

  void shrink_to_size(const size_t size) {
  }
};

struct VariablySizedKey {
  size_t hash;
  const GroupRunElementType* group;
  size_t size;
};

struct VariablySizedCompare {
#if VERBOSE
  mutable size_t counter{};
#endif

  bool operator==(const VariablySizedKey& lhs, const VariablySizedKey& rhs) const {
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
  using HashTableKey = FixedSizeRemoteKey;
  using HashTableCompare = FixedSizeRemoteCompare;

  uninitialized_vector<size_t> group_end_offsets;

  // Number of `data` elements occupied after `append_buffer` is flushed into `data`
  size_t data_watermark{0};

  VariableGroupSizePolicy(const AggregateHashSortDefinition& definition, const size_t group_capacity): group_end_offsets(group_capacity) {}

  template<typename Run>
  bool can_append(const Run& target_run, const Run& source_run, const size_t source_group_idx) const {
    const auto [group_begin_offset, group_length] = source_run.get_group_range(source_group_idx);

    // Checking for sufficient group data capacity as well as group-slot capactiy (`hashes` has the same length as
    // `group_end_offsets`)
    return data_watermark + group_length <= target_run.group_data.size() && target_run.size + target_run.append_buffer.size() + 1 <= target_run.hashes.size();
  }

  template<typename Run>
  void schedule_append(const Run& source_run, const size_t source_group_idx) const {
    const auto [group_begin_offset, group_length] = source_run.get_group_range(source_group_idx);
    data_watermark += group_length;
  }

  template<typename Run>
  auto get_append_iterator(Run& run) const {
    return run.group_data.begin() + (run.size == 0 ? 0 : group_end_offsets[run.size - 1]);
  }

  template<typename Run>
  void append(Run& target_run, const Run& source_run, const size_t source_group_idx) const {
    const auto group_size = source_run.get_group_size(source_group_idx);
    group_end_offsets[target_run.size] = (target_run.size == 0 ? 0 : group_end_offsets[target_run.size - 1]) + group_size;
  }

  std::pair<size_t, size_t> get_group_range(const size_t group_idx) const {
    const auto group_begin_offset = group_idx == 0 ? size_t{0} : group_end_offsets[group_idx - 1];
    return {group_begin_offset, group_end_offsets[group_idx] - group_begin_offset};
  }

  size_t get_group_size(const size_t group_idx) const {
    return group_idx == 0 ? group_end_offsets.front() : group_end_offsets[group_idx] - group_end_offsets[group_idx - 1];
  }

  template<typename Run>
  HashTableKey make_key(const Run& run, const size_t group_idx) const {
    const auto [group_begin_offset, group_size] = get_group_range(group_idx);
    return {run.hashes[group_idx],
            &run.group_data[group_begin_offset], group_size};
  }

  HashTableCompare make_compare() const {
    return {};
  }

  void shrink_to_size(const size_t size) {
    group_end_offsets.resize(size);
  }
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
    Assert(level <= max_level, "Recursion level too deep, all hash bits exhausted. This is very likely caused by a bad hash function.");

    const auto shift = (hash_bit_count - partition_bit_count) - level * partition_bit_count;

    return {partition_count, shift, mask};
  }

  RadixFanOut(const size_t partition_count, const size_t hash_shift, const size_t hash_mask)
  : partition_count(partition_count), hash_shift(hash_shift), hash_mask(hash_mask) {}

  size_t get_partition_for_hash(const size_t hash) const { return (hash >> hash_shift) & hash_mask; }
};

// For gtest
inline bool operator==(const RadixFanOut& lhs, const RadixFanOut& rhs) {
  return std::tie(lhs.partition_count, lhs.hash_shift, lhs.hash_mask) ==std::tie(rhs.partition_count, rhs.hash_shift, rhs.hash_mask);
}

/**
 * @return hash table size
 */
inline size_t configure_hash_table(const AggregateHashSortConfig& config, const size_t row_count) {
#ifdef USE_DENSE_HASH_MAP
  return std::min(config.hash_table_size, static_cast<size_t>(std::ceil(row_count / config.hash_table_max_load_factor) / 2));
#endif

#ifdef USE_UNORDERED_MAP
  return static_cast<size_t>(
      std::ceil(std::min(config.hash_table_size * config.hash_table_max_load_factor, static_cast<float>(row_count)) /
                config.hash_table_max_load_factor));
#endif
}

template<typename T>
size_t hash(const T* key, const size_t size, const size_t seed) {
  DebugAssert(size <= std::numeric_limits<int>::max(), "MurmurHash only accepts ints as lengths");
  return MurmurHash64A(key, static_cast<int>(size), seed);
}

template<typename T>
size_t hash(const T& key, const size_t seed) {
  return MurmurHash64A(&key, sizeof(key), seed);
}

enum class HashSortMode { Hashing, Partition };

template<typename GroupSizePolicy>
struct RunProducer {
  /**
   * Build a `VariablySizedGroupRun` from a range of `row_count` rows starting at `begin_row_id` of a Table.
   *
   * @returns   The VariablySizedGroupRun and the end RowID, i.e., the RowID the next run starts at
   */
  static std::pair<BasicRun<GroupSizePolicy>, RowID> from_table_range(
  const AggregateHashSortDefinition& definition,
  const std::shared_ptr<const Table>& table,
  const std::vector<ColumnID>& group_by_column_ids,
  const RowID& begin_row_id,
  const size_t row_count) {

    Assert(row_count > 0, "Cannot materialize zero rows");

    const auto variably_sized_column_count = definition.variably_sized_column_ids.size();
    const auto meta_data_size = sizeof(size_t) * variably_sized_column_count;

    /**
     * Materialize the variably sized values separately first, then compute the size of the opaque `group_data` blob
     */
    const auto [data_per_column, value_end_offsets, variably_sized_end_row_id] = materialize_variably_sized_columns(definition, table, group_by_column_ids, begin_row_id, row_count);

    auto run = BasicRun<GroupSizePolicy>{};
    run.size = row_count;

    run.group_end_offsets = determine_group_end_offsets(definition.variably_sized_column_ids.size(), row_count, value_end_offsets);
    run.data.resize(run.group_end_offsets.back());

    /**
     * Initialize the gaps between group data and meta data in the groups with zero
     */
    {
      auto value_end_offsets_iter = value_end_offsets.begin() + variably_sized_column_count - 1;
      for (auto group_idx = size_t{0}; group_idx < row_count; ++group_idx) {
        const auto [group_begin_offset, group_size] = run.get_group_range(group_idx);

        if (run.group_end_offsets[group_idx] > 0) {
          auto *target = reinterpret_cast<char*>(&run.data[group_begin_offset]) + *value_end_offsets_iter;
          const auto gap_size = group_size * GROUP_RUN_ELEMENT_SIZE - *value_end_offsets_iter - meta_data_size;

          memset(target, 0, gap_size);
        }

        value_end_offsets_iter += variably_sized_column_count;
      }
    }

    /**
     * Materialize the fixed size values into the opaque blob
     */
    const auto fixed_size_column_count = definition.fixed_size_column_ids.size();

    for (auto column_id = ColumnID{0}; column_id < fixed_size_column_count; ++column_id) {
      const auto group_by_column_id = definition.fixed_size_column_ids[column_id];
      const auto base_offset = definition.fixed_size_value_offsets[column_id];

      materialize_fixed_size_column(run, table, base_offset, group_by_column_id, begin_row_id, row_count);
    }

    /**
     * Copy the value end offsets into the interleaved blob
     */
    {
      auto* source = value_end_offsets.data();

      for (auto group_idx = size_t{0}; group_idx < row_count; ++group_idx) {
        const auto [group_begin_offset, group_size] = run.get_group_range(group_idx);

        auto* target = reinterpret_cast<char*>(run.data.data() + group_begin_offset + group_size) - meta_data_size;

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
        const auto [target_offset, value_size] = run.get_variably_sized_value_range(row_idx, column_id);

        auto* target = &reinterpret_cast<char*>(run.data.data())[target_offset];

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
      run.hashes[group_idx] = hash(&run.data[group_begin_offset], group_size * sizeof(GroupRunElementType), 0);
    }

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
  materialize_variably_sized_columns(
    const std::shared_ptr<const Table>& table,
    const VariablySizedGroupRunLayout* layout,
    const std::vector<ColumnID>& group_by_column_ids,
    const RowID& begin_row_id,
    const size_t row_count) {

    const auto variably_sized_column_count = layout->variably_sized_column_ids.size();

    // Return values
    auto end_row_id = RowID{};
    auto value_end_offsets = uninitialized_vector<size_t>(row_count * variably_sized_column_count);
    auto data_per_column = std::vector<uninitialized_vector<char>>(variably_sized_column_count);

    for (auto column_id = ColumnID{0}; column_id < variably_sized_column_count; ++column_id) {
      // Initial budget is one byte per row. Grown aggressively below.
      auto& column_data = data_per_column[column_id];
      column_data.resize(row_count);

      const auto group_by_column_id = layout->variably_sized_column_ids[column_id];
      const auto column_is_nullable = table->column_is_nullable(group_by_column_id);

      // Number of values from this column already materialized
      auto column_row_count = size_t{0};

      // If this is exhausted, we need to grow `column_data`
      auto column_remaining_data_budget_bytes = column_data.size();

      auto* target = reinterpret_cast<char*>(column_data.data());

      auto value_end_offsets_iter = value_end_offsets.begin() + column_id;

      const auto column_iterable = ColumnIterable{table, group_by_column_id};
      end_row_id = column_iterable.for_each<pmr_string>([&](const auto& segment_position, const RowID& row_id) {
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
        const auto previous_value_end = column_id == 0 ? layout->variably_sized_values_begin_offset : *(value_end_offsets_iter - 1);
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
      }, begin_row_id);

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
  static uninitialized_vector<size_t> determine_group_end_offsets(const size_t variably_sized_column_count, const size_t row_count, const uninitialized_vector<size_t>& value_end_offsets) {
    auto group_end_offsets = uninitialized_vector<size_t>(row_count);
    const auto meta_data_size = variably_sized_column_count * sizeof(size_t);

    auto previous_group_end_offset = size_t{0};
    auto value_end_offset_iter = value_end_offsets.begin() + (variably_sized_column_count - 1);
    for (auto row_idx = size_t{0}; row_idx < row_count; ++row_idx) {
      const auto group_size = *value_end_offset_iter + meta_data_size;

      // Round up to a full DATA_ELEMENT_SIZE for each group
      const auto group_element_count = divide_and_ceil(group_size, DATA_ELEMENT_SIZE);

      previous_group_end_offset += group_element_count;

      group_end_offsets[row_idx] = previous_group_end_offset;
      value_end_offset_iter += variably_sized_column_count;
    }

    return group_end_offsets;
  }

  /**
   * Subroutine of `from_table_range()`. Exposed for better testability.
   * Materialize the fixed-size columns into the opaque data blob
   */
  static void materialize_fixed_size_column(
  VariablySizedGroupRun& group_run, const std::shared_ptr<const Table>& table, const size_t base_offset, const ColumnID column_id,
  const RowID& begin_row_id,
  const size_t row_count) {

    auto group_idx = size_t{0};

    const auto column_is_nullable = table->column_is_nullable(column_id);

    auto column_iterable = ColumnIterable{table, column_id};
    column_iterable.for_each<ResolveDataTypeTag>([&](const auto &segment_position, const RowID &row_id) {
      using ColumnDataType = typename std::decay_t<decltype(segment_position)>::Type;

      const auto [group_begin_offset, group_size] = group_run.get_group_range(group_idx);

      auto *target = reinterpret_cast<char*>(&group_run.data[group_begin_offset]) + base_offset;

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
    }, begin_row_id);

    Assert(group_idx == row_count, "Illegal row_count parameter passed");
  }
};
//
///**
// * Abstraction over the fact that group and aggregate data can either stem from a Table or a previously created
// * Partition
// */
//template <typename GroupRun>
//struct AbstractRunSource {
//  const typename GroupRun::LayoutType* layout;
//  std::vector<Run<GroupRun>> runs;
//
//  // Current position of the read cursor
//  size_t run_idx{};
//  size_t run_offset{};
//
//  // Amount of data not yet processed (i.e., partitioned/hashed) by the reader. Provides hint for run sizes
//  // during partitioning
//  size_t remaining_fetched_group_count{};
//  size_t remaining_fetched_group_data_size{};
//
//  AbstractRunSource(const typename GroupRun::LayoutType* layout, std::vector<Run<GroupRun>>&& runs)
//  : layout(layout), runs(std::move(runs)) {
//    for (const auto& run : this->runs) {
//      remaining_fetched_group_count += run.groups.size;
//      remaining_fetched_group_data_size += run.groups.data.size();
//    }
//  }
//
//  virtual ~AbstractRunSource() = default;
//
//  virtual bool can_fetch_run() const = 0;
//  virtual void fetch_run() = 0;
//  virtual size_t size() const = 0;
//
//  /**
//   * @return The Run that the read cursor points to
//   */
//  const Run<GroupRun>& current_run() const {
//    DebugAssert(run_idx < runs.size(), "No current run");
//    return runs[run_idx];
//  }
//
//  /**
//   * @return Whether all groups of this run source have been retrieved and no new runs can be fetched
//   */
//  bool exhausted() const {
//    return run_idx >= runs.size() && !can_fetch_run();
//  }
//
//  /**
//   * @return Whether the read cursor is past all previously fetched runs
//   */
//  bool need_to_fetch_run() const {
//    DebugAssert(can_fetch_run(), "Should not be called of no run can be fetched");
//    return run_idx >= runs.size();
//  }
//};
//
///**
// * Determines the size of a run to be allocated, based on the remaining data in the data source and the partitioning
// * fan out and the assumption that partitioning distributes the data evenly
// */
//template<typename GroupRun>
//struct RunAllocationStrategy {
//  std::shared_ptr<AbstractRunSource<GroupRun>> run_source;
//  RadixFanOut radix_fan_out;
//
//  RunAllocationStrategy() = default;
//
//  RunAllocationStrategy(const std::shared_ptr<AbstractRunSource<GroupRun>>& run_source, const RadixFanOut& radix_fan_out) : run_source(run_source), radix_fan_out(radix_fan_out) {}
//
//  /**
//   * @return {group_count, group_data_size}
//   */
//  std::pair<size_t, size_t> next_run_size() const {
//    auto group_count_per_partition = std::ceil(static_cast<float>(run_source->remaining_fetched_group_count) / radix_fan_out.partition_count);
//    auto data_size_per_partition = std::ceil(static_cast<float>(run_source->remaining_fetched_group_data_size) / radix_fan_out.partition_count);
//
//    if (radix_fan_out.partition_count > 1) {
//      // Add a bit of additional space to account for possible imperfect uniformity of the hash values
//      group_count_per_partition *= 1.2f;
//      data_size_per_partition *= 1.2f;
//    }
//
//    Assert(group_count_per_partition > 0 && data_size_per_partition > 0, "Values should be greater than zero");
//
//    return {group_count_per_partition, 0};
//  }
//};
//
///**
// * A set of Runs
// */
//template <typename GroupRun>
//struct Partition {  std::vector<Run<GroupRun>> runs;
//
//  void flush_buffers(const Run<GroupRun>& input_run) {
//    if (runs.empty()) {
//      return;
//    }
//
//    auto& target_run = runs.back();
//
//    auto& append_buffer = runs.back().groups.append_buffer;
//    if (append_buffer.empty() && aggregation_buffer.empty()) {
//      return;
//    }
//
//    auto& target_groups = target_run.groups;
//    const auto target_offset = target_groups.size;
//
//    for (auto aggregate_idx = size_t{0}; aggregate_idx < target_run.aggregates.size(); ++aggregate_idx) {
//      auto& target_aggregate_run = target_run.aggregates[aggregate_idx];
//      const auto& input_aggregate_run = input_run.aggregates[aggregate_idx];
//      target_aggregate_run->flush_append_buffer(target_offset, append_buffer, *input_aggregate_run);
//      target_aggregate_run->flush_aggregation_buffer(aggregation_buffer, *input_aggregate_run);
//    }
//
//    target_groups.flush_append_buffer(input_run.groups);
//
//    aggregation_buffer.clear();
//  }
//
//  void append(const Run<GroupRun>& source_run, const size_t source_offset, const RunAllocationStrategy<GroupRun>& run_allocation_strategy, const size_t level,
//              const size_t partition_idx) {
//    if (runs.empty() || !runs.back().groups.can_append(source_run.groups, source_offset)) {
//      flush_buffers(source_run);
//
//      // Make sure the group to be appended, at the very least, fits into the new run
//      auto new_run_group_count = size_t{};
//      auto new_run_group_data_size = size_t{};
//
//      std::tie(new_run_group_count, new_run_group_data_size) = run_allocation_strategy.next_run_size();
//
//      new_run_group_data_size = std::max(source_run.groups.get_group_size(source_offset), new_run_group_data_size);
//      DebugAssert(new_run_group_count > 0, "Run size cannot be zero");
//#if VERBOSE
//      std::cout << indent(level) << "Partition " << partition_idx << " allocates new run for "
//                << new_run_group_count << " groups and " << new_run_group_data_size << " bytes\n";
//#endif
//
//      runs.emplace_back(source_run.new_instance(new_run_group_count, new_run_group_data_size));
//    }
//    DebugAssert(runs.back().groups.can_append(source_run.groups, source_offset), "");
//
//    auto& target_run = runs.back();
//
//    target_run.groups.schedule_append(source_run.groups, source_offset);
//    if (target_run.groups.append_buffer.size() > 255) {
//      flush_buffers(source_run);
//    }
//  }
//
//  void aggregate(const size_t target_offset, const Run<GroupRun>& source_run, const size_t source_offset) {
//    if (source_run.aggregates.empty()) {
//      return;
//    }
//
//    aggregation_buffer.emplace_back(AggregationBufferEntry{target_offset, source_offset});
//    if (aggregation_buffer.size() > 255) {
//      flush_buffers(source_run);
//    }
//  }
//
//  size_t size() const {
//    return std::accumulate(runs.begin(), runs.end(), size_t{0},
//                           [&](const auto size, const auto& run) { return size + run.size(); });
//  }
//
//  void shrink_to_size(const RunIsAggregated is_aggregated) {
//    for (auto run_iter = runs.rbegin(); run_iter != runs.rend(); ++run_iter) {
//      if (run_iter->is_aggregated != RunIsAggregated::NotSet) {
//        break;
//      }
//      run_iter->shrink_to_size(is_aggregated);
//    }
//  }
//};
//
//
//// Wraps a Partition
//template <typename GroupRun>
//struct PartitionRunSource : public AbstractRunSource<GroupRun> {
//  using AbstractRunSource<GroupRun>::AbstractRunSource;
//
//  bool can_fetch_run() const override { return false; }
//
//  void fetch_run() override { Fail("Shouldn't be called, can_fetch_run() is always false."); }
//
//  size_t size() const override {
//    return std::accumulate(AbstractRunSource<GroupRun>::runs.begin(), AbstractRunSource<GroupRun>::runs.end(),
//                           size_t{0}, [](const size_t count, const auto& run) { return count + run.size(); });
//  }
//};
//
//template <typename GetGroupSize>
//inline std::pair<FixedSizeGroupRun<GetGroupSize>, RowID> produce_initial_groups(
//    const std::shared_ptr<const Table>& table, const FixedSizeGroupRunLayout* layout,
//    const std::vector<ColumnID>& group_by_column_ids, const RowID& begin_row_id, const size_t row_count) {
//  constexpr auto GROUP_DATA_ELEMENT_SIZE = sizeof(GroupRunElementType);
//
//  const auto group_by_column_count = group_by_column_ids.size();
//
//  auto group_run = FixedSizeGroupRun<GetGroupSize>{layout, row_count};
//
//  auto end_row_id = RowID{};
//
//  // Memzero the last element of each group, as it may contain bytes not written to during materialization
//  {
//    auto* target = group_run.data.data() + layout->group_size - 1;
//    const auto* end = group_run.data.data() + row_count * layout->group_size;
//    while (target < end) {
//      *target = 0;
//      target += layout->group_size;
//    }
//  }
//
//  for (auto output_group_by_column_idx = ColumnID{0}; output_group_by_column_idx < group_by_column_count;
//       ++output_group_by_column_idx) {
//    auto* target =
//        reinterpret_cast<char*>(group_run.data.data()) + layout->column_base_offsets[output_group_by_column_idx];
//    auto run_offset = size_t{0};
//
//    const auto column_is_nullable = table->column_is_nullable(group_by_column_ids[output_group_by_column_idx]);
//
//    auto column_iterable = ColumnIterable{table, group_by_column_ids[output_group_by_column_idx]};
//    end_row_id = column_iterable.for_each<ResolveDataTypeTag>(
//        [&](const auto& segment_position, const RowID& row_id) {
//          using ColumnDataType = typename std::decay_t<decltype(segment_position)>::Type;
//
//          if (column_is_nullable) {
//            if (segment_position.is_null()) {
//              *target = 1;
//              memset(target + 1, 0, sizeof(ColumnDataType));
//            } else {
//              *target = 0;
//              memcpy(target + 1, &segment_position.value(), sizeof(ColumnDataType));
//            }
//          } else {
//            memcpy(target, &segment_position.value(), sizeof(ColumnDataType));
//          }
//
//          target += group_run.get_group_size() * GROUP_DATA_ELEMENT_SIZE;
//          ++run_offset;
//
//          return run_offset == row_count ? ColumnIteration::Break : ColumnIteration::Continue;
//        },
//        begin_row_id);
//
//    DebugAssert(run_offset == row_count, "Illegal row_count parameter passed");
//  }
//
//
//  // Compute hashes
//  {
//    const auto* source = group_run.data.data();
//    for (auto run_offset = size_t{0}; run_offset < row_count; ++run_offset) {
//      group_run.hashes[run_offset] = hash(source, group_run.get_group_size() * sizeof(GroupRunElementType), 0);
//      source += group_run.get_group_size();
//    }
//  }
//
//  group_run.size = row_count;
//
//  if (group_by_column_count == 0) {
//    std::fill(group_run.hashes.begin(), group_run.hashes.end(), 0);
//  }
//
//  return {std::move(group_run), end_row_id};
//}
//
//// Wraps a Table from which the runs are materialized
//template <typename GroupRun>
//struct TableRunSource : public AbstractRunSource<GroupRun> {
//  std::shared_ptr<const Table> table;
//  const AggregateHashSortConfig& config;
//  std::vector<AggregateColumnDefinition> aggregate_column_definitions;
//  std::vector<ColumnID> groupby_column_ids;
//
//  RowID begin_row_id{ChunkID{0}, ChunkOffset{0}};
//  size_t remaining_rows;
//
//  TableRunSource(const std::shared_ptr<const Table>& table, const typename GroupRun::LayoutType* layout,
//                 const AggregateHashSortConfig& config,
//                 const std::vector<AggregateColumnDefinition>& aggregate_column_definitions,
//                 const std::vector<ColumnID>& groupby_column_ids)
//      : AbstractRunSource<GroupRun>(layout, {}),
//        table(table),
//        config(config),
//        aggregate_column_definitions(aggregate_column_definitions),
//        groupby_column_ids(groupby_column_ids) {
//    remaining_rows = table->row_count();
//  }
//
//  bool can_fetch_run() const override { return remaining_rows > 0; }
//
//  void fetch_run() override {
//    DebugAssert(can_fetch_run(), "fetch_run() should not have been called");
//
//    if constexpr (std::is_same_v<VariablySizedGroupRun, GroupRun>) {
//      const auto row_count = std::min(remaining_rows, size_t{300'000});
//
//      auto pair = VariablySizedGroupRunProducer::from_table_range(table, this->layout, groupby_column_ids, begin_row_id, row_count);
//      auto input_aggregates = produce_initial_aggregates(table, aggregate_column_definitions, begin_row_id, pair.first.size);
//
//      remaining_rows -= row_count;
//
//      begin_row_id = pair.second;
//      this->runs.emplace_back(std::move(pair.first), std::move(input_aggregates));
//    } else {
//      const auto run_row_count = std::min(remaining_rows, size_t{300'000});
//      remaining_rows -= run_row_count;
//
//      auto pair = produce_initial_groups<typename GroupRun::GetGroupSizeType>(table, this->layout, groupby_column_ids,
//                                                                              begin_row_id, run_row_count);
//      auto input_aggregates = produce_initial_aggregates(table, aggregate_column_definitions,
//      begin_row_id, pair.first.size);
//
//      begin_row_id = pair.second;
//      this->runs.emplace_back(std::move(pair.first), std::move(input_aggregates));
//    }
//
//    AbstractRunSource<GroupRun>::remaining_fetched_group_count += this->runs.back().groups.size;
//    AbstractRunSource<GroupRun>::remaining_fetched_group_data_size += this->runs.back().groups.data.size();
//  }
//
//  size_t size() const override { return table->row_count(); }
//};
//
///**
// * Radix partition the data from `run_source` by hash values via `radix_fan_out` into `partitions`
// *
// * @param remaining_row_count   (Remaining) number of groups to partition
// * @param run_idx               InOut: Together with `run_offset`, the position of the next group to read from
// *                              `run_source`
// * @param run_offset            InOut: Together with `run_idx`, the position of the next group to read from
// *                              `run_source`
// */
//template <typename GroupRun>
//void partition(size_t remaining_row_count,
//               const std::shared_ptr<AbstractRunSource<GroupRun>>& run_source,
//               const RadixFanOut& radix_fan_out,
//               size_t& run_idx,
//               size_t& run_offset,
//               std::vector<Partition<GroupRun>>& partitions,
//               const size_t level) {
//
//  DebugAssert(radix_fan_out.partition_count == partitions.size(), "Partition count mismatch");
//  DebugAssert(remaining_row_count > 0, "partition() shouldn't have been called");
//
//#if VERBOSE
//  std::cout << indent(level) << "partition() processing " << remaining_row_count << " elements"
//            << "\n";
//  Timer t;
//#endif
//
//  auto done = false;
//
//  while (run_idx < run_source->runs.size() || run_source->can_fetch_run()) {
//    if (run_idx == run_source->runs.size()) {
//#if VERBOSE
//      Timer t2;
//#endif
//      run_source->fetch_run();
//#if VERBOSE
//      std::cout << indent(level) << "partition(): fetch_run() of " << run_source->runs.back().size() << " rows took "
//                << t2.lap_formatted() << "\n";
//#endif
//    }
//
//    auto run_allocation_strategy = RunAllocationStrategy{run_source, radix_fan_out};
//
//    const auto& input_run = run_source->runs[run_idx];
//
//    for (; run_offset < input_run.size() && !done; ++run_offset) {
//      const auto partition_idx = radix_fan_out.get_partition_for_hash(input_run.groups.hashes[run_offset]);
//
//      auto& partition = partitions[partition_idx];
//      partition.append(input_run, run_offset, run_allocation_strategy, level, partition_idx);
//      --remaining_row_count;
//
//      if (remaining_row_count == 0) {
//        done = true;
//      }
//
//      --run_source->remaining_fetched_group_count;
//      run_source->remaining_fetched_group_data_size -= input_run.groups.get_group_size(run_offset);
//    }
//
//    if (run_offset >= input_run.size()) {
//      ++run_idx;
//      run_offset = 0;
//    }
//
//    for (auto& partition : partitions) {
//      partition.flush_buffers(input_run);
//    }
//
//    if (done) {
//      break;
//    }
//  }
//
//  for (auto& partition : partitions) {
//    partition.shrink_to_size(RunIsAggregated::No);
//  }
//
//#if VERBOSE
//  std::cout << indent(level) << "partition(): took " << t.lap_formatted() << "\n";
//#endif
//
//  DebugAssert(remaining_row_count == 0, "Invalid `remaining_row_count` parameter passed");
//}
//
///**
// * @return  {Partitioned and aggregated runs, Number of input groups processed}
// */
//template <typename GroupRun>
//std::pair<std::vector<Run<GroupRun>>, size_t> hashing(const size_t hash_table_size,
//                                const float hash_table_max_load_factor,
//                                const std::shared_ptr<AbstractRunSource<GroupRun>>& run_source,
//                                const RadixFanOut& radix_fan_out,
//                                const size_t level) {
//  const auto run_allocation_strategy = RunAllocationStrategy{run_source, radix_fan_out};
//
//  // Initialize one Run per partition
//  auto run_per_partition = std::vector<Run<GroupRun>>(radix_fan_out.partition_count);
//  for (auto& run : run_per_partition) {
//    run = run_source->current_run().new_instance(run_allocation_strategy, run_source->run_offset);
//  }
//
//  auto group_key_counter_per_partition = std::vector<size_t>(radix_fan_out.partition_count);
//
//#if VERBOSE
//  Timer t;
////  auto hash_counter = size_t{0};
////  auto compare_counter = size_t{0};
//#endif
//
//  using HashTableKey = typename GroupRun::HashTableKey;
//
//  struct Hasher {
//    mutable size_t hash_counter{};
//
//    size_t operator()(const HashTableKey& key) const {
//#if VERBOSE
//      ++hash_counter;
//#endif
//      return key.hash;
//    }
//  };
//
//  const auto hasher = Hasher{};
//
//#ifdef USE_UNORDERED_MAP
//  auto compare_fn = [&](const auto& lhs, const auto& rhs) {
//#if VERBOSE
//    ++compare_counter;
//#endif
//    return typename GroupRun::HashTableCompare{run_source.layout}(lhs, rhs);
//  };
//
//  auto hash_table =
//      std::unordered_map<typename GroupRun::HashTableKey, size_t, decltype(hash_fn), decltype(compare_fn)>{
//          hash_table_size, hash_fn, compare_fn};
//#endif
//
//#ifdef USE_DENSE_HASH_MAP
//  using HashTableCompare = typename GroupRun::HashTableCompare;
//
//  const auto compare = typename GroupRun::HashTableCompare{run_source->layout};
//
//  auto hash_table = google::dense_hash_map<HashTableKey, size_t, Hasher, HashTableCompare>{
//      static_cast<size_t>(hash_table_size), hasher, compare};
//#if VERBOSE
//  Timer timer_set_empty_key;
//#endif
//  hash_table.set_empty_key(HashTableKey::EMPTY_KEY);
//#if VERBOSE
//  std::cout << indent(level) << "hashing(): set_empty_key() on hash table of size " << hash_table.bucket_count() << " took " << timer_set_empty_key.lap_formatted() << "\n";
//#endif
//  hash_table.min_load_factor(0.0f);
//#endif
//
//#if VERBOSE
//  std::cout << indent(level) << "hashing(): requested hash_table_size: " << hash_table_size <<
//  "; actual hash table size: " << hash_table.bucket_count() << "\n";
//#endif
//
//  hash_table.max_load_factor(1.0f);
//
//  auto group_counter = size_t{0};
//  auto done = false;
//
//  while (!run_source->exhausted()) {
//    if (run_source->need_to_fetch_run()) {
//#if VERBOSE
//      Timer t2;
//#endif
//      run_source->fetch_run();
//#if VERBOSE
//      std::cout << indent(level) << "hashing(): fetch_run() of " << run_source->runs.back().size() << " rows took "
//                << t2.lap_formatted() << "\n";
//#endif
//    }
//
//    const auto& source_run = run_source->current_run();
//
//    for (; run_offset < source_run.size() && !done; ++run_offset) {
//      const auto partition_idx = radix_fan_out.get_partition_for_hash(source_run.groups.hashes[run_offset]);
//
//      auto& target_run = run_per_partition[partition_idx];
//      if (!target_run) {
//        target_run.emplace(source_run.new_instance(run_allocation_strategy, run_offset));
//      }
//
//      const auto key = source_run.groups.make_key(run_offset);
//
//      auto hash_table_iter = hash_table.find(key);
//
//      if (hash_table_iter == hash_table.end()) {
//        if (!target_run.groups.can_append(source_run, run_offset)) {
//          done = true;
//        } else {
//          auto& group_key_counter = group_key_counter_per_partition[partition_idx];
//          hash_table.insert({key, group_key_counter});
//          target_run.append(source_run, run_offset, run_allocation_strategy, level, partition_idx);
//          ++group_key_counter;
//          ++group_counter;
//        }
//      } else {
//        target_run.aggregate(hash_table_iter->second, source_run, run_offset);
//        ++group_counter;
//      }
//
//      if (done) {
//        break;
//      }
//
//      --run_source->remaining_fetched_group_count;
//      run_source->remaining_fetched_group_data_size -= source_run.groups.get_group_size(run_offset);
//
//      if (hash_table.load_factor() >= hash_table_max_load_factor) {
//        done = true;
//      }
//    }
//
//    if (run_offset >= source_run.size()) {
//      ++run_idx;
//      run_offset = 0;
//    }
//
//    for (auto& run : run_per_partition) {
//      run.flush_buffers(source_run);
//    }
//
//    if (done) {
//      break;
//    }
//  }
//
//#if VERBOSE
//  std::cout << indent(level) << "hashing(): processed " << counter << " elements into " << hash_table.size() <<
//  " groups in " << t.lap_formatted() << "; hash_counter: "<<hash_table.hash_function().hash_counter << "; compare_counter: " << hash_table.key_eq().compare_counter <<
//  "; load_factor: " << hash_table.load_factor() << "; continue_hashing: " << continue_hashing << "\n";
//#endif
//
//  const auto continue_hashing = static_cast<float>(group_counter) / hash_table.size() < 3;
//
//  for (auto& run : run_per_partition) {
//    run.is_aggregated = true;
//  }
//
//  return {continue_hashing, group_counter};
//}
//
//template <typename GroupRun>
//std::vector<Partition<GroupRun>> adaptive_hashing_and_partition(const AggregateHashSortConfig& config,
//                                                                const std::shared_ptr<AbstractRunSource<GroupRun>>& run_source,
//                                                                const RadixFanOut& radix_fan_out, const size_t level) {
//
//#if VERBOSE
//  std::cout << indent(level) << "adaptive_hashing_and_partition() {"
//            << "\n";
//  Timer t;
//#endif
//
//  auto remaining_row_count = run_source->size();
//
//  // Start with a single partition and expand to `radix_fan_out.partition_count` partitions if `hashing()` detects a too
//  // low density of groups and determines the switch to HashSortMode::RadixFanOut
//  auto partitions = std::vector<Partition<GroupRun>>{radix_fan_out.partition_count};
//  auto mode = HashSortMode::Hashing;
//  auto partition_while_hashing = false;
//
//  auto run_idx = size_t{0};
//  auto run_offset = size_t{0};
//
//  if (!partition_while_hashing) {
//#if VERBOSE
//    std::cout << indent(level) << "adaptive_hashing_and_partition() remaining_rows: " << remaining_row_count << "\n";
//#endif
//    const auto hash_table_size = configure_hash_table(config, remaining_row_count);
//    const auto [continue_hashing, hashing_row_count] =
//        hashing(hash_table_size, config.hash_table_max_load_factor, run_source, RadixFanOut{1, 0, 0}, run_idx,
//                run_offset, partitions, level);
//    if (!continue_hashing) {
//      mode = HashSortMode::Partition;
//    }
//
//    remaining_row_count -= hashing_row_count;
//
//    if (run_idx < run_source->runs.size() || run_source->can_fetch_run() || partitions.front().runs.size() > 1) {
//#if VERBOSE
//      std::cout << indent(level) << "adaptive_hashing_and_partition() partitioning initial hash table\n";
//#endif
//      const auto initial_fan_out_row_count = partitions.front().size();
//      auto initial_fan_out_source =
//          std::static_pointer_cast<AbstractRunSource<GroupRun>>(std::make_shared<PartitionRunSource<GroupRun>>(run_source->layout, std::move(partitions.front().runs)));
//      auto initial_fan_out_run_idx = size_t{0};
//      auto initial_fan_out_run_offset = size_t{0};
//      partitions = std::vector<Partition<GroupRun>>{radix_fan_out.partition_count};
//      partition(initial_fan_out_row_count, initial_fan_out_source, radix_fan_out, initial_fan_out_run_idx,
//                initial_fan_out_run_offset, partitions, level);
//    }
//  }
//
//  while (run_idx < run_source->runs.size() || run_source->can_fetch_run()) {
//#if VERBOSE
//    std::cout << indent(level) << "adaptive_hashing_and_partition() remaining_rows: " << remaining_row_count << "\n";
//#endif
//
//    if (mode == HashSortMode::Hashing) {
//      const auto hash_table_size = configure_hash_table(config, remaining_row_count);
//      const auto [continue_hashing, hashing_row_count] =
//          hashing(hash_table_size, config.hash_table_max_load_factor, run_source, radix_fan_out, run_idx, run_offset,
//                  partitions, level);
//      if (!continue_hashing) {
//        mode = HashSortMode::Partition;
//      }
//      remaining_row_count -= hashing_row_count;
//
//    } else {
//      const auto partition_row_count = std::min(config.max_partitioning_counter, remaining_row_count);
//      partition(partition_row_count, run_source, radix_fan_out, run_idx, run_offset, partitions, level);
//      mode = HashSortMode::Hashing;
//      remaining_row_count -= partition_row_count;
//    }
//  }
//
//#if VERBOSE
//  std::cout << indent(level) << "} // adaptive_hashing_and_partition() took " << t.lap_formatted() << "\n";
//#endif
//
//  return partitions;
//}
//
//template <typename GroupRun>
//std::vector<Run<GroupRun>> aggregate(const AggregateHashSortConfig& config,
//                                     const std::shared_ptr<AbstractRunSource<GroupRun>>& run_source, const size_t level) {
//  if (run_source->runs.empty() && !run_source->can_fetch_run()) {
//    return {};
//  }
//
//#if VERBOSE
//  std::cout << indent(level) << "aggregate() at level " << level << " - ";
//
//  if (const auto* table_run_source = dynamic_cast<TableRunSource<GroupRun>*>(run_source.get())) {
//    std::cout << "groups from Table with " << table_run_source->table->row_count() << " rows";
//  } else if (const auto* partition_run_source = dynamic_cast<PartitionRunSource<GroupRun>*>(run_source.get())) {
//    auto rows = size_t{0};
//    for (const auto& run : partition_run_source->runs) {
//      rows += run.size();
//    }
//
//    std::cout << "groups from Partition with " << partition_run_source->runs.size() << " runs and " << rows << " rows";
//  } else {
//    Fail("");
//  }
//
//  std::cout << "\n";
//#endif
//
//  auto output_runs = std::vector<Run<GroupRun>>{};
//
//  if (run_source->runs.size() == 1 && run_source->runs.front().is_aggregated == RunIsAggregated::Yes) {
//#if VERBOSE
//    std::cout << indent(level) << " Single run in partition is aggregated, our job here is done"
//              << "\n";
//#endif
//    output_runs.emplace_back(std::move(run_source->runs.front()));
//    return output_runs;
//  }
//
//  const auto radix_fan_out = RadixFanOut::for_level(level);
//
//  auto partitions = adaptive_hashing_and_partition(config, run_source, radix_fan_out, level);
//
//  for (auto&& partition : partitions) {
//    if (partition.size() == 0) {
//      continue;
//    }
//
//    const auto* layout = partition.runs.front().groups.layout;
//
//    const auto partition_run_source = std::static_pointer_cast<AbstractRunSource<GroupRun>>(std::make_shared<PartitionRunSource<GroupRun>>(layout, std::move(partition.runs)));
//    auto aggregated_partition = aggregate(config, partition_run_source, level + 1);
//    output_runs.insert(output_runs.end(), std::make_move_iterator(aggregated_partition.begin()),
//                       std::make_move_iterator(aggregated_partition.end()));
//  }
//
//  return output_runs;
//}
//
//template <typename GroupRunLayout>
//GroupRunLayout produce_initial_groups_layout(const Table& table, const std::vector<ColumnID>& group_by_column_ids);
//
//template <>
//inline FixedSizeGroupRunLayout produce_initial_groups_layout<FixedSizeGroupRunLayout>(
//    const Table& table, const std::vector<ColumnID>& group_by_column_ids) {
//  auto group_size_in_bytes = size_t{};
//  auto column_base_offsets = std::vector<size_t>(group_by_column_ids.size());
//  auto nullable_column_indices = std::vector<std::optional<ColumnID>>(group_by_column_ids.size());
//  auto nullable_column_count = size_t{0};
//
//  for (auto output_group_by_column_id = size_t{0}; output_group_by_column_id < group_by_column_ids.size();
//       ++output_group_by_column_id) {
//    const auto group_column_id = group_by_column_ids[output_group_by_column_id];
//    const auto group_column_size = data_type_sizes.at(table.column_data_type(group_column_id));
//
//    column_base_offsets[output_group_by_column_id] = group_size_in_bytes;
//
//    if (table.column_is_nullable(group_column_id)) {
//      nullable_column_indices[output_group_by_column_id] = nullable_column_count;
//      ++nullable_column_count;
//      ++group_size_in_bytes;
//    }
//
//    group_size_in_bytes += group_column_size;
//  }
//
//  const auto group_size_in_elements = divide_and_ceil(group_size_in_bytes, sizeof(GroupRunElementType));
//
//  return FixedSizeGroupRunLayout{group_size_in_elements, nullable_column_indices, column_base_offsets};
//}

}  // namespace aggregate_hashsort

}  // namespace opossum
