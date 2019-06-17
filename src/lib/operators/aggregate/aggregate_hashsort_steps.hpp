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

#define VERBOSE 1

// #define USE_UNORDERED_MAP
#define USE_DENSE_HASH_MAP

namespace opossum {

namespace aggregate_hashsort {

inline std::string indent(const size_t depth) { return std::string(depth * 2, ' '); }

template <typename T>
T divide_and_ceil(const T& a, const T& b) {
  return a / b + (a % b > 0 ? 1 : 0);
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

template <typename SegmentPosition>
size_t hash_segment_position(const SegmentPosition& segment_position, const size_t seed) {
  static_assert(sizeof(size_t) == sizeof(uint64_t), "Assuming 64-bit platform. If this doesn't hold, use another entry point of MurmurHash*");

  if (segment_position.is_null()) {
    return hash(0, seed);
  } else {
    using ValueType = typename SegmentPosition::Type;

    if constexpr (std::is_same_v<ValueType, pmr_string>) {
      return hash(segment_position.value().data(), segment_position.value().size(), seed);
    } else {
      return hash(segment_position.value(), seed);
    }
  }
}

const auto data_type_sizes = std::unordered_map<DataType, size_t>{
    {DataType::Int, 4}, {DataType::Long, 8}, {DataType::Float, 4}, {DataType::Double, 8}};

enum class HashSortMode { Hashing, Partition };

struct FixedSizeGroupRunLayout {
  FixedSizeGroupRunLayout(const size_t group_size, const std::vector<std::optional<ColumnID>>& nullable_column_indices,
                          const std::vector<size_t>& column_base_offsets)
      : group_size(group_size),
        nullable_column_indices(nullable_column_indices),
        column_base_offsets(column_base_offsets) {
    nullable_column_count = std::count_if(nullable_column_indices.begin(), nullable_column_indices.end(),
                                          [&](const auto& column_id) { return column_id.has_value(); });
  }

  // Number of entries in `data` per group
  size_t group_size{};

  std::vector<std::optional<ColumnID>> nullable_column_indices;
  size_t nullable_column_count{};

  // Per GroupBy-column, its base offset in `data`.
  // I.e. the n-th entry of GroupBy-column 2 is at
  // `reinterpret_cast<char*>(data.data()) + column_base_offsets[2] + group_size * n`;
  std::vector<size_t> column_base_offsets;
};

using GroupRunElementType = uint32_t;

struct FixedSizeGroupRemoteKey {
  static FixedSizeGroupRemoteKey EMPTY_KEY;

  size_t hash{};
  const GroupRunElementType* group{};
};

inline FixedSizeGroupRemoteKey FixedSizeGroupRemoteKey::EMPTY_KEY{0, nullptr};

template <size_t group_size>
struct FixedSizeGroupInPlaceKey {
  static FixedSizeGroupInPlaceKey EMPTY_KEY;

  bool empty{};
  size_t hash{};
  std::array<GroupRunElementType, group_size> group{};
};

template<size_t group_size>
FixedSizeGroupInPlaceKey<group_size> FixedSizeGroupInPlaceKey<group_size>::EMPTY_KEY{true, {}, {}};

struct FixedSizeGroupKeyCompare {
  const FixedSizeGroupRunLayout* layout{};
#if VERBOSE
  mutable size_t compare_counter{};
#endif

  bool operator()(const FixedSizeGroupRemoteKey& lhs, const FixedSizeGroupRemoteKey& rhs) const {
#if VERBOSE
    ++compare_counter;
#endif
    if (!lhs.group && !rhs.group) return true;
    if (!lhs.group ^ !rhs.group) return false;

    return std::equal(lhs.group, lhs.group + layout->group_size, rhs.group);
  }

  template <size_t group_size>
  bool operator()(const FixedSizeGroupInPlaceKey<group_size>& lhs,
                  const FixedSizeGroupInPlaceKey<group_size>& rhs) const {
#if VERBOSE
    ++compare_counter;
#endif
    if (lhs.empty && rhs.empty) return true;
    if (lhs.empty ^ rhs.empty) return false;

    return lhs.group == rhs.group;
  }
};

template <size_t group_size>
struct GetStaticGroupSize {
  using KeyType = FixedSizeGroupInPlaceKey<group_size>;

  const FixedSizeGroupRunLayout* layout{};

  explicit GetStaticGroupSize(const FixedSizeGroupRunLayout* layout) {
    DebugAssert(group_size == layout->group_size, "Invalid static group size");
  }

  constexpr size_t operator()() const { return group_size; }
};

struct GetDynamicGroupSize {
  using KeyType = FixedSizeGroupRemoteKey;

  const FixedSizeGroupRunLayout* layout{};

  explicit GetDynamicGroupSize(const FixedSizeGroupRunLayout* layout) : layout(layout) {}

  size_t operator()() const { return layout->group_size; }
};

template <typename GetGroupSize>
struct FixedSizeGroupRun {
  using LayoutType = FixedSizeGroupRunLayout;
  using HashTableKey = typename GetGroupSize::KeyType;
  using HashTableCompare = FixedSizeGroupKeyCompare;
  using GetGroupSizeType = GetGroupSize;

  FixedSizeGroupRun(const FixedSizeGroupRunLayout* layout, const size_t size) : layout(layout), get_group_size(layout) {
    data.resize(size * get_group_size());
    hashes.resize(size);
  }

  const FixedSizeGroupRunLayout* layout;
  GetGroupSize get_group_size;

  std::vector<size_t> _append_buffer;

  uninitialized_vector<GroupRunElementType> data;

  // Hash per group - using std::vector because we build the hash incrementally and thus want this to be
  // zero-initialized
  uninitialized_vector<size_t> hashes;

  // Number of groups in this run
  size_t end{0};

  const std::vector<size_t>& append_buffer() const { return _append_buffer; }

  bool can_append(const FixedSizeGroupRun& source_run, const size_t source_offset) const {
    return end + _append_buffer.size() < hashes.size();
  }

  HashTableKey make_key(const size_t offset) const {
    if constexpr (std::is_same_v<GetGroupSize, GetDynamicGroupSize>) {
      return {hashes[offset], &data[offset * get_group_size()]};
    } else {
      HashTableKey key;
      key.hash = hashes[offset];
      const auto iter = data.begin() + offset * get_group_size();
      std::copy(iter, iter + get_group_size(), key.group.begin());
      return key;
    }
  }

  template <typename T>
  std::shared_ptr<BaseSegment> materialize_output(const ColumnID column_id, const bool column_is_nullable) const {
    const auto* source = reinterpret_cast<const char*>(data.data()) + layout->column_base_offsets[column_id];
    const auto* source_end = reinterpret_cast<const char*>(data.data() + end * get_group_size());

    auto output_values = std::vector<T>(end);
    auto output_null_values = std::vector<bool>(column_is_nullable ? end : 0u);

    auto output_values_iter = output_values.begin();
    auto output_null_values_iter = output_null_values.begin();

    while (source < source_end) {
      if (column_is_nullable) {
        if (*source != 0) {
          *output_null_values_iter = true;
        } else {
          *output_null_values_iter = false;
          memcpy(reinterpret_cast<char*>(&(*output_values_iter)), source + 1, sizeof(T));
        }
        ++output_null_values_iter;
      } else {
        memcpy(reinterpret_cast<char*>(&(*output_values_iter)), source, sizeof(T));
      }

      ++output_values_iter;
      source += get_group_size() * sizeof(GroupRunElementType);
    }

    if (column_is_nullable) {
      return std::make_shared<ValueSegment<T>>(std::move(output_values), std::move(output_null_values));
    } else {
      return std::make_shared<ValueSegment<T>>(std::move(output_values));
    }
  }

  void schedule_append(const FixedSizeGroupRun& source, const size_t source_offset) {
    _append_buffer.emplace_back(source_offset);
  }

  void flush_append_buffer(const FixedSizeGroupRun& source) {
    DebugAssert(end + _append_buffer.size() <= hashes.size(), "Append buffer to big for this run");

    for (const auto& source_offset : _append_buffer) {
      std::copy(source.data.begin() + source_offset * get_group_size(),
                source.data.begin() + (source_offset + 1) * get_group_size(), data.begin() + end * get_group_size());
      hashes[end] = source.hashes[source_offset];
      ++end;
    }
    _append_buffer.clear();
  }

  size_t hash(const size_t offset) const { return hashes[offset]; }

  size_t size() const { return end; }

  void resize(const size_t size) {
    DebugAssert(size >= end, "Resize would discard data");

    data.resize(size * get_group_size());
    hashes.resize(size);
  }

  void finish() {
    DebugAssert(_append_buffer.empty(), "Cannot finish run when append operations are pending");
    resize(end);
  }
};

struct VariablySizedGroupRunLayout {
  std::vector<ColumnID> variably_sized_column_ids;
  std::vector<ColumnID> fixed_size_column_ids;

  // Equal to variably_sized_column_ids.size()
  size_t column_count;
  size_t nullable_column_count;

  // Mapping a ColumnID (the index of the vector) to whether the column's values is stored either in the
  // VariablySizeGroupRun itself, or the FixedSizeGroupRun and the value index in the respective group run.
  using Column =
      std::tuple<bool /* is_variably_sized */, ColumnID /* index */, std::optional<size_t> /* nullable_index */
                 >;
  std::vector<Column> column_mapping;

  FixedSizeGroupRunLayout fixed_layout;

  VariablySizedGroupRunLayout(const std::vector<ColumnID>& variably_sized_column_ids,
                              const std::vector<ColumnID>& fixed_size_column_ids,
                              const std::vector<Column>& column_mapping, const FixedSizeGroupRunLayout& fixed_layout)
      : variably_sized_column_ids(variably_sized_column_ids),
        fixed_size_column_ids(fixed_size_column_ids),
        column_count(variably_sized_column_ids.size()),
        nullable_column_count(0u),  // Initialized below
        column_mapping(column_mapping),
        fixed_layout(fixed_layout) {
    nullable_column_count = std::count_if(column_mapping.begin(), column_mapping.end(),
                                          [&](const auto& tuple) { return std::get<2>(tuple).has_value(); });
  }
};

using GroupRunElementType = uint32_t;

struct VariablySizedRemoteGroupKey {
  const GroupRunElementType* variably_sized_group;
  size_t variably_sized_group_length;
  const size_t* value_end_offsets;
  const GroupRunElementType* fixed_sized_group;
};

struct VariablySizedInPlaceGroupKey {
  constexpr static auto CAPACITY = 1;  // sizeof(VariablySizedRemoteGroupKey) / sizeof(GroupRunElementType);

  const size_t* value_end_offsets;
  size_t size;
  std::array<GroupRunElementType, CAPACITY> group;
};

struct VariablySizedGroupKey {
  static VariablySizedGroupKey EMPTY_KEY;

  size_t hash{};
  std::optional<boost::variant<VariablySizedInPlaceGroupKey, VariablySizedRemoteGroupKey>> variant;
};

inline VariablySizedGroupKey VariablySizedGroupKey::EMPTY_KEY{};

template <typename GetFixedGroupSize>
struct VariablySizedGroupKeyCompare {
  const VariablySizedGroupRunLayout* layout{};
#if VERBOSE
  mutable size_t compare_counter{};
#endif

  bool operator()(const VariablySizedGroupKey& lhs, const VariablySizedGroupKey& rhs) const {
#if VERBOSE
    ++compare_counter;
#endif
    if (!lhs.variant ^ !rhs.variant) return false;
    if (!lhs.variant && !rhs.variant) return true;

    if (lhs.variant->which() != rhs.variant->which()) return false;

    if (lhs.variant->which() == 0) {
      const auto& in_place_lhs = boost::get<VariablySizedInPlaceGroupKey>(*lhs.variant);
      const auto& in_place_rhs = boost::get<VariablySizedInPlaceGroupKey>(*rhs.variant);

      if (in_place_lhs.size != in_place_rhs.size) {
        return false;
      }

      if (!std::equal(in_place_lhs.group.begin(), in_place_lhs.group.begin() + in_place_lhs.size,
                      in_place_rhs.group.begin())) {
        return false;
      }

      // Compare intra-group layout
      return std::equal(in_place_lhs.value_end_offsets, in_place_lhs.value_end_offsets + layout->column_count,
                        in_place_rhs.value_end_offsets);
    } else {
      const auto& remote_lhs = boost::get<VariablySizedRemoteGroupKey>(*lhs.variant);
      const auto& remote_rhs = boost::get<VariablySizedRemoteGroupKey>(*rhs.variant);

      if (remote_lhs.variably_sized_group_length != remote_rhs.variably_sized_group_length) {
        return false;
      }

      // Compare intra-group layout
      if (!std::equal(remote_lhs.value_end_offsets, remote_lhs.value_end_offsets + layout->column_count,
                      remote_rhs.value_end_offsets)) {
        return false;
      }

      // Compare the variably sized group data
      if (!std::equal(remote_lhs.variably_sized_group,
                      remote_lhs.variably_sized_group + remote_lhs.variably_sized_group_length,
                      remote_rhs.variably_sized_group)) {
        return false;
      }

      // Compare the fixed size group data
      DebugAssert((remote_lhs.fixed_sized_group == nullptr) == (remote_rhs.fixed_sized_group == nullptr), "");
      if (!remote_lhs.fixed_sized_group) {
        return true;
      }

      const auto fixed_group_size = GetFixedGroupSize{&layout->fixed_layout}();
      return std::equal(remote_lhs.fixed_sized_group, remote_lhs.fixed_sized_group + fixed_group_size,
                        remote_rhs.fixed_sized_group);
    }
  }
};

template <typename GetFixedGroupSize>
struct VariablySizedGroupRun {
  using LayoutType = VariablySizedGroupRunLayout;
  using HashTableKey = VariablySizedGroupKey;
  using HashTableCompare = VariablySizedGroupKeyCompare<GetFixedGroupSize>;
  using GetFixedGroupSizeType = GetFixedGroupSize;

  const VariablySizedGroupRunLayout* layout;

  uninitialized_vector<GroupRunElementType> data;

  // Number of `data` elements occupied after append_buffer is flushed
  size_t data_watermark{0};

  // End indices in `data`
  uninitialized_vector<size_t> group_end_offsets;

  // Offsets (in bytes) for all values in all groups, relative to the beginning of the group.
  uninitialized_vector<size_t> value_end_offsets;

  // Non-variably sized values in the group and hash values
  FixedSizeGroupRun<GetFixedGroupSize> fixed;

  explicit VariablySizedGroupRun(const VariablySizedGroupRunLayout* layout, const size_t size, const size_t data_size)
      : layout(layout), fixed(&layout->fixed_layout, size) {
    data.resize(data_size);
    group_end_offsets.resize(size);
    value_end_offsets.resize(size * layout->column_count);
  }

  const std::vector<size_t>& append_buffer() const { return fixed.append_buffer(); }

  VariablySizedGroupKey make_key(const size_t offset) {
    const auto group_begin_offset = offset == 0 ? size_t{0} : group_end_offsets[offset - 1];
    const auto variably_sized_group_size = group_end_offsets[offset] - group_begin_offset;
    const auto* variably_sized_group = &data[group_begin_offset];
    const auto* fixed_size_group = fixed.get_group_size() == 0 ? nullptr : &fixed.data[offset * fixed.get_group_size()];

    const auto total_group_size = variably_sized_group_size + fixed.get_group_size();

    if (total_group_size <= VariablySizedInPlaceGroupKey::CAPACITY && false) {
      auto key = VariablySizedGroupKey{};
      key.hash = fixed.hashes[offset];

      key.variant = VariablySizedInPlaceGroupKey{};
      auto& in_place = boost::get<VariablySizedInPlaceGroupKey>(*key.variant);
      in_place.size = total_group_size;
      in_place.value_end_offsets = &value_end_offsets[offset * layout->column_count];
      std::copy(variably_sized_group, variably_sized_group + variably_sized_group_size, in_place.group.begin());
      std::copy(fixed_size_group, fixed_size_group + fixed.get_group_size(),
                in_place.group.begin() + variably_sized_group_size);

      return key;
    } else {
      return {fixed.hashes[offset],
              VariablySizedRemoteGroupKey{variably_sized_group, variably_sized_group_size,
                                          &value_end_offsets[offset * layout->column_count], fixed_size_group}};
    }
  }

  bool can_append(const VariablySizedGroupRun& source_run, const size_t source_offset) const {
    if (!fixed.can_append(source_run.fixed, source_offset)) {
      return false;
    }

    const auto source_size = source_offset == 0 ? source_run.group_end_offsets[0]
                                                : source_run.group_end_offsets[source_offset] -
                                                      source_run.group_end_offsets[source_offset - 1];
    return data_watermark + source_size < data.size();
  }

  void schedule_append(const VariablySizedGroupRun& source, const size_t source_offset) {
    fixed.schedule_append(source.fixed, source_offset);

    const auto source_size =
        source_offset == 0 ? source.group_end_offsets[0]
                           : source.group_end_offsets[source_offset] - source.group_end_offsets[source_offset - 1];
    data_watermark += source_size;
  }

  template <typename T>
  std::shared_ptr<BaseSegment> materialize_output(const ColumnID column_id, const bool column_is_nullable) const {
    const auto& [is_variably_sized, local_column_id, nullable_column_idx] = layout->column_mapping[column_id];

    if (!is_variably_sized) {
      return fixed.template materialize_output<T>(ColumnID{local_column_id}, column_is_nullable);
    }

    const auto row_count = fixed.end;

    auto output_values = std::vector<pmr_string>(row_count);
    auto output_null_values = std::vector<bool>();
    if (column_is_nullable) {
      output_null_values.resize(row_count);
    }

    for (auto row_idx = size_t{0}; row_idx < row_count; ++row_idx) {
      const auto group_begin_offset = row_idx == 0 ? 0 : group_end_offsets[row_idx - 1];
      const auto value_begin_offset =
          local_column_id == 0 ? 0 : value_end_offsets[row_idx * layout->column_count + local_column_id - 1];
      const auto value_size = value_end_offsets[row_idx * layout->column_count + local_column_id] - value_begin_offset;

      const auto* source = reinterpret_cast<const char*>(&data[group_begin_offset]) + value_begin_offset;

      output_values[row_idx] = {source, value_size};

      if (column_is_nullable) {
        if (*source != 0) {
          output_null_values[row_idx] = true;
        } else {
          output_values[row_idx] = {source + 1, value_size - 1};
        }
      } else {
        output_values[row_idx] = {source, value_size};
      }
    }

    if (column_is_nullable) {
      return std::make_shared<ValueSegment<pmr_string>>(std::move(output_values), std::move(output_null_values));
    } else {
      return std::make_shared<ValueSegment<pmr_string>>(std::move(output_values));
    }
  }

  void flush_append_buffer(const VariablySizedGroupRun& source) {
    auto end = fixed.end;

    for (const auto source_offset : fixed.append_buffer()) {
      // Copy intra-group layout
      const auto source_value_end_offsets_iter =
          source.value_end_offsets.begin() + source_offset * layout->column_count;
      std::copy(source_value_end_offsets_iter, source_value_end_offsets_iter + layout->column_count,
                value_end_offsets.begin() + end * layout->column_count);

      // Copy group size
      const auto source_group_offset = source_offset == 0 ? 0 : source.group_end_offsets[source_offset - 1];
      const auto source_group_size = source.group_end_offsets[source_offset] - source_group_offset;
      group_end_offsets[end] = end == 0 ? source_group_size : group_end_offsets[end - 1] + source_group_size;

      // Copy group data
      const auto source_data_iter = source.data.begin() + source_group_offset;
      const auto target_data_offset = end == 0 ? 0 : group_end_offsets[end - 1];
      const auto target_data_iter = data.begin() + target_data_offset;
      DebugAssert(target_data_offset + source_group_size <= data.size(), "");
      std::copy(source_data_iter, source_data_iter + source_group_size, target_data_iter);

      ++end;
    }

    fixed.flush_append_buffer(source.fixed);
  }

  size_t size() const { return fixed.end; }

  size_t hash(const size_t offset) const { return fixed.hash(offset); }

  void finish() {
    fixed.finish();
    group_end_offsets.resize(fixed.end);
    value_end_offsets.resize(fixed.end * layout->column_count);
    data.resize(fixed.end == 0 ? 0 : group_end_offsets.back());
  }
};

template <typename T>
struct IsVariablySizedGroupRun {
  static constexpr auto value = false;
};
template <typename T>
struct IsVariablySizedGroupRun<VariablySizedGroupRun<T>> {
  static constexpr auto value = true;
};

template <typename T>
constexpr auto is_variably_sized_group_run_v = IsVariablySizedGroupRun<T>::value;

enum class RunIsAggregated { NotSet, Yes, No };

template <typename GroupRun>
struct Run {
  Run(GroupRun&& groups, std::vector<std::unique_ptr<BaseAggregateRun>>&& aggregates)
      : groups(std::move(groups)), aggregates(std::move(aggregates)) {}

  size_t size() const { return groups.size(); }

  Run<GroupRun> new_instance(const size_t memory_budget) const {
    auto new_aggregates = std::vector<std::unique_ptr<BaseAggregateRun>>(this->aggregates.size());
    for (auto aggregate_idx = size_t{0}; aggregate_idx < new_aggregates.size(); ++aggregate_idx) {
      new_aggregates[aggregate_idx] = this->aggregates[aggregate_idx]->new_instance(memory_budget);
    }

    if constexpr (is_variably_sized_group_run_v<GroupRun>) {
      auto new_groups = GroupRun{groups.layout, memory_budget, memory_budget * 5};
      return Run<GroupRun>{std::move(new_groups), std::move(new_aggregates)};
    } else {
      auto new_groups = GroupRun{groups.layout, memory_budget};
      return Run<GroupRun>{std::move(new_groups), std::move(new_aggregates)};
    }
  }

  void finish(const RunIsAggregated is_aggregated2) {
    groups.finish();
    for (auto& aggregate : aggregates) {
      aggregate->resize(groups.size());
    }
    is_aggregated = is_aggregated2;
  }

  GroupRun groups;
  std::vector<std::unique_ptr<BaseAggregateRun>> aggregates;

  RunIsAggregated is_aggregated{RunIsAggregated::NotSet};
};

struct RunAllocationStrategy {
  size_t next_run_size{};
  size_t max_run_size{};
  float scalar{};

  RunAllocationStrategy() = default;

  RunAllocationStrategy(const size_t fixed_run_size) : RunAllocationStrategy(fixed_run_size, fixed_run_size, 1.0f) {}

  RunAllocationStrategy(const size_t first_run_size, const size_t max_run_size, const float scalar)
      : next_run_size(first_run_size), max_run_size(max_run_size), scalar(scalar) {
    Assert(next_run_size > 0, "RunAllocationStrategy should not create empty runs");
    Assert(max_run_size >= next_run_size, "max_run_size needs to be greater than or equals the initial next_run_size");
  }

  size_t fetch_next_run_size() {
    const auto result = next_run_size;
    next_run_size = std::min(static_cast<size_t>(next_run_size * scalar), max_run_size);
    return result;
  }
};

template <typename GroupRun>
struct Partition {
  std::vector<AggregationBufferEntry> aggregation_buffer;

  size_t group_key_counter{0};

  std::vector<Run<GroupRun>> runs;

  RunAllocationStrategy run_allocation_strategy;

  void flush_buffers(const Run<GroupRun>& input_run) {
    if (runs.empty()) {
      return;
    }

    auto& append_buffer = runs.back().groups.append_buffer();
    if (append_buffer.empty() && aggregation_buffer.empty()) {
      return;
    }

    auto& target_run = runs.back();
    auto& target_groups = target_run.groups;
    const auto target_offset = target_groups.size();

    for (auto aggregate_idx = size_t{0}; aggregate_idx < target_run.aggregates.size(); ++aggregate_idx) {
      auto& target_aggregate_run = target_run.aggregates[aggregate_idx];
      const auto& input_aggregate_run = input_run.aggregates[aggregate_idx];
      target_aggregate_run->flush_append_buffer(target_offset, append_buffer, *input_aggregate_run);
      target_aggregate_run->flush_aggregation_buffer(aggregation_buffer, *input_aggregate_run);
    }

    target_groups.flush_append_buffer(input_run.groups);

    aggregation_buffer.clear();
  }

  void append(const Run<GroupRun>& source_run, const size_t source_offset, const size_t level,
              const size_t partition_idx) {
    if (runs.empty() || !runs.back().groups.can_append(source_run.groups, source_offset)) {
      flush_buffers(source_run);
      const auto new_run_size = run_allocation_strategy.fetch_next_run_size();
      DebugAssert(new_run_size > 0, "Run size cannot be zero");
#if VERBOSE
      std::cout << indent(level) << "Partition " << partition_idx << " allocates new run with a budget of "
                << new_run_size << "\n";
#endif

      runs.emplace_back(source_run.new_instance(new_run_size));
    }
    DebugAssert(runs.back().groups.can_append(source_run.groups, source_offset), "");

    auto& target_run = runs.back();

    target_run.groups.schedule_append(source_run.groups, source_offset);
    if (target_run.groups.append_buffer().size() > 255) {
      flush_buffers(source_run);
    }
  }

  void aggregate(const size_t target_offset, const Run<GroupRun>& source_run, const size_t source_offset) {
    if (source_run.aggregates.empty()) {
      return;
    }

    aggregation_buffer.emplace_back(AggregationBufferEntry{target_offset, source_offset});
    if (aggregation_buffer.size() > 255) {
      flush_buffers(source_run);
    }
  }

  size_t size() const {
    return std::accumulate(runs.begin(), runs.end(), size_t{0},
                           [&](const auto size, const auto& run) { return size + run.size(); });
  }

  void finish(const RunIsAggregated is_aggregated) {
    for (auto run_iter = runs.rbegin(); run_iter != runs.rend(); ++run_iter) {
      if (run_iter->is_aggregated != RunIsAggregated::NotSet) {
        break;
      }
      run_iter->finish(is_aggregated);
    }
  }
};

template <typename GroupRun>
struct AbstractRunSource {
  const typename GroupRun::LayoutType* layout;
  std::vector<Run<GroupRun>> runs;

  AbstractRunSource(const typename GroupRun::LayoutType* layout, std::vector<Run<GroupRun>>&& runs)
      : layout(layout), runs(std::move(runs)) {}
  virtual ~AbstractRunSource() = default;

  virtual bool can_fetch_run() const = 0;
  virtual void fetch_run() = 0;
  virtual size_t size() const = 0;
};

// Wraps a Partition
template <typename GroupRun>
struct PartitionRunSource : public AbstractRunSource<GroupRun> {
  using AbstractRunSource<GroupRun>::AbstractRunSource;

  bool can_fetch_run() const override { return false; }

  void fetch_run() override { Fail("Shouldn't be called, can_fetch_run() is always false."); }

  size_t size() const override {
    return std::accumulate(AbstractRunSource<GroupRun>::runs.begin(), AbstractRunSource<GroupRun>::runs.end(),
                           size_t{0}, [](const size_t count, const auto& run) { return count + run.size(); });
  }
};

struct FanOut {
  size_t partition_count;
  size_t hash_shift;
  size_t hash_mask;

  static FanOut for_level(const size_t level, const size_t partition_bit_count = 4) {
    // 1 << 4 == 16
    const auto partition_count = 1u << partition_bit_count;

    // 16 - 1 == 0b1111
    const auto mask = partition_count - 1u;

    const auto hash_bit_count = sizeof(size_t) * CHAR_BIT;

    // 64 total bits / 6 bits per partition - 1 = 9 levels max
    const auto max_level = hash_bit_count / partition_bit_count - 1;
    Assert(level <= max_level, "Recursion level too deep, all hash bits exhausted. This is very likely caused by a bad hash function.");

    const auto shift = (hash_bit_count - partition_bit_count) - level * partition_bit_count;

    return {partition_count, shift, mask};
  }

  FanOut(const size_t partition_count, const size_t hash_shift, const size_t hash_mask)
      : partition_count(partition_count), hash_shift(hash_shift), hash_mask(hash_mask) {}

  size_t get_partition_for_hash(const size_t hash) const { return (hash >> hash_shift) & hash_mask; }
};

// For gtest
inline bool operator==(const FanOut& lhs, const FanOut& rhs) {
  return std::tie(lhs.partition_count, lhs.hash_shift, lhs.hash_mask) ==std::tie(rhs.partition_count, rhs.hash_shift, rhs.hash_mask);
}

template <typename GetGroupSize>
inline std::pair<FixedSizeGroupRun<GetGroupSize>, RowID> produce_initial_groups(
    const std::shared_ptr<const Table>& table, const FixedSizeGroupRunLayout* layout,
    const std::vector<ColumnID>& group_by_column_ids, const RowID& begin_row_id, const size_t row_count) {
  constexpr auto GROUP_DATA_ELEMENT_SIZE = sizeof(GroupRunElementType);

  const auto group_by_column_count = group_by_column_ids.size();

  auto group_run = FixedSizeGroupRun<GetGroupSize>{layout, row_count};

  auto end_row_id = RowID{};

  // Memzero the last element of each group, as it may contain bytes not written to during materialization
  {
    auto* target = group_run.data.data() + layout->group_size - 1;
    const auto* end = group_run.data.data() + row_count * layout->group_size;
    while (target < end) {
      *target = 0;
      target += layout->group_size;
    }
  }

  for (auto output_group_by_column_idx = ColumnID{0}; output_group_by_column_idx < group_by_column_count;
       ++output_group_by_column_idx) {
    auto* target =
        reinterpret_cast<char*>(group_run.data.data()) + layout->column_base_offsets[output_group_by_column_idx];
    auto run_offset = size_t{0};

    const auto column_is_nullable = table->column_is_nullable(group_by_column_ids[output_group_by_column_idx]);

    auto column_iterable = ColumnIterable{table, group_by_column_ids[output_group_by_column_idx]};
    end_row_id = column_iterable.for_each<ResolveDataTypeTag>(
        [&](const auto& segment_position, const RowID& row_id) {
          using ColumnDataType = typename std::decay_t<decltype(segment_position)>::Type;

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

          target += group_run.get_group_size() * GROUP_DATA_ELEMENT_SIZE;
          ++run_offset;

          return run_offset == row_count ? ColumnIteration::Break : ColumnIteration::Continue;
        },
        begin_row_id);

    DebugAssert(run_offset == row_count, "Illegal row_count parameter passed");
  }


  // Compute hashes
  {
    const auto* source = group_run.data.data();
    for (auto run_offset = size_t{0}; run_offset < row_count; ++run_offset) {
      group_run.hashes[run_offset] = hash(source, group_run.get_group_size() * sizeof(GroupRunElementType), 0);
      source += group_run.get_group_size();
    }
  }

  group_run.end = row_count;

  if (group_by_column_count == 0) {
    std::fill(group_run.hashes.begin(), group_run.hashes.end(), 0);
  }

  return {std::move(group_run), end_row_id};
}

template <typename GetFixedGroupSize>
inline std::pair<VariablySizedGroupRun<GetFixedGroupSize>, RowID> produce_initial_groups(
    const std::shared_ptr<const Table>& table, const VariablySizedGroupRunLayout* layout,
    const std::vector<ColumnID>& group_by_column_ids, const RowID& begin_row_id, const size_t row_budget,
    const size_t per_column_data_budget_bytes) {
  constexpr auto DATA_ELEMENT_SIZE = sizeof(GroupRunElementType);

  Assert(row_budget > 0, "");
  Assert(!layout->variably_sized_column_ids.empty(),
         "FixedGroupRun should be used if there are no variably sized columns");

  const auto column_count = layout->variably_sized_column_ids.size();

  auto run_row_count = std::numeric_limits<size_t>::max();
  auto end_row_id = RowID{};

  /**
   * Materialize each variable-width column into a continuous blob
   * While doing so
   *    - determine the layout of the interleaved layout in the final VariablySizedGroupRun (`value_end_offsets`)
   *    - materialize `null_values`
   */
  auto value_end_offsets = uninitialized_vector<size_t>(row_budget * column_count);
  auto data_per_column = std::vector<uninitialized_vector<GroupRunElementType>>(column_count);

  const auto per_column_element_budget = divide_and_ceil(per_column_data_budget_bytes, DATA_ELEMENT_SIZE);

  {
    auto nullable_column_idx = size_t{0};

    for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
      auto& column_data = data_per_column[column_id];
      column_data.resize(per_column_element_budget);

      const auto group_by_column_id = layout->variably_sized_column_ids[column_id];
      const auto column_is_nullable = table->column_is_nullable(group_by_column_id);

      auto column_row_count = size_t{0};
      auto column_remaining_data_budget_bytes = per_column_data_budget_bytes;
      auto* target = reinterpret_cast<char*>(column_data.data());

      auto value_end_offsets_iter = value_end_offsets.begin() + column_id;

      auto column_end_row_id = begin_row_id;

      const auto column_iterable = ColumnIterable{table, group_by_column_id};
      column_iterable.for_each<pmr_string>(
          [&](const auto& segment_position, const RowID& row_id) {
            column_end_row_id = row_id;

            if (column_row_count == row_budget) {
              return ColumnIteration::Break;
            }

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

            if (value_size > column_remaining_data_budget_bytes) {
              return ColumnIteration::Break;
            }

            // Set `value_end_offsets`
            const auto previous_value_end = column_id == 0 ? 0 : *(value_end_offsets_iter - 1);
            *value_end_offsets_iter = previous_value_end + value_size;
            value_end_offsets_iter += column_count;

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

            return ColumnIteration::Continue;
          },
          begin_row_id);

      Assert(column_row_count > 0, "");

      if (column_row_count < run_row_count) {
        run_row_count = column_row_count;
        end_row_id = column_end_row_id;
      }

      if (column_is_nullable) {
        ++nullable_column_idx;
      }
    }
  }

  value_end_offsets.resize(run_row_count * layout->column_count);

  /**
   * From `value_end_offsets`, determine `group_end_offsets`
   */
  auto group_end_offsets = uninitialized_vector<size_t>(run_row_count);
  auto previous_group_end_offset = size_t{0};
  auto value_end_offset_iter = value_end_offsets.begin() + (column_count - 1);
  for (auto row_idx = size_t{0}; row_idx < run_row_count; ++row_idx) {
    // Round up to a full DATA_ELEMENT_SIZE
    const auto group_element_count =
        *value_end_offset_iter / DATA_ELEMENT_SIZE + (*value_end_offset_iter % DATA_ELEMENT_SIZE > 0 ? 1 : 0);

    previous_group_end_offset += group_element_count;

    group_end_offsets[row_idx] = previous_group_end_offset;
    value_end_offset_iter += column_count;
  }

  const auto data_element_count = group_end_offsets.back();

  /**
   * Materialize fixed-size group-by columns, now that we know the row count we want to materialize for this run
   */
  auto group_run = VariablySizedGroupRun<GetFixedGroupSize>{layout, 0, 0};
  group_run.fixed = produce_initial_groups<GetFixedGroupSize>(
                        table, &layout->fixed_layout, layout->fixed_size_column_ids, begin_row_id, run_row_count)
                        .first;

  /**
   * Initialize the interleaved group data blob
   */
  auto group_data = uninitialized_vector<GroupRunElementType>(data_element_count);
  for (auto row_idx = size_t{0}; row_idx < run_row_count; ++row_idx) {
    if (group_end_offsets[row_idx] > 0) {
      group_data[group_end_offsets[row_idx] - 1] = 0;
    }
  }

  /**
   * Rearrange `data_per_column` into an interleaved blob
   */

  for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
    auto& column_data = data_per_column[column_id];

    auto* source = reinterpret_cast<char*>(column_data.data());

    for (auto row_idx = size_t{0}; row_idx < run_row_count; ++row_idx) {
      const auto group_begin_offset = row_idx == 0 ? 0 : group_end_offsets[row_idx - 1];
      const auto value_begin_offset = column_id == 0 ? 0 : value_end_offsets[row_idx * column_count + column_id - 1];
      auto* target = &reinterpret_cast<char*>(&group_data[group_begin_offset])[value_begin_offset];

      const auto value_end = value_end_offsets[row_idx * column_count + column_id];
      const auto previous_value_end = column_id == 0 ? 0 : value_end_offsets[row_idx * column_count + column_id - 1];
      const auto source_size = value_end - previous_value_end;

      // Avoid calling memcpy()/hash() for NULLs/empty strings
      if (source_size == 0) {
        continue;
      }

      memcpy(target, source, source_size);
      source += source_size;
    }
  }

  // Compute hashes
  {
    auto group_begin_offset = size_t{0};
    for (auto run_offset = size_t{0}; run_offset < run_row_count; ++run_offset) {
      const auto group_size = group_end_offsets[run_offset] - group_begin_offset;
      if (group_size == 0) {
        continue;
      }

      group_run.fixed.hashes[run_offset] = hash(&group_data[group_begin_offset], group_size * sizeof(GroupRunElementType), group_run.fixed.hashes[run_offset]);
      group_begin_offset = group_end_offsets[run_offset];
    }
  }

  group_run.data = std::move(group_data);
  group_run.group_end_offsets = std::move(group_end_offsets);
  group_run.value_end_offsets = std::move(value_end_offsets);

  return {std::move(group_run), end_row_id};
}

// Wraps a Table from which the runs are materialized
template <typename GroupRun>
struct TableRunSource : public AbstractRunSource<GroupRun> {
  std::shared_ptr<const Table> table;
  const AggregateHashSortConfig& config;
  std::vector<AggregateColumnDefinition> aggregate_column_definitions;
  std::vector<ColumnID> groupby_column_ids;

  RowID begin_row_id{ChunkID{0}, ChunkOffset{0}};
  size_t remaining_rows;

  TableRunSource(const std::shared_ptr<const Table>& table, const typename GroupRun::LayoutType* layout,
                 const AggregateHashSortConfig& config,
                 const std::vector<AggregateColumnDefinition>& aggregate_column_definitions,
                 const std::vector<ColumnID>& groupby_column_ids)
      : AbstractRunSource<GroupRun>(layout, {}),
        table(table),
        config(config),
        aggregate_column_definitions(aggregate_column_definitions),
        groupby_column_ids(groupby_column_ids) {
    remaining_rows = table->row_count();
  }

  bool can_fetch_run() const override { return remaining_rows > 0; }

  void fetch_run() override {
    if constexpr (is_variably_sized_group_run_v<GroupRun>) {
      const auto run_row_budget = std::min(remaining_rows, size_t{300'000});
      const auto run_group_data_budget = std::max(size_t{128}, run_row_budget * 7u);

      auto pair = produce_initial_groups<typename GroupRun::GetFixedGroupSizeType>(
          table, this->layout, groupby_column_ids, begin_row_id, run_row_budget, run_group_data_budget);
      auto input_aggregates = produce_initial_aggregates(table, aggregate_column_definitions,
                                                         !groupby_column_ids.empty(), begin_row_id, pair.first.size());

      remaining_rows -= pair.first.fixed.size();

      begin_row_id = pair.second;
      this->runs.emplace_back(std::move(pair.first), std::move(input_aggregates));
    } else {
      const auto run_row_count = std::min(remaining_rows, size_t{300'000});
      remaining_rows -= run_row_count;

      auto pair = produce_initial_groups<typename GroupRun::GetGroupSizeType>(table, this->layout, groupby_column_ids,
                                                                              begin_row_id, run_row_count);
      auto input_aggregates = produce_initial_aggregates(table, aggregate_column_definitions,
                                                         !groupby_column_ids.empty(), begin_row_id, pair.first.size());

      begin_row_id = pair.second;
      this->runs.emplace_back(std::move(pair.first), std::move(input_aggregates));
    }
  }

  size_t size() const override { return table->row_count(); }
};

template <typename GroupRun>
void partition(size_t remaining_row_count, AbstractRunSource<GroupRun>& run_source, const FanOut& fan_out,
               size_t& run_idx, size_t& run_offset, std::vector<Partition<GroupRun>>& partitions, const size_t level) {
  DebugAssert(remaining_row_count > 0, "partition() shouldn't have been called");

#if VERBOSE
  std::cout << indent(level) << "partition() processing " << remaining_row_count << " elements"
            << "\n";
  Timer t;
#endif

  const auto estimated_row_count_per_partition = std::max(size_t{1},
      static_cast<size_t>(std::ceil((remaining_row_count / fan_out.partition_count) * 1.2f)));
  for (auto& partition : partitions) {
    partition.run_allocation_strategy = {estimated_row_count_per_partition};
  }

  auto done = false;

  while (run_idx < run_source.runs.size() || run_source.can_fetch_run()) {
    if (run_idx == run_source.runs.size()) {
#if VERBOSE
      Timer t2;
#endif
      run_source.fetch_run();
#if VERBOSE
      std::cout << indent(level) << "partition(): fetch_run() of " << run_source.runs.back().size() << " rows took "
                << t2.lap_formatted() << "\n";
#endif
    }

    auto&& input_run = run_source.runs[run_idx];

    for (; run_offset < input_run.size() && !done; ++run_offset) {
      const auto partition_idx = fan_out.get_partition_for_hash(input_run.groups.hash(run_offset));

      auto& partition = partitions[partition_idx];
      partition.append(input_run, run_offset, level, partition_idx);
      --remaining_row_count;

      if (remaining_row_count == 0) {
        done = true;
      }
    }

    if (run_offset >= input_run.size()) {
      ++run_idx;
      run_offset = 0;
    }

    for (auto& partition : partitions) {
      partition.flush_buffers(input_run);
    }

    if (done) {
      break;
    }
  }

  for (auto& partition : partitions) {
    partition.finish(RunIsAggregated::No);
  }

#if VERBOSE
  std::cout << indent(level) << "partition(): took " << t.lap_formatted() << "\n";
#endif

  DebugAssert(remaining_row_count == 0, "Didn't process all input rows");
}

/**
 * @return              {Whether to continue hashing, Number of input groups processed}
 */
template <typename GroupRun>
std::pair<bool, size_t> hashing(const size_t hash_table_size, const float hash_table_max_load_factor,
                                AbstractRunSource<GroupRun>& run_source, const FanOut& fan_out,
                                size_t& run_idx, size_t& run_offset, std::vector<Partition<GroupRun>>& partitions,
                                const size_t level) {
#if VERBOSE
  Timer t;
//  auto hash_counter = size_t{0};
//  auto compare_counter = size_t{0};
#endif

  using HashTableKey = typename GroupRun::HashTableKey;

  struct Hasher {
    mutable size_t hash_counter{};

    size_t operator()(const HashTableKey& key) const {
#if VERBOSE
      ++hash_counter;
#endif
      return key.hash;
    }
  };

  const auto hasher = Hasher{};

#ifdef USE_UNORDERED_MAP
  auto compare_fn = [&](const auto& lhs, const auto& rhs) {
#if VERBOSE
    ++compare_counter;
#endif
    return typename GroupRun::HashTableCompare{run_source.layout}(lhs, rhs);
  };

  auto hash_table =
      std::unordered_map<typename GroupRun::HashTableKey, size_t, decltype(hash_fn), decltype(compare_fn)>{
          hash_table_size, hash_fn, compare_fn};
#endif

#ifdef USE_DENSE_HASH_MAP
  using HashTableCompare = typename GroupRun::HashTableCompare;

  const auto compare = typename GroupRun::HashTableCompare{run_source.layout};

  auto hash_table = google::dense_hash_map<HashTableKey, size_t, Hasher, HashTableCompare>{
      static_cast<size_t>(hash_table_size), hasher, compare};
#if VERBOSE
  Timer timer_set_empty_key;
#endif
  hash_table.set_empty_key(HashTableKey::EMPTY_KEY);
#if VERBOSE
  std::cout << indent(level) << "hashing(): set_empty_key() on hash table of size " << hash_table.bucket_count() << " took " << timer_set_empty_key.lap_formatted() << "\n";
#endif
  hash_table.min_load_factor(0.0f);
#endif

#if VERBOSE
  std::cout << indent(level) << "hashing(): requested hash_table_size: " << hash_table_size <<
  "; actual hash table size: " << hash_table.bucket_count() << "\n";
#endif

  hash_table.max_load_factor(1.0f);

  const auto max_group_count = hash_table.bucket_count() * hash_table_max_load_factor;
  const auto max_estimated_group_count_per_partition =
      static_cast<size_t>(std::ceil((max_group_count / fan_out.partition_count) * 1.2f));
  for (auto& partition : partitions) {
    partition.run_allocation_strategy = {max_estimated_group_count_per_partition};
  }

  auto counter = size_t{0};
  auto continue_hashing = true;
  auto done = false;

  while (run_idx < run_source.runs.size() || run_source.can_fetch_run()) {
    if (run_idx == run_source.runs.size()) {
#if VERBOSE
      Timer t2;
#endif
      run_source.fetch_run();
#if VERBOSE
      std::cout << indent(level) << "hashing(): fetch_run() of " << run_source.runs.back().size() << " rows took "
                << t2.lap_formatted() << "\n";
#endif
    }

    auto&& input_run = run_source.runs[run_idx];

    for (; run_offset < input_run.size() && !done; ++run_offset) {
      const auto partition_idx = fan_out.get_partition_for_hash(input_run.groups.hash(run_offset));
      auto& partition = partitions[partition_idx];

      const auto key = input_run.groups.make_key(run_offset);

      auto hash_table_iter = hash_table.find(key);
      if (hash_table_iter == hash_table.end()) {
        hash_table.insert({key, partition.group_key_counter});
        partition.append(input_run, run_offset, level, partition_idx);
        ++partition.group_key_counter;
      } else {
        partition.aggregate(hash_table_iter->second, input_run, run_offset);
      }

      ++counter;

      //std::cout << "Load factor: " << hash_table.load_factor() << "\n";
      if (hash_table.load_factor() >= hash_table_max_load_factor) {
        if (static_cast<float>(counter) / hash_table.size() < 3) {
          continue_hashing = false;
        }

        done = true;
      }
    }

    if (run_offset >= input_run.size()) {
      ++run_idx;
      run_offset = 0;
    }

    for (auto& partition : partitions) {
      partition.flush_buffers(input_run);
    }

    if (done) {
      break;
    }
  }

  for (auto& partition : partitions) {
    partition.finish(RunIsAggregated::Yes);
  }

#if VERBOSE
  std::cout << indent(level) << "hashing(): processed " << counter << " elements into " << hash_table.size() <<
  " groups in " << t.lap_formatted() << "; hash_counter: "<<hash_table.hash_function().hash_counter << "; compare_counter: " << hash_table.key_eq().compare_counter <<
  "; load_factor: " << hash_table.load_factor() << "; continue_hashing: " << continue_hashing << "\n";
#endif

  return {continue_hashing, counter};
}

template <typename GroupRun>
std::vector<Partition<GroupRun>> adaptive_hashing_and_partition(const AggregateHashSortConfig& config,
                                                                std::unique_ptr<AbstractRunSource<GroupRun>> run_source,
                                                                const FanOut& fan_out, const size_t level) {

#if VERBOSE
  std::cout << indent(level) << "adaptive_hashing_and_partition() {"
            << "\n";
  Timer t;
#endif

  auto remaining_row_count = run_source->size();

  // Start with a single partition and expand to `fan_out.partition_count` partitions if `hashing()` detects a too
  // low density of groups and determines the switch to HashSortMode::FanOut
  auto partitions = std::vector<Partition<GroupRun>>{fan_out.partition_count};
  auto mode = HashSortMode::Hashing;
  auto partition_while_hashing = false;

  auto run_idx = size_t{0};
  auto run_offset = size_t{0};

  if (!partition_while_hashing) {
#if VERBOSE
    std::cout << indent(level) << "adaptive_hashing_and_partition() remaining_rows: " << remaining_row_count << "\n";
#endif
    const auto hash_table_size = configure_hash_table(config, remaining_row_count);
    const auto [continue_hashing, hashing_row_count] =
        hashing(hash_table_size, config.hash_table_max_load_factor, *run_source, FanOut{1, 0, 0}, run_idx,
                run_offset, partitions, level);
    if (!continue_hashing) {
      mode = HashSortMode::Partition;
    }

    remaining_row_count -= hashing_row_count;

    if (run_idx < run_source->runs.size() || run_source->can_fetch_run() || partitions.front().runs.size() > 1) {
#if VERBOSE
      std::cout << indent(level) << "adaptive_hashing_and_partition() partitioning initial hash table\n";
#endif
      const auto initial_fan_out_row_count = partitions.front().size();
      auto initial_fan_out_source =
          PartitionRunSource<GroupRun>(run_source->layout, std::move(partitions.front().runs));
      auto initial_fan_out_run_idx = size_t{0};
      auto initial_fan_out_run_offset = size_t{0};
      partitions = std::vector<Partition<GroupRun>>{fan_out.partition_count};
      partition(initial_fan_out_row_count, initial_fan_out_source, fan_out, initial_fan_out_run_idx,
                initial_fan_out_run_offset, partitions, level);
    }
  }

  while (run_idx < run_source->runs.size() || run_source->can_fetch_run()) {
#if VERBOSE
    std::cout << indent(level) << "adaptive_hashing_and_partition() remaining_rows: " << remaining_row_count << "\n";
#endif

    if (mode == HashSortMode::Hashing) {
      const auto hash_table_size = configure_hash_table(config, remaining_row_count);
      const auto [continue_hashing, hashing_row_count] =
          hashing(hash_table_size, config.hash_table_max_load_factor, *run_source, fan_out, run_idx, run_offset,
                  partitions, level);
      if (!continue_hashing) {
        mode = HashSortMode::Partition;
      }
      remaining_row_count -= hashing_row_count;

    } else {
      const auto partition_row_count = std::min(config.max_partitioning_counter, remaining_row_count);
      partition(partition_row_count, *run_source, fan_out, run_idx, run_offset, partitions, level);
      mode = HashSortMode::Hashing;
      remaining_row_count -= partition_row_count;
    }
  }

#if VERBOSE
  std::cout << indent(level) << "} // adaptive_hashing_and_partition() took " << t.lap_formatted() << "\n";
#endif

  return partitions;
}

template <typename GroupRun>
std::vector<Run<GroupRun>> aggregate(const AggregateHashSortConfig& config,
                                     std::unique_ptr<AbstractRunSource<GroupRun>> run_source, const size_t level) {
  if (run_source->runs.empty() && !run_source->can_fetch_run()) {
    return {};
  }

#if VERBOSE
  std::cout << indent(level) << "aggregate() at level " << level << " - ";

  if (const auto* table_run_source = dynamic_cast<TableRunSource<GroupRun>*>(run_source.get())) {
    std::cout << "groups from Table with " << table_run_source->table->row_count() << " rows";
  } else if (const auto* partition_run_source = dynamic_cast<PartitionRunSource<GroupRun>*>(run_source.get())) {
    auto rows = size_t{0};
    for (const auto& run : partition_run_source->runs) {
      rows += run.size();
    }

    std::cout << "groups from Partition with " << partition_run_source->runs.size() << " runs and " << rows << " rows";
  } else {
    Fail("");
  }

  std::cout << "\n";
#endif

  auto output_runs = std::vector<Run<GroupRun>>{};

  if (run_source->runs.size() == 1 && run_source->runs.front().is_aggregated == RunIsAggregated::Yes) {
#if VERBOSE
    std::cout << indent(level) << " Single run in partition is aggregated, our job here is done"
              << "\n";
#endif
    output_runs.emplace_back(std::move(run_source->runs.front()));
    return output_runs;
  }

  const auto fan_out = FanOut::for_level(level);

  auto partitions = adaptive_hashing_and_partition(config, std::move(run_source), fan_out, level);

  for (auto&& partition : partitions) {
    if (partition.size() == 0) {
      continue;
    }

    const auto* layout = partition.runs.front().groups.layout;

    std::unique_ptr<AbstractRunSource<GroupRun>> partition_run_source =
        std::make_unique<PartitionRunSource<GroupRun>>(layout, std::move(partition.runs));
    auto aggregated_partition = aggregate(config, std::move(partition_run_source), level + 1);
    output_runs.insert(output_runs.end(), std::make_move_iterator(aggregated_partition.begin()),
                       std::make_move_iterator(aggregated_partition.end()));
  }

  return output_runs;
}

template <typename GroupRunLayout>
GroupRunLayout produce_initial_groups_layout(const Table& table, const std::vector<ColumnID>& group_by_column_ids);

template <>
inline FixedSizeGroupRunLayout produce_initial_groups_layout<FixedSizeGroupRunLayout>(
    const Table& table, const std::vector<ColumnID>& group_by_column_ids) {
  auto group_size_in_bytes = size_t{};
  auto column_base_offsets = std::vector<size_t>(group_by_column_ids.size());
  auto nullable_column_indices = std::vector<std::optional<ColumnID>>(group_by_column_ids.size());
  auto nullable_column_count = size_t{0};

  for (auto output_group_by_column_id = size_t{0}; output_group_by_column_id < group_by_column_ids.size();
       ++output_group_by_column_id) {
    const auto group_column_id = group_by_column_ids[output_group_by_column_id];
    const auto group_column_size = data_type_sizes.at(table.column_data_type(group_column_id));

    column_base_offsets[output_group_by_column_id] = group_size_in_bytes;

    if (table.column_is_nullable(group_column_id)) {
      nullable_column_indices[output_group_by_column_id] = nullable_column_count;
      ++nullable_column_count;
      ++group_size_in_bytes;
    }

    group_size_in_bytes += group_column_size;
  }

  const auto group_size_in_elements = divide_and_ceil(group_size_in_bytes, sizeof(GroupRunElementType));

  return FixedSizeGroupRunLayout{group_size_in_elements, nullable_column_indices, column_base_offsets};
}

template <>
inline VariablySizedGroupRunLayout produce_initial_groups_layout<VariablySizedGroupRunLayout>(
    const Table& table, const std::vector<ColumnID>& group_by_column_ids) {
  Assert(!group_by_column_ids.empty(), "");

  auto column_mapping = std::vector<VariablySizedGroupRunLayout::Column>(group_by_column_ids.size());
  auto nullable_count = size_t{0};
  auto variably_sized_column_ids = std::vector<ColumnID>{};
  auto fixed_size_column_ids = std::vector<ColumnID>{};

  for (auto output_group_by_column_id = ColumnID{0}; output_group_by_column_id < group_by_column_ids.size();
       ++output_group_by_column_id) {
    const auto group_by_column_id = group_by_column_ids[output_group_by_column_id];
    const auto data_type = table.column_data_type(group_by_column_id);

    if (data_type == DataType::String) {
      const auto local_column_id = ColumnID{static_cast<ColumnID::base_type>(variably_sized_column_ids.size())};
      if (table.column_is_nullable(group_by_column_id)) {
        column_mapping[output_group_by_column_id] = {true, local_column_id, nullable_count};
        ++nullable_count;
      } else {
        column_mapping[output_group_by_column_id] = {true, local_column_id, std::nullopt};
      }
      variably_sized_column_ids.emplace_back(group_by_column_id);
    } else {
      const auto local_column_id = ColumnID{static_cast<ColumnID::base_type>(fixed_size_column_ids.size())};
      column_mapping[output_group_by_column_id] = {false, local_column_id, std::nullopt};
      fixed_size_column_ids.emplace_back(group_by_column_id);
    }
  }

  const auto fixed_layout = produce_initial_groups_layout<FixedSizeGroupRunLayout>(table, fixed_size_column_ids);

  return VariablySizedGroupRunLayout{variably_sized_column_ids, fixed_size_column_ids, column_mapping, fixed_layout};
}

}  // namespace aggregate_hashsort

}  // namespace opossum
