#include <unordered_map>
#include <vector>

#include "sparsehash/dense_hash_map"

#include "all_type_variant.hpp"
#include "operators/abstract_aggregate_operator.hpp"
#include "operators/aggregate/aggregate_hashsort_aggregates.hpp"
#include "operators/aggregate/aggregate_hashsort_config.hpp"
#include "operators/aggregate/aggregate_traits.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

namespace aggregate_hashsort {

template<typename T>
T divide_and_ceil(const T& a, const T& b) {
  return a / b + (a % b > 0 ? 1 : 0);
}

template<typename F>
void resolve_bool(const bool v, const F& f) {
  if (v) {
    f(true);
  } else {
    f(false);
  }
}

const auto data_type_sizes = std::unordered_map<DataType, size_t>{
    {DataType::Int, 4}, {DataType::Long, 8}, {DataType::Float, 4}, {DataType::Double, 8}};

enum class HashSortMode { Hashing, Partition };

struct FixedSizeGroupRunLayout {
  FixedSizeGroupRunLayout(const size_t group_size, const std::vector<std::optional<ColumnID>>& nullable_column_indices, const std::vector<size_t>& column_base_offsets)
      : group_size(group_size), nullable_column_indices(nullable_column_indices), column_base_offsets(column_base_offsets) {
    nullable_column_count = std::count_if(nullable_column_indices.begin(), nullable_column_indices.end(), [&](const auto& column_id) {
      return column_id.has_value();
    });
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
  size_t hash{};
  const GroupRunElementType* group{};
};

template<size_t group_size>
struct FixedSizeGroupInPlaceKey {
  size_t hash{};
  std::array<GroupRunElementType, group_size> group{};
};

struct FixedSizeGroupKeyCompare {
  const FixedSizeGroupRunLayout* layout{};

  bool operator()(const FixedSizeGroupRemoteKey& lhs, const FixedSizeGroupRemoteKey& rhs) const {
    return std::equal(lhs.group, lhs.group + layout->group_size, rhs.group);
  }

  template<size_t group_size>
  bool operator()(const FixedSizeGroupInPlaceKey<group_size>& lhs, const FixedSizeGroupInPlaceKey<group_size>& rhs) const {
    return lhs.group == rhs.group;
  }
};

template<size_t group_size>
struct GetStaticGroupSize {
  using KeyType = FixedSizeGroupInPlaceKey<group_size>;

  const FixedSizeGroupRunLayout* layout{};

  explicit GetStaticGroupSize(const FixedSizeGroupRunLayout* layout) {
    DebugAssert(group_size == layout->group_size, "Invalid static group size");
  }

  constexpr size_t operator()() const {
    return group_size;
  }
};

struct GetDynamicGroupSize {
  using KeyType = FixedSizeGroupRemoteKey;

  const FixedSizeGroupRunLayout* layout{};

  explicit GetDynamicGroupSize(const FixedSizeGroupRunLayout* layout): layout(layout) { }

  size_t operator()() const {
    return layout->group_size;
  }
};

template<typename GetGroupSize>
struct FixedSizeGroupRun {
  using LayoutType = FixedSizeGroupRunLayout;
  using HashTableKey = typename GetGroupSize::KeyType;
  using HashTableCompare = FixedSizeGroupKeyCompare;
  using GetGroupSizeType = GetGroupSize;

  FixedSizeGroupRun(const FixedSizeGroupRunLayout* layout, const size_t size) : layout(layout), get_group_size(layout) {
    data.resize(size * layout->group_size);
    hashes.resize(size);
  }

  const FixedSizeGroupRunLayout* layout;
  GetGroupSize get_group_size;

  std::vector<size_t> _append_buffer;

  std::vector<GroupRunElementType> data;

  // Hash per group
  std::vector<size_t> hashes;

  // Number of groups in this run
  size_t end{0};

  const std::vector<size_t>& append_buffer() const {
    return _append_buffer;
  }

  bool can_append(const FixedSizeGroupRun& source_run, const size_t source_offset) const {
    return end + _append_buffer.size() < hashes.size();
  }

  HashTableKey make_key(const size_t offset) const {
    if constexpr (std::is_same_v<GetGroupSize, GetDynamicGroupSize>) {
      return {hashes[offset], &data[offset * get_group_size()]};
    } else {
      HashTableKey key;
      key.hash = hashes[offset];
      memcpy(&key.group, &data[offset * get_group_size()], get_group_size());
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
      source += layout->group_size * sizeof(GroupRunElementType);
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
                source.data.begin() + (source_offset + 1) * get_group_size(),
                data.begin() + end * get_group_size());
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
  using Column = std::tuple<bool /* is_variably_sized */, ColumnID /* index */, std::optional<size_t> /* nullable_index */
                            >;
  std::vector<Column> column_mapping;

  FixedSizeGroupRunLayout fixed_layout;

  VariablySizedGroupRunLayout(const std::vector<ColumnID>& variably_sized_column_ids, const std::vector<ColumnID>& fixed_size_column_ids, const std::vector<Column>& column_mapping,
                              const FixedSizeGroupRunLayout& fixed_layout)
      :
        variably_sized_column_ids(variably_sized_column_ids),
        fixed_size_column_ids(fixed_size_column_ids),
        column_count(variably_sized_column_ids.size()),
        nullable_column_count(0u), // Initialized below
        column_mapping(column_mapping),
        fixed_layout(fixed_layout) {
    nullable_column_count = std::count_if(column_mapping.begin(), column_mapping.end(), [&](const auto& tuple) {
      return std::get<2>(tuple).has_value();
    });
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
  constexpr static auto CAPACITY = 4;// sizeof(VariablySizedRemoteGroupKey) / sizeof(GroupRunElementType);

  const size_t* value_end_offsets;
  size_t size;
  std::array<GroupRunElementType, CAPACITY> group;
};

struct VariablySizedGroupKey {
  size_t hash{};
  boost::variant<VariablySizedInPlaceGroupKey, VariablySizedRemoteGroupKey> variant;
};

struct VariablySizedGroupKeyCompare {
  const VariablySizedGroupRunLayout* layout{};

  bool operator()(const VariablySizedGroupKey& lhs, const VariablySizedGroupKey& rhs) const {
    if (lhs.variant.which() != rhs.variant.which()) return false;

    if (lhs.variant.which() == 0) {
      const auto& in_place_lhs = boost::get<VariablySizedInPlaceGroupKey>(lhs.variant);
      const auto& in_place_rhs = boost::get<VariablySizedInPlaceGroupKey>(rhs.variant);

      if (in_place_lhs.size != in_place_rhs.size) {
        return false;
      }

      if (!std::equal(in_place_lhs.group.begin(), in_place_lhs.group.begin() + in_place_lhs.size, in_place_rhs.group.begin())) {
        return false;
      }

      // Compare intra-group layout
      return std::equal(in_place_lhs.value_end_offsets, in_place_lhs.value_end_offsets + layout->column_count,  in_place_rhs.value_end_offsets);
    } else {
      const auto& remote_lhs = boost::get<VariablySizedRemoteGroupKey>(lhs.variant);
      const auto& remote_rhs = boost::get<VariablySizedRemoteGroupKey>(rhs.variant);

      if (remote_lhs.variably_sized_group_length != remote_rhs.variably_sized_group_length) {
        return false;
      }

      // Compare intra-group layout
      if (!std::equal(remote_lhs.value_end_offsets, remote_lhs.value_end_offsets + layout->column_count,  remote_rhs.value_end_offsets)) {
        return false;
      }

      // Compare the variably sized group data
      if (!std::equal(remote_lhs.variably_sized_group, remote_lhs.variably_sized_group + remote_lhs.variably_sized_group_length, remote_rhs.variably_sized_group)) {
        return false;
      }

      // Compare the fixed size group data
      DebugAssert((remote_lhs.fixed_sized_group == nullptr) == (remote_rhs.fixed_sized_group == nullptr), "");
      if (!remote_lhs.fixed_sized_group) {
        return true;
      }

      return std::equal(remote_lhs.fixed_sized_group, remote_lhs.fixed_sized_group + layout->fixed_layout.group_size, remote_rhs.fixed_sized_group);
    }

  }
};

struct VariablySizedGroupRun {
  using LayoutType = VariablySizedGroupRunLayout;
  using HashTableKey = VariablySizedGroupKey;
  using HashTableCompare = VariablySizedGroupKeyCompare;

  const VariablySizedGroupRunLayout* layout;

  std::vector<GroupRunElementType> data;

  // Number of `data` elements occupied after append_buffer is flushed
  size_t data_watermark{0};

  // End indices in `data`
  std::vector<size_t> group_end_offsets;

  // Offsets (in bytes) for all values in all groups, relative to the beginning of the group.
  std::vector<size_t> value_end_offsets;

  // Non-variably sized values in the group and hash values
  FixedSizeGroupRun<GetDynamicGroupSize> fixed;

  explicit VariablySizedGroupRun(const VariablySizedGroupRunLayout* layout, const size_t size, const size_t data_size)
      : layout(layout), fixed(&layout->fixed_layout, size) {
    data.resize(data_size);
    group_end_offsets.resize(size);
    value_end_offsets.resize(size * layout->column_count);
  }

  const std::vector<size_t>& append_buffer() const {
    return fixed.append_buffer();
  }

  VariablySizedGroupKey make_key(const size_t offset) {
    const auto group_begin_offset = offset == 0 ? size_t{0} : group_end_offsets[offset - 1];
    const auto variably_sized_group_size = group_end_offsets[offset] - group_begin_offset;
    const auto* variably_sized_group = &data[group_begin_offset];
    const auto* fixed_size_group = layout->fixed_layout.group_size == 0 ? nullptr : &fixed.data[offset * layout->fixed_layout.group_size];

    const auto total_group_size = variably_sized_group_size + layout->fixed_layout.group_size;

    if (total_group_size <= VariablySizedInPlaceGroupKey::CAPACITY) {
      auto key = VariablySizedGroupKey{};
      key.hash = fixed.hashes[offset];

      key.variant = VariablySizedInPlaceGroupKey{};
      auto& in_place = boost::get<VariablySizedInPlaceGroupKey>(key.variant);
      in_place.size = total_group_size;
      in_place.value_end_offsets = &value_end_offsets[offset * layout->column_count];
      std::copy(variably_sized_group, variably_sized_group + variably_sized_group_size, in_place.group.begin());
      std::copy(fixed_size_group, fixed_size_group + layout->fixed_layout.group_size, in_place.group.begin() + variably_sized_group_size);

      return key;
    } else {
      return {fixed.hashes[offset], VariablySizedRemoteGroupKey{
        variably_sized_group,
        variably_sized_group_size,
        &value_end_offsets[offset * layout->column_count],
        fixed_size_group
      }};
    }

  }

  bool can_append(const VariablySizedGroupRun& source_run, const size_t source_offset) const {
    if (!fixed.can_append(source_run.fixed, source_offset)) {
      return false;
    }

    const auto source_size = source_offset == 0 ? source_run.group_end_offsets[0] : source_run.group_end_offsets[source_offset] - source_run.group_end_offsets[source_offset - 1];
    return data_watermark + source_size < data.size();
  }

  void schedule_append(const VariablySizedGroupRun& source, const size_t source_offset) {
    fixed.schedule_append(source.fixed, source_offset);

    const auto source_size = source_offset == 0 ? source.group_end_offsets[0] : source.group_end_offsets[source_offset] - source.group_end_offsets[source_offset - 1];
    data_watermark += source_size;
  }

  template <typename T>
  std::shared_ptr<BaseSegment> materialize_output(const ColumnID column_id, const bool column_is_nullable) const {
    const auto& [is_variably_sized, local_column_id, nullable_column_idx] = layout->column_mapping[column_id];

    if (!is_variably_sized) {
      return fixed.materialize_output<T>(ColumnID{local_column_id}, column_is_nullable);
    }

    const auto row_count = fixed.end;

    auto output_values = std::vector<pmr_string>(row_count);
    auto output_null_values = std::vector<bool>();
    if (column_is_nullable) {
      output_null_values.resize(row_count);
    }

    for (auto row_idx = size_t{0}; row_idx < row_count; ++row_idx) {
      const auto group_begin_offset = row_idx == 0 ? 0 : group_end_offsets[row_idx - 1];
      const auto value_begin_offset = local_column_id == 0 ? 0 : value_end_offsets[row_idx * layout->column_count + local_column_id - 1];
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
      const auto source_value_end_offsets_iter = source.value_end_offsets.begin() + source_offset * layout->column_count;
      std::copy(source_value_end_offsets_iter, source_value_end_offsets_iter + layout->column_count, value_end_offsets.begin() + end * layout->column_count);

      // Copy group size
      const auto source_group_offset = source_offset == 0 ? 0 : source.group_end_offsets[source_offset - 1];
      const auto source_group_size = source.group_end_offsets[source_offset] - source_group_offset;
      group_end_offsets[end] = end == 0 ? source_group_size : group_end_offsets[end - 1] + source_group_size;

      // Copy group data
      const auto source_data_iter = source.data.begin() + source_group_offset;
      const auto target_data_offset = end == 0 ? 0 : group_end_offsets[end - 1];
      const auto target_data_iter = data.begin() + target_data_offset;
      DebugAssert(target_data_offset + source_group_size < data.size(), "");
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

    if constexpr (std::is_same_v<GroupRun, VariablySizedGroupRun>) {
      auto new_groups = GroupRun{groups.layout, memory_budget, memory_budget * 2};
      return Run<GroupRun>{std::move(new_groups), std::move(new_aggregates)};
    } else {
      auto new_groups = GroupRun{groups.layout, memory_budget};
      return Run<GroupRun>{std::move(new_groups), std::move(new_aggregates)};
    }
  }

  void finish() {
    groups.finish();
    for (auto& aggregate : aggregates) {
      aggregate->resize(groups.size());
    }
  }

  GroupRun groups;
  std::vector<std::unique_ptr<BaseAggregateRun>> aggregates;

  bool is_aggregated{false};
};

template <typename GroupRun>
struct Partition {
  std::vector<AggregationBufferEntry> aggregation_buffer;

  size_t group_key_counter{0};
  size_t next_run_memory_budget{1'000};

  std::vector<Run<GroupRun>> runs;

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

  void append(const Run<GroupRun>& source_run, const size_t source_offset) {
    if (runs.empty() || !runs.back().groups.can_append(source_run.groups, source_offset)) {
      flush_buffers(source_run);
      next_run_memory_budget = std::min(2 * next_run_memory_budget, size_t{1'000'000});
      runs.emplace_back(source_run.new_instance(next_run_memory_budget));
    }

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

  void finish() {
    for (auto& run : runs) {
      run.finish();
    }
  }
};

template<typename GroupRun>
struct AbstractRunSource {
  const typename GroupRun::LayoutType* layout;
  std::vector<Run<GroupRun>> runs;

  AbstractRunSource(const typename GroupRun::LayoutType* layout, std::vector<Run<GroupRun>>&& runs): layout(layout), runs(std::move(runs)) {}
  virtual ~AbstractRunSource() = default;

  virtual bool can_fetch_run() const = 0;
  virtual void fetch_run() = 0;
};

// Wraps a Partition
template<typename GroupRun>
struct PartitionRunSource : public AbstractRunSource<GroupRun> {
  using AbstractRunSource<GroupRun>::AbstractRunSource;

  bool can_fetch_run() const override {return false;}
  void fetch_run() override {Fail("Shouldn't be called");}
};

struct Partitioning {
  size_t partition_count;
  size_t hash_shift;
  size_t hash_mask;

  Partitioning(const size_t partition_count, const size_t hash_shift, const size_t hash_mask)
      : partition_count(partition_count), hash_shift(hash_shift), hash_mask(hash_mask) {}

  size_t get_partition_index(const size_t hash) const { return (hash >> hash_shift) & hash_mask; }
};

inline Partitioning determine_partitioning(const size_t level) {
  // Best partitioning as determined by magic
  return {16, level * 4, 0b1111};
}

template <typename GroupRun>
std::vector<Partition<GroupRun>> partition(const AggregateHashSortConfig& config,
                                           AbstractRunSource<GroupRun>& run_source, const Partitioning& partitioning,
                                           size_t& run_idx, size_t& run_offset) {
  auto partitions = std::vector<Partition<GroupRun>>(partitioning.partition_count);

  auto counter = size_t{0};
  auto done = false;

  while (run_idx < run_source.runs.size() || run_source.can_fetch_run()) {
    if (run_idx == run_source.runs.size()) {
      run_source.fetch_run();
    }

    auto&& input_run = run_source.runs[run_idx];

    for (; run_offset < input_run.size() && !done; ++run_offset) {
      const auto partition_idx = partitioning.get_partition_index(input_run.groups.hash(run_offset));

      auto& partition = partitions[partition_idx];
      partition.append(input_run, run_offset);
      ++counter;

      if (counter >= config.max_partitioning_counter) {
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
    partition.finish();
  }

  return partitions;
}

template <typename GroupRun>
std::pair<bool, std::vector<Partition<GroupRun>>> hashing(const AggregateHashSortConfig& config,
                                                          AbstractRunSource<GroupRun>& run_source,
                                                          const Partitioning& partitioning, size_t& run_idx,
                                                          size_t& run_offset) {
  auto partitions = std::vector<Partition<GroupRun>>(partitioning.partition_count);

#if 1
  auto hash_fn = [&](const auto& key) { return key.hash; };

  auto compare_fn = typename GroupRun::HashTableCompare{run_source.layout};

  auto hash_table = std::unordered_map<typename GroupRun::HashTableKey, size_t, decltype(hash_fn), decltype(compare_fn)>{
      config.hash_table_size, hash_fn, compare_fn};
#else
  using HashTableKey = std::pair<size_t, size_t>;

  constexpr static auto EMPTY_KEY = std::make_pair(std::numeric_limits<size_t>::max(), size_t{0});

  struct Hasher {
    AbstractRunSource<GroupRun>* run_source{};
    size_t operator()(const HashTableKey& key) const {
      //std::cout << "Hash: " << key.first << " " << key.second << std::endl;
      if (key.first == EMPTY_KEY.first) return 0;
      return run_source->runs[key.first].groups.hash(key.second);
    }
  };

  struct EqualTo {
    AbstractRunSource<GroupRun>* run_source{};
    bool operator()(const HashTableKey& lhs, const HashTableKey& rhs) const {
      //std::cout << "EqualTo: " << lhs.first << " " << lhs.second << " vs "  << rhs.first << " " << rhs.second << std::endl;
      if (lhs.first == EMPTY_KEY.first && rhs.first == EMPTY_KEY.first) return true;
      if (lhs.first == EMPTY_KEY.first || rhs.first == EMPTY_KEY.first) return false;

      return run_source->runs[lhs.first].groups.compare(lhs.second, run_source->runs[rhs.first].groups, rhs.second);
    }
  };

  auto hasher = Hasher{&run_source};
  auto equal_to = EqualTo{&run_source};

  auto hash_table = google::dense_hash_map<std::pair<size_t, size_t>, size_t, Hasher, EqualTo>{
  static_cast<size_t>(config.hash_table_size *  config.hash_table_max_load_factor), hasher, equal_to};
  hash_table.set_empty_key(EMPTY_KEY);
  hash_table.min_load_factor(0.0f);
#endif

  hash_table.max_load_factor(1.0f);

  auto counter = size_t{0};
  auto continue_hashing = true;
  auto done = false;

  while (run_idx < run_source.runs.size() || run_source.can_fetch_run()) {
    if (run_idx == run_source.runs.size()) {
      run_source.fetch_run();
    }

    auto&& input_run = run_source.runs[run_idx];

    for (; run_offset < input_run.size() && !done; ++run_offset) {
      const auto partition_idx = partitioning.get_partition_index(input_run.groups.hash(run_offset));
      auto& partition = partitions[partition_idx];

      const auto key = input_run.groups.make_key(run_offset);

      auto hash_table_iter = hash_table.find(key);
      if (hash_table_iter == hash_table.end()) {
        hash_table.insert({key, partition.group_key_counter});
        partition.append(input_run, run_offset);
        ++partition.group_key_counter;
      } else {
        partition.aggregate(hash_table_iter->second, input_run, run_offset);
      }

      ++counter;

      //std::cout << "Load factor: " << hash_table.load_factor() << std::endl;
      if (hash_table.load_factor() >= config.hash_table_max_load_factor) {
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
    for (auto& run : partition.runs) {
      run.is_aggregated = true;
    }
    partition.finish();
  }

  return {continue_hashing, std::move(partitions)};
}

template <typename GroupRun>
std::vector<Partition<GroupRun>> adaptive_hashing_and_partition(const AggregateHashSortConfig& config,
                                                                std::unique_ptr<AbstractRunSource<GroupRun>> run_source,
                                                                const Partitioning& partitioning) {
  auto partitions = std::vector<Partition<GroupRun>>(partitioning.partition_count);

  auto mode = HashSortMode::Hashing;

  auto run_idx = size_t{0};
  auto run_offset = size_t{0};

  while (run_idx < run_source->runs.size() || run_source->can_fetch_run()) {
    auto phase_partitions = std::vector<Partition<GroupRun>>{};

    if (mode == HashSortMode::Hashing) {
      auto [continue_hashing, hashing_partitions] =
          hashing(config, *run_source, partitioning, run_idx, run_offset);
      if (!continue_hashing) {
        mode = HashSortMode::Partition;
      }
      phase_partitions = std::move(hashing_partitions);
    } else {
      phase_partitions = partition(config, *run_source, partitioning, run_idx, run_offset);
      mode = HashSortMode::Hashing;
    }

    DebugAssert(phase_partitions.size() == partitions.size(), "");

    for (auto partition_idx = size_t{0}; partition_idx < partitioning.partition_count; ++partition_idx) {
      auto& partition = partitions[partition_idx];
      auto& phase_partition = phase_partitions[partition_idx];
      partition.runs.insert(partition.runs.end(), std::make_move_iterator(phase_partition.runs.begin()),
                            std::make_move_iterator(phase_partition.runs.end()));
    }
  }

  return partitions;
}

template <typename GroupRun>
std::vector<Run<GroupRun>> aggregate(const AggregateHashSortConfig& config, std::unique_ptr<AbstractRunSource<GroupRun>> run_source,
                                     const size_t level) {
  if (run_source->runs.empty() && !run_source->can_fetch_run()) {
    return {};
  }

  auto output_runs = std::vector<Run<GroupRun>>{};

  if (run_source->runs.size() == 1 && run_source->runs.front().is_aggregated) {
    output_runs.emplace_back(std::move(run_source->runs.front()));
    return output_runs;
  }

  const auto partitioning = determine_partitioning(level);

  auto partitions = adaptive_hashing_and_partition(config, std::move(run_source), partitioning);

  for (auto&& partition : partitions) {
    if (partition.size() == 0) {
      continue;
    }

    const auto* layout = partition.runs.front().groups.layout;

    std::unique_ptr<AbstractRunSource<GroupRun>> partition_run_source = std::make_unique<PartitionRunSource<GroupRun>>(layout, std::move(partition.runs));
    auto aggregated_partition = aggregate(config, std::move(partition_run_source), level + 1);
    output_runs.insert(output_runs.end(), std::make_move_iterator(aggregated_partition.begin()),
                       std::make_move_iterator(aggregated_partition.end()));
  }

  return output_runs;
}

template <typename SegmentPosition>
size_t hash_segment_position(const SegmentPosition& segment_position) {
  if (segment_position.is_null()) {
    return 0;
  } else {
    return boost::hash_value(segment_position.value());
  }
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

  const auto fixed_layout =
      produce_initial_groups_layout<FixedSizeGroupRunLayout>(table, fixed_size_column_ids);

  return VariablySizedGroupRunLayout{variably_sized_column_ids, fixed_size_column_ids, column_mapping, fixed_layout};
}

template<typename GetGroupSize>
inline std::pair<FixedSizeGroupRun<GetGroupSize>, RowID> produce_initial_groups(const std::shared_ptr<const Table>& table,
                                                                   const FixedSizeGroupRunLayout* layout,
                                                                   const std::vector<ColumnID>& group_by_column_ids,
                                                                   const RowID& begin_row_id,
                                                                   const size_t row_count) {
  constexpr auto GROUP_DATA_ELEMENT_SIZE = sizeof(GroupRunElementType);

  auto group_run = FixedSizeGroupRun<GetGroupSize>{layout, row_count};

  auto end_row_id = RowID{};

  for (auto output_group_by_column_idx = ColumnID{0}; output_group_by_column_idx < group_by_column_ids.size();
       ++output_group_by_column_idx) {

    auto* target = reinterpret_cast<char*>(group_run.data.data()) + layout->column_base_offsets[output_group_by_column_idx];
    auto run_offset = size_t{0};
    const auto column_is_nullable = table->column_is_nullable(group_by_column_ids[output_group_by_column_idx]);

    auto column_iterable = ColumnIterable{table, group_by_column_ids[output_group_by_column_idx]};
    end_row_id = column_iterable.for_each<ResolveDataTypeTag>([&](const auto& segment_position, const RowID& row_id) {
      using ColumnDataType = typename std::decay_t<decltype(segment_position)>::Type;

      if (column_is_nullable) {
        if (segment_position.is_null()) {
          *target = 1;
        } else {
          memcpy(target + 1, &segment_position.value(), sizeof(ColumnDataType));
        }
      } else {
        memcpy(target, &segment_position.value(), sizeof(ColumnDataType));
      }

      boost::hash_combine(group_run.hashes[run_offset], hash_segment_position(segment_position));

      target += layout->group_size * GROUP_DATA_ELEMENT_SIZE;
      ++run_offset;

      return run_offset == row_count ? ColumnIteration::Break : ColumnIteration::Continue;
    }, begin_row_id);
  }

  group_run.end = row_count;

  return {std::move(group_run), end_row_id};
}

inline std::pair<VariablySizedGroupRun, RowID> produce_initial_groups(
    const std::shared_ptr<const Table>& table,
    const VariablySizedGroupRunLayout* layout,
    const std::vector<ColumnID>& group_by_column_ids,
    const RowID& begin_row_id,
    const size_t row_budget,
    const size_t per_column_data_budget_bytes) {

  constexpr auto DATA_ELEMENT_SIZE = sizeof(GroupRunElementType);

  Assert(row_budget > 0, "");
  Assert(!layout->variably_sized_column_ids.empty(), "FixedGroupRun should be used if there are no variably sized columns");

  const auto column_count = layout->variably_sized_column_ids.size();

  auto run_row_count = std::numeric_limits<size_t>::max();
  auto end_row_id = RowID{};

  /**
   * Materialize each variable-width column into a continuous blob
   * While doing so
   *    - determine the layout of the interleaved layout in the final VariablySizedGroupRun (`value_end_offsets`)
   *    - materialize `null_values`
   */
  auto value_end_offsets = std::vector<size_t>(row_budget * column_count);
  auto data_per_column = std::vector<std::vector<GroupRunElementType>>(column_count);

  const auto per_column_element_budget = divide_and_ceil(per_column_data_budget_bytes, DATA_ELEMENT_SIZE);

  {
    auto nullable_column_idx = size_t{0};

    for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
      auto &column_data = data_per_column[column_id];
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
      [&](const auto &segment_position, const RowID &row_id) {
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
      }, begin_row_id);

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
  auto group_end_offsets = std::vector<size_t>(run_row_count);
  auto previous_group_end_offset = size_t{0};
  auto value_end_offset_iter = value_end_offsets.begin() + (column_count - 1);
  for (auto row_idx = size_t{0}; row_idx < run_row_count; ++row_idx) {
    // Round up to a full DATA_ELEMENT_SIZE
    const auto group_element_count = *value_end_offset_iter / DATA_ELEMENT_SIZE + (*value_end_offset_iter % DATA_ELEMENT_SIZE > 0 ? 1 : 0);

    previous_group_end_offset += group_element_count;

    group_end_offsets[row_idx] = previous_group_end_offset;
    value_end_offset_iter += column_count;
  }

  const auto data_element_count = group_end_offsets.back();

  /**
   * Materialize fixed-size group-by columns, now that we know the row count we want to materialize for this run
   */
  auto group_run = VariablySizedGroupRun{layout, 0, 0};
  group_run.fixed = produce_initial_groups<GetDynamicGroupSize>(table, &layout->fixed_layout, layout->fixed_size_column_ids, begin_row_id, run_row_count).first;

  /**
   * Rearrange `data_per_column` into an interleaved blob
   */
  auto group_data = std::vector<GroupRunElementType>(data_element_count);

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

      memcpy(target, source, source_size);

      boost::hash_combine(group_run.fixed.hashes[row_idx], boost::hash_range(source, source + source_size));

      source += source_size;
    }
  }

  group_run.data = std::move(group_data);
  group_run.group_end_offsets = std::move(group_end_offsets);
  group_run.value_end_offsets = std::move(value_end_offsets);

  return {std::move(group_run), end_row_id};
}

// Wraps a Table from which the runs are materialized
template<typename GroupRun>
struct TableRunSource : public AbstractRunSource<GroupRun> {
  std::shared_ptr<const Table> table;
  const AggregateHashSortConfig& config;
  std::vector<AggregateColumnDefinition> aggregate_column_definitions;
  std::vector<ColumnID> groupby_column_ids;

  RowID begin_row_id{ChunkID{0}, ChunkOffset{0}};
  size_t remaining_rows;

  TableRunSource(const std::shared_ptr<const Table>& table, const typename GroupRun::LayoutType* layout, const AggregateHashSortConfig& config, const std::vector<AggregateColumnDefinition>& aggregate_column_definitions,
                 const std::vector<ColumnID>& groupby_column_ids):
  AbstractRunSource<GroupRun>(layout, {}), table(table), config(config), aggregate_column_definitions(aggregate_column_definitions), groupby_column_ids(groupby_column_ids) {
    remaining_rows = table->row_count();
  }

  bool can_fetch_run() const override { return remaining_rows > 0; }

  void fetch_run() override {
    if constexpr (std::is_same_v<GroupRun, VariablySizedGroupRun>) {
      const auto run_row_budget = std::min(remaining_rows, size_t{300'000});
      const auto run_group_data_budget = std::max(size_t{128}, run_row_budget * 7u);

      auto pair = produce_initial_groups(table, this->layout, groupby_column_ids, begin_row_id, run_row_budget, run_group_data_budget);
      auto input_aggregates = produce_initial_aggregates(table, aggregate_column_definitions, !groupby_column_ids.empty(), begin_row_id, pair.first.size());

      remaining_rows -= pair.first.fixed.size();

      begin_row_id = pair.second;
      this->runs.emplace_back(std::move(pair.first), std::move(input_aggregates));
    } else {
      const auto run_row_count = std::min(remaining_rows, size_t{300'000});
      remaining_rows -= run_row_count;

      auto pair = produce_initial_groups<typename GroupRun::GetGroupSizeType>(table, this->layout, groupby_column_ids, begin_row_id, run_row_count);
      auto input_aggregates = produce_initial_aggregates(table, aggregate_column_definitions, !groupby_column_ids.empty(), begin_row_id, pair.first.size());

      begin_row_id = pair.second;
      this->runs.emplace_back(std::move(pair.first), std::move(input_aggregates));
    }
  }
};

}  // namespace aggregate_hashsort

}  // namespace opossum
