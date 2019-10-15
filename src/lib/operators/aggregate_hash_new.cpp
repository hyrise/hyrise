#include "aggregate_hash.hpp"

#include <boost/container/pmr/monotonic_buffer_resource.hpp>

#include "resolve_type.hpp"
#include "storage/segment_iterate.hpp"

namespace opossum {

// TODO replace most vectors with uninitialized_vectors?

AggregateHash::AggregateHash(const std::shared_ptr<AbstractOperator>& in,
                             const std::vector<AggregateColumnDefinition>& aggregates,
                             const std::vector<ColumnID>& groupby_column_ids)
    : AbstractAggregateOperator(in, aggregates, groupby_column_ids) {}

using EmptyGroupbyKey = bool;  // TODO

namespace {
template <typename Key, typename Value>
using ResultStore = std::conditional_t<!std::is_same_v<Key, EmptyGroupbyKey>, std::unordered_map<Key, Value>, Value>;

template <typename GroupbyKey>
pmr_vector<GroupbyKey> _materialize_groupby_keys(const Chunk& chunk, const std::vector<ColumnID>& groupby_column_ids) {
  const auto chunk_size = chunk.size();

  auto temp_buffer = new boost::container::pmr::monotonic_buffer_resource(1'000'000);  // TODO hart am leaken dran :)
  auto allocator = PolymorphicAllocator<GroupbyKey>{temp_buffer};

  auto groupby_keys = pmr_vector<GroupbyKey>(chunk_size, allocator);

  if constexpr (std::is_same_v<GroupbyKey, std::vector<AllTypeVariant>>) {
    for (auto chunk_offset = ChunkOffset{0}; chunk_offset < chunk_size; ++chunk_offset) {
      groupby_keys[chunk_offset].reserve(groupby_column_ids.size());
    }
  }

  for (const auto& groupby_column_id : groupby_column_ids) {
    auto chunk_offset = ChunkOffset{0};
    segment_iterate(*chunk.get_segment(groupby_column_id), [&](const auto& position) {
      if constexpr (std::is_same_v<GroupbyKey, EmptyGroupbyKey>) {
        Fail("This should not happen, as EmptyGroupbyKey is only used for an empty list of group by columns");
      } else if constexpr (std::is_same_v<GroupbyKey, std::vector<AllTypeVariant>>) {
        groupby_keys[chunk_offset].emplace_back(position.is_null() ? NULL_VALUE : position.value());
      } else if constexpr (std::is_same_v<GroupbyKey, typename std::decay_t<decltype(
                                                          position)>::Type>) {  // TODO hash of nullopt is UB
        // } else if constexpr (std::is_same_v<GroupbyKey, std::optional<typename std::decay_t<decltype(position)>::Type>>) {  // TODO hash of nullopt is UB
        // Any regular data type
        groupby_keys[chunk_offset] = GroupbyKey{position.value()};
        // groupby_keys[chunk_offset] = position.is_null() ? std::nullopt : GroupbyKey{position.value()};  optional
      } else {
        Fail("Unexpected type");
      }

      ++chunk_offset;
    });
  }

  return groupby_keys;
}

template <typename GroupbyKey>
std::pair<pmr_vector<GroupbyKey>, std::vector<uintptr_t>> _gather_groups_and_ids(
    const pmr_vector<GroupbyKey>& groupby_keys) {
  auto temp_buffer = boost::container::pmr::monotonic_buffer_resource(1'000'000);
  auto allocator = PolymorphicAllocator<GroupbyKey>{&temp_buffer};

  auto distinct_values = pmr_vector<GroupbyKey>(groupby_keys.begin(), groupby_keys.end());
  std::sort(distinct_values.begin(), distinct_values.end());
  distinct_values.erase(std::unique(distinct_values.begin(), distinct_values.end()), distinct_values.end());
  std::cout << distinct_values.size() << " distinct values found" << std::endl;

  auto group_ids = std::vector<uintptr_t>(groupby_keys.size());
  // for (auto key_idx = size_t{0}; key_idx < groupby_keys.size(); ++key_idx) {
  //   // Oh no, you didn't
  //   group_ids[key_idx] = reinterpret_cast<uintptr_t>(&*distinct_values.find(groupby_keys[key_idx]));
  // }

  return {distinct_values, group_ids};
}

template <typename AggregateColumnType>
void _aggregate_single_column(const Chunk&, const std::vector<uintptr_t>& group_ids, ColumnID aggregate_column) {}

template <typename GroupbyKey>
std::shared_ptr<const Table> _aggregate(const Table& input_table, const std::vector<ColumnID>& groupby_column_ids,
                                        const std::vector<AggregateColumnDefinition>& aggregates) {
  std::shared_ptr<const Table> result_table;

  auto groupby_keys_by_chunk = std::vector<pmr_vector<GroupbyKey>>(input_table.chunk_count());
  auto distinct_values_by_chunk = std::vector<pmr_vector<GroupbyKey>>(input_table.chunk_count());
  for (auto chunk_id = ChunkID{0}; chunk_id < input_table.chunk_count(); ++chunk_id) {
    const auto& chunk = *input_table.get_chunk(chunk_id);
    groupby_keys_by_chunk[chunk_id] = _materialize_groupby_keys<GroupbyKey>(chunk, groupby_column_ids);

    auto mapping = _gather_groups_and_ids<GroupbyKey>(groupby_keys_by_chunk[chunk_id]);
    distinct_values_by_chunk[chunk_id] = std::move(mapping.first);
    auto group_ids = std::move(mapping.second);

    for (const auto& [aggregate_column, aggregate_function] : aggregates) {
      Assert(aggregate_column, "COUNT(*) not supported yet");

      // Redeclare aggregate_function because structured bindings and lambdas don't like each other:
      // https://stackoverflow.com/questions/46114214/
      resolve_data_type(input_table.column_data_type(*aggregate_column),
                        [&, aggregate_column = aggregate_column, aggregate_function = aggregate_function](auto type) {
                          using AggregateColumnType = typename decltype(type)::type;
                          switch (aggregate_function) {
                            case AggregateFunction::Min:
                              _aggregate_single_column<AggregateColumnType>(chunk, group_ids, *aggregate_column);
                              break;
                            case AggregateFunction::Max:
                              _aggregate_single_column<AggregateColumnType>(chunk, group_ids, *aggregate_column);
                              break;
                            case AggregateFunction::Sum:
                              _aggregate_single_column<AggregateColumnType>(chunk, group_ids, *aggregate_column);
                              break;
                            case AggregateFunction::Avg:
                              _aggregate_single_column<AggregateColumnType>(chunk, group_ids, *aggregate_column);
                              break;
                            case AggregateFunction::Count:
                              _aggregate_single_column<AggregateColumnType>(chunk, group_ids, *aggregate_column);
                              break;
                            case AggregateFunction::CountDistinct:
                              _aggregate_single_column<AggregateColumnType>(chunk, group_ids, *aggregate_column);
                              break;
                            case AggregateFunction::StandardDeviationSample:
                              _aggregate_single_column<AggregateColumnType>(chunk, group_ids, *aggregate_column);
                              break;
                          }
                        });
    }
  }

  return result_table;
}

}  // namespace

std::shared_ptr<const Table> AggregateHash::_on_execute() {
  auto input_table = input_table_left();
  std::shared_ptr<const Table> result_table;

  if (_groupby_column_ids.empty()) {
    result_table = _aggregate<EmptyGroupbyKey>(*input_table, _groupby_column_ids, _aggregates);
  } else if (_groupby_column_ids.size() == 1) {
    resolve_data_type(input_table->column_data_type(_groupby_column_ids[0]), [&](auto type) {
      using GroupbyDataType = typename decltype(type)::type;
      result_table = _aggregate<GroupbyDataType>(*input_table, _groupby_column_ids, _aggregates);
      // result_table = _aggregate<std::optional<GroupbyDataType>>(*input_table, _groupby_column_ids, _aggregates);
    });
  } else {
    // result_table = _aggregate<std::vector<AllTypeVariant>>(*input_table, _groupby_column_ids, _aggregates);
  }

  return result_table;
}

const std::string AggregateHash::name() const { return "Aggregate"; }

std::shared_ptr<AbstractOperator> AggregateHash::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<AggregateHash>(copied_input_left, _aggregates, _groupby_column_ids);
}

void AggregateHash::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

void AggregateHash::_on_cleanup() {
  // TODO
}

}  // namespace opossum
