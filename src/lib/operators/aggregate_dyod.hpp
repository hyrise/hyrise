#pragma once

#include <atomic>
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include <boost/container_hash/hash.hpp>
#include <boost/unordered/unordered_flat_map.hpp>
#include <oneapi/tbb/concurrent_unordered_map.h>  // NOLINT(build/include_order)
#include <oneapi/tbb/concurrent_vector.h>         // NOLINT(build/include_order)

#include "abstract_aggregate_operator.hpp"
#include "aggregate/aggregate_vector.hpp"
#include "aggregate/types.hpp"
#include "aggregate/window_function_traits.hpp"
#include "expression/window_function_expression.hpp"
#include "types.hpp"

namespace hyrise {

class WorkerState : public Noncopyable {
 public:
  explicit WorkerState(const std::vector<std::shared_ptr<WindowFunctionExpression>>& aggregates,
                       std::function<std::pair<GroupID, GroupID>()> init_group_id_range);
  void merge(WorkerState& other);

  GroupID next_group_id();

  AbstractAggregateVector& aggregate_vector(size_t index);
  std::vector<std::unique_ptr<AbstractAggregateVector>>& aggregate_vectors();

 protected:
  // This is the next local group ID that a worker can assign to a group key
  GroupID _next_group_id;

  // This is the largest local group ID that a worker can assign to a group key
  GroupID _max_group_id;

  std::function<std::pair<GroupID, GroupID>()> _get_new_group_id_range;
  std::vector<std::unique_ptr<AbstractAggregateVector>> _vectors;
};

struct SingleThreadedState {
  boost::unordered_flat_map<GroupKey, GroupID, GroupKeyHash, GroupKeyEqual> group_id_map;
  GroupID next_group_id{0};
  std::vector<GroupKey> group_keys;
  std::vector<std::vector<RowID>> row_ids;
  std::vector<bool> occupied_group_ids;
};

struct MultiThreadedState {
  tbb::concurrent_unordered_map<GroupKey, GroupID, GroupKeyHash, GroupKeyEqual> group_id_map;
  std::atomic<GroupID> next_group_id{0};
  tbb::concurrent_vector<GroupKey> group_keys;
  tbb::concurrent_vector<std::vector<RowID>> row_ids;
  std::mutex group_keys_mutex;
  tbb::concurrent_vector<bool> occupied_group_ids;
};

class AggregateDYOD : public AbstractAggregateOperator {
 public:
  AggregateDYOD(const std::shared_ptr<AbstractOperator>& input_operator,
                const std::vector<std::shared_ptr<WindowFunctionExpression>>& aggregates,
                const std::vector<ColumnID>& groupby_column_ids);

  const std::string& name() const override;

 protected:
  // The paper uses a default step size of 256
  // https://github.com/danielxue/global-hash-tables-strike-back/blob/main/common/src/fuzzy_counter.rs#L56
  static constexpr GroupID FUZZY_STEP_SIZE = 512;

  // Initial size of the group ID map and vectors
  // TODO(anyone): Replace with proper estimate of group cardinality based on input table.
  static constexpr GroupID GROUP_ID_INITIAL_SIZE = 100'000;

  std::vector<DataType> _aggregate_data_types;

  std::variant<SingleThreadedState, MultiThreadedState> _state;

  // One group key buffer per chunk. Group keys entries (i.e., the serialized groupby column values) for a
  // single row are stored sequentially in the buffer.
  std::vector<std::vector<std::byte>> _group_key_buffers;

  std::shared_ptr<const Table> _on_execute() override;

  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_left_input,
      const std::shared_ptr<AbstractOperator>& copied_right_input,
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const override;

  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  void _on_cleanup() override;

  void _prepare_aggregate_vectors();

  std::shared_ptr<Table> _write_output_table(WorkerState& worker_state);

  TableColumnDefinitions _output_column_definitions();

  std::shared_ptr<Chunk> _write_output_chunk(WorkerState& worker_state, const std::vector<size_t>& occupied_group_ids,
                                             size_t start_index, size_t end_index);

  std::shared_ptr<AbstractSegment> _write_groupby_segment(size_t groupby_column_index,
                                                          const std::vector<size_t>& occupied_group_ids,
                                                          size_t start_index, size_t end_index);

  template <typename ColumnDataType, WindowFunction aggregate_function>
    requires(aggregate_function == WindowFunction::Avg && std::is_arithmetic_v<ColumnDataType>)
  std::shared_ptr<AbstractSegment> _write_aggregate_segment(
      TypedAggregateVector<ColumnDataType, aggregate_function>& aggregate_vector, bool is_nullable,
      const std::vector<size_t>& occupied_group_ids, size_t start_index, size_t end_index);

  template <typename ColumnDataType, WindowFunction aggregate_function>
    requires(aggregate_function == WindowFunction::Count)
  std::shared_ptr<AbstractSegment> _write_aggregate_segment(
      TypedAggregateVector<ColumnDataType, aggregate_function>& aggregate_vector, bool is_nullable,
      const std::vector<size_t>& occupied_group_ids, size_t start_index, size_t end_index);

  template <typename ColumnDataType, WindowFunction aggregate_function>
    requires(aggregate_function == WindowFunction::CountDistinct)
  std::shared_ptr<AbstractSegment> _write_aggregate_segment(
      TypedAggregateVector<ColumnDataType, aggregate_function>& aggregate_vector, bool is_nullable,
      const std::vector<size_t>& occupied_group_ids, size_t start_index, size_t end_index);

  template <typename ColumnDataType, WindowFunction aggregate_function>
  std::shared_ptr<AbstractSegment> _write_aggregate_segment(
      TypedAggregateVector<ColumnDataType, aggregate_function>& aggregate_vector, bool is_nullable,
      const std::vector<size_t>& occupied_group_ids, size_t start_index, size_t end_index);

  template <typename ColumnDataType, WindowFunction aggregate_function>
    requires(WindowFunctionTraits<ColumnDataType, aggregate_function>::RESULT_TYPE == DataType::Null)
  std::shared_ptr<AbstractSegment> _write_aggregate_segment(
      TypedAggregateVector<ColumnDataType, aggregate_function>& aggregate_vector, bool is_nullable,
      const std::vector<size_t>& occupied_group_ids, size_t start_index, size_t end_index);

  template <typename StateType>
  GroupID _group_id(StateType& state, std::vector<RowID>& row_ids, const GroupKey& group_key,
                    WorkerState& worker_state);

  std::pair<std::vector<GroupID>, GroupID> _group_ids_for_chunk(ChunkID chunk_id, const Chunk& chunk,
                                                                WorkerState& worker_state);

  std::pair<GroupID, GroupID> _get_new_group_id_range();

  std::pair<GroupID, GroupID> _get_new_group_id_range(SingleThreadedState& state);

  std::pair<GroupID, GroupID> _get_new_group_id_range(MultiThreadedState& state);

  std::vector<size_t> _get_occupied_group_ids();

  void _aggregate_chunk(WorkerState& state, ChunkID chunk_id, const Chunk& chunk);

  template <typename ColumnDataType, WindowFunction aggregate_function>
  void _aggregate_segment(TypedAggregateVector<ColumnDataType, aggregate_function>& aggregate_vector,
                          const AbstractSegment& segment, const std::vector<GroupID>& group_ids);

  template <typename ColumnDataType, WindowFunction aggregate_function>
    requires(aggregate_function == WindowFunction::CountDistinct)
  void _aggregate_segment(TypedAggregateVector<ColumnDataType, aggregate_function>& aggregate_vector,
                          const AbstractSegment& segment, const std::vector<GroupID>& group_ids);

  template <typename ColumnDataType, WindowFunction aggregate_function>
    requires(aggregate_function == WindowFunction::Any)
  void _aggregate_segment(TypedAggregateVector<ColumnDataType, aggregate_function>& aggregate_vector,
                          const AbstractSegment& segment, const std::vector<GroupID>& group_ids);

  void _aggregate_count_star(AbstractAggregateVector& state, const std::vector<GroupID>& group_ids);

  DataType _aggregate_data_type(size_t aggregate_index);

  std::string _aggregate_column_name(size_t aggregate_index);

  bool _aggregate_is_nullable(size_t aggregate_index);

  DataType _aggregate_column_data_type(size_t aggregate_index);
};

}  // namespace hyrise
