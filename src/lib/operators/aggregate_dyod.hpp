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

// WorkerState holds thread-local state (e.g., the intermediate aggregation results of all chunks
// a worker has processed and the reserved group ID range).
class WorkerState : public Noncopyable {
 public:
  explicit WorkerState(const std::vector<std::shared_ptr<WindowFunctionExpression>>& aggregates,
                       const std::function<std::pair<GroupID, GroupID>()>& reserve_new_group_id_range);

  // Merge another worker state into this instance.
  void merge(WorkerState& other);

  // Return the next group ID. If the worker doesn’t have any reserved group IDs left, it
  // transparently reserves a new range before returning.
  GroupID next_group_id();

  AbstractAggregateVector& aggregate_vector(const size_t index);
  std::vector<std::unique_ptr<AbstractAggregateVector>>& aggregate_vectors();

 protected:
  // This is the next locally reserved group ID that a worker can assign to a group key.
  GroupID _next_group_id;

  // This is the largest locally reserved group ID that a worker can assign to a group key.
  GroupID _max_group_id;

  // A function passed during initialization of the WorkerState. This is called when there
  // are no remaining locally reserved group IDs left.
  std::function<std::pair<GroupID, GroupID>()> _reserve_new_group_id_range;

  // AggregateVectors holding the intermediate aggregation results of one worker.
  std::vector<std::unique_ptr<AbstractAggregateVector>> _vectors;
};

// Operator state for single-threaded execution. Uses (faster) non-concurrent data structures.
struct SingleThreadedState {
  explicit SingleThreadedState(const GroupID group_id_initial_cardinality = 0) {
    row_ids.reserve(group_id_initial_cardinality);
    occupied_group_ids.reserve(group_id_initial_cardinality);
  }

  boost::unordered_flat_map<GroupKey, GroupID, GroupKeyHash, GroupKeyEqual> group_id_map;
  GroupID next_group_id{0};

  // Two parallel vectors storing for each group ID the row IDs (used to construct ReferenceSegments
  // for the groupby columns) and whether a group ID is occupied.
  std::vector<RowIDs> row_ids;
  std::vector<bool> occupied_group_ids;
};

// Operator state for multi-threaded execution. Uses concurrency-safe data structures.
struct MultiThreadedState {
  explicit MultiThreadedState(const GroupID group_id_initial_cardinality = 0) {
    row_ids.reserve(group_id_initial_cardinality);
    occupied_group_ids.reserve(group_id_initial_cardinality);
  }

  tbb::concurrent_unordered_map<GroupKey, GroupID, GroupKeyHash, GroupKeyEqual> group_id_map;
  std::atomic<GroupID> next_group_id{0};

  // Two parallel vectors storing for each group ID the row IDs (used to construct ReferenceSegments
  // for the groupby columns) and whether a group ID is occupied.
  tbb::concurrent_vector<RowIDs> row_ids;
  tbb::concurrent_vector<bool> occupied_group_ids;

  // Used when resizing `row_ids` and `occupied_group_ids`
  std::mutex lock;
};

/*
 * Aggregate operator using a global hash table to aggregate concurrently. This is based on the approach described
 * by Xue and Marcus in https://doi.org/10.14778/3778092.3778110.
 *
 * Every worker, when processing a row, retrieves an integer group ID based on the row’s group key. A global hash
 * table stores a mapping from group keys to group IDs. The aggregation results are stored in a thread-local vector
 * indexed by the group ID and merged once all rows have been processed. A global atomic counter is used to store
 * the next available group ID. To construct the output, we also keep track of the RowIDs of the input table in a
 * parallel vector. To reduce contention on the atomic counter, workers reserve tickets in batches of 256. Our 
 *
 * The chunks of the input table are processed in parallel. A worker first computes the group IDs for each row in the
 * chunk, then computes the aggregates. We currently use an off-the-shelf concurrent map (tbb::concurrent_unordered_map)
 * making it relatively simple. However, the global map is the main bottleneck and performance degrades significantly
 * with the number of threads. Xue and Marcus suggest using a specialized map (a Folklore variant) that supports only
 * the GET_OR_INSERT operation required for this use case, and this would be a natural next step in optimizing the
 * implementation.
 */
class AggregateDYOD : public AbstractAggregateOperator {
 public:
  AggregateDYOD(const std::shared_ptr<AbstractOperator>& input_operator,
                const std::vector<std::shared_ptr<WindowFunctionExpression>>& aggregates,
                const std::vector<ColumnID>& groupby_column_ids);

  const std::string& name() const override;

 protected:
  // The paper uses a default step size of 256. Benchmarks for 128 and 512 didn’t improve runtime performance.
  // A higher step sizes leads to sparser aggregate vectors (but the impact of that is negligible).
  // https://github.com/danielxue/global-hash-tables-strike-back/blob/main/common/src/fuzzy_counter.rs#L56
  static constexpr GroupID FUZZY_STEP_SIZE = 256;

  // Initial cardinality of the group ID map and vectors
  // TODO(anyone): Replace with proper estimate of group cardinality based on input table.
  static constexpr GroupID GROUP_ID_INITIAL_CARDINALITY = 100'000;

  // Set before execution depending on scheduler type.
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

  std::shared_ptr<Table> _write_output_table(WorkerState& worker_state);

  // Return column definitions for the groupby and aggregate columns
  TableColumnDefinitions _output_column_definitions() const;

  // Return column definitions of the groupby columns
  TableColumnDefinitions _groupby_column_definitions() const;

  // Return column definitions of the aggregate columns
  TableColumnDefinitions _aggregate_column_definitions() const;

  // Write a chunk for the temporary data table containing only ValueSegments for all aggregates
  std::shared_ptr<Chunk> _write_aggregate_output_chunk(WorkerState& worker_state,
                                                       const std::vector<size_t>& occupied_group_ids,
                                                       const size_t start_index, const size_t end_index);

  // Write a chunk for the output reference table containing ReferenceSegments for groupby and aggregate columns.
  // The segments for groupby columns reference the input table while the segments for aggregate columns
  // reference the temporary data table.
  std::shared_ptr<Chunk> _write_reference_output_chunk(const std::shared_ptr<Table>& aggregates_table,
                                                       const ChunkID chunk_id,
                                                       const std::vector<size_t>& occupied_group_ids,
                                                       const size_t start_index, const size_t end_index);

  // Write a ReferenceSegment for the given groupby column. The segment references the input table.
  std::shared_ptr<AbstractSegment> _write_groupby_segment(const size_t groupby_column_index,
                                                          const std::vector<size_t>& occupied_group_ids,
                                                          const size_t start_index, const size_t end_index);

  // Write a ValueSegment for the given aggregate. This overload is selected for AVG aggregates.
  template <typename ColumnDataType, WindowFunction aggregate_function>
    requires(aggregate_function == WindowFunction::Avg && std::is_arithmetic_v<ColumnDataType>)
  std::shared_ptr<AbstractSegment> _write_aggregate_segment(
      TypedAggregateVector<ColumnDataType, aggregate_function>& aggregate_vector, const bool is_nullable,
      const std::vector<size_t>& occupied_group_ids, const size_t start_index, const size_t end_index);

  // Write a ValueSegment for the given aggregate. This overload is selected for COUNT aggregates.
  template <typename ColumnDataType, WindowFunction aggregate_function>
    requires(aggregate_function == WindowFunction::Count)
  std::shared_ptr<AbstractSegment> _write_aggregate_segment(
      TypedAggregateVector<ColumnDataType, aggregate_function>& aggregate_vector, const bool is_nullable,
      const std::vector<size_t>& occupied_group_ids, const size_t start_index, const size_t end_index);

  // Write a ValueSegment for the given aggregate. This overload is selected for COUNT DISTINCT aggregates.
  template <typename ColumnDataType, WindowFunction aggregate_function>
    requires(aggregate_function == WindowFunction::CountDistinct)
  std::shared_ptr<AbstractSegment> _write_aggregate_segment(
      TypedAggregateVector<ColumnDataType, aggregate_function>& aggregate_vector, const bool is_nullable,
      const std::vector<size_t>& occupied_group_ids, const size_t start_index, const size_t end_index);

  // Write a ValueSegment for the given aggregate. This overload is selected for MIN, MAX, SUM, ANY aggregates.
  template <typename ColumnDataType, WindowFunction aggregate_function>
  std::shared_ptr<AbstractSegment> _write_aggregate_segment(
      TypedAggregateVector<ColumnDataType, aggregate_function>& aggregate_vector, const bool is_nullable,
      const std::vector<size_t>& occupied_group_ids, const size_t start_index, const size_t end_index);

  // Insert the group key into the global group ID map and store the row IDs so we can access them when
  // writing the output and return the corresponding group ID. If the group key already exists in the
  // global group ID map, return the existing group ID.
  template <typename StateType>
  GroupID _group_id(StateType& state, RowIDs& row_ids, const GroupKey& group_key, WorkerState& worker_state);

  // Serialize group keys for all rows in the chunk. Returns a vector of group IDs for all rows and the
  // maximum group ID in the chunk.
  std::pair<std::vector<GroupID>, GroupID> _group_ids_for_chunk(const ChunkID chunk_id, const Chunk& chunk,
                                                                WorkerState& worker_state);

  // Reserve a new range of group IDs. Returns the inclusive start and inclusive end of the new range.
  std::pair<GroupID, GroupID> _reserve_new_group_id_range();
  static std::pair<GroupID, GroupID> _reserve_new_group_id_range(SingleThreadedState& state);
  static std::pair<GroupID, GroupID> _reserve_new_group_id_range(MultiThreadedState& state);

  // Due to fuzzy ticketing, some group IDs may have been reserved, but never used. Returns a vector
  // of occupied (i.e., used) group IDs.
  std::vector<size_t> _get_occupied_group_ids();

  void _aggregate_chunk(WorkerState& state, const ChunkID chunk_id, const Chunk& chunk);

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

  static void _aggregate_count_star(AbstractAggregateVector& aggregate_vector, const std::vector<GroupID>& group_ids);

  std::string _aggregate_column_name(const size_t aggregate_index) const;

  bool _aggregate_is_nullable(const size_t aggregate_index) const;

  DataType _aggregate_column_data_type(const size_t aggregate_index) const;
};

}  // namespace hyrise
