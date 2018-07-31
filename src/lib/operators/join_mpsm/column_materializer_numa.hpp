#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "resolve_type.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/job_task.hpp"
#include "scheduler/topology.hpp"
#include "storage/create_iterable_from_column.hpp"
#include "types.hpp"
#include "utils/numa_memory_resource.hpp"

namespace opossum {

template <typename T>
struct MaterializedValue {
  MaterializedValue() = default;
  MaterializedValue(RowID row, T v) : row_id{row}, value{v} {}

  RowID row_id;
  T value;
};

template <typename T>
using MaterializedValueAllocator = PolymorphicAllocator<MaterializedValue<T>>;

template <typename T>
using MaterializedChunk = std::vector<MaterializedValue<T>, MaterializedValueAllocator<T>>;

template <typename T>
struct MaterializedNUMAPartition {
  NodeID _node_id;
  MaterializedValueAllocator<T> _alloc;
  std::vector<std::shared_ptr<MaterializedChunk<T>>> _chunk_columns;

  explicit MaterializedNUMAPartition(NodeID node_id, size_t reserve_size)
      : _node_id{node_id}, _alloc{Topology::get().get_memory_resource(node_id)}, _chunk_columns(reserve_size) {}

  MaterializedNUMAPartition() {}

  void shrink_to_fit() {
    _chunk_columns.erase(
        std::remove(_chunk_columns.begin(), _chunk_columns.end(), std::shared_ptr<MaterializedChunk<T>>{}),
        _chunk_columns.end());
  }
};

template <typename T>
using MaterializedNUMAPartitionList = std::vector<MaterializedNUMAPartition<T>>;

/**
 * Materializes a table for a specific column and sorts it if required. Row-Ids are kept in order to enable
 * the construction of pos lists for the algorithms that are using this class.
 **/
template <typename T>
class ColumnMaterializerNUMA {
 public:
  explicit ColumnMaterializerNUMA(bool materialize_null) : _materialize_null{materialize_null} {}

 public:
  /**
   * Materializes and sorts all the chunks of an input table in parallel
   * by creating multiple jobs that materialize chunks.
   * Returns the materialized columns and a list of null row ids if materialize_null is enabled.
   **/
  std::pair<std::unique_ptr<MaterializedNUMAPartitionList<T>>, std::unique_ptr<PosList>> materialize(
      std::shared_ptr<const Table> input, ColumnID column_id) {
    auto output = std::make_unique<MaterializedNUMAPartitionList<T>>();
    const auto node_count = Topology::get().nodes().size();

    // ensure we have enough lists to represent the NUMA Nodes
    output->reserve(node_count);

    for (auto node_id = NodeID{0}; node_id < node_count; node_id++) {
      // The vectors only contain pointers so the higher bound estimate won't really hurt us here
      // Also we shrink this in the end
      output->emplace_back(MaterializedNUMAPartition<T>{node_id, input->chunk_count()});
    }
    auto null_rows = std::make_unique<PosList>();

    auto jobs = std::vector<std::shared_ptr<AbstractTask>>();
    for (auto chunk_id = ChunkID{0}; chunk_id < input->chunk_count(); ++chunk_id) {
      // This allocator is used to ensure that materialized chunks are colocated with the original chunks
      auto alloc = MaterializedValueAllocator<T>{input->get_chunk(chunk_id)->get_allocator()};

      auto numa_node_id = NodeID{0};  // default NUMA Node, everything is on the same node for non numa systems

      // Find out whether we actually are on a NUMA System, if so, remember the numa node
      auto numa_res = dynamic_cast<NUMAMemoryResource*>(alloc.resource());
      if (numa_res != nullptr) {
        numa_node_id = NodeID{static_cast<uint32_t>(numa_res->get_node_id())};
      }

      jobs.push_back(_create_chunk_materialization_job(output, null_rows, chunk_id, input, column_id, numa_node_id));
      // we schedule each job on the same node as the chunk it operates on
      // this drastically minimizes reads to foreign numa nodes
      jobs.back()->schedule(numa_node_id);
    }

    CurrentScheduler::wait_for_tasks(jobs);

    for (auto& partition : (*output)) {
      // removes null pointers, this is important since we currently opt against using mutexes so we have sparse vectors
      partition.shrink_to_fit();
    }

    return std::make_pair(std::move(output), std::move(null_rows));
  }

 private:
  /**
   * Creates a job to materialize and sort a chunk.
   **/
  std::shared_ptr<AbstractTask> _create_chunk_materialization_job(
      std::unique_ptr<MaterializedNUMAPartitionList<T>>& output, std::unique_ptr<PosList>& null_rows_output,
      ChunkID chunk_id, std::shared_ptr<const Table> input, ColumnID column_id, NodeID numa_node_id) {
    // This allocator ensures that materialized values are colocated with the actual values.
    auto alloc = MaterializedValueAllocator<T>{input->get_chunk(chunk_id)->get_allocator()};

    const auto column = input->get_chunk(chunk_id)->get_column(column_id);

    return std::make_shared<JobTask>(
        [this, &output, &null_rows_output, column, chunk_id, alloc, numa_node_id] {
          resolve_column_type<T>(*column, [&](auto& typed_column) {
            _materialize_column(typed_column, chunk_id, null_rows_output, (*output)[numa_node_id]);
          });
        },
        false);
  }

  /**
   * Materialization works for all types of columns
   */
  template <typename ColumnType>
  void _materialize_column(const ColumnType& column, ChunkID chunk_id, std::unique_ptr<PosList>& null_rows_output,
                           MaterializedNUMAPartition<T>& partition) {
    auto output = std::make_shared<MaterializedChunk<T>>(partition._alloc);

    output->reserve(column.size());

    auto iterable = create_iterable_from_column<T>(column);

    iterable.for_each([&](const auto& column_value) {
      const auto row_id = RowID{chunk_id, column_value.chunk_offset()};
      if (column_value.is_null()) {
        if (_materialize_null) {
          null_rows_output->emplace_back(row_id);
        }
      } else {
        output->emplace_back(row_id, column_value.value());
      }
    });

    partition._chunk_columns[chunk_id] = output;
  }

  /**
   * Specialization for dictionary columns
   */
  std::shared_ptr<MaterializedChunk<T>> _materialize_column(const DictionaryColumn<T>& column, ChunkID chunk_id,
                                                            std::unique_ptr<PosList>& null_rows_output,
                                                            MaterializedValueAllocator<T> alloc) {
    auto output = MaterializedChunk<T>{alloc};
    output.reserve(column.size());

    auto value_ids = column.attribute_vector();
    auto dict = column.dictionary();

    auto iterable = create_iterable_from_column(column);
    iterable.for_each([&](const auto& column_value) {
      const auto row_id = RowID{chunk_id, column_value.chunk_offset()};
      if (column_value.is_null()) {
        if (_materialize_null) {
          null_rows_output->emplace_back(row_id);
        }
      } else {
        output.emplace_back(row_id, column_value.value());
      }
    });

    return std::make_shared<MaterializedChunk<T>>(std::move(output));
  }

 private:
  bool _materialize_null;
};

}  // namespace opossum
