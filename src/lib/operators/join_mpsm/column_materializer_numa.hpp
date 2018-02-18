#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "resolve_type.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/job_task.hpp"
#include "storage/base_attribute_vector.hpp"
#include "storage/iterables/attribute_vector_iterable.hpp"
#include "storage/iterables/create_iterable_from_column.hpp"
#include "types.hpp"
#include "utils/numa_memory_resource.hpp"
#include "storage/numa_placement_manager.hpp"

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
struct MaterializedNUMAPartition{
  std::vector<std::shared_ptr<MaterializedChunk<T>>> _chunk_columns;
  NodeID _node_id;

  explicit MaterializedNUMAPartition(NodeID node_id, size_t reserve_size) : _node_id{node_id} {
    _chunk_columns.resize(reserve_size);
  }

  void fit(){
      _chunk_columns.erase(std::remove(_chunk_columns.begin(), _chunk_columns.end(), nullptr));
  }
};

/*template <typename T>
using MaterializedColumn = std::vector<MaterializedValue<T>, MaterializedValueAllocator<T>>;*/

template <typename T>
using MaterializedNUMAPartitionList = std::vector<MaterializedNUMAPartition<T>>;



/**
 * Materializes a table for a specific column and sorts it if required. Row-Ids are kept in order to enable
 * the construction of pos lists for the algorithms that are using this class.
 **/
template <typename T>
class ColumnMaterializer {
 public:
  explicit ColumnMaterializer(bool sort, bool materialize_null) : _sort{sort}, _materialize_null{materialize_null} {}

 public:
  /**
   * Materializes and sorts all the chunks of an input table in parallel
   * by creating multiple jobs that materialize chunks.
   * Returns the materialized columns and a list of null row ids if materialize_null is enabled.
   **/
  std::pair<std::unique_ptr<MaterializedNUMAPartitionList<T>>, std::unique_ptr<PosList>> materialize(
      std::shared_ptr<const Table> input, ColumnID column_id) {
    auto output = std::make_unique<MaterializedNUMAPartitionList<T>>();
    // ensure we have enough lists to represent the NUMA Nodes
    const auto topology = NUMAPlacementManager::get().topology();
    output->reserve(topology->nodes().size());

    for(NodeID node_id{0}; node_id < topology->nodes().size(); node_id++){
        // The vectors only contain pointers so the higher bound estimate won't really hurt us here
        // Also we shrink this in the end
        output->emplace_back(MaterializedNUMAPartition<T>{node_id, input->row_count()});
    }
    auto null_rows = std::make_unique<PosList>();

    std::vector<std::shared_ptr<AbstractTask>> jobs;
    for (ChunkID chunk_id{0}; chunk_id < input->chunk_count(); ++chunk_id) {

        // This allocator is used to ensure that materialized chunks are colocated with the original chunks
        MaterializedValueAllocator<T> alloc{input->get_chunk(chunk_id).get_allocator()};

        NodeID numa_node_id{0}; // default NUMA Node, everything is on the same node for non numa systems

        // Find out whether we actually are on a NUMA System, if so, remember the numa node
        auto numa_res = dynamic_cast<NUMAMemoryResource *>(alloc.resource());
        if(numa_res != nullptr){
            // TODO(florian): NodeID vs int for node adressing
            numa_node_id = NodeID{static_cast<uint32_t>(numa_res->get_node_id())};
        }

      jobs.push_back(_create_chunk_materialization_job(output, null_rows, chunk_id, input, column_id, numa_node_id));
      // we schedule each job on the same node as the chunk it operates on
      // this drastically minimizes reads to foreign numa nodes
      jobs.back()->schedule(numa_node_id, SchedulePriority::Unstealable);
    }

    CurrentScheduler::wait_for_tasks(jobs);

    for(auto& partition : (*output)){
        // removes null pointers, this is important since we currently opt against using mutexes so we have sparse vectors
        partition.fit();
    }

    return std::make_pair(std::move(output), std::move(null_rows));
  }

 private:
  /**
   * Creates a job to materialize and sort a chunk.
   **/
  std::shared_ptr<JobTask> _create_chunk_materialization_job(std::unique_ptr<MaterializedNUMAPartitionList<T>>& output,
                                                             std::unique_ptr<PosList>& null_rows_output,
                                                             ChunkID chunk_id, std::shared_ptr<const Table> input,
                                                             ColumnID column_id, NodeID numa_node_id) {

    // TODO(florian): pass this in from the outside
    // This allocator ensures that materialized values are colocated with the actual values.
    // This colocation is important on NUMA systems, since there it is important to have control over the location of memory
    // This introduces runtime overhead that is not amortized for non-NUMA systems, so maybe this should be a compile time switch.
    MaterializedValueAllocator<T> alloc{input->get_chunk(chunk_id).get_allocator()};

    return std::make_shared<JobTask>([this, &output, &null_rows_output, input, column_id, chunk_id, alloc, numa_node_id] {
      auto column = input->get_chunk(chunk_id).get_column(column_id);
      resolve_column_type<T>(*column, [&](auto& typed_column) {
        // TODO: think about how to write the chunks to multiple
        (*output)[numa_node_id]._chunk_columns[chunk_id] = _materialize_column(typed_column, chunk_id, null_rows_output, alloc);
      });
    });
  }

  /**
   * Materialization works of all types of columns
   */
  template <typename ColumnType>
  std::shared_ptr<MaterializedChunk<T>> _materialize_column(const ColumnType& column, ChunkID chunk_id,
                                                             std::unique_ptr<PosList>& null_rows_output,
                                                             MaterializedValueAllocator<T> alloc) {

    // TODO(florian): think long and hard about allocator lifetimes
    auto output = MaterializedChunk<T>(alloc);
    output.reserve(column.size());

    auto iterable = create_iterable_from_column<T>(column);

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

    // TODO(florian): think about whether this sorting makes sense for the NUMA case
    // probably this is a good presorting for the merge in the radix phase
    if (_sort) {
      std::sort(output.begin(), output.end(),
                [](const auto& left, const auto& right) { return left.value < right.value; });
    }

    return std::make_shared<MaterializedChunk<T>>(std::move(output));
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

    if (_sort) {
      // Works like Bucket Sort
      // Collect for every value id, the set of rows that this value appeared in
      // value_count is used as an inverted index
      // TODO(florian): think about NUMA implications, probably make this NUMA aware
      //    Since the jobs are scheduled on Nodes already we do not expect this to be problematic
      auto rows_with_value = std::vector<std::vector<RowID>>(dict->size());

      // Reserve correct size of the vectors by assuming a uniform distribution
      for (auto& row : rows_with_value) {
        row.reserve(value_ids->size() / dict->size());
      }

      // Collect the rows for each value id
      for (ChunkOffset chunk_offset{0}; chunk_offset < value_ids->size(); ++chunk_offset) {
        auto value_id = value_ids->get(chunk_offset);

        if (value_id != NULL_VALUE_ID) {
          rows_with_value[value_id].push_back(RowID{chunk_id, chunk_offset});
        } else {
          if (_materialize_null) {
            null_rows_output->emplace_back(RowID{chunk_id, chunk_offset});
          }
        }
      }

      // Now that we know the row ids for every value, we can output all the materialized values in a sorted manner.
      ChunkOffset chunk_offset{0};
      for (ValueID value_id{0}; value_id < dict->size(); ++value_id) {
        for (auto& row_id : rows_with_value[value_id]) {
          output.emplace_back(row_id, (*dict)[value_id]);
          ++chunk_offset;
        }
      }
    } else {
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
    }

    return std::make_shared<MaterializedChunk<T>>(std::move(output));
  }

 private:
  bool _sort;
  bool _materialize_null;
};

}  // namespace opossum
