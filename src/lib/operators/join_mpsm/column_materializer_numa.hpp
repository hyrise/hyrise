#pragma once

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "hyrise.hpp"
#include "memory/numa_memory_resource.hpp"
#include "resolve_type.hpp"
#include "scheduler/job_task.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "storage/segment_iterate.hpp"
#include "types.hpp"

namespace opossum {

template <typename T>
struct MaterializedValueNUMA {
  MaterializedValueNUMA() = default;
  MaterializedValueNUMA(RowID row, T v) : row_id{row}, value{v} {}

  RowID row_id;
  T value;
};

template <typename T>
using MaterializedValueAllocatorNUMA = PolymorphicAllocator<MaterializedValueNUMA<T>>;

template <typename T>
using MaterializedSegmentNUMA = std::vector<MaterializedValueNUMA<T>, MaterializedValueAllocatorNUMA<T>>;

template <typename T>
struct MaterializedNUMAPartition {
  NodeID node_id;
  MaterializedValueAllocatorNUMA<T> alloc;
  std::vector<std::shared_ptr<MaterializedSegmentNUMA<T>>> materialized_segments;

  explicit MaterializedNUMAPartition(NodeID node_id, size_t reserve_size)
      : node_id{node_id},
        alloc{Hyrise::get().topology.get_memory_resource(node_id)},
        materialized_segments(reserve_size) {}

  MaterializedNUMAPartition() {}

  void shrink_to_fit() {
    materialized_segments.erase(std::remove(materialized_segments.begin(), materialized_segments.end(),
                                            std::shared_ptr<MaterializedSegmentNUMA<T>>{}),
                                materialized_segments.end());
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
    const auto node_count = Hyrise::get().topology.nodes().size();

    // ensure we have enough lists to represent the NUMA Nodes
    output->reserve(node_count);

    for (auto node_id = NodeID{0}; node_id < node_count; node_id++) {
      // The vectors only contain pointers so the higher bound estimate won't really hurt us here
      // Also we shrink this in the end
      output->emplace_back(MaterializedNUMAPartition<T>{node_id, input->chunk_count()});
    }
    auto null_rows = std::make_unique<PosList>();

    auto jobs = std::vector<std::shared_ptr<AbstractTask>>();
    const auto chunk_count = input->chunk_count();
    for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
      const auto chunk = input->get_chunk(chunk_id);
      Assert(chunk, "Did not expect deleted chunk here.");  // see #1686

      // This allocator is used to ensure that materialized chunks are colocated with the original chunks
      auto alloc = MaterializedValueAllocatorNUMA<T>{chunk->get_allocator()};

      auto numa_node_id = NodeID{0};  // default NUMA Node, everything is on the same node for non numa systems

      // Find out whether we actually are on a NUMA System, if so, remember the numa node
      auto numa_res = dynamic_cast<NUMAMemoryResource*>(alloc.resource());
      if (numa_res) {
        numa_node_id = NodeID{static_cast<uint32_t>(numa_res->get_node_id())};
      }

      jobs.push_back(_create_chunk_materialization_job(output, null_rows, chunk_id, input, column_id, numa_node_id));
      // we schedule each job on the same node as the chunk it operates on
      // this drastically minimizes reads to foreign numa nodes
      jobs.back()->schedule(numa_node_id);
    }

    Hyrise::get().scheduler()->wait_for_tasks(jobs);

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
    auto alloc = MaterializedValueAllocatorNUMA<T>{input->get_chunk(chunk_id)->get_allocator()};

    const auto segment = input->get_chunk(chunk_id)->get_segment(column_id);

    return std::make_shared<JobTask>(
        [this, &output, &null_rows_output, segment, chunk_id, alloc, numa_node_id] {
          if (const auto dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<T>>(segment)) {
            _materialize_dictionary_segment(*dictionary_segment, chunk_id, null_rows_output, (*output)[numa_node_id]);
          } else {
            _materialize_generic_segment(*segment, chunk_id, null_rows_output, (*output)[numa_node_id]);
          }
        },
        SchedulePriority::Default, false);
  }

  /**
   * Materialization works for all types of segments
   */
  void _materialize_generic_segment(const BaseSegment& segment, ChunkID chunk_id,
                                    std::unique_ptr<PosList>& null_rows_output,
                                    MaterializedNUMAPartition<T>& partition) {
    auto output = std::make_shared<MaterializedSegmentNUMA<T>>(partition.alloc);
    output->reserve(segment.size());

    segment_iterate<T>(segment, [&](const auto& position) {
      const auto row_id = RowID{chunk_id, position.chunk_offset()};
      if (position.is_null()) {
        if (_materialize_null) {
          null_rows_output->emplace_back(row_id);
        }
      } else {
        output->emplace_back(row_id, position.value());
      }
    });

    partition.materialized_segments[chunk_id] = output;
  }

  /**
   * Specialization for dictionary segments
   */
  void _materialize_dictionary_segment(const DictionarySegment<T>& segment, ChunkID chunk_id,
                                       std::unique_ptr<PosList>& null_rows_output,
                                       MaterializedNUMAPartition<T>& partition) {
    auto output = std::make_shared<MaterializedSegmentNUMA<T>>(partition.alloc);
    output->reserve(segment.size());

    auto value_ids = segment.attribute_vector();
    auto dict = segment.dictionary();

    auto iterable = create_iterable_from_segment(segment);
    iterable.for_each([&](const auto& position) {
      const auto row_id = RowID{chunk_id, position.chunk_offset()};
      if (position.is_null()) {
        if (_materialize_null) {
          null_rows_output->emplace_back(row_id);
        }
      } else {
        output->emplace_back(row_id, position.value());
      }
    });

    partition.materialized_segments[chunk_id] = output;
  }

 private:
  bool _materialize_null;
};

}  // namespace opossum
