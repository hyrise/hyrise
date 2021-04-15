#pragma once

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <boost/sort/sort.hpp>

#include "hyrise.hpp"
#include "resolve_type.hpp"
#include "scheduler/job_task.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/vector_compression/resolve_compressed_vector_type.hpp"
#include "types.hpp"

namespace opossum {

template <typename T>
struct MaterializedValue {
  MaterializedValue() = default;
  MaterializedValue(RowID row, T v) : row_id{row}, value{v} {}

  RowID row_id;
  T value;
};

template <typename T>
using MaterializedSegment = std::vector<MaterializedValue<T>>;

template <typename T>
using MaterializedSegmentList = std::vector<MaterializedSegment<T>>;

// This data structure is passed as a reference to the jobs which materialize the chunks. Each job then adds
// `samples_to_collect` samples to its passed SampleRequest. All SampleRequests are later merged to gather a global
// sample list with which the split values for the radix partitioning are determined.
template <typename T>
struct Subsample {
  explicit Subsample(ChunkOffset sample_count) : samples_to_collect(sample_count), samples(sample_count) {}
  const ChunkOffset samples_to_collect;
  std::vector<T> samples;
};

// Materializes a column and sorts it if requested. Result is a triple of materialized values, positions of NULL
// values, and a list of samples.
template <typename T>
class ColumnMaterializer {
 public:
  explicit ColumnMaterializer(bool sort, bool materialize_null) : _sort{sort}, _materialize_null{materialize_null} {}

 public:
  // Materializes and sorts (if requested) all the chunks of an input table. For sufficiently large chunks, the
  // materialization is parallelized. Returns the materialized segments and a list of null row ids if _materialize_null
  // is true.
  std::tuple<MaterializedSegmentList<T>, RowIDPosList, std::vector<T>> materialize(
      const std::shared_ptr<const Table>& input, const ColumnID column_id) {
    constexpr ChunkOffset SAMPLES_PER_CHUNK = 10;  // rather arbitrarily chosen number
    const auto chunk_count = input->chunk_count();

    auto output = MaterializedSegmentList<T>(chunk_count);

    auto null_rows_per_chunk = std::vector<RowIDPosList>(chunk_count);
    auto subsamples = std::vector<Subsample<T>>{};
    subsamples.reserve(chunk_count);

    auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
    for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
      const auto& chunk = input->get_chunk(chunk_id);
      Assert(chunk, "Physically deleted chunk should not reach this point, see get_chunk / #1686.");

      const auto samples_to_write = std::min(SAMPLES_PER_CHUNK, chunk->size());
      subsamples.push_back(Subsample<T>(samples_to_write));

      auto materialize_job = [&, chunk_id] {
        const auto& segment = input->get_chunk(chunk_id)->get_segment(column_id);
        output[chunk_id] = _materialize_segment(segment, chunk_id, null_rows_per_chunk[chunk_id], subsamples[chunk_id]);
      };

      if (chunk->size() > 500) {
        jobs.push_back(std::make_shared<JobTask>(materialize_job));
      } else {
        materialize_job();
      }
    }

    Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);

    auto gathered_samples = std::vector<T>();
    gathered_samples.reserve(SAMPLES_PER_CHUNK * chunk_count);
    auto null_rows = RowIDPosList{};
    for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
      const auto& subsample = subsamples[chunk_id];
      gathered_samples.insert(gathered_samples.end(), subsample.samples.begin(), subsample.samples.end());

      const auto& chunk_null_rows = null_rows_per_chunk[chunk_id];
      null_rows.insert(null_rows.end(), chunk_null_rows.begin(), chunk_null_rows.end());
    }

    return {std::move(output), std::move(null_rows), std::move(gathered_samples)};
  }

 private:
  // Sample values from a materialized segment. We collect samples locally and write once to the global sample
  // collection to limit non-local writes.
  void _gather_samples_from_segment(const MaterializedSegment<T>& segment, Subsample<T>& subsample) const {
    const auto samples_to_collect = subsample.samples_to_collect;
    auto collected_samples = std::vector<T>{};
    collected_samples.reserve(samples_to_collect);

    if (segment.size() > 0 && samples_to_collect > 0) {
      const auto step_width = segment.size() / std::max(1u, samples_to_collect);

      for (auto sample_count = size_t{0}; sample_count < samples_to_collect; ++sample_count) {
        // NULL values in passed `segment` vector have already been
        // removed, thus we do not have to check for NULL samples.
        collected_samples.push_back(segment[static_cast<size_t>(sample_count * step_width)].value);
      }

      // Sequential write of result
      subsample.samples.insert(subsample.samples.end(), collected_samples.begin(), collected_samples.end());
    }
  }

  MaterializedSegment<T> _materialize_segment(const std::shared_ptr<AbstractSegment>& segment, const ChunkID chunk_id,
                                              RowIDPosList& null_rows_output, Subsample<T>& subsample) {
    auto output = MaterializedSegment<T>{};
    output.reserve(segment->size());

    segment_iterate<T>(*segment, [&](const auto& position) {
      const auto row_id = RowID{chunk_id, position.chunk_offset()};
      if (position.is_null()) {
        if (_materialize_null) {
          null_rows_output.emplace_back(row_id);
        }
      } else {
        output.emplace_back(row_id, position.value());
      }
    });

    if (_sort) {
      boost::sort::pdqsort(output.begin(), output.end(),
                           [](const auto& left, const auto& right) { return left.value < right.value; });
    }

    _gather_samples_from_segment(output, subsample);

    return output;
  }

 private:
  bool _sort;
  bool _materialize_null;
};

}  // namespace opossum
