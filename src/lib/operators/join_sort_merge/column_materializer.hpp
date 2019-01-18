#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "resolve_type.hpp"
#include "scheduler/current_scheduler.hpp"
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
using MaterializedSegmentList = std::vector<std::shared_ptr<MaterializedSegment<T>>>;

template <typename T>
struct SampleRequest {
  SampleRequest(uint16_t sample_count) : samples_to_collect(sample_count) { }

  const uint16_t samples_to_collect;
  std::vector<T> collected_samples;
};

/**
 * Materializes a table for a specific segment and sorts it if required. Row-Ids are kept in order to enable
 * the construction of pos lists for the algorithms that are using this class.
 TODO: sampling explain
 **/
template <typename T>
class ColumnMaterializer {
 public:
  explicit ColumnMaterializer(bool sort, bool materialize_null) : _sort{sort}, _materialize_null{materialize_null} {}

 public:
  /**
   * Materializes and sorts all the chunks of an input table in parallel
   * by creating multiple jobs that materialize chunks.
   * Returns the materialized segments and a list of null row ids if materialize_null is enabled.
   **/
  std::tuple<std::unique_ptr<MaterializedSegmentList<T>>, std::unique_ptr<PosList>, std::vector<T>> materialize(
      const std::shared_ptr<const Table> input, const ColumnID column_id) {
    const uint32_t samples_per_chunk = 10;
    const auto chunk_count = input->chunk_count();

    auto output = std::make_unique<MaterializedSegmentList<T>>(chunk_count);
    auto null_rows = std::make_unique<PosList>();

    std::vector<SampleRequest<T>> sample_requests;

    std::vector<std::shared_ptr<AbstractTask>> jobs;
    for (ChunkID chunk_id{0}; chunk_id < chunk_count; ++chunk_id) {
      const uint16_t samples_to_write =
          static_cast<uint16_t>(std::min(samples_per_chunk, input->get_chunk(chunk_id)->size()));
      sample_requests.push_back(SampleRequest<T>(samples_to_write));
      jobs.push_back(_create_chunk_materialization_job(output, null_rows, chunk_id, input, column_id, sample_requests.back()));
      jobs.back()->schedule();
    }

    auto samples = std::vector<T>();
    samples.reserve(samples_per_chunk * chunk_count);

    for (const auto& sample_request : sample_requests) {
      samples.insert(samples.end(), sample_request.collected_samples.begin(), sample_request.collected_samples.end());
    }

    CurrentScheduler::wait_for_tasks(jobs);

    return {std::move(output), std::move(null_rows), std::move(samples)};
  }

 private:
  /**
   * Creates a job to materialize and sort a chunk.
   **/
  std::shared_ptr<AbstractTask> _create_chunk_materialization_job(
      std::unique_ptr<MaterializedSegmentList<T>>& output, std::unique_ptr<PosList>& null_rows_output,
      const ChunkID chunk_id, std::shared_ptr<const Table> input, const ColumnID column_id, SampleRequest<T>& sample_request) {
    return std::make_shared<JobTask>([this, &output, &null_rows_output, input, column_id, chunk_id, &sample_request] {
      auto segment = input->get_chunk(chunk_id)->get_segment(column_id);

      if (const auto dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<T>>(segment)) {
        (*output)[chunk_id] = _materialize_dictionary_segment(*dictionary_segment, chunk_id, null_rows_output, sample_request);
      } else {
        (*output)[chunk_id] = _materialize_generic_segment(*segment, chunk_id, null_rows_output, sample_request);
      }
    });
  }

  /**
   * Materialization works of all types of segments
   */
  std::shared_ptr<MaterializedSegment<T>> _materialize_generic_segment(const BaseSegment& segment, ChunkID chunk_id,
                                                                       std::unique_ptr<PosList>& null_rows_output,
                                                                       SampleRequest<T>& sample_request) {
    auto output = MaterializedSegment<T>{};
    output.reserve(segment.size());

    segment_iterate<T>(segment, [&](const auto& position) {
      const auto row_id = RowID{chunk_id, position.chunk_offset()};
      if (position.is_null()) {
        if (_materialize_null) {
          null_rows_output->emplace_back(row_id);
        }
      } else {
        output.emplace_back(row_id, position.value());
      }
    });

    if (_sort) {
      std::sort(output.begin(), output.end(),
                [](const auto& left, const auto& right) { return left.value < right.value; });
    }

    const auto jump_width = (output.size() - 1) / std::max(1, sample_request.samples_to_collect - 1);
    for (auto sample_count = 0; sample_count < sample_request.samples_to_collect; ++sample_count) {
      // output vector does not contain NULL values, thus we do not have to check for NULL samples.
      sample_request.collected_samples.push_back(output[static_cast<size_t>(sample_count * jump_width)].value);
    }
    // for (auto v = sample_offset; v < sample_offset + samples_to_write; ++v) {
    //   std::cout << "inner print: " << samples[v] << std::endl;
    // }
    // std::cout << "====" << std::endl;

    return std::make_shared<MaterializedSegment<T>>(std::move(output));
  }

  /**
   * Specialization for dictionary segments
   */
  std::shared_ptr<MaterializedSegment<T>> _materialize_dictionary_segment(const DictionarySegment<T>& segment, ChunkID chunk_id,
                                                                          std::unique_ptr<PosList>& null_rows_output,
                                                                          SampleRequest<T>& sample_request) {
    auto output = MaterializedSegment<T>{};
    output.reserve(segment.size());

    auto base_attribute_vector = segment.attribute_vector();
    auto dict = segment.dictionary();

    if (_sort) {
      // Works like Bucket Sort
      // Collect for every value id, the set of rows that this value appeared in
      // value_count is used as an inverted index
      auto rows_with_value = std::vector<std::vector<RowID>>(dict->size());

      // Reserve correct size of the vectors by assuming a uniform distribution
      for (auto& row : rows_with_value) {
        row.reserve(base_attribute_vector->size() / dict->size());
      }

      // Collect the rows for each value id
      resolve_compressed_vector_type(*base_attribute_vector, [&](const auto& attribute_vector) {
        auto chunk_offset = ChunkOffset{0u};
        auto value_id_it = attribute_vector.cbegin();
        for (; value_id_it != attribute_vector.cend(); ++value_id_it, ++chunk_offset) {
          auto value_id = static_cast<ValueID>(*value_id_it);

          if (value_id != NULL_VALUE_ID) {
            rows_with_value[value_id].push_back(RowID{chunk_id, chunk_offset});
          } else {
            if (_materialize_null) {
              null_rows_output->push_back(RowID{chunk_id, chunk_offset});
            }
          }
        }
      });

      // Now that we know the row ids for every value, we can output all the materialized values in a sorted manner.
      ChunkOffset chunk_offset{0};
      for (ValueID value_id{0}; value_id < dict->size(); ++value_id) {
        for (auto& row_id : rows_with_value[value_id]) {
          output.emplace_back(row_id, (*dict)[value_id]);
          ++chunk_offset;
        }
      }
    } else {
      auto iterable = create_iterable_from_segment(segment);
      iterable.for_each([&](const auto& position) {
        const auto row_id = RowID{chunk_id, position.chunk_offset()};
        if (position.is_null()) {
          if (_materialize_null) {
            null_rows_output->emplace_back(row_id);
          }
        } else {
          output.emplace_back(row_id, position.value());
        }
      });
    }

    const auto jump_width = (output.size() - 1) / std::max(1, sample_request.samples_to_collect - 1);
    for (auto sample_count = 0; sample_count < sample_request.samples_to_collect; ++sample_count) {
      // output vector does not contain NULL values, thus we do not have to check for NULL samples.
      sample_request.collected_samples.push_back(output[static_cast<size_t>(sample_count * jump_width)].value);
    }

    return std::make_shared<MaterializedSegment<T>>(std::move(output));
  }

 private:
  bool _sort;
  bool _materialize_null;
};

}  // namespace opossum
