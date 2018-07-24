#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "resolve_type.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/job_task.hpp"
#include "storage/create_iterable_from_column.hpp"
#include "storage/dictionary_column.hpp"
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
using MaterializedColumn = std::vector<MaterializedValue<T>>;

template <typename T>
using MaterializedColumnList = std::vector<std::shared_ptr<MaterializedColumn<T>>>;

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
  std::pair<std::unique_ptr<MaterializedColumnList<T>>, std::unique_ptr<PosList>> materialize(
      std::shared_ptr<const Table> input, ColumnID column_id) {
    auto output = std::make_unique<MaterializedColumnList<T>>(input->chunk_count());
    auto null_rows = std::make_unique<PosList>();

    std::vector<std::shared_ptr<JobTask>> jobs;
    for (ChunkID chunk_id{0}; chunk_id < input->chunk_count(); ++chunk_id) {
      jobs.push_back(_create_chunk_materialization_job(output, null_rows, chunk_id, input, column_id));
      jobs.back()->schedule();
    }

    CurrentScheduler::wait_for_tasks(jobs);

    return std::make_pair(std::move(output), std::move(null_rows));
  }

 private:
  /**
   * Creates a job to materialize and sort a chunk.
   **/
  std::shared_ptr<JobTask> _create_chunk_materialization_job(std::unique_ptr<MaterializedColumnList<T>>& output,
                                                             std::unique_ptr<PosList>& null_rows_output,
                                                             ChunkID chunk_id, std::shared_ptr<const Table> input,
                                                             ColumnID column_id) {
    return std::make_shared<JobTask>([this, &output, &null_rows_output, input, column_id, chunk_id] {
      auto column = input->get_chunk(chunk_id)->get_column(column_id);
      resolve_column_type<T>(*column, [&](auto& typed_column) {
        (*output)[chunk_id] = _materialize_column(typed_column, chunk_id, null_rows_output);
      });
    });
  }

  /**
   * Materialization works of all types of columns
   */
  template <typename ColumnType>
  std::shared_ptr<MaterializedColumn<T>> _materialize_column(const ColumnType& column, ChunkID chunk_id,
                                                             std::unique_ptr<PosList>& null_rows_output) {
    auto output = MaterializedColumn<T>{};
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

    if (_sort) {
      std::sort(output.begin(), output.end(),
                [](const auto& left, const auto& right) { return left.value < right.value; });
    }

    return std::make_shared<MaterializedColumn<T>>(std::move(output));
  }

  /**
   * Specialization for dictionary columns
   */
  std::shared_ptr<MaterializedColumn<T>> _materialize_column(const DictionaryColumn<T>& column, ChunkID chunk_id,
                                                             std::unique_ptr<PosList>& null_rows_output) {
    auto output = MaterializedColumn<T>{};
    output.reserve(column.size());

    auto base_attribute_vector = column.attribute_vector();
    auto dict = column.dictionary();

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
          auto value_id = *value_id_it;

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

    return std::make_shared<MaterializedColumn<T>>(std::move(output));
  }

 private:
  bool _sort;
  bool _materialize_null;
};

}  // namespace opossum
