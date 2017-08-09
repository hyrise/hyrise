#include <algorithm>
#include <memory>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include "resolve_type.hpp"

namespace opossum {

template <typename T>
class TableMaterializer : public ColumnVisitable {
 public:
  explicit TableMaterializer(bool sort) : _sort{sort} {};

 protected:
  bool _sort;
  /**
  * Context for the visitor pattern implementation for column materialization and sorting.
  **/
  struct MaterializationContext : ColumnVisitableContext {
    explicit MaterializationContext(ChunkID id) : chunk_id(id) {}

    // The id of the chunk to be materialized
    ChunkID chunk_id;
    std::shared_ptr<MaterializedChunk<T>> output;
  };

  /**
  * Materializes and sorts an input chunk by the specified column in ascending order.
  **/
  std::shared_ptr<MaterializedChunk<T>> _materialize_chunk(const Chunk& chunk, ChunkID chunk_id, ColumnID column_id) {
    auto column = chunk.get_column(column_id);
    auto context = std::make_shared<MaterializationContext>(chunk_id);
    column->visit(*this, context);
    return context->output;
  }

  /**
  * Creates a job to sort multiple chunks.
  **/
  std::shared_ptr<JobTask> _materialize_chunks_job(std::shared_ptr<MaterializedTable<T>> output,
                            std::vector<ChunkID> chunks, std::shared_ptr<const Table> input, std::string column_name) {
    return std::make_shared<JobTask>([=] {
      for (auto chunk_id : chunks) {
        output->at(chunk_id) = _materialize_chunk(input->get_chunk(chunk_id),
                                                  chunk_id, input->column_id_by_name(column_name));
      }
    });
  }

  /**
  * ColumnVisitable implementation to materialize and sort a value column.
  **/
  void handle_value_column(BaseColumn& column, std::shared_ptr<ColumnVisitableContext> context) override {
    auto& value_column = static_cast<ValueColumn<T>&>(column);
    auto materialization_context = std::static_pointer_cast<MaterializationContext>(context);
    auto output = std::make_shared<MaterializedChunk<T>>(value_column.values().size());

    // Copy over every entry
    for (ChunkOffset chunk_offset{0}; chunk_offset < value_column.values().size(); chunk_offset++) {
      RowID row_id{materialization_context->chunk_id, chunk_offset};
      output->at(chunk_offset) = MaterializedValue<T>(row_id, value_column.values()[chunk_offset]);
    }

    // Sort the entries
    if(_sort) {
      std::sort(output->begin(), output->end(), [](auto& left, auto& right) { return left.value < right.value; });
    }

    materialization_context->output = output;
  }

  /**
  * ColumnVisitable implementaion to materialize and sort a dictionary column.
  **/
  void handle_dictionary_column(BaseColumn& column, std::shared_ptr<ColumnVisitableContext> context) override {
    auto& dictionary_column = dynamic_cast<DictionaryColumn<T>&>(column);
    auto materialization_context = std::static_pointer_cast<MaterializationContext>(context);
    auto output = std::make_shared<MaterializedChunk<T>>(column.size());

    auto value_ids = dictionary_column.attribute_vector();
    auto dict = dictionary_column.dictionary();

    // Collect for every value id, the set of rows that this value appeared in
    // value_count is used as an inverted index
    auto rows_with_value = std::vector<std::vector<RowID>>(dict->size());
    for (ChunkOffset chunk_offset{0}; chunk_offset < value_ids->size(); chunk_offset++) {
      rows_with_value[value_ids->get(chunk_offset)].push_back(RowID{materialization_context->chunk_id, chunk_offset});
    }

    // Now that we know the row ids for every value, we can output all the materialized values in a sorted manner.
    ChunkOffset chunk_offset{0};
    for (ValueID value_id{0}; value_id < dict->size(); value_id++) {
      for (auto& row_id : rows_with_value[value_id]) {
        output->at(chunk_offset) = MaterializedValue<T>(row_id, dict->at(value_id));
        chunk_offset++;
      }
    }

    // The result is already sorted because the dictionaries are sorted.
    // Therefore, no additional sorting is required.

    materialization_context->output = output;
  }

  /**
  * Sorts the contents of a reference column into a sorted chunk
  **/
  void handle_reference_column(ReferenceColumn& ref_column, std::shared_ptr<ColumnVisitableContext> context) override {
    auto referenced_table = ref_column.referenced_table();
    auto referenced_column_id = ref_column.referenced_column_id();
    auto materialization_context = std::static_pointer_cast<MaterializationContext>(context);
    auto pos_list = ref_column.pos_list();
    auto output = std::make_shared<MaterializedChunk<T>>(ref_column.size());

    // Retrieve the columns from the referenced table so they only have to be casted once
    auto v_columns = std::vector<std::shared_ptr<ValueColumn<T>>>(referenced_table->chunk_count());
    auto d_columns = std::vector<std::shared_ptr<DictionaryColumn<T>>>(referenced_table->chunk_count());
    for (ChunkID chunk_id{0}; chunk_id < referenced_table->chunk_count(); chunk_id++) {
      v_columns[chunk_id] = std::dynamic_pointer_cast<ValueColumn<T>>(
      referenced_table->get_chunk(chunk_id).get_column(referenced_column_id));
      d_columns[chunk_id] = std::dynamic_pointer_cast<DictionaryColumn<T>>(
      referenced_table->get_chunk(chunk_id).get_column(referenced_column_id));
    }

    // Retrieve the values from the referenced columns
    for (ChunkOffset chunk_offset{0}; chunk_offset < pos_list->size(); chunk_offset++) {
      const auto& row_id = pos_list->at(chunk_offset);

      // Dereference the value
      T value;
      auto& v_column = v_columns[row_id.chunk_id];
      auto& d_column = d_columns[row_id.chunk_id];
      DebugAssert(v_column || d_column, "Referenced column is neither value nor dictionary column!");
      if (v_column) {
        value = v_column->values()[row_id.chunk_offset];
      } else {
        ValueID value_id = d_column->attribute_vector()->get(row_id.chunk_offset);
        value = d_column->dictionary()->at(value_id);
      }
      output->at(chunk_offset) = MaterializedValue<T>(RowID{materialization_context->chunk_id, chunk_offset}, value);
    }

    // Sort the entries
    if(_sort) {
      std::sort(output->begin(), output->end(), [](auto& left, auto& right) { return left.value < right.value; });
    }

    materialization_context->output = output;
  }

public:
  /**
  * Materializes and sorts all the chunks of an input table in parallel
  * by creating multiple jobs that sort multiple chunks.
  **/
  std::shared_ptr<MaterializedTable<T>> materialize(std::shared_ptr<const Table> input, std::string column) {
    auto output = std::make_shared<MaterializedTable<T>>(input->chunk_count());

    // Can be extended to find that value dynamically later on (depending on hardware etc.)
    const size_t job_size_threshold = 10000;
    std::vector<std::shared_ptr<AbstractTask>> jobs;

    size_t job_size = 0;
    std::vector<ChunkID> chunk_ids;
    for (ChunkID chunk_id{0}; chunk_id < input->chunk_count(); chunk_id++) {
      job_size += input->get_chunk(chunk_id).size();
      chunk_ids.push_back(chunk_id);
      if (job_size > job_size_threshold || chunk_id == input->chunk_count() - 1) {
        jobs.push_back(_materialize_chunks_job(output, chunk_ids, input, column));
        jobs.back()->schedule();
        job_size = 0;
        chunk_ids.clear();
      }
    }

    CurrentScheduler::wait_for_tasks(jobs);

    return output;
  }
};

}  // namespace opossum
