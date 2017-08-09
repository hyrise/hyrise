#include <algorithm>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include "resolve_type.hpp"

namespace opossum {

template <typename T>
class RadixPartitionSort : public ColumnVisitable {
 public:
  RadixPartitionSort(const std::shared_ptr<const AbstractOperator> left,
                        const std::shared_ptr<const AbstractOperator> right,
                        std::pair<std::string, std::string> column_names, bool equi_case, size_t partition_count)
    : _left_column_name{column_names.first}, _right_column_name{column_names.second}, _equi_case{equi_case},
      _partition_count{partition_count} {
    DebugAssert(partition_count > 0, "partition_count must be > 0");
    DebugAssert(left != nullptr, "left input operator is null");
    DebugAssert(right != nullptr, "right input operator is null");

    _input_table_left = left->get_output();
    _input_table_right = right->get_output();

    // Check column_type
    const auto left_column_id = _input_table_left->column_id_by_name(_left_column_name);
    const auto right_column_id = _input_table_right->column_id_by_name(_right_column_name);
    const auto& left_column_type = _input_table_left->column_type(left_column_id);
    const auto& right_column_type = _input_table_right->column_type(right_column_id);

    DebugAssert(left_column_type == right_column_type, "left and right column types do not match");
  }

  virtual ~RadixPartitionSort() = default;

 protected:
  /**
  * The ChunkStatistics structure is used to gather statistics regarding a chunks values in order to
  * be able to pick appropriate value ranges for the partitions.
  **/
  struct ChunkStatistics {
    ChunkStatistics(size_t partition_count) {
      partition_histogram.resize(partition_count);
      insert_position.resize(partition_count);
    }
    // Used to count the number of entries for each partition from this chunk
    std::vector<size_t> partition_histogram;
    std::vector<size_t> insert_position;

    //std::map<T, uint32_t> value_histogram;
    //std::map<T, uint32_t> prefix_v;
  };

  /**
  * The TablelStatistics structure is used to gather statistics regarding the value distribution of a table
  *  and its chunks in order to be able to pick appropriate value ranges for the partitions.
  **/
  struct TableStatistics {
    TableStatistics(size_t chunk_count, size_t partition_count) {
      partition_histogram.resize(partition_count);
      chunk_statistics.reserve(chunk_count);
      for(size_t i = 0; i < chunk_count; i++) {
        chunk_statistics.push_back(ChunkStatistics(partition_count));
      }

    }
    std::vector<ChunkStatistics> chunk_statistics;

    // used to count the number of entries for each partition from the whole table
    std::vector<size_t> partition_histogram;
    //std::map<T, uint32_t> value_histogram;
  };

  /**
  * Context for the visitor pattern implementation for column materialization and sorting.
  **/
  struct SortContext : ColumnVisitableContext {
    explicit SortContext(ChunkID id) : chunk_id(id) {}

    // The id of the chunk to be sorted
    ChunkID chunk_id;
    std::shared_ptr<MaterializedChunk<T>> sort_output;
  };

  // Input parameters
  std::shared_ptr<const Table> _input_table_left;
  std::shared_ptr<const Table> _input_table_right;
  const std::string _left_column_name;
  const std::string _right_column_name;
  bool _equi_case;

  // the partition count should be a power of two, i.e. 1, 2, 4, 8, 16, ...
  size_t _partition_count;

  std::shared_ptr<MaterializedTable<T>> _output_left;
  std::shared_ptr<MaterializedTable<T>> _output_right;

  // Radix calculation for arithmetic types
  template <typename T2>
  typename std::enable_if<std::is_arithmetic<T2>::value, uint32_t>::type get_radix(T2 value, uint8_t radix_bitmask) {
    return static_cast<uint32_t>(value) & radix_bitmask;
  }

  // Radix calculation for non-arithmetic types
  template <typename T2>
  typename std::enable_if<!std::is_arithmetic<T2>::value, uint32_t>::type get_radix(T2 value, uint32_t radix_bitmask) {
    auto result = reinterpret_cast<const uint32_t*>(value.c_str());
    return *result & radix_bitmask;
  }

  /**
  * Concatenates multiple materialized chunks to a single materialized chunk.
  **/
  std::shared_ptr<MaterializedTable<T>> _concatenate_chunks(std::shared_ptr<MaterializedTable<T>> input_chunks) {
    auto output_table = std::make_shared<MaterializedTable<T>>(1);
    output_table->at(0) = std::make_shared<MaterializedChunk<T>>();

    // Reserve the required space and move the data to the output
    auto output_chunk = output_table->at(0);
    output_chunk->reserve(_materialized_table_size(input_chunks));
    for (auto& chunk : *input_chunks) {
      output_chunk->insert(output_chunk->end(), chunk->begin(), chunk->end());
    }

    return output_table;
  }

  /**
  * Determines the total size of a materialized table.
  **/
  size_t _materialized_table_size(std::shared_ptr<MaterializedTable<T>> table) {
    size_t total_size = 0;
    for (auto chunk : *table) {
      total_size += chunk->size();
    }

    return total_size;
  }

  /**
  * Performs least significant bit radix partitioning which is used in the equi join case.
  **/
  std::shared_ptr<MaterializedTable<T>> _equi_join_partition(std::shared_ptr<MaterializedTable<T>> input_chunks) {
    auto output_table = std::make_shared<MaterializedTable<T>>(_partition_count);
    TableStatistics table_statistics(input_chunks->size(), _partition_count);

    // Each chunk should prepare additional data to enable partitioning
    for (size_t chunk_number = 0; chunk_number < input_chunks->size(); chunk_number++) {
      auto& chunk_statistics = table_statistics.chunk_statistics[chunk_number];
      auto input_chunk = input_chunks->at(chunk_number);

      // Count the number of entries for each partition
      // Each partition corresponds to a radix value
      for (auto& entry : *input_chunk) {
        auto radix = get_radix<T>(entry.value, _partition_count - 1);
        chunk_statistics.partition_histogram[radix]++;
      }
    }

    // Aggregate the chunks histograms to a table histogram and initialize the insert positions for each chunk
    for (auto& chunk_statistics : table_statistics.chunk_statistics) {
      for (size_t partition_id = 0; partition_id < _partition_count; partition_id++) {
        chunk_statistics.insert_position[partition_id] = table_statistics.partition_histogram[partition_id];
        table_statistics.partition_histogram[partition_id] += chunk_statistics.partition_histogram[partition_id];
      }
    }

    // Reserve the appropriate output space for the partitions
    for (size_t partition_id = 0; partition_id < _partition_count; partition_id++) {
      auto partition_size = table_statistics.partition_histogram[partition_id];
      output_table->at(partition_id) = std::make_shared<MaterializedChunk<T>>(partition_size);
    }

    // Move each entry into its appropriate partition
    // Since _partition_count is a power of two, the radix bitmask will look like 00...011...1
    uint32_t radix_bitmask = _partition_count - 1;
    for (size_t chunk_number = 0; chunk_number < input_chunks->size(); chunk_number++) {
      auto chunk = input_chunks->at(chunk_number);
      auto& chunk_statistics = table_statistics.chunk_statistics[chunk_number];

      for (auto& entry : *chunk) {
        auto partition_id = get_radix<T>(entry.value, radix_bitmask);
        output_table->at(partition_id)->at(chunk_statistics.insert_position[partition_id]) = entry;
        chunk_statistics.insert_position[partition_id]++;
      }
    }

    return output_table;
  }

  /**
  * Sorts all partitions of a materialized table.
  **/
  void _sort_partitions(std::shared_ptr<MaterializedTable<T>> partitions) {
    for (auto partition : *partitions) {
      std::sort(partition->begin(), partition->end(), [](auto& left, auto& right) {
        return left.value < right.value;
      });
    }
  }

  /**
  * Materializes and sorts an input chunk by the specified column in ascending order.
  **/
  std::shared_ptr<MaterializedChunk<T>> _sort_chunk(const Chunk& chunk, ChunkID chunk_id, ColumnID column_id) {
    auto column = chunk.get_column(column_id);
    auto context = std::make_shared<SortContext>(chunk_id);
    column->visit(*this, context);
    DebugAssert(_validate_is_sorted(context->sort_output), "Chunk should be sorted but is not");
    DebugAssert(chunk.size() == context->sort_output->size(), "Chunk has wrong size");
    return context->sort_output;
  }

  /**
  * Creates a job to sort multiple chunks.
  **/
  std::shared_ptr<JobTask> _sort_chunks_job(std::shared_ptr<MaterializedTable<T>> output, std::vector<ChunkID> chunks,
                                            std::shared_ptr<const Table> input, std::string column_name) {
    return std::make_shared<JobTask>([=] {
      for (auto chunk_id : chunks) {
        output->at(chunk_id) = _sort_chunk(input->get_chunk(chunk_id), chunk_id, input->column_id_by_name(column_name));
      }
    });
  }

  /**
  * Sorts all the chunks of an input table in parallel by creating multiple jobs that sort multiple chunks.
  **/
  std::shared_ptr<MaterializedTable<T>> _sort_input_chunks(std::shared_ptr<const Table> input, std::string column) {
    auto sorted_table = std::make_shared<MaterializedTable<T>>(input->chunk_count());

    // Can be extended to find that value dynamically later on (depending on hardware etc.)
    const size_t job_size_threshold = 10000;
    std::vector<std::shared_ptr<AbstractTask>> jobs;

    size_t job_size = 0;
    std::vector<ChunkID> chunk_ids;
    for (ChunkID chunk_id{0}; chunk_id < input->chunk_count(); chunk_id++) {
      job_size += input->get_chunk(chunk_id).size();
      chunk_ids.push_back(chunk_id);
      if (job_size > job_size_threshold || chunk_id == input->chunk_count() - 1) {
        jobs.push_back(_sort_chunks_job(sorted_table, chunk_ids, input, column));
        jobs.back()->schedule();
        job_size = 0;
        chunk_ids.clear();
      }
    }

    CurrentScheduler::wait_for_tasks(jobs);

    return sorted_table;
  }

  /**
  * ColumnVisitable implementation to materialize and sort a value column.
  **/
  void handle_value_column(BaseColumn& column, std::shared_ptr<ColumnVisitableContext> context) override {
    auto& value_column = static_cast<ValueColumn<T>&>(column);
    auto sort_context = std::static_pointer_cast<SortContext>(context);
    auto output = std::make_shared<MaterializedChunk<T>>(value_column.values().size());

    // Copy over every entry
    for (ChunkOffset chunk_offset{0}; chunk_offset < value_column.values().size(); chunk_offset++) {
      RowID row_id{sort_context->chunk_id, chunk_offset};
      output->at(chunk_offset) = MaterializedValue<T>(row_id, value_column.values()[chunk_offset]);
    }

    // Sort the entries
    std::sort(output->begin(), output->end(), [](auto& left, auto& right) { return left.value < right.value; });
    sort_context->sort_output = output;
  }

  /**
  * ColumnVisitable implementaion to materialize and sort a dictionary column.
  **/
  void handle_dictionary_column(BaseColumn& column, std::shared_ptr<ColumnVisitableContext> context) override {
    auto& dictionary_column = dynamic_cast<DictionaryColumn<T>&>(column);
    auto sort_context = std::static_pointer_cast<SortContext>(context);
    auto output = std::make_shared<MaterializedChunk<T>>(column.size());

    auto value_ids = dictionary_column.attribute_vector();
    auto dict = dictionary_column.dictionary();

    // Collect for every value id, the set of rows that this value appeared in
    // value_count is used as an inverted index
    auto rows_with_value = std::vector<std::vector<RowID>>(dict->size());
    for (ChunkOffset chunk_offset{0}; chunk_offset < value_ids->size(); chunk_offset++) {
      rows_with_value[value_ids->get(chunk_offset)].push_back(RowID{sort_context->chunk_id, chunk_offset});
    }

    // Now that we know the row ids for every value, we can output all the materialized values in a sorted manner.
    ChunkOffset chunk_offset{0};
    for (ValueID value_id{0}; value_id < dict->size(); value_id++) {
      for (auto& row_id : rows_with_value[value_id]) {
        output->at(chunk_offset) = MaterializedValue<T>(row_id, dict->at(value_id));
        chunk_offset++;
      }
    }

    sort_context->sort_output = output;
  }

  /**
  * Sorts the contents of a reference column into a sorted chunk
  **/
  void handle_reference_column(ReferenceColumn& ref_column, std::shared_ptr<ColumnVisitableContext> context) override {
    auto referenced_table = ref_column.referenced_table();
    auto referenced_column_id = ref_column.referenced_column_id();
    auto sort_context = std::static_pointer_cast<SortContext>(context);
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
      output->at(chunk_offset) = MaterializedValue<T>(RowID{sort_context->chunk_id, chunk_offset}, value);
    }

    // Sort the values
    std::sort(output->begin(), output->end(), [](auto& left, auto& right) { return left.value < right.value; });
    sort_context->sort_output = output;
  }

  T pick_sample_values(std::vector<std::map<T, size_t>>& sample_values, std::shared_ptr<MaterializedTable<T>> table) {
    auto max_value = table->at(0)->at(0).value;

    for (size_t chunk_number = 0; chunk_number < table->size(); chunk_number++) {
      auto chunk_values = table->at(chunk_number);

      // Since the chunks are sorted, the maximum values are at the back of them
      if (max_value < chunk_values->back().value) {
        max_value = chunk_values->back().value;
      }

      for(size_t partition_id = 0; partition_id < _partition_count; partition_id++) {
        size_t pos = static_cast<size_t>(chunk_values->size() * (partition_id / static_cast<float>(_partition_count)));
        sample_values[partition_id][chunk_values->at(pos).value]++;
      }
    }

    return max_value;
  }

  /**
  * Performs the radix partition sort for the non equi case which requires the complete table to be sorted
  * and not only the partitions in themselves.
  **/
  std::pair<std::shared_ptr<MaterializedTable<T>>, std::shared_ptr<MaterializedTable<T>>> _non_equi_join_partition(
                  std::shared_ptr<MaterializedTable<T>> input_left, std::shared_ptr<MaterializedTable<T>> input_right) {
    std::cout << "Partitioning for non equi join" << std::endl;
    std::vector<std::map<T, size_t>> sample_values(_partition_count);

    auto max_value_left = pick_sample_values(sample_values, input_left);
    auto max_value_right = pick_sample_values(sample_values, input_right);

    auto max_value = std::max(max_value_left, max_value_right);

    // Pick the split values to be the most common sample value for each partition
    std::vector<T> split_values(_partition_count);
    for (size_t partition_id = 0; partition_id < _partition_count - 1; partition_id++) {
    split_values[partition_id] = std::max_element(sample_values[partition_id].begin(),
                                                  sample_values[partition_id].end(),
      [] (auto& a, auto& b) {
        return a.second < b.second;
      })->second;
    }
    split_values.back() = max_value;

    auto output_left = _range_partition(input_left, split_values);
    auto output_right = _range_partition(input_right, split_values);

    return std::pair<std::shared_ptr<MaterializedTable<T>>,
              std::shared_ptr<MaterializedTable<T>>>(output_left, output_right);
  }

  /**
  * Determines in which range a value falls and returns the corresponding partition id.
  **/
  size_t _determine_partition(T& value, std::vector<T>& split_values) {
    for(size_t partition_id = 0; partition_id < _partition_count; partition_id++) {
      if(value <= split_values[partition_id]) {
        return partition_id;
      }
    }
    throw std::logic_error("There is no partition for this value");
  }

  std::shared_ptr<MaterializedTable<T>> _range_partition(std::shared_ptr<MaterializedTable<T>> table,
                                                                       std::vector<T>& split_values) {
    auto partitions = std::make_shared<MaterializedTable<T>>(_partition_count);
    TableStatistics table_statistics(table->size(), _partition_count);

    // Each chunk should prepare additional data to enable partitioning
    for (size_t chunk_number = 0; chunk_number < table->size(); chunk_number++) {
      auto& chunk_statistics = table_statistics.chunk_statistics[chunk_number];

      // Fill histogram
      for (auto& entry : *table->at(chunk_number)) {
        auto partition_id = _determine_partition(entry.value, split_values);
        chunk_statistics.partition_histogram[partition_id]++;
      }
    }

    // Aggregate the chunk statistics to table statistics
    for (auto& chunk_statistics : table_statistics.chunk_statistics) {
      for (size_t partition_id = 0; partition_id < _partition_count; partition_id++) {
        chunk_statistics.insert_position[partition_id] = table_statistics.partition_histogram[partition_id];
        table_statistics.partition_histogram[partition_id] += chunk_statistics.partition_histogram[partition_id];
      }
    }

    // prepare for parallel access later on
    for (size_t partition_id = 0; partition_id < _partition_count; partition_id++) {
      auto partition_size = table_statistics.partition_histogram[partition_id];
      partitions->at(partition_id) = std::make_shared<MaterializedChunk<T>>(partition_size);
    }

    DebugAssert(_materialized_table_size(partitions) == _materialized_table_size(table),
                "Value based partitioning: input and output table do not have the same size");

    // Move the entries to the appropriate output position
    // Note: this should be parallelized
    for (size_t chunk_number = 0; chunk_number < table->size(); chunk_number++) {
      auto& chunk_statistics = table_statistics.chunk_statistics[chunk_number];
      for (auto& entry : *table->at(chunk_number)) {
        auto partition_id = _determine_partition(entry.value, split_values);
        auto insert_position = chunk_statistics.insert_position[partition_id];
        partitions->at(partition_id)->at(insert_position) = entry;
        chunk_statistics.insert_position[partition_id]++;
      }
    }

    return partitions;
  }

  /**
  * Determines whether a materialized chunk is sorted in ascending order.
  **/
  bool _validate_is_sorted(std::shared_ptr<MaterializedChunk<T>> chunk) {
    for (size_t i = 0; i + 1 < chunk->size(); i++) {
      if (chunk->at(i).value > chunk->at(i + 1).value) {
        return false;
      }
    }

    return true;
  }

  /**
  * Prints out the values of a materialized table.
  **/
  void print_table(std::shared_ptr<MaterializedTable<T>> table) {
    std::cout << "----" << std::endl;
    for (auto chunk : *table) {
      for (auto& row : *chunk) {
        std::cout << row.value << std::endl;
      }
      std::cout << "----" << std::endl;
    }
  }

  bool _validate_is_sorted(std::shared_ptr<MaterializedTable<T>> table, bool inter_chunk_sorting_required) {
    for (size_t chunk_number = 0; chunk_number < table->size(); chunk_number++) {
      // Every chunk must be sorted in its self
      if (!_validate_is_sorted(table->at(chunk_number))) {
        print_table(table);
        return false;
      }

      // Check if this chunks elements are smaller than the next ones
      if (inter_chunk_sorting_required && chunk_number + 1 < table->size() && table->at(chunk_number + 1)->size() > 0
                            && table->at(chunk_number)->size() > 0
                            && table->at(chunk_number)->back().value > table->at(chunk_number + 1)->at(0).value) {
        print_table(table);
        return false;
      }
    }

    return true;
  }

 public:
  void execute() {
    // Is this really necessary??
    // Sort the chunks of the input tables
    auto sorted_chunks_left = _sort_input_chunks(_input_table_left, _left_column_name);
    auto sorted_chunks_right = _sort_input_chunks(_input_table_right, _right_column_name);

    if (_partition_count == 1) {
      _output_left = _concatenate_chunks(sorted_chunks_left);
      _output_right = _concatenate_chunks(sorted_chunks_right);
    } else if (_equi_case) {
      _output_left = _equi_join_partition(sorted_chunks_left);
      _output_right = _equi_join_partition(sorted_chunks_right);
    } else {
      auto result = _non_equi_join_partition(sorted_chunks_left, sorted_chunks_right);
      _output_left = result.first;
      _output_right = result.second;
    }

    // Sort each partition (right now std::sort -> but maybe can be replaced with
    // an algorithm more efficient, if subparts are already sorted [InsertionSort?!])
    _sort_partitions(_output_left);
    _sort_partitions(_output_right);

    // Validate the results for simpler testing
    DebugAssert(_validate_is_sorted(_output_left, !_equi_case), "left output table is not sorted");
    DebugAssert(_validate_is_sorted(_output_right, !_equi_case), "right output table is not sorted");

    DebugAssert(_materialized_table_size(_output_left) == _input_table_left->row_count(),
                "left output has wrong size");
    DebugAssert(_materialized_table_size(_output_right) == _input_table_right->row_count(),
                "right output has wrong size");
  }

  std::pair<std::shared_ptr<MaterializedTable<T>>, std::shared_ptr<MaterializedTable<T>>> get_output() {
    return std::make_pair(_output_left, _output_right);
  }
};

}  // namespace opossum
