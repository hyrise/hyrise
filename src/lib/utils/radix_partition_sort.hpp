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
                        std::pair<std::string, std::string> column_names, const std::string& op,
                        const JoinMode mode, size_t partition_count)
    : _left_column_name{column_names.first}, _right_column_name{column_names.second}, _op{op}, _mode{mode},
      _partition_count{partition_count} {

     DebugAssert(partition_count > 0, "partition_count must be > 0");
     DebugAssert(mode != Cross, "this operator does not support cross joins");
     DebugAssert(left != nullptr, "left input operator is null");
     DebugAssert(right != nullptr, "right input operator is null");
     DebugAssert(op == "=" || op == "<" || op == ">" || op == "<=" || op == ">=", "unknown operator " + op);

     _input_table_left = left->get_output();
     _input_table_right = right->get_output();

     // Check column_type
     const auto left_column_id = _input_table_left->column_id_by_name(_left_column_name);
     const auto right_column_id = _input_table_right->column_id_by_name(_right_column_name);
     const auto& left_column_type = _input_table_left->column_type(left_column_id);
     const auto& right_column_type = _input_table_right->column_type(right_column_id);

     DebugAssert(left_column_type == right_column_type, "left and right column types do not match");
     DebugAssert(left_column_type != "string", "string support has not been implemented yet");
   }

  virtual ~RadixPartitionSort() = default;

 protected:
  /**
  * The ChunkStatistics structure is used to gather statistics regarding a chunks values in order to
  * be able to pick appropriate value ranges for the partitions.
  **/
  struct ChunkStatistics {
    // Used to count the number of entries for each partition from this chunk
    std::vector<size_t> partition_histogram;
    std::vector<size_t> insert_position;

    std::map<T, uint32_t> value_histogram;
    std::map<T, uint32_t> prefix_v;
  };

  /**
  * The TablelStatistics structure is used to gather statistics regarding the value distribution of a table
  *  and its chunks in order to be able to pick appropriate value ranges for the partitions.
  **/
  struct TableStatistics {
    std::vector<ChunkStatistics> chunk_statistics;

    // used to count the number of entries for each partition from the whole table
    std::vector<size_t> partition_histogram;
    std::map<T, uint32_t> value_histogram;
  };

  /**
  * Context for the visitor pattern implementation for column materialization and sorting.
  **/
  struct SortContext : ColumnVisitableContext {
    SortContext(ChunkID id) : chunk_id(id) {}

    // The id of the chunk to be sorted
    ChunkID chunk_id;
    std::shared_ptr<MaterializedChunk<T>> sort_output;
  };

  // Input parameters
  std::shared_ptr<const Table> _input_table_left;
  std::shared_ptr<const Table> _input_table_right;
  const std::string _left_column_name;
  const std::string _right_column_name;
  const std::string _op;
  const JoinMode _mode;

  // the partition count should be a power of two, i.e. 1, 2, 4, 8, 16, ...
  size_t _partition_count;

  TableStatistics _left_table_statistics;
  TableStatistics _right_table_statistics;

  std::shared_ptr<MaterializedTable<T>> _output_left;
  std::shared_ptr<MaterializedTable<T>> _output_right;

  // Radix calculation for arithmetic types
  template <typename T2>
  typename std::enable_if<std::is_arithmetic<T2>::value, uint32_t>::type get_radix(T2 value, uint8_t radix_bits) {
    return static_cast<uint32_t>(value) & radix_bits;
  }

  // Radix calculation for non-arithmetic types
  template <typename T2>
  typename std::enable_if<!std::is_arithmetic<T2>::value, uint32_t>::type get_radix(T2 value, uint32_t radix_bits) {
    auto result = reinterpret_cast<const uint32_t*>(value.c_str());
    return *result & radix_bits;
  }

     // Sort functions
   std::shared_ptr<MaterializedTable<T>> sort_table(std::shared_ptr<const Table> input, const std::string& column_name,
                                                   TableStatistics& table_statistics) {
     auto sorted_input_chunks = sort_input_chunks(input, column_name);
     auto output_table = std::make_shared<MaterializedTable<T>>(_partition_count);
     table_statistics.chunk_statistics.resize(sorted_input_chunks->size());

      std::cout << "rp: " << __LINE__ << std::endl;

     if (_partition_count == 1) {
       output_table->at(0) = std::make_shared<MaterializedChunk<T>>();
       auto output_chunk = output_table->at(0);
       output_chunk->reserve(input->row_count());
       for (auto& sorted_chunk : *sorted_input_chunks) {
         output_chunk->insert(output_chunk->end(), sorted_chunk->begin(), sorted_chunk->end());
       }
       std::cout << "rp: " << __LINE__ << std::endl;
     } else {
       // Do radix-partitioning here for _partition_count > 1 partitions
       std::cout << "op: " << _op << std::endl;
       if (_op == "=") {
         table_statistics.partition_histogram.resize(_partition_count);

         // Each chunk should prepare additional data to enable partitioning
         for(size_t chunk_number = 0; chunk_number < sorted_input_chunks->size(); chunk_number++) {
           auto& chunk_statistics = table_statistics.chunk_statistics[chunk_number];
           auto sorted_chunk = sorted_input_chunks->at(chunk_number);

           chunk_statistics.insert_position.resize(_partition_count);
           chunk_statistics.partition_histogram.resize(_partition_count);

           // Count the number of entries for each partition
           // Each partition corresponds to a radix value
           for (auto& entry : *sorted_chunk) {
             auto radix = get_radix<T>(entry.value, _partition_count - 1);
             chunk_statistics.partition_histogram[radix]++;
           }
         }

         // Each chunk needs to sequentially fill _prefix map to actually fill partition of tables in parallel
         for (auto& chunk_statistics : table_statistics.chunk_statistics) {
           for (size_t partition_id = 0; partition_id < _partition_count; partition_id++) {
             chunk_statistics.insert_position[partition_id] = table_statistics.partition_histogram[partition_id];
             table_statistics.partition_histogram[partition_id] += chunk_statistics.partition_histogram[partition_id];
           }
         }

        std::cout << "rp: " << __LINE__ << std::endl;
         // prepare for parallel access later on
         for (size_t partition_id = 0; partition_id < _partition_count; partition_id++) {
           output_table->at(partition_id) = std::make_shared<MaterializedChunk<T>>();
           output_table->at(partition_id)->resize(table_statistics.partition_histogram[partition_id]);
         }

         std::cout << "rp: " << __LINE__ << std::endl;
         // Move each entry into its appropriate partition
         for (size_t chunk_number = 0; chunk_number < sorted_input_chunks->size(); chunk_number++) {
           auto sorted_chunk = sorted_input_chunks->at(chunk_number);
           auto& chunk_statistics = table_statistics.chunk_statistics[chunk_number];

           for (auto& entry : *sorted_chunk) {
             auto partition_id = get_radix<T>(entry.value, _partition_count - 1);
             std::cout << "value: " << entry.value << ", radix: " << partition_id << std::endl;
             output_table->at(partition_id)->at(chunk_statistics.insert_position[partition_id]) = entry;
             chunk_statistics.insert_position[partition_id]++;
           }
         }
       } else {
         output_table = sorted_input_chunks;
       }
     }

     // Sort each partition (right now std::sort -> but maybe can be replaced with
     // an algorithm more efficient, if subparts are already sorted [InsertionSort?!])
     for (auto partition : *output_table) {
       std::sort(partition->begin(), partition->end(), [](auto& left, auto& right) {
         return left.value < right.value;
       });
     }

     return output_table;
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
        auto sorted_chunk = _sort_chunk(input->get_chunk(chunk_id), chunk_id, input->column_id_by_name(column_name));
        output->at(chunk_id) = sorted_chunk;
      }
    });
  }

  /**
  * Sorts all the chunks of an input table in parallel by creating multiple jobs that sort multiple chunks.
  **/
  std::shared_ptr<MaterializedTable<T>> sort_input_chunks(std::shared_ptr<const Table> input, std::string column) {
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

  T pick_sample_values(std::vector<std::map<T, uint32_t>>& sample_values, std::shared_ptr<MaterializedTable<T>> table) {
    auto max_value = table->at(0)->at(0).value;

    for (size_t chunk_number = 0; chunk_number < table->size(); chunk_number++) {
      auto chunk_values = table->at(chunk_number);

      // Since the chunks are sorted, the maximum values are at the back of them
      if (max_value < chunk_values->back().value) {
        max_value = chunk_values->back().value;
      }

      // Get samples
      size_t step_size = chunk_values->size() / _partition_count;
      DebugAssert((step_size >= 1), "SortMergeJoin value_based_partitioning: step size is <= 0");
      for (size_t pos = step_size, partition_id = 0; pos < chunk_values->size() - 1; pos += step_size, partition_id++) {
        if (sample_values[partition_id].count(chunk_values->at(pos).value) == 0) {
          sample_values[partition_id].insert(std::pair<T, uint32_t>(chunk_values->at(pos).value, 1));
        } else {
          sample_values[partition_id].at(chunk_values->at(pos).value)++;
        }
      }
    }

    return max_value;
  }

  // Partitioning in case of Non-Equi-Join
  void value_based_partitioning() {
    std::cout << "Partitioning for non equi join" << std::endl;
    std::vector<std::map<T, uint32_t>> sample_values(_partition_count);

    auto max_value_left = pick_sample_values(sample_values, _output_left);
    auto max_value_right = pick_sample_values(sample_values, _output_right);

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

    value_based_table_partitioning(_output_left, split_values, _left_table_statistics);
    value_based_table_partitioning(_output_right, split_values, _right_table_statistics);
  }

  void value_based_table_partitioning(std::shared_ptr<MaterializedTable<T>> table, std::vector<T>& split_values,
                                      TableStatistics table_statistics) {
     auto partitions = std::make_shared<MaterializedTable<T>>(_partition_count);

     for(size_t partition_id = 0; partition_id < _partition_count; partition_id++) {
       partitions->at(partition_id) = std::make_shared<MaterializedChunk<T>>();
     }

     // for prefix computation we need to table-wide know how many entries there are for each partition
     // right now we expect an equally randomized entryset
     for (auto& value : split_values) {
       table_statistics.value_histogram.insert(std::pair<T, uint32_t>(value, 0));
     }

     std::cout << "rp: " << __LINE__ << std::endl;

     // Each chunk should prepare additional data to enable partitioning
     for (size_t chunk_number = 0; chunk_number < table->size(); chunk_number++) {
       auto& chunk_statistics = table_statistics.chunk_statistics[chunk_number];
       auto chunk_values = table->at(chunk_number);

       for (auto& value : split_values) {
         chunk_statistics.value_histogram.insert(std::pair<T, uint32_t>(value, 0));
         chunk_statistics.prefix_v.insert(std::pair<T, uint32_t>(value, 0));
       }

       // fill histogram
       for (auto& entry : *chunk_values) {
         for (auto& split_value : split_values) {
           if (entry.value <= split_value) {
             chunk_statistics.value_histogram[split_value]++;
             break;
           }
         }
       }
     }

     std::cout << "rp: " << __LINE__ << std::endl;

     // Each chunk need to sequentially fill _prefix map to actually fill partition of tables in parallel
     for (auto& chunk_statistics : table_statistics.chunk_statistics) {
       for (auto& split_value : split_values) {
         chunk_statistics.prefix_v[split_value] = table_statistics.value_histogram[split_value];
         table_statistics.value_histogram[split_value] += chunk_statistics.value_histogram[split_value];
       }
     }

     // prepare for parallel access later on
     size_t partition_id = 0;
     for (auto& split_value : split_values) {
       partitions->at(partition_id)->resize(table_statistics.value_histogram[split_value]);
       partition_id++;
     }

     std::cout << "rp: " << __LINE__ << std::endl;

     for (size_t chunk_number = 0; chunk_number < table->size(); chunk_number++) {
       auto& chunk_statistics = table_statistics.chunk_statistics[chunk_number];
       auto chunk_values = table->at(chunk_number);
       for (auto& entry : *chunk_values) {
         auto partition_id = get_radix<T>(entry.value, _partition_count - 1);
         partitions->at(partition_id)->at(chunk_statistics.insert_position[partition_id]++) = entry;
       }
     }

     std::cout << "rp: " << __LINE__ << std::endl;

     // Each chunk fills (parallel) partition
     for (size_t chunk_number = 0; chunk_number < table->size(); chunk_number++) {
       auto& chunk_statistics = table_statistics.chunk_statistics[chunk_number];
       auto chunk_values = table->at(chunk_number);
       for (auto& entry : *chunk_values) {
         uint32_t partition_id = 0;
         for (auto& radix : split_values) {
           if (entry.value <= radix) {
             std::cout << "s_chunk.prefix_v.size(): " << chunk_statistics.prefix_v.size() << std::endl;
             std::cout << "radix: " << radix << std::endl;
             partitions->at(partition_id)->at(chunk_statistics.prefix_v[radix]) = entry;
             chunk_statistics.prefix_v[radix]++;
             partition_id++;
           }
         }
       }
     }

     std::cout << "rp: " << __LINE__ << std::endl;

     // move result to table
     table->resize(partitions->size());
     for (size_t partition_id = 0; partition_id < partitions->size(); partition_id++) {
       table->at(partition_id) = partitions->at(partition_id);
     }

     std::cout << "rp: " << __LINE__ << std::endl;

     // Sort partitions (right now std:sort -> but maybe can be replaced with
     // an algorithm more efficient, if subparts are already sorted [InsertionSort?])
     for (auto partition : *table) {
       std::sort(partition->begin(), partition->end(), [](auto& left, auto& right) {
                  return left.value < right.value;
       });
     }

     std::cout << "rp: " << __LINE__ << std::endl;
   }

   bool _validate_is_sorted(std::shared_ptr<MaterializedChunk<T>> chunk) {
     for(size_t i = 0; i + 1 < chunk->size(); i++) {
       if(chunk->at(i).value > chunk->at(i + 1).value) {
         return false;
       }
     }

     return true;
   }

   void print_table(std::shared_ptr<MaterializedTable<T>> table) {
     std::cout << "----" << std::endl;
     for(auto chunk : *table) {
       for(auto& row : *chunk) {
         std::cout << row.value << std::endl;
       }
       std::cout << "----" << std::endl;
     }
   }

   bool _validate_is_sorted(std::shared_ptr<MaterializedTable<T>> table, bool inter_chunk_sorting_required) {
     std::cout << "Validating sortedness" << std::endl;
     for(size_t chunk_number = 0; chunk_number < table->size(); chunk_number++) {
       if(!_validate_is_sorted(table->at(chunk_number))) {
         print_table(table);
         return false;
       }


       if(inter_chunk_sorting_required && chunk_number + 1 < table->size() && table->at(chunk_number + 1)->size() > 0
                                && table->at(chunk_number)->size() > 0
                                && table->at(chunk_number)->back().value > table->at(chunk_number + 1)->at(0).value) {
         print_table(table);
         return false;
       }
     }

     return true;
   }

   bool _validate_size(std::shared_ptr<MaterializedTable<T>> table, size_t size) {
     size_t total_size = 0;
     for(auto chunk : *table) {
       total_size += chunk->size();
     }

     return total_size == size;
   }

  public:
   void execute() {
     DebugAssert(_partition_count > 0, "partition count is <= 0!");

     // Sort and partition the input tables
     _output_left = sort_table(_input_table_left, _left_column_name, _left_table_statistics);
     _output_right = sort_table(_input_table_right, _right_column_name, _right_table_statistics);

     std::cout << "Finished sorting tables" << std::endl;

     if (_op != "=" && _partition_count > 1) {
       //std::cout << "entering value based partitioning..." << std::endl;
       value_based_partitioning();
       //std::cout << "value based partitioning ran through" << std::endl;
     }

     bool inter_chunk_order = (_op != "=");
     DebugAssert(_validate_is_sorted(_output_left, inter_chunk_order), "left output table is not sorted");
     DebugAssert(_validate_is_sorted(_output_right, inter_chunk_order), "right output table is not sorted");

     std::cout << "finished validating sortedness" << std::endl;

     DebugAssert(_validate_size(_output_left, _input_table_left->row_count()), "left output has wrong size");
     DebugAssert(_validate_size(_output_right, _input_table_right->row_count()), "right output has wrong size");
   }

   std::pair<std::shared_ptr<MaterializedTable<T>>, std::shared_ptr<MaterializedTable<T>>> get_output() {
     return std::make_pair(_output_left, _output_right);
   }
};

}  // namespace opossum
