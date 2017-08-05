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
struct SortedChunk {
  SortedChunk() : values{std::make_shared<std::vector<std::pair<RowID, T>>>()} {}

  std::shared_ptr<std::vector<std::pair<RowID, T>>> values;

  // Used to count the number of entries for each partition from this chunk
  std::map<uint32_t, uint32_t> partition_histogram;
  std::map<uint32_t, uint32_t> prefix;

  std::map<T, uint32_t> value_histogram;
  std::map<T, uint32_t> prefix_v;
};

template <typename T>
struct SortedTable {
  SortedTable() {}

  std::vector<SortedChunk<T>> partitions;

  // used to count the number of entries for each partition from the whole table
  std::map<uint32_t, uint32_t> partition_histogram;
  std::map<T, uint32_t> value_histogram;
 };

template <typename T>
class RadixPartitionSort : public ColumnVisitable {
 public:
   RadixPartitionSort(const std::shared_ptr<const AbstractOperator> left,
                        const std::shared_ptr<const AbstractOperator> right,
                        std::pair<std::string, std::string> column_names, const std::string& op,
                        const JoinMode mode, size_t partition_count)
    : _left_column_name{column_names.first}, _right_column_name{column_names.second}, _op{op}, _mode{mode},
      _partition_count{partition_count} {

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
   }

  virtual ~RadixPartitionSort() = default;

  protected:
   struct SortContext : ColumnVisitableContext {
     SortContext(ChunkID id) : chunk_id(id) {}

     ChunkID chunk_id;
     std::shared_ptr<std::vector<std::pair<RowID, T>>> sort_output;
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

   std::shared_ptr<SortedTable<T>> _sorted_left_table;
   std::shared_ptr<SortedTable<T>> _sorted_right_table;

   // Radix calculation functions
   template <typename T2>
   typename std::enable_if<std::is_arithmetic<T2>::value, uint32_t>::type get_radix(T2 value, uint32_t radix_bits) {
     return static_cast<uint32_t>(value) & radix_bits;
   }

   template <typename T2>
   typename std::enable_if<!std::is_arithmetic<T2>::value, uint32_t>::type get_radix(T2 value, uint32_t radix_bits) {
     auto result = reinterpret_cast<const uint32_t*>(value.c_str());
     return *result & radix_bits;
   }

     // Sort functions
   std::shared_ptr<SortedTable<T>> sort_table(std::shared_ptr<const Table> input, const std::string& column_name) {
     auto sorted_input_chunks_ptr = sort_input_chunks(input, column_name);
     auto& sorted_input_chunks = *sorted_input_chunks_ptr;
     auto sorted_table = std::make_shared<SortedTable<T>>();

     DebugAssert((_partition_count >= 1), "_partition_count is < 1");

     if (_partition_count == 1) {
       sorted_table->partitions.resize(1);
       for (auto& sorted_chunk : sorted_input_chunks) {
         for (auto& entry : *(sorted_chunk.values)) {
           sorted_table->partitions[0].values->push_back(entry);
         }
       }
     } else {
       // Do radix-partitioning here for _partition_count > 1 partitions
       if (_op == "=") {
         sorted_table->partitions.resize(_partition_count);

         // for prefix computation we need to table-wide know how many entries there are for each partition
         for (uint32_t partition_id = 0; partition_id < _partition_count; ++partition_id) {
           sorted_table->partition_histogram.insert(std::pair<uint32_t, uint32_t>(partition_id, 0));
         }

         // Each chunk should prepare additional data to enable partitioning
         for (auto& sorted_chunk : sorted_input_chunks) {
           for (uint32_t partition_id = 0; partition_id < _partition_count; partition_id++) {
             sorted_chunk.partition_histogram.insert(std::pair<uint32_t, uint32_t>(partition_id, 0));
             sorted_chunk.prefix.insert(std::pair<uint32_t, uint32_t>(partition_id, 0));
           }

           // fill histogram
           // count the number of entries for each partition_id
           // each partition corresponds to a radix
           for (auto& entry : *(sorted_chunk.values)) {
             auto radix = get_radix<T>(entry.second, _partition_count - 1);
             sorted_chunk.partition_histogram[radix]++;
           }
         }

         // Each chunk need to sequentially fill _prefix map to actually fill partition of tables in parallel
         for (auto& sorted_chunk : sorted_input_chunks) {
           for (uint32_t partition_id = 0; partition_id < _partition_count; ++partition_id) {
             sorted_chunk.prefix[partition_id] = sorted_table->partition_histogram[partition_id];
             sorted_table->partition_histogram[partition_id] += sorted_chunk.partition_histogram[partition_id];
           }
         }

         // prepare for parallel access later on
         for (uint32_t partition_id = 0; partition_id < _partition_count; ++partition_id) {
           sorted_table->partitions[partition_id].values->resize(sorted_table->partition_histogram[partition_id]);
         }

         // Move each entry into its appropriate partition
         for (auto& sorted_chunk : sorted_input_chunks) {
           for (auto& entry : *(sorted_chunk.values)) {
             auto radix = get_radix<T>(entry.second, _partition_count - 1);
             sorted_table->partitions[radix].values->at(sorted_chunk.prefix[radix]++) = entry;
           }
         }
       }
     }

     // Sort each partition (right now std::sort -> but maybe can be replaced with
     // an algorithm more efficient, if subparts are already sorted [InsertionSort?!])
     for (auto& partition : sorted_table->partitions) {
       std::sort(partition.values->begin(), partition.values->end(),
                 [](auto& value_left, auto& value_right) { return value_left.second < value_right.second; });
     }

     return sorted_table;
   }

   std::shared_ptr<std::vector<SortedChunk<T>>> sort_input_chunks(std::shared_ptr<const Table> input,
                                                               const std::string& column_name) {
     auto sorted_chunks = std::make_shared<std::vector<SortedChunk<T>>>(input->chunk_count());

     // Can be extended to find that value dynamically later on (depending on hardware etc.)
     const uint32_t partitionSizeThreshold = 10000;
     std::vector<std::shared_ptr<AbstractTask>> jobs;

     uint32_t size = 0;
     std::vector<ChunkID> chunk_ids;

     for (ChunkID chunk_id{0}; chunk_id < input->chunk_count(); chunk_id++) {
       size += input->chunk_size();
       chunk_ids.push_back(chunk_id);
       if (size > partitionSizeThreshold || chunk_id == input->chunk_count() - 1) {
         // Create a job responsible for sorting multiple chunks of the input table
         jobs.push_back(std::make_shared<JobTask>([this, sorted_chunks, chunk_ids, input, column_name] {
           for (auto chunk_id : chunk_ids) {
             auto column = input->get_chunk(chunk_id).get_column(input->column_id_by_name(column_name));
             auto context = std::make_shared<SortContext>(chunk_id);
             column->visit(*this, context);
             sorted_chunks->at(chunk_id).values = context->sort_output;
           }
         }));
         size = 0;
         chunk_ids.clear();
         jobs.back()->schedule();
       }
     }

     CurrentScheduler::wait_for_tasks(jobs);

     DebugAssert((static_cast<ChunkID>(sorted_chunks->size()) == input->chunk_count()),
                                                                   "# of sorted chunks != # of input chunks");
     return sorted_chunks;
   }

     // ColumnVisitable implementations to sort concrete input chunks
   void handle_value_column(BaseColumn& column, std::shared_ptr<ColumnVisitableContext> context) override {
     auto& value_column = static_cast<ValueColumn<T>&>(column);
     auto sort_context = std::static_pointer_cast<SortContext>(context);
     sort_context->sort_output = value_column.materialize(sort_context->chunk_id, nullptr);

     // Sort the entries
     std::sort(sort_context->sort_output->begin(), sort_context->sort_output->end(),
     [](auto& value_left, auto& value_right) { return value_left.second < value_right.second; });
   }

   void handle_dictionary_column(BaseColumn& column, std::shared_ptr<ColumnVisitableContext> context) override {
     auto& dictionary_column = dynamic_cast<DictionaryColumn<T>&>(column);
     auto sort_context = std::static_pointer_cast<SortContext>(context);
     sort_context->sort_output = std::make_shared<std::vector<std::pair<RowID, T>>>();
     auto output = sort_context->sort_output;
     output->resize(column.size());

     auto value_ids = dictionary_column.attribute_vector();
     auto dict = dictionary_column.dictionary();

     std::vector<std::vector<RowID>> value_count = std::vector<std::vector<RowID>>(dict->size());

     // Collect the rows for each value id
     for (ChunkOffset chunk_offset{0}; chunk_offset < value_ids->size(); chunk_offset++) {
       value_count[value_ids->get(chunk_offset)].push_back(RowID{sort_context->chunk_id, chunk_offset});
     }

     // Append the rows to the sorted chunk
     ChunkOffset chunk_offset{0};
     for (ValueID value_id{0}; value_id < dict->size(); value_id++) {
       for (auto& row_id : value_count[value_id]) {
         output->at(chunk_offset) = std::pair<RowID, T>(row_id, dict->at(value_id));
         chunk_offset++;
       }
     }
   }

     /**
     * Sorts the contents of a reference column into a sorted chunk
     **/
   void handle_reference_column(ReferenceColumn& ref_column, std::shared_ptr<ColumnVisitableContext> context) override {
     auto referenced_table = ref_column.referenced_table();
     auto referenced_column_id = ref_column.referenced_column_id();
     auto sort_context = std::static_pointer_cast<SortContext>(context);
     auto pos_list = ref_column.pos_list();
     sort_context->sort_output = std::make_shared<std::vector<std::pair<RowID, T>>>();
     auto output = sort_context->sort_output;
     output->resize(ref_column.size());

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
       output->at(chunk_offset) = (std::pair<RowID, T>(RowID{sort_context->chunk_id, chunk_offset}, value));
     }

     // Sort the values
     std::sort(output->begin(), output->end(),
                   [](auto& value_left, auto& value_right) { return value_left.second < value_right.second; });
   }

   T pick_sample_values(std::vector<std::map<T, uint32_t>>& sample_values, std::vector<SortedChunk<T>> partitions) {
     auto max_value = partitions[0].values->at(0).second;

     for (size_t partition_number = 0; partition_number < partitions.size(); ++partition_number) {
       auto values = partitions[partition_number].values;

       // Since the chunks are sorted, the maximum values are at the back of them
       if (max_value < values->back().second) {
         max_value = values->back().second;
       }

       // get samples
       size_t step_size = values->size() / _partition_count;
       DebugAssert((step_size >= 1), "SortMergeJoin value_based_partitioning: step size is <= 0");
       for (size_t pos = step_size, partition_id = 0; pos < values->size() - 1; pos += step_size, partition_id++) {
         if (sample_values[partition_id].count(values->at(pos).second) == 0) {
           sample_values[partition_id].insert(std::pair<T, uint32_t>(values->at(pos).second, 1));
         } else {
           ++(sample_values[partition_id].at(values->at(pos).second));
         }
       }
     }

     return max_value;
   }

     // Partitioning in case of Non-Equi-Join
   void value_based_partitioning() {
     std::vector<std::map<T, uint32_t>> sample_values(_partition_count);

     auto max_value_left = pick_sample_values(sample_values, _sorted_left_table->partitions);
     auto max_value_right = pick_sample_values(sample_values, _sorted_right_table->partitions);

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

     value_based_table_partitioning(_sorted_left_table, split_values);
     value_based_table_partitioning(_sorted_right_table, split_values);
   }

  void value_based_table_partitioning(std::shared_ptr<SortedTable<T>> sort_table, std::vector<T>& split_values) {
     std::vector<std::shared_ptr<std::vector<std::pair<RowID, T>>>> partitions;
     partitions.resize(_partition_count);
     for(size_t partition = 0; partition < _partition_count; partition++) {
       partitions[partition] = std::make_shared<std::vector<std::pair<RowID, T>>>();
     }

     // for prefix computation we need to table-wide know how many entries there are for each partition
     // right now we expect an equally randomized entryset
     for (auto& value : split_values) {
       sort_table->value_histogram.insert(std::pair<T, uint32_t>(value, 0));
     }

     std::cout << "450" << std::endl;

     // Each chunk should prepare additional data to enable partitioning
     for (auto& s_chunk : sort_table->partitions) {
       for (auto& value : split_values) {
         s_chunk.value_histogram.insert(std::pair<T, uint32_t>(value, 0));
         s_chunk.prefix_v.insert(std::pair<T, uint32_t>(value, 0));
       }

       // fill histogram
       for (auto& entry : *(s_chunk.values)) {
         for (auto& split_value : split_values) {
           if (entry.second <= split_value) {
             s_chunk.value_histogram[split_value]++;
             break;
           }
         }
       }
     }

     std::cout << "470" << std::endl;

     // Each chunk need to sequentially fill _prefix map to actually fill partition of tables in parallel
     for (auto& s_chunk : sort_table->partitions) {
       for (auto& split_value : split_values) {
         s_chunk.prefix_v[split_value] = sort_table->value_histogram[split_value];
         sort_table->value_histogram[split_value] += s_chunk.value_histogram[split_value];
       }
     }

     // prepare for parallel access later on
     uint32_t i = 0;
     for (auto& split_value : split_values) {
       partitions[i]->resize(sort_table->value_histogram[split_value]);
       ++i;
     }

     std::cout << "487" << std::endl;

     for (auto& s_chunk : sort_table->partitions) {
       for (auto& entry : *(s_chunk.values)) {
         auto radix = get_radix<T>(entry.second, _partition_count - 1);
         partitions[radix]->at(s_chunk.prefix[radix]++) = entry;
       }
     }

     std::cout << "496" << std::endl;

     // Each chunk fills (parallel) partition
     for (auto& s_chunk : sort_table->partitions) {
       for (auto& entry : *(s_chunk.values)) {
         uint32_t partition_id = 0;
         for (auto& radix : split_values) {
           if (entry.second <= radix) {
             std::cout << "s_chunk.prefix_v.size(): " << s_chunk.prefix_v.size() << std::endl;
             std::cout << "radix: " << radix << std::endl;
             partitions[partition_id]->at(s_chunk.prefix_v[radix]) = entry;
             s_chunk.prefix_v[radix]++;
             partition_id++;
           }
         }
       }
     }

     std::cout << "513" << std::endl;

     // move result to table
     sort_table->partitions.resize(partitions.size());
     for (size_t index = 0; index < partitions.size(); ++index) {
       sort_table->partitions[index].values = partitions[index];
     }

     std::cout << "522" << std::endl;

     // Sort partitions (right now std:sort -> but maybe can be replaced with
     // an algorithm more efficient, if subparts are already sorted [InsertionSort?])
     for (auto& partition : sort_table->partitions) {
       std::sort(partition.values->begin(), partition.values->end(),
                 [](auto& value_left, auto& value_right) { return value_left.second < value_right.second; });
     }

     std::cout << "531" << std::endl;
   }



  public:
   void execute() {
     DebugAssert(_partition_count > 0, "partition count is <= 0!");

     // Sort and partition the input tables
     _sorted_left_table = sort_table(_input_table_left, _left_column_name);
     _sorted_right_table = sort_table(_input_table_right, _right_column_name);

     // std::cout << "Table sorting ran through" << std::endl;

     if (_op != "=" && _partition_count > 1) {
       value_based_partitioning();
       std::cout << "value based partitioning ran through" << std::endl;
     }
   }

   std::pair<std::shared_ptr<SortedTable<T>>, std::shared_ptr<SortedTable<T>>> get_output() {
     return std::make_pair(_sorted_left_table, _sorted_right_table);
   }
};

}  // namespace opossum
