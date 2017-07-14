#include "sort_merge_join.hpp"

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

SortMergeJoin::SortMergeJoin(const std::shared_ptr<const AbstractOperator> left,
                             const std::shared_ptr<const AbstractOperator> right,
                             optional<std::pair<std::string, std::string>> column_names, const std::string& op,
                             const JoinMode mode, const std::string& prefix_left, const std::string& prefix_right)
    : AbstractJoinOperator(left, right, column_names, op, mode, prefix_left, prefix_right) {

  DebugAssert(mode != Cross && column_names, "this operator does not support cross joins");
  DebugAssert(left != nullptr, "left input operator is null");
  DebugAssert(right != nullptr, "right input operator is null");
  DebugAssert(op == "=" || op == "<" || op == ">" || op == "<=" || op == ">=", "unknown operator " + op);

  auto left_column_name = column_names->first;
  auto right_column_name = column_names->second;

  // Check column_type
  const auto left_column_id = input_table_left()->column_id_by_name(left_column_name);
  const auto right_column_id = input_table_right()->column_id_by_name(right_column_name);
  const auto& left_column_type = input_table_left()->column_type(left_column_id);
  const auto& right_column_type = input_table_right()->column_type(right_column_id);

  DebugAssert(left_column_type == right_column_type, "left and right column types do not match");

  // Create implementation to compute join result
  _impl = make_unique_by_column_type<AbstractJoinOperatorImpl, SortMergeJoinImpl>(left_column_type,
    *this, left_column_name, right_column_name, op, mode, 1 /* partition count */);
}

/**
** Start of implementation
**/

template <typename T>
class SortMergeJoin::SortMergeJoinImpl : public AbstractJoinOperatorImpl, public ColumnVisitable {
 public:
  SortMergeJoinImpl<T>(SortMergeJoin& sort_merge_join, std::string left_column_name,
            std::string right_column_name, std::string op, JoinMode mode, size_t partition_count)
            : _sort_merge_join{sort_merge_join}, _left_column_name{left_column_name},
              _right_column_name{right_column_name}, _op(op), _mode(mode), _partition_count{partition_count} {

    _output_pos_lists_left.resize(_partition_count);
    _output_pos_lists_right.resize(_partition_count);
    for(size_t i = 0; i < _partition_count; i++)
    {
      _output_pos_lists_left[i] = std::make_shared<PosList>();
      _output_pos_lists_right[i] = std::make_shared<PosList>();
    }
  }

  virtual ~SortMergeJoinImpl() = default;

 protected:
  // struct used for materialized sorted Chunk
  struct SortedChunk {
    SortedChunk() {}

    std::vector<std::pair<T, RowID>> values;

    // Used to count the number of entries for each partition from this chunk
    std::map<uint32_t, uint32_t> partition_histogram;
    std::map<uint32_t, uint32_t> prefix;

    std::map<T, uint32_t> value_histogram;
    std::map<T, uint32_t> prefix_v;
  };

  // struct used for a materialized sorted Table
  struct SortedTable {
    SortedTable() {}

    std::vector<SortedChunk> partitions;

    // used to count the number of entries for each partition from the whole table
    std::map<uint32_t, uint32_t> partition_histogram;
    std::map<T, uint32_t> value_histogram;
  };

  struct SortContext : ColumnVisitableContext {
    SortContext(ChunkID id, std::vector<std::pair<T, RowID>>& output) : chunk_id(id), sort_output(output) {}

    ChunkID chunk_id;
    std::vector<std::pair<T, RowID>>& sort_output;
  };

  SortMergeJoin& _sort_merge_join;
  std::shared_ptr<SortedTable> _sorted_left_table;
  std::shared_ptr<SortedTable> _sorted_right_table;

  const std::string _left_column_name;
  const std::string _right_column_name;

  const std::string _op;
  const JoinMode _mode;

  // the partition count should be a power of two, i.e. 1, 2, 4, 8, 16, ...
  size_t _partition_count;

  std::vector<std::shared_ptr<PosList>> _output_pos_lists_left;
  std::vector<std::shared_ptr<PosList>> _output_pos_lists_right;
  std::shared_ptr<PosList> _output_pos_list_left;
  std::shared_ptr<PosList> _output_pos_list_right;

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
  std::shared_ptr<SortedTable> sort_table(std::shared_ptr<const Table> input, const std::string& column_name) {

    auto sorted_input_chunks_ptr = sort_input_chunks(input, column_name);
    auto& sorted_input_chunks = *sorted_input_chunks_ptr;
    auto sorted_table = std::make_shared<SortedTable>();

    DebugAssert((_partition_count >= 1), "_partition_count is < 1");

    if (_partition_count == 1) {
      sorted_table->partitions.resize(1);
      for (auto& sorted_chunk : sorted_input_chunks) {
        for (auto& entry : sorted_chunk.values) {
          sorted_table->partitions[0].values.push_back(entry);
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
          for (auto& entry : sorted_chunk.values) {
            auto radix = get_radix<T>(entry.first, _partition_count - 1);
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
          sorted_table->partitions[partition_id].values.resize(sorted_table->partition_histogram[partition_id]);
        }

        // Move each entry into its appropriate partition
        for (auto& sorted_chunk : sorted_input_chunks) {
          for (auto& entry : sorted_chunk.values) {
            auto radix = get_radix<T>(entry.first, _partition_count - 1);
            sorted_table->partitions[radix].values.at(sorted_chunk.prefix[radix]++) = entry;
          }
        }
      }
    }

    // Sort each partition (right now std::sort -> but maybe can be replaced with
    // an algorithm more efficient, if subparts are already sorted [InsertionSort?!])
    for (auto& partition : sorted_table->partitions) {
      std::sort(partition.values.begin(), partition.values.end(),
                [](auto& value_left, auto& value_right) { return value_left.first < value_right.first; });
    }

    return sorted_table;
  }

  std::shared_ptr<std::vector<SortedChunk>> sort_input_chunks(std::shared_ptr<const Table> input,
                                                              const std::string& column_name) {

    auto sorted_chunks = std::make_shared<std::vector<SortedChunk>>(input->chunk_count());

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
            auto context = std::make_shared<SortContext>(chunk_id, sorted_chunks->at(chunk_id).values);
            column->visit(*this, context);
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

    auto& value_column = dynamic_cast<ValueColumn<T>&>(column);
    auto sort_context = std::static_pointer_cast<SortContext>(context);
    auto& output = sort_context->sort_output;
    output.resize(column.size());

    // Copy over every entry
    for (ChunkOffset chunk_offset{0}; chunk_offset < value_column.values().size(); chunk_offset++) {
    RowID row_id{sort_context->chunk_id, chunk_offset};
    output[chunk_offset] = std::pair<T, RowID>(value_column.values()[chunk_offset], row_id);
    }

    // Sort the entries
    std::sort(output.begin(), output.end(),
    [](auto& value_left, auto& value_right) { return value_left.first < value_right.first; });
  }

  void handle_dictionary_column(BaseColumn& column, std::shared_ptr<ColumnVisitableContext> context) override {

    auto& dictionary_column = dynamic_cast<DictionaryColumn<T>&>(column);
    auto sort_context = std::static_pointer_cast<SortContext>(context);
    auto& output = sort_context->sort_output;
    output.resize(column.size());

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
        output[chunk_offset] = std::pair<T, RowID>(dict->at(value_id), row_id);
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
    auto& output = sort_context->sort_output;
    output.resize(ref_column.size());

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
      output[chunk_offset] = (std::pair<T, RowID>(value, RowID{sort_context->chunk_id, chunk_offset}));
    }

    // Sort the values
    std::sort(output.begin(), output.end(),
                  [](auto& value_left, auto& value_right) { return value_left.first < value_right.first; });
  }

  T pick_sample_values(std::vector<std::map<T, uint32_t>>& sample_values, std::vector<SortedChunk> partitions) {
    auto max_value = partitions[0].values[0].first;

    for (size_t partition_number = 0; partition_number < partitions.size(); ++partition_number) {

      auto & values = partitions[partition_number].values;

      // Since the chunks are sorted, the maximum values are at the back of them
      if (max_value < values.back().first) {
        max_value = values.back().first;
      }

      // get samples
      size_t step_size = values.size() / _partition_count;
      //DebugAssert((step_size >= 1), "SortMergeJoin value_based_partitioning: step size is <= 0");
      for (size_t pos = step_size, partition_id = 0; pos < values.size() - 1; pos += step_size, partition_id++) {
        if (sample_values[partition_id].count(values[pos].first) == 0) {
          sample_values[partition_id].insert(std::pair<T, uint32_t>(values[pos].first, 1));
        } else {
          ++(sample_values[partition_id].at(values[pos].first));
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
    std::vector<T> p_values(_partition_count);
    for (size_t partition_id = 0; partition_id < _partition_count - 1; partition_id++) {
      T value{0};
      uint32_t count = 0;

      for (auto& v : sample_values[partition_id]) {
        if (v.second > count) {
          value = v.first;
          count = v.second;
        }
      }

      p_values[partition_id] = value;
    }
    p_values.back() = max_value;

    value_based_table_partitioning(_sorted_left_table, p_values);
    value_based_table_partitioning(_sorted_right_table, p_values);
  }

  void value_based_table_partitioning(std::shared_ptr<SortedTable> sort_table, std::vector<T>& p_values) {

    std::vector<std::vector<std::pair<T, RowID>>> partitions;
    partitions.resize(_partition_count);

    // for prefix computation we need to table-wide know how many entries there are for each partition
    // right now we expect an equally randomized entryset
    for (auto& i : p_values) {
      sort_table->value_histogram.insert(std::pair<T, uint32_t>(i, 0));
    }

    std::cout << "450" << std::endl;

    // Each chunk should prepare additional data to enable partitioning
    for (auto& s_chunk : sort_table->partitions) {
      for (auto& i : p_values) {
        s_chunk.value_histogram.insert(std::pair<T, uint32_t>(i, 0));
        s_chunk.prefix_v.insert(std::pair<T, uint32_t>(i, 0));
      }

      // fill histogram
      for (auto& entry : s_chunk.values) {
        for (auto& i : p_values) {
          if (entry.first <= i) {
            ++(s_chunk.value_histogram[i]);
            break;
          }
        }
      }
    }

    std::cout << "470" << std::endl;

    // Each chunk need to sequentially fill _prefix map to actually fill partition of tables in parallel
    for (auto& s_chunk : sort_table->partitions) {
      for (auto& radix : p_values) {
        s_chunk.prefix_v[radix] = sort_table->value_histogram[radix];
        sort_table->value_histogram[radix] += s_chunk.value_histogram[radix];
      }
    }

    // prepare for parallel access later on
    uint32_t i = 0;
    for (auto& radix : p_values) {
      partitions[i].resize(sort_table->value_histogram[radix]);
      ++i;
    }

    std::cout << "487" << std::endl;

    for (auto& s_chunk : sort_table->partitions) {
      for (auto& entry : s_chunk.values) {
        auto radix = get_radix<T>(entry.first, _partition_count - 1);
        partitions[radix].at(s_chunk.prefix[radix]++) = entry;
      }
    }

    std::cout << "496" << std::endl;

    // Each chunk fills (parallel) partition
    for (auto& s_chunk : sort_table->partitions) {
      for (auto& entry : s_chunk.values) {
        uint32_t partition_id = 0;
        for (auto& radix : p_values) {
          if (entry.first <= radix) {
            std::cout << "s_chunk.prefix_v.size(): " << s_chunk.prefix_v.size() << std::endl;
            std::cout << "radix: " << radix << std::endl;
            partitions[partition_id].at(s_chunk.prefix_v[radix]++) = entry;
            partition_id++;
          }
        }
      }
    }

    std::cout << "513" << std::endl;

    // move result to table
    sort_table->partitions.clear();
    sort_table->partitions.resize(partitions.size());
    for (size_t index = 0; index < partitions.size(); ++index) {
      sort_table->partitions[index].values = partitions[index];
    }

    std::cout << "522" << std::endl;

    // Sort partitions (right now std:sort -> but maybe can be replaced with
    // an algorithm more efficient, if subparts are already sorted [InsertionSort?])
    for (auto& partition : sort_table->partitions) {
      std::sort(partition.values.begin(), partition.values.end(),
                [](auto& value_left, auto& value_right) { return value_left.first < value_right.first; });
    }

    std::cout << "531" << std::endl;
  }

  uint32_t run_length(uint32_t start_index, std::vector<std::pair<T, RowID>>& values) {
    auto& value = values[start_index].first;
    uint32_t offset = 1u;
    while (start_index + offset < values.size() && value == values[start_index + offset].first) {
      offset++;
    }

    return offset;
  }

  void join_runs(size_t partition_number, size_t left_run_start, size_t left_run_end,
                 size_t right_run_start, size_t right_run_end)
  {
    std::cout << "now joining a run" << std::endl;

    auto& left_partition = _sorted_left_table->partitions[partition_number];
    auto& right_partition = _sorted_right_table->partitions[partition_number];

    auto output_left = _output_pos_lists_left[partition_number];
    auto output_right = _output_pos_lists_right[partition_number];

    auto& left_value = left_partition.values[left_run_start].first;
    auto& right_value = right_partition.values[right_run_start].first;

    std::cout << "op: " << _op << std::endl;
    if (_op == "=") {
      // Search for matching values of both partitions
      if (left_value == right_value) {
        // Match found
        // Find all same values in each table then add cross product to _output
        for (auto l = left_run_start; l <= left_run_end; l++) {
          auto& left_row_id = left_partition.values[l].second;
          for (auto r = right_run_start; r <= right_run_end; r++) {
            auto& right_row_id = right_partition.values[r].second;
            output_left->push_back(left_row_id);
            output_right->push_back(right_row_id);
          }
        }
      } else {
        // No Match found
        // Add null values when appropriate on the side which index gets increased at the end of one loop run
        if (left_value < right_value) {
          // Check for correct mode
          if (_mode == Left || _mode == Outer) {
            for (auto l = left_run_start; l <= left_run_end; l++) {
              auto& left_row_id = left_partition.values[l].second;
              output_left->push_back(left_row_id);
              output_right->push_back(RowID{ChunkID{0}, INVALID_CHUNK_OFFSET});
            }
          }
        } else {
          // Check for correct mode
          if (_mode == Right || _mode == Outer) {
            for (auto r = right_run_start; r <= right_run_end; r++) {
              auto& right_row_id = right_partition.values[r].second;
              output_left->push_back(RowID{ChunkID{0}, INVALID_CHUNK_OFFSET});
              output_right->push_back(right_row_id);
            }
          }
        }
      }
    }

    // Logic for ">" and ">=" operators
    if (_op == ">" || _op == ">=") {
      if (left_value == right_value) {
        // Fewer values have to be added in the ">" case, so traversal will end at "right_index"
        // In the ">=" case the value at "right_run_end" has still to be added, so "dependend_max_index" has to be of
        // one greater value
        auto dependend_max_index = (_op == ">") ? right_run_start : right_run_end + 1;

        // Add all smaller values of the right side (addSmallerValues method) to each left side representant (for loop)
        for (auto l = left_run_start; l <= left_run_end; l++) {
          auto& left_row_id = left_partition.values[l].second;
          addSmallerValues(partition_number, _sorted_right_table, output_right, output_left,
                           dependend_max_index, left_row_id);
        }
      } else {
        // Found right_value that is greater than left_value. That means all potential right_values before are smaller
        if (left_value < right_value) {
          for (auto l = left_run_start; l <= left_run_end; l++) {
            auto& left_row_id = left_partition.values[l].second;
            addSmallerValues(partition_number, _sorted_right_table, output_right, output_left,
                            right_run_start, left_row_id);
          }
        }
      }
    }

    // Turn around the logic of ">" and ">=" operators
    if (_op == "<" || _op == "<=") {
      if (left_value == right_value) {
        // Viewer values have to be added in the "<" case, so traversal will begin at "left_run_end + 1"
        // In the "<=" case the traversal has to start at "left_index"
        auto dependend_max_index = (_op == "<") ? right_run_end + 1 : right_run_start;

        for (auto l = left_run_start; l <= left_run_end; l++) {
          auto& left_row_id = left_partition.values[l].second;
          addGreaterValues(partition_number, _sorted_right_table, output_left, output_right,
                           dependend_max_index, left_row_id);
        }

      } else {
        // Found right_value that is greater than left_value. That means all potantial right_values following are
        // greater too
        if (left_value < right_value) {
          for (auto l = left_run_start; l <= left_run_end; l++) {
            auto& left_row_id = left_partition.values[l].second;
            addGreaterValues(partition_number, _sorted_right_table, output_left, output_right,
                             right_run_start, left_row_id);
          }
        }
      }
    }
  }

  /*
  ** Performs the join on a single partition. Looks for matches.
  */
  void join_partition(size_t partition_number) {

    size_t left_run_start = 0;
    size_t right_run_start = 0;

    auto& left_partition = _sorted_left_table->partitions[partition_number];
    auto& right_partition = _sorted_right_table->partitions[partition_number];

    const size_t left_size = left_partition.values.size();
    const size_t right_size = right_partition.values.size();

    while (left_run_start < left_size && right_run_start < right_size) {
      auto& left_value = left_partition.values[left_run_start].first;
      auto& right_value = right_partition.values[right_run_start].first;

      auto left_run_end = left_run_start + run_length(left_run_start, left_partition.values) - 1;
      auto right_run_end = right_run_start + run_length(right_run_start, right_partition.values) - 1;

      join_runs(partition_number, left_run_start, left_run_end, right_run_start, right_run_end);

      // Advance to the next run on the smaller side
      if(left_value == right_value) {
        left_run_start = left_run_end + 1;
        right_run_start = right_run_end + 1;
      }else if(left_value < right_value) {
        left_run_start = left_run_end + 1;
      }
      else{
        right_run_start = right_run_end + 1;
      }

    }

    // There is an edge case in which the last loop run was a "equi hit" and one index
    // reached its maximum size, but one element is potentially still present on the other side.
    // It is important for "Outer-Joins" to include this element of course.

    // The left side has finished -> add the remaining ones on the right side
    if (left_run_start == left_size && (_mode == Right || _mode == Outer)) {
      while(right_run_start < right_size) {
        auto right_row_id = right_partition.values[right_run_start].second;
        _output_pos_lists_left[partition_number]->push_back(RowID{ChunkID{0}, INVALID_CHUNK_OFFSET});
        _output_pos_lists_right[partition_number]->push_back(right_row_id);
        right_run_start++;
      }
    }

    // The right side has finished -> add the remaining ones on the left side
    if (right_run_start == right_size && (_mode == Left || _mode == Outer)) {
      while(left_run_start < left_size) {
        auto left_row_id = left_partition.values[left_run_start].second;
        _output_pos_lists_left[partition_number]->push_back(left_row_id);
        _output_pos_lists_right[partition_number]->push_back(RowID{ChunkID{0}, INVALID_CHUNK_OFFSET});
        left_run_start++;
      }
    }
  }

  void addSmallerValues(uint32_t partition_number, std::shared_ptr<SortedTable> table_smaller_values,
                        std::shared_ptr<PosList> output_smaller, std::shared_ptr<PosList> output_greater,
                        uint32_t max_index_smaller_values, RowID greaterId) {
    RowID smaller_value_row_id;

    for (uint32_t p_number = 0; p_number <= partition_number; ++p_number) {
      if (p_number != partition_number) {
        // Add values from previous partitions
        auto& partition_smaller_values = table_smaller_values->partitions[p_number];

        for (auto& values : partition_smaller_values.values) {
          smaller_value_row_id = values.second;
          output_smaller->push_back(smaller_value_row_id);
          output_greater->push_back(greaterId);
        }
      } else {
        // Add values from current partition
        for (uint32_t index = 0; index < max_index_smaller_values; ++index) {
          smaller_value_row_id = table_smaller_values->partitions[partition_number].values[index].second;
          output_smaller->push_back(smaller_value_row_id);
          output_greater->push_back(greaterId);
        }
      }
    }
  }

  void addGreaterValues(size_t partition_number, std::shared_ptr<SortedTable> table_greater_values,
                        std::shared_ptr<PosList> output_smaller, std::shared_ptr<PosList> output_greater,
                        size_t start_index_greater_values, RowID smallerId) {
    RowID greater_value_row_id;
    size_t partition_size = table_greater_values->partitions[partition_number].values.size();

    for (size_t p_number = partition_number; p_number < _partition_count; ++p_number) {
      if (p_number != partition_number) {
        // Add values from previous partitions
        auto& partition_greater_values = table_greater_values->partitions[p_number];

        for (auto& values : partition_greater_values.values) {
          greater_value_row_id = values.second;
          output_smaller->push_back(smallerId);
          output_greater->push_back(greater_value_row_id);
        }
      } else {
        // Add values from current partition
        for (size_t index = start_index_greater_values; index < partition_size; ++index) {
          greater_value_row_id = table_greater_values->partitions[partition_number].values[index].second;
          output_smaller->push_back(smallerId);
          output_greater->push_back(greater_value_row_id);
        }
      }
    }
  }

  std::shared_ptr<PosList> concatenate_pos_lists(std::vector<std::shared_ptr<PosList>>& pos_lists) {
    auto output = std::make_shared<PosList>();

    for (auto pos_list : pos_lists) {
      output->reserve(output->size() + pos_list->size());
      output->insert(output->end(), pos_list->begin(), pos_list->end());
    }

    return output;
  }

  void perform_join() {

    std::vector<std::shared_ptr<AbstractTask>> jobs;

    // Parallel join for each partition
    for (size_t partition_number = 0; partition_number < _partition_count; ++partition_number) {
      jobs.push_back(std::make_shared<JobTask>([this, partition_number]{
        this->join_partition(partition_number);
      }));
      jobs.back()->schedule();
    }

    CurrentScheduler::wait_for_tasks(jobs);

    // merge the pos lists into single pos lists
    _output_pos_list_left = concatenate_pos_lists(_output_pos_lists_left);
    _output_pos_list_right = concatenate_pos_lists(_output_pos_lists_right);
  }

  /**
  * Adds the columns from an input table to the operator output
  **/
  void add_output_columns(std::shared_ptr<Table> output_table, std::shared_ptr<const Table> input_table,
                          const std::string& prefix, std::shared_ptr<const PosList> pos_list) {

    auto column_count = input_table->col_count();
    for (ColumnID column_id{0}; column_id < column_count; column_id++) {

      // Add the column definition
      auto column_name = prefix + input_table->column_name(column_id);
      auto column_type = input_table->column_type(column_id);
      output_table->add_column_definition(column_name, column_type);

      // Add the column data (in the form of a poslist)
      // Check whether the referenced column is already a reference column
      const auto base_column = input_table->get_chunk(ChunkID{0}).get_column(column_id);
      const auto ref_column = std::dynamic_pointer_cast<ReferenceColumn>(base_column);
      if (ref_column) {
        // Create a pos_list referencing the original column instead of the reference column
        auto new_pos_list = dereference_pos_list(input_table, column_id, pos_list);
        auto new_ref_column = std::make_shared<ReferenceColumn>(ref_column->referenced_table(),
                                                            ref_column->referenced_column_id(), new_pos_list);
        output_table->get_chunk(ChunkID{0}).add_column(new_ref_column);
      } else {
        auto new_ref_column = std::make_shared<ReferenceColumn>(input_table, column_id, pos_list);
        output_table->get_chunk(ChunkID{0}).add_column(new_ref_column);
      }
    }
  }

  std::shared_ptr<PosList> dereference_pos_list(std::shared_ptr<const Table> input_table, ColumnID column_id,
                                                std::shared_ptr<const PosList> pos_list) {

    // Get all the input pos lists so that we only have to pointer cast the columns once
    auto input_pos_lists = std::vector<std::shared_ptr<const PosList>>();
    for (ChunkID chunk_id{0}; chunk_id < input_table->chunk_count(); chunk_id++) {
      auto b_column = input_table->get_chunk(chunk_id).get_column(column_id);
      auto r_column = std::dynamic_pointer_cast<ReferenceColumn>(b_column);
      input_pos_lists.push_back(r_column->pos_list());
    }

    // Get the row ids that are referenced
    auto new_pos_list = std::make_shared<PosList>();
    for (const auto& row : *pos_list) {
      new_pos_list->push_back(input_pos_lists.at(row.chunk_id)->at(row.chunk_offset));
    }

    return new_pos_list;
  }

 public:
  std::shared_ptr<const Table> on_execute() {
    DebugAssert(_partition_count > 0, "partition count is <= 0!");

    // Sort and partition the input tables
    _sorted_left_table = sort_table(_sort_merge_join.input_table_left(), _left_column_name);
    _sorted_right_table = sort_table(_sort_merge_join.input_table_right(), _right_column_name);

    std::cout << "Table sorting ran through" << std::endl;

    if (_op != "=" && _partition_count > 1) {
      value_based_partitioning();
      std::cout << "value based partitioning ran through" << std::endl;
    }

    perform_join();

    std::cout << "perform join ran through" << std::endl;

    auto output = std::make_shared<Table>();

    // Add the columns from both input tables to the output
    add_output_columns(output, _sort_merge_join.input_table_left(), _sort_merge_join._prefix_left, _output_pos_list_left);
    add_output_columns(output, _sort_merge_join.input_table_right(), _sort_merge_join._prefix_right, _output_pos_list_right);

    return output;
  }
};

/*
** Only used for Non-Equi Join(s) does nearly the same except we partition our tables differently
*/



/*
** End of Non-Equi Join methods
*/

}  // namespace opossum
