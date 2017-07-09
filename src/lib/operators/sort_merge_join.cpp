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

  // Validate the parameters
  if (_mode == Cross || !column_names) {
    throw std::logic_error(
        "SortMergeJoin: this operator does not support Cross Joins, the optimizer should use Product operator "
        "instead.");
  }

  if (left == nullptr) {
    throw std::runtime_error("SortMergeJoin::SortMergeJoin: left input operator is null");
  }

  if (right == nullptr) {
    throw std::runtime_error("SortMergeJoin::SortMergeJoin: right input operator is null");
  }

  // Check for valid operators "=", "<", ">", "<=", ">="
  if (op != "=" && op != "<" && op != ">" && op != "<=" && op != ">=") {
    throw std::runtime_error("SortMergeJoin::SortMergeJoin: Unknown operator " + op);
  }

  auto left_column_name = column_names->first;
  auto right_column_name = column_names->second;

  // Check column_type
  const auto& left_column_id = input_table_left()->column_id_by_name(left_column_name);
  const auto& right_column_id = input_table_right()->column_id_by_name(right_column_name);
  const auto& left_column_type = input_table_left()->column_type(left_column_id);
  const auto& right_column_type = input_table_right()->column_type(right_column_id);

  if (left_column_type != right_column_type) {
    throw std::runtime_error("SortMergeJoin::SortMergeJoin: column type \"" + left_column_type +
                             "\" of left column \"" + left_column_name + "\" does not match colum type \"" +
                             right_column_type + "\" of right column \"" + right_column_name + "\"!");
  }

  // Create implementation to compute join result
  _impl = make_unique_by_column_type<AbstractJoinOperatorImpl, SortMergeJoinImpl>(left_column_type,
    *this, left_column_name, right_column_name, 1 /* partition count */);
}

std::shared_ptr<const Table> SortMergeJoin::on_execute() { return _impl->on_execute(); }

const std::string SortMergeJoin::name() const { return "SortMergeJoin"; }

uint8_t SortMergeJoin::num_in_tables() const { return 2u; }

uint8_t SortMergeJoin::num_out_tables() const { return 1u; }

/**
** Start of implementation
**/

template <typename T>
SortMergeJoin::SortMergeJoinImpl<T>::SortMergeJoinImpl(SortMergeJoin& sort_merge_join, std::string left_column_name,
  std::string right_column_name, uint32_t partition_count) : _sort_merge_join{sort_merge_join},
  _left_column_name{left_column_name}, _right_column_name{right_column_name}, _partition_count{partition_count} {}

/**
** Executes the sort-merge-join algorithm
**/

template <typename T>
std::shared_ptr<const Table> SortMergeJoin::SortMergeJoinImpl<T>::on_execute() {
  if (_partition_count == 0u) {
    std::runtime_error("SortMergeJoinImpl::on_execute: Partition count is 0!");
  }

  // Sort the input tables
  _sorted_left_table = sort_table(_sort_merge_join.input_table_left(), _left_column_name);
  _sorted_right_table = sort_table(_sort_merge_join.input_table_right(), _right_column_name);

  std::cout << "Table sorting ran through" << std::endl;

  if (_sort_merge_join._op != "=" && _partition_count > 1) {
    value_based_partitioning();
  }

  std::cout << "value based partitioning ran through" << std::endl;

  perform_join();

  return build_output();
}

template <typename T>
std::shared_ptr<std::vector<typename SortMergeJoin::SortMergeJoinImpl<T>::SortedChunk>>
  SortMergeJoin::SortMergeJoinImpl<T>::sort_input_chunks(std::shared_ptr<const Table> input, const std::string& column_name) {

    // Resize the output vectors to the input sizes
    auto sorted_chunks = std::make_shared<std::vector<SortedChunk>>(input->chunk_count());
    for (ChunkID chunk_id{0}; chunk_id < input->chunk_count(); chunk_id++) {
      sorted_chunks->at(chunk_id).values.resize(input->get_chunk(chunk_id).size());
    }

    // can be extended to find that value dynamically later on (depending on hardware etc.)
    const uint32_t partitionSizeThreshold = 10000;
    std::vector<std::shared_ptr<AbstractTask>> jobs;

    uint32_t size = 0;
    std::vector<ChunkID> chunk_ids;

    for (ChunkID chunk_id{0}; chunk_id < input->chunk_count(); chunk_id++) {
      size += input->chunk_size();
      chunk_ids.push_back(chunk_id);
      if (size > partitionSizeThreshold || chunk_id == input->chunk_count() - 1) {
        // Spawn a job responsible for sorting multiple chunks of the input table
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

template <typename T>
std::shared_ptr<typename SortMergeJoin::SortMergeJoinImpl<T>::SortedTable>
  SortMergeJoin::SortMergeJoinImpl<T>::sort_table(std::shared_ptr<const Table> input, const std::string& column_name) {

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
    if (_sort_merge_join._op == "=") {
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

template <typename T>
void SortMergeJoin::SortMergeJoinImpl<T>::handle_value_column(BaseColumn& column,
                                                              std::shared_ptr<ColumnVisitableContext> context) {
  auto& value_column = dynamic_cast<ValueColumn<T>&>(column);
  auto sort_context = std::static_pointer_cast<SortContext>(context);
  auto& output = sort_context->sort_output;
  DebugAssert(column.size() == output.size(), "An input chunk has a different size than a sorted chunk!");

  // Copy over every entry
  for (ChunkOffset chunk_offset{0}; chunk_offset < value_column.values().size(); chunk_offset++) {
    RowID row_id{sort_context->chunk_id, chunk_offset};
    output[chunk_offset] = std::pair<T, RowID>(value_column.values()[chunk_offset], row_id);
  }

  // Sort the entries
  std::sort(output.begin(), output.end(),
            [](auto& value_left, auto& value_right) { return value_left.first < value_right.first; });
}

template <typename T>
void SortMergeJoin::SortMergeJoinImpl<T>::handle_dictionary_column(BaseColumn& column,
                                                                   std::shared_ptr<ColumnVisitableContext> context) {
  auto& dictionary_column = dynamic_cast<DictionaryColumn<T>&>(column);
  auto sort_context = std::static_pointer_cast<SortContext>(context);
  auto& output = sort_context->sort_output;
  DebugAssert(column.size() == output.size(), "An input chunk has a different size than a sorted chunk!");

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
* Sorts the contents of a reference column into a sorted_chunk
**/
template <typename T>
void SortMergeJoin::SortMergeJoinImpl<T>::handle_reference_column(ReferenceColumn& ref_column,
                                                                  std::shared_ptr<ColumnVisitableContext> context) {
  auto referenced_table = ref_column.referenced_table();
  auto referenced_column_id = ref_column.referenced_column_id();
  auto sort_context = std::static_pointer_cast<SortContext>(context);
  auto pos_list = ref_column.pos_list();
  auto& output = sort_context->sort_output;
  DebugAssert(ref_column.size() == output.size(), "An input chunk has a different size than a sorted chunk!");


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
    if (v_columns[row_id.chunk_id]) {
      value = v_columns[row_id.chunk_id]->values()[row_id.chunk_offset];
    } else if (d_columns[row_id.chunk_id]) {
      ValueID value_id = d_columns[row_id.chunk_id]->attribute_vector()->get(row_id.chunk_offset);
      value = d_columns[row_id.chunk_id]->dictionary()->at(value_id);
    } else {
      throw std::runtime_error(
          "SortMergeJoinImpl::handle_reference_column: Referenced column is neither value nor dictionary column!");
    }

    output[chunk_offset] = (std::pair<T, RowID>(value, RowID{sort_context->chunk_id, chunk_offset}));
  }

  // Sort the values
  std::sort(output.begin(), output.end(),
                [](auto& value_left, auto& value_right) { return value_left.first < value_right.first; });
}

/*
** Only used for Non-Equi Join(s) does nearly the same except we partition our tables differently
*/

template <typename T>
void SortMergeJoin::SortMergeJoinImpl<T>::value_based_partitioning() {
  // get minimum and maximum values for tables to have a somewhat reliable partitioning
  T max_value = _sorted_left_table->partitions[0].values[0].first;
  std::vector<T> p_values(_partition_count);
  std::vector<std::map<T, uint32_t>> sample_values(_partition_count);

  for (uint32_t partition_number = 0; partition_number < _sorted_left_table->partitions.size(); ++partition_number) {

    auto & values = _sorted_left_table->partitions[partition_number].values;
    // left side
    if (max_value < values.back().first) {
      max_value = values.back().first;
    }

    // get samples
    uint32_t step_size = values.size() / _partition_count;
    //DebugAssert((step_size >= 1), "SortMergeJoin value_based_partitioning: step size is <= 0");
    for (uint32_t pos = step_size, partition_id = 0; pos < values.size() - 1; pos += step_size, partition_id++) {
      if (sample_values[partition_id].count(values[pos].first) == 0) {
        sample_values[partition_id].insert(std::pair<T, uint32_t>(values[pos].first, 1));
      } else {
        ++(sample_values[partition_id].at(values[pos].first));
      }
    }
  }

  for (uint32_t partition_number = 0; partition_number < _sorted_right_table->partitions.size(); ++partition_number) {
    // right side
    auto& values = _sorted_right_table->partitions[partition_number].values;
    if (max_value < values.back().first) {
      max_value = values.back().first;
    }

    // get samples
    uint32_t step_size = values.size() / _partition_count;
    uint32_t i = 0;
    for (uint32_t pos = step_size; pos < values.size() - 1; pos += step_size) {
      if (sample_values[i].count(values[pos].first) == 0) {
        sample_values[i].insert(std::pair<T, uint32_t>(values[pos].first, 1));
      } else {
        ++(sample_values[i].at(values[pos].first));
      }
      ++i;
    }
  }

  // Pick from sample values most common split values
  for (uint32_t i = 0; i < _partition_count - 1; ++i) {
    T value{0};
    uint32_t count = 0;
    for (auto& v : sample_values[i]) {
      if (v.second > count) {
        value = v.first;
        count = v.second;
      }
    }

    p_values[i] = value;
  }
  p_values.back() = max_value;

  value_based_table_partitioning(_sorted_left_table, p_values);
  value_based_table_partitioning(_sorted_right_table, p_values);
}

template <typename T>
void SortMergeJoin::SortMergeJoinImpl<T>::value_based_table_partitioning(std::shared_ptr<SortedTable> sort_table,
                                                                         std::vector<T>& p_values) {
  std::vector<std::vector<std::pair<T, RowID>>> partitions;
  partitions.resize(_partition_count);

  // for prefix computation we need to table-wide know how many entries there are for each partition
  // right now we expect an equally randomized entryset
  for (auto& i : p_values) {
    sort_table->value_histogram.insert(std::pair<T, uint32_t>(i, 0));
  }

  std::cout << "406" << std::endl;

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

  std::cout << "426" << std::endl;

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

  std::cout << "443" << std::endl;

  /*
  for (auto& s_chunk : sorted_table->partition) {
    for (auto& entry : s_chunk.values) {
      auto radix = get_radix<T>(entry.first, _partition_count - 1);
      partitions[radix].at(s_chunk.prefix[radix]++) = entry;
    }
  }
  */

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

  std::cout << "458" << std::endl;

  // move result to table
  sort_table->partitions.clear();
  sort_table->partitions.resize(partitions.size());
  for (size_t index = 0; index < partitions.size(); ++index) {
    sort_table->partitions[index].values = partitions[index];
  }

  std::cout << "467" << std::endl;

  // Sort partitions (right now std:sort -> but maybe can be replaced with
  // an algorithm more efficient, if subparts are already sorted [InsertionSort?])
  for (auto& partition : sort_table->partitions) {
    std::sort(partition.values.begin(), partition.values.end(),
              [](auto& value_left, auto& value_right) { return value_left.first < value_right.first; });
  }

  std::cout << "476" << std::endl;
}


/*
** End of Non-Equi Join methods
*/

/*
** Performs the join on a single partition.
*/
template <typename T>
void SortMergeJoin::SortMergeJoinImpl<T>::partition_join(uint32_t partition_number,
                                                         std::vector<PosList>& pos_lists_left,
                                                         std::vector<PosList>& pos_lists_right) {
  uint32_t left_index = 0;
  uint32_t right_index = 0;

  auto& left_current_partition = _sorted_left_table->partitions[partition_number];
  auto& right_current_partition = _sorted_right_table->partitions[partition_number];

  const size_t left_size = left_current_partition.values.size();
  const size_t right_size = right_current_partition.values.size();

  uint32_t left_index_offset = 0u;
  uint32_t right_index_offset = 0u;

  uint32_t max_left_index;
  uint32_t max_right_index;
  uint32_t dependend_max_index;

  RowID left_row_id;
  RowID right_row_id;

  while (left_index < left_size && right_index < right_size) {
    T left_value = left_current_partition.values[left_index].first;
    T right_value = right_current_partition.values[right_index].first;

    left_index_offset = 0u;
    right_index_offset = 0u;

    // Determine offset up to which all values are the same
    // Left side
    for (; left_index_offset < left_size - left_index; ++left_index_offset) {
      if (left_index + left_index_offset + 1 == left_size ||
          left_value != left_current_partition.values[left_index + left_index_offset + 1].first) {
        break;
      }
    }

    // Right side
    for (; right_index_offset < right_size - right_index; ++right_index_offset) {
      if (right_index_offset + right_index + 1 == right_size ||
          right_value != right_current_partition.values[right_index + right_index_offset + 1].first) {
        break;
      }
    }

    max_left_index = left_index + left_index_offset;
    max_right_index = right_index + right_index_offset;

    if (_sort_merge_join._op == "=") {
      // Search for matching values of both partitions
      if (left_value == right_value) {
        // Match found
        // Find all same values in each table then add cross product to _output
        for (uint32_t l_index = left_index; l_index <= max_left_index; ++l_index) {
          left_row_id = left_current_partition.values[l_index].second;

          for (uint32_t r_index = right_index; r_index <= max_right_index; ++r_index) {
            right_row_id = right_current_partition.values[r_index].second;
            pos_lists_left[partition_number].push_back(left_row_id);
            pos_lists_right[partition_number].push_back(right_row_id);
          }
        }

        // Afterwards set index for both tables to next new value
        left_index += left_index_offset + 1u;
        right_index += right_index_offset + 1u;

      } else {
        // No Match found
        // Add null values when appropriate on the side which index gets increased at the end of one loop run
        if (left_value < right_value) {
          // Check for correct mode
          if (_sort_merge_join._mode == Left || _sort_merge_join._mode == Outer) {
            uint32_t max_left_index = left_index + left_index_offset;
            RowID left_row_id;

            for (uint32_t l_index = left_index; l_index <= max_left_index; ++l_index) {
              left_row_id = left_current_partition.values[l_index].second;

              pos_lists_left[partition_number].push_back(left_row_id);
              pos_lists_right[partition_number].push_back(RowID{ChunkID{0}, INVALID_CHUNK_OFFSET});
            }
          }
        } else {
          // Check for correct mode
          if (_sort_merge_join._mode == Right || _sort_merge_join._mode == Outer) {
            uint32_t max_right_index = right_index_offset + right_index;
            RowID right_row_id;

            for (uint32_t r_index = right_index; r_index <= max_right_index; ++r_index) {
              right_row_id = right_current_partition.values[r_index].second;

              pos_lists_left[partition_number].push_back(RowID{ChunkID{0}, INVALID_CHUNK_OFFSET});
              pos_lists_right[partition_number].push_back(right_row_id);
            }
          }
        }

        // Determine which index has to get incremented
        // Set the smaller side to next new value
        if (left_value < right_value) {
          left_index += left_index_offset + 1u;
        } else {
          right_index += right_index_offset + 1u;
        }
      }
    }

    // Logic for ">" and ">=" operators
    if (_sort_merge_join._op == ">" || _sort_merge_join._op == ">=") {
      if (left_value == right_value) {
        // Viewer values have to be added in the ">" case, so traversal will end at "right_index"
        // In the ">=" case the value at "max_right_index" has still to be added, so "dependend_max_index" has to be of
        // one greater value
        dependend_max_index = (_sort_merge_join._op == ">") ? right_index : max_right_index + 1;

        // Add all smaller values of the right side (addSmallerValues method) to each left side representant (for loop)
        for (uint32_t l_index = left_index; l_index <= max_left_index; ++l_index) {
          left_row_id = left_current_partition.values[l_index].second;
          addSmallerValues(partition_number, _sorted_right_table, pos_lists_right, pos_lists_left, dependend_max_index,
                           left_row_id);
        }

        // Afterwards set index for both tables to next new value
        left_index += left_index_offset + 1u;
        right_index += right_index_offset + 1u;

      } else {
        // Found right_value that is greater than left_value. That means all potantial right_values before are smaller
        if (left_value < right_value) {
          for (uint32_t l_index = left_index; l_index <= max_left_index; ++l_index) {
            left_row_id = left_current_partition.values[l_index].second;
            addSmallerValues(partition_number, _sorted_right_table, pos_lists_right, pos_lists_left, right_index,
                             left_row_id);
          }
        }

        // Determine which index has to get incremented
        // Set the smaller side to next new value
        if (left_value < right_value) {
          left_index += left_index_offset + 1u;
        } else {
          right_index += right_index_offset + 1u;
        }
      }
    }

    // Turn around the logic of ">" and ">=" operators
    if (_sort_merge_join._op == "<" || _sort_merge_join._op == "<=") {
      if (left_value == right_value) {
        // Viewer values have to be added in the "<" case, so traversal will begin at "max_left_index + 1"
        // In the "<=" case the traversal has to start at "left_index"
        dependend_max_index = (_sort_merge_join._op == "<") ? max_right_index + 1 : right_index;

        for (uint32_t l_index = left_index; l_index <= max_left_index; ++l_index) {
          left_row_id = left_current_partition.values[l_index].second;
          addGreaterValues(partition_number, _sorted_right_table, pos_lists_left, pos_lists_right, dependend_max_index,
                           left_row_id);
        }

        // Afterwards set index for both tables to next new value
        left_index += left_index_offset + 1u;
        right_index += right_index_offset + 1u;

      } else {
        // Found right_value that is greater than left_value. That means all potantial right_values following are
        // greater too
        if (left_value < right_value) {
          for (uint32_t l_index = left_index; l_index <= max_left_index; ++l_index) {
            left_row_id = left_current_partition.values[l_index].second;
            addGreaterValues(partition_number, _sorted_right_table, pos_lists_left, pos_lists_right, right_index,
                             left_row_id);
          }
        }

        // Determine which index has to get incremented
        // Set the smaller side to next new value
        if (left_value < right_value) {
          left_index += left_index_offset + 1u;
        } else {
          right_index += right_index_offset + 1u;
        }
      }
    }
  }

  // The while loop may have ended, but the last element gets skipped in certain cases.
  // But it is important for "Outer-Joins" to include this element of course.
  // The first if-condition is mandatory: Putting this whole part into the while loop in the "not equal part" both
  // indexis reaching their maximum size is not possible. So one could think to put it there instead, to safe up this
  // additional if-condition. But there is an edge case in which the last loop run was a "equi hit" and one index
  // reaching its maximum size, but one element potentially is still present on the other side.
  if (!(left_index == left_size && right_index == right_size)) {
    // The left side has finished -> add the remaining ones on the right side
    if (left_index == left_size) {
      if (_sort_merge_join._mode == Right || _sort_merge_join._mode == Outer) {
        RowID right_row_id;

        for (; right_index < right_size; ++right_index) {
          right_row_id = right_current_partition.values[right_index].second;
          pos_lists_left[partition_number].push_back(RowID{ChunkID{0}, INVALID_CHUNK_OFFSET});
          pos_lists_right[partition_number].push_back(right_row_id);
        }
      }
    }

    // The right side has finished -> add the remaining ones on the left side
    if (right_index == right_size) {
      if (_sort_merge_join._mode == Left || _sort_merge_join._mode == Outer) {
        RowID left_row_id;

        for (; left_index < left_size; ++left_index) {
          left_row_id = left_current_partition.values[left_index].second;
          pos_lists_left[partition_number].push_back(left_row_id);
          pos_lists_right[partition_number].push_back(RowID{ChunkID{0}, INVALID_CHUNK_OFFSET});
        }
      }
    }
  }
}

template <typename T>
void SortMergeJoin::SortMergeJoinImpl<T>::addSmallerValues(
    uint32_t partition_number, std::shared_ptr<SortMergeJoinImpl::SortedTable>& table_smaller_values,
    std::vector<PosList>& pos_list_smaller, std::vector<PosList>& pos_list_greater, uint32_t max_index_smaller_values,
    RowID greaterId) {
  RowID smaller_value_row_id;

  for (uint32_t p_number = 0; p_number <= partition_number; ++p_number) {
    if (p_number != partition_number) {
      // Add values from previous partitions
      auto& partition_smaller_values = table_smaller_values->partitions[p_number];

      for (auto& values : partition_smaller_values.values) {
        smaller_value_row_id = values.second;
        pos_list_smaller[partition_number].push_back(smaller_value_row_id);
        pos_list_greater[partition_number].push_back(greaterId);
      }
    } else {
      // Add values from current partition
      for (uint32_t index = 0; index < max_index_smaller_values; ++index) {
        smaller_value_row_id = table_smaller_values->partitions[partition_number].values[index].second;
        pos_list_smaller[partition_number].push_back(smaller_value_row_id);
        pos_list_greater[partition_number].push_back(greaterId);
      }
    }
  }
}

template <typename T>
void SortMergeJoin::SortMergeJoinImpl<T>::addGreaterValues(
    uint32_t partition_number, std::shared_ptr<SortMergeJoinImpl::SortedTable>& table_greater_values,
    std::vector<PosList>& pos_list_smaller, std::vector<PosList>& pos_list_greater, uint32_t start_index_greater_values,
    RowID smallerId) {
  RowID greater_value_row_id;
  size_t partition_size = table_greater_values->partitions[partition_number].values.size();

  for (uint32_t p_number = partition_number; p_number < _partition_count; ++p_number) {
    if (p_number != partition_number) {
      // Add values from previous partitions
      auto& partition_greater_values = table_greater_values->partitions[p_number];

      for (auto& values : partition_greater_values.values) {
        greater_value_row_id = values.second;
        pos_list_smaller[partition_number].push_back(smallerId);
        pos_list_greater[partition_number].push_back(greater_value_row_id);
      }
    } else {
      // Add values from current partition
      for (uint32_t index = start_index_greater_values; index < partition_size; ++index) {
        greater_value_row_id = table_greater_values->partitions[partition_number].values[index].second;
        pos_list_smaller[partition_number].push_back(smallerId);
        pos_list_greater[partition_number].push_back(greater_value_row_id);
      }
    }
  }
}

template <typename T>
void SortMergeJoin::SortMergeJoinImpl<T>::perform_join() {
  _pos_list_left = std::make_shared<PosList>();
  _pos_list_right = std::make_shared<PosList>();

  std::vector<PosList> pos_lists_left(_partition_count);
  std::vector<PosList> pos_lists_right(_partition_count);

  std::vector<std::shared_ptr<AbstractTask>> jobs;

  // Parallel join for each partition
  for (uint32_t partition_number = 0; partition_number < _partition_count; ++partition_number) {
    jobs.push_back(std::make_shared<JobTask>([this, partition_number, &pos_lists_left, &pos_lists_right]{
      this->partition_join(partition_number, std::ref(pos_lists_left), std::ref(pos_lists_right));
    }));
    jobs.back()->schedule();
  }

  CurrentScheduler::wait_for_tasks(jobs);

  // Merge pos_lists_left of partitions together
  for (auto& p_list : pos_lists_left) {
    _pos_list_left->reserve(_pos_list_left->size() + p_list.size());
    _pos_list_left->insert(_pos_list_left->end(), p_list.begin(), p_list.end());
  }

  // Merge pos_lists_right of partitions together
  for (auto& p_list : pos_lists_right) {
    _pos_list_right->reserve(_pos_list_right->size() + p_list.size());
    _pos_list_right->insert(_pos_list_right->end(), p_list.begin(), p_list.end());
  }
}

/**
* Adds the columns from an input table to the operator output
**/
template <typename T>
void SortMergeJoin::SortMergeJoinImpl<T>::add_output_columns(std::shared_ptr<Table> output_table,
                          std::shared_ptr<const Table> input_table, const std::string& prefix,
                          std::shared_ptr<const PosList> pos_list) {
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

template <typename T>
std::shared_ptr<Table> SortMergeJoin::SortMergeJoinImpl<T>::build_output() {

  auto output = std::make_shared<Table>();

  // Add the columns from both input tables to the output
  add_output_columns(output, _sort_merge_join.input_table_left(), _sort_merge_join._prefix_left, _pos_list_left);
  add_output_columns(output, _sort_merge_join.input_table_right(), _sort_merge_join._prefix_right, _pos_list_right);

  return output;
}

template <typename T>
std::shared_ptr<PosList> SortMergeJoin::SortMergeJoinImpl<T>::dereference_pos_list(
    std::shared_ptr<const Table> input_table, ColumnID column_id, std::shared_ptr<const PosList> pos_list) {
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

}  // namespace opossum
