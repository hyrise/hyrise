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
#include "../utils/radix_partition_sort.hpp"

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
class SortMergeJoin::SortMergeJoinImpl : public AbstractJoinOperatorImpl {
 public:
  SortMergeJoinImpl<T>(SortMergeJoin& sort_merge_join, std::string left_column_name,
            std::string right_column_name, std::string op, JoinMode mode, size_t partition_count)
            : _sort_merge_join{sort_merge_join}, _left_column_name{left_column_name},
              _right_column_name{right_column_name}, _op(op), _mode(mode), _partition_count{partition_count} {
    _output_pos_lists_left.resize(_partition_count);
    _output_pos_lists_right.resize(_partition_count);
    for (size_t i = 0; i < _partition_count; i++) {
      _output_pos_lists_left[i] = std::make_shared<PosList>();
      _output_pos_lists_right[i] = std::make_shared<PosList>();
    }
  }

  virtual ~SortMergeJoinImpl() = default;

 protected:
  SortMergeJoin& _sort_merge_join;
  std::shared_ptr<SortedTable<T>> _sorted_left_table;
  std::shared_ptr<SortedTable<T>> _sorted_right_table;

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

  uint32_t run_length(uint32_t start_index, std::vector<std::pair<T, RowID>>& values) {
    auto& value = values[start_index].first;
    uint32_t offset = 1u;
    while (start_index + offset < values.size() && value == values[start_index + offset].first) {
      offset++;
    }

    return offset;
  }

  struct TableRange;
  struct TablePosition {
    TablePosition(int partition, int index) : partition{partition}, index{index} {}

    int partition;
    int index;

    TableRange to(TablePosition position) {
      return TableRange(*this, position);
    }
  };

  struct TableRange {
    TableRange(TablePosition start_position, TablePosition end_position) : start(start_position), end(end_position) {}
    TableRange(int partition, int start_index, int end_index)
      : start{TablePosition(partition, start_index)}, end{TablePosition(partition, end_index)} {}

    TablePosition start;
    TablePosition end;

    void for_every_row_id(std::function<void(RowID&)> action, std::shared_ptr<SortedTable<T>> table) {
      for (int partition = start.partition; partition <= end.partition; partition++) {
        auto& values = table->partitions[partition].values;
        int start_index = (partition == start.partition) ? start.index : 0;
        int end_index = (partition == end.partition) ? end.index : values.size();
        for (int index = start_index; index < end_index; index++) {
          action(values[index].second);
        }
      }
    }
  };

  TablePosition end_of_table(std::shared_ptr<SortedTable<T>> table) {
    auto last_partition = table->partitions.size() - 1;
    return TablePosition(last_partition, table->partitions[last_partition].values.size());
  }

  void join_runs(size_t partition_number, TableRange left_run, TableRange right_run) {
    auto& left_partition = _sorted_left_table->partitions[partition_number];
    auto& right_partition = _sorted_right_table->partitions[partition_number];

    auto output_left = _output_pos_lists_left[partition_number];
    auto output_right = _output_pos_lists_right[partition_number];

    auto& left_value = left_partition.values[left_run.start.index].first;
    auto& right_value = right_partition.values[right_run.start.index].first;

    auto end_of_left_table = end_of_table(_sorted_left_table);
    auto end_of_right_table = end_of_table(_sorted_right_table);

    // std::cout << "op: " << _op << std::endl;
    if (_op == "=") {
      // Check for a match
      if (left_value == right_value) {
        emit_combinations(partition_number, left_run, right_run);
      } else {
        // No Match found
        // Add null values when appropriate on the side which index gets increased at the end of one loop run
        if (left_value < right_value) {
          // Check for correct mode
          if (_mode == Left || _mode == Outer) {
            emit_right_null_combinations(partition_number, left_run);
          }
        } else {
          // Check for correct mode
          if (_mode == Right || _mode == Outer) {
            emit_left_null_combinations(partition_number, right_run);
          }
        }
      }
    }

    if (_op == ">") {
      if (left_value > right_value) {
        emit_combinations(partition_number, left_run.start.to(end_of_left_table), right_run);
      }
      if (left_value == right_value) {
        emit_combinations(partition_number, left_run.end.to(end_of_left_table), right_run);
      }
    }

    if (_op == ">=" && left_value >= right_value) {
      emit_combinations(partition_number, left_run.start.to(end_of_left_table), right_run);
    }

    if (_op == "<") {
      if (left_value < right_value) {
        emit_combinations(partition_number, left_run, right_run.start.to(end_of_right_table));
      }
      if (left_value == right_value) {
        emit_combinations(partition_number, left_run, right_run.end.to(end_of_right_table));
      }
    }

    if (_op == "<=" && left_value <= right_value) {
      emit_combinations(partition_number, left_run, right_run.start.to(end_of_right_table));
    }
  }

  void emit_combination(size_t output_partition, RowID& left, RowID& right) {
    _output_pos_lists_left[output_partition]->push_back(left);
    _output_pos_lists_right[output_partition]->push_back(right);
  }

  void emit_combinations(size_t output_partition, TableRange left_range, TableRange right_range) {
    left_range.for_every_row_id([this, output_partition, &right_range](RowID& left_row_id) {
      right_range.for_every_row_id([this, output_partition, &left_row_id](RowID& right_row_id){
        this->emit_combination(output_partition, left_row_id, right_row_id);
      }, _sorted_right_table);
    }, _sorted_left_table);
  }

  void emit_right_null_combinations(size_t output_partition, TableRange left_range) {
    left_range.for_every_row_id([this, output_partition](RowID& left_row_id) {
      RowID null_row{ChunkID{0}, INVALID_CHUNK_OFFSET};
      this->emit_combination(output_partition, left_row_id, null_row);
    }, _sorted_left_table);
  }

  void emit_left_null_combinations(size_t output_partition, TableRange right_range) {
    right_range.for_every_row_id([this, output_partition](RowID& right_row_id) {
      RowID null_row{ChunkID{0}, INVALID_CHUNK_OFFSET};
      this->emit_combination(output_partition, null_row, right_row_id);
    }, _sorted_right_table);
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

      auto left_run_end = left_run_start + run_length(left_run_start, left_partition.values);
      auto right_run_end = right_run_start + run_length(right_run_start, right_partition.values);

      TableRange left_run(partition_number, left_run_start, left_run_end);
      TableRange right_run(partition_number, right_run_start, right_run_end);
      join_runs(partition_number, left_run, right_run);

      // Advance to the next run on the smaller side
      if (left_value == right_value) {
        left_run_start = left_run_end;
        right_run_start = right_run_end;
      } else if (left_value < right_value) {
        left_run_start = left_run_end;
      } else {
        right_run_start = right_run_end;
      }
    }

    // There is an edge case in which the last loop run was a "equi hit" and one index
    // reached its maximum size, but one element is potentially still present on the other side.
    // It is important for "Outer-Joins" to include this element of course.

    // The left side has finished -> add the remaining ones on the right side
    if (left_run_start == left_size && (_mode == Right || _mode == Outer)) {
      emit_left_null_combinations(partition_number, TableRange(partition_number, right_run_start, right_size));
    }

    // The right side has finished -> add the remaining ones on the left side
    if (right_run_start == right_size && (_mode == Left || _mode == Outer)) {
      emit_right_null_combinations(partition_number, TableRange(partition_number, left_run_start, left_size));
    }
  }

  std::shared_ptr<PosList> concatenate_pos_lists(std::vector<std::shared_ptr<PosList>>& pos_lists) {
    auto output = std::make_shared<PosList>();

    size_t total_size = 0;
    for(auto pos_list : pos_lists) {
      total_size += pos_list->size();
    }

    output->reserve(total_size);
    for (auto pos_list : pos_lists) {
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

    auto radix_partitioner = RadixPartitionSort<T>(_sort_merge_join._input_left, _sort_merge_join._input_right,
                              *_sort_merge_join._column_names, _op, _mode, _partition_count);
    // Sort and partition the input tables
    radix_partitioner.execute();
    auto sort_output = radix_partitioner.get_output();
    _sorted_left_table = sort_output.first;
    _sorted_right_table = sort_output.second;

    perform_join();

    // std::cout << "perform join ran through" << std::endl;

    auto output = std::make_shared<Table>();

    // Add the columns from both input tables to the output
    add_output_columns(output, _sort_merge_join.input_table_left(),
                        _sort_merge_join._prefix_left, _output_pos_list_left);
    add_output_columns(output, _sort_merge_join.input_table_right(),
                        _sort_merge_join._prefix_right, _output_pos_list_right);

    return output;
  }
};

}  // namespace opossum
