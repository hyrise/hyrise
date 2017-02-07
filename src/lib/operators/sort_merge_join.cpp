#include "sort_merge_join.hpp"

#include <algorithm>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace opossum {
// TODO(Fabian): Add Numa-realted comments/information!

SortMergeJoin::SortMergeJoin(const std::shared_ptr<AbstractOperator> left,
                             const std::shared_ptr<AbstractOperator> right,
                             optional<std::pair<const std::string&, const std::string&>> column_names,
                             const std::string& op, const JoinMode mode)
    : AbstractOperator(left, right), _op{op}, _mode{mode} {
  // Check optional column names
  // Per definition either two names are specified or none
  if (column_names) {
    _left_column_name = column_names->first;
    _right_column_name = column_names->second;

    if (left == nullptr) {
      std::string message = "SortMergeJoin::SortMergeJoin: left input operator is null";
      std::cout << message << std::endl;
      throw std::exception(std::runtime_error(message));
    }

    if (right == nullptr) {
      std::string message = "SortMergeJoin::SortMergeJoin: right input operator is null";
      std::cout << message << std::endl;
      throw std::exception(std::runtime_error(message));
    }
    // Check column_type
    auto left_column_id = _input_left->column_id_by_name(_left_column_name);
    auto right_column_id = _input_right->column_id_by_name(_right_column_name);
    auto left_column_type = _input_left->column_type(left_column_id);
    auto right_column_type = _input_right->column_type(right_column_id);

    if (left_column_type != right_column_type) {
      std::string message = "SortMergeJoin::execute: column type \"" + left_column_type + "\" of left column \"" +
                            _left_column_name + "\" does not match colum type \"" + right_column_type +
                            "\" of right column \"" + _right_column_name + "\"!";
      std::cout << message << std::endl;
      throw std::exception(std::runtime_error(message));
    }
    // Create implementation to compute join result
    if (_mode != JoinMode::Cross) {
      _impl = make_unique_by_column_type<AbstractOperatorImpl, SortMergeJoinImpl>(left_column_type, *this);
    } else {
      _product = std::make_shared<Product>(left, right, "left", "right");
    }
  } else {
    // No names specified --> this is only valid if we want to cross-join
    if (_mode != JoinMode::Cross) {
      std::string message = "NestedLoopJoin::NestedLoopJoin: No columns specified for join operator";
      std::cout << message << std::endl;
      throw std::exception(std::runtime_error(message));
    } else {
      _product = std::make_shared<Product>(left, right, "left", "right");
    }
  }

  /*_pos_list_left = std::make_shared<PosList>();
  _pos_list_right = std::make_shared<PosList>();*/
}

void SortMergeJoin::execute() {
  if (_mode != JoinMode::Cross) {
    _impl->execute();
  } else {
    _product->execute();
  }
}

std::shared_ptr<const Table> SortMergeJoin::get_output() const {
  if (_mode != JoinMode::Cross) {
    return _impl->get_output();
  } else {
    return _product->get_output();
  }
}

const std::string SortMergeJoin::name() const { return "SortMergeJoin"; }

uint8_t SortMergeJoin::num_in_tables() const { return 2u; }

uint8_t SortMergeJoin::num_out_tables() const { return 1u; }

/**
** Start of implementation
**/

template <typename T>
SortMergeJoin::SortMergeJoinImpl<T>::SortMergeJoinImpl(SortMergeJoin& sort_merge_join)
    : _sort_merge_join{sort_merge_join} {
  if (_sort_merge_join._op == "=") {
    _compare = [](const T& value_left, const T& value_right) -> bool { return value_left == value_right; };
  } else {
    std::string message = "SortMergeJoinImpl::SortMergeJoinImpl: Unknown operator " + _sort_merge_join._op;
    std::cout << message << std::endl;
    throw std::exception(std::runtime_error(message));
  }
  /* right now only equi-joins supported
  if (_nested_loop_join._op == "=") {
  _compare = [](const T& value_left, const T& value_right) -> bool { return value_left == value_right; };
  } else if (_nested_loop_join._op == "<") {
  _compare = [](const T& value_left, const T& value_right) -> bool { return value_left < value_right; };
  } else if (_nested_loop_join._op == ">") {
  _compare = [](const T& value_left, const T& value_right) -> bool { return value_left < value_right; };
  } else if (_nested_loop_join._op == ">=") {
  _compare = [](const T& value_left, const T& value_right) -> bool { return value_left >= value_right; };
  } else if (_nested_loop_join._op == "<=") {
  _compare = [](const T& value_left, const T& value_right) -> bool { return value_left <= value_right; };
  } else if (_nested_loop_join._op == "!=") {
  _compare = [](const T& value_left, const T& value_right) -> bool { return value_left != value_right; };
  } else {
  std::string message = "SortMergeJoinImpl::SortMergeJoinImpl: Unknown operator " + _nested_loop_join._op;
  std::cout << message << std::endl;
  throw std::exception(std::runtime_error(message));
  }*/
}

// TODO(SvenFabian) one method to sort them all
template <typename T>
void SortMergeJoin::SortMergeJoinImpl<T>::sort_left_partition(ChunkID chunk_id) {
  auto& chunk = _sort_merge_join._input_left->get_chunk(chunk_id);
  auto column = chunk.get_column(_sort_merge_join._input_left->column_id_by_name(_sort_merge_join._left_column_name));
  // TODO(Fabian->Sven) context can be a unique_ptr right? Does it have to be a pointer at all?
  auto context = std::make_shared<SortContext>(chunk_id, true);
  column->visit(*this, context);
}

template <typename T>
void SortMergeJoin::SortMergeJoinImpl<T>::sort_left_table() {
  _sorted_left_table = std::make_shared<SortMergeJoin::SortMergeJoinImpl<T>::SortedTable>();
  _sorted_left_table->_partition.resize(_sort_merge_join._input_left->chunk_count());
  for (ChunkID chunk_id = 0; chunk_id < _sort_merge_join._input_left->chunk_count(); ++chunk_id) {
    _sorted_left_table->_partition[chunk_id]._values.resize(_sort_merge_join._input_left->chunk_size());
  }

  std::vector<std::thread> threads(_sort_merge_join._input_left->chunk_count());
  for (ChunkID chunk_id = 0; chunk_id < _sort_merge_join._input_left->chunk_count(); ++chunk_id) {
    /*
    auto& chunk = _sort_merge_join._input_left->get_chunk(chunk_id);
    auto column = chunk.get_column(_sort_merge_join._input_left->column_id_by_name(_sort_merge_join._left_column_name));
    auto context = std::make_shared<SortContext>(chunk_id, true);
    column->visit(*this, context);
    */
    threads.at(chunk_id) = std::thread(&SortMergeJoin::SortMergeJoinImpl<T>::sort_left_partition, *this, chunk_id);
  }

  for (ChunkID chunk_id = 0; chunk_id < _sort_merge_join._input_left->chunk_count(); ++chunk_id) {
    threads.at(chunk_id).join();
  }

  if (_partition_count == 1) {
    std::vector<std::pair<T, RowID>> partition_values;
    for (auto& s_chunk : _sorted_left_table->_partition) {
      for (auto& entry : s_chunk._values) {
        partition_values.push_back(entry);
      }
    }

    _sorted_left_table->_partition.clear();
    _sorted_left_table->_partition.resize(1);

    for (auto& entry : partition_values) {
      _sorted_left_table->_partition[0]._values.push_back(entry);
    }
  } else {
    // Do radix-partitioning here for _partition_count partitions
  }
  // Sort partitions (right now std:sort -> but maybe can be replaced with
  // an algorithm that is more efficient, if subparts are already sorted [InsertionSort?])
  for (auto& partition : _sorted_left_table->_partition) {
    std::sort(partition._values.begin(), partition._values.end(),
              [](auto& value_left, auto& value_right) { return value_left.first < value_right.first; });
  }
}

template <typename T>
void SortMergeJoin::SortMergeJoinImpl<T>::sort_right_partition(ChunkID chunk_id) {
  auto& chunk = _sort_merge_join._input_right->get_chunk(chunk_id);
  auto column = chunk.get_column(_sort_merge_join._input_right->column_id_by_name(_sort_merge_join._right_column_name));
  auto context = std::make_shared<SortContext>(chunk_id, false);
  column->visit(*this, context);
}

template <typename T>
void SortMergeJoin::SortMergeJoinImpl<T>::sort_right_table() {
  _sorted_right_table = std::make_shared<SortedTable>();
  _sorted_right_table->_partition.resize(_sort_merge_join._input_right->chunk_count());
  for (ChunkID chunk_id = 0; chunk_id < _sort_merge_join._input_left->chunk_count(); ++chunk_id) {
    _sorted_right_table->_partition[chunk_id]._values.resize(_sort_merge_join._input_right->chunk_size());
  }

  std::vector<std::thread> threads(_sort_merge_join._input_left->chunk_count());
  for (ChunkID chunk_id = 0; chunk_id < _sort_merge_join._input_right->chunk_count(); ++chunk_id) {
    /*
    auto& chunk = _sort_merge_join._input_right->get_chunk(chunk_id);
    auto column =
        chunk.get_column(_sort_merge_join._input_right->column_id_by_name(_sort_merge_join._right_column_name));
    auto context = std::make_shared<SortContext>(chunk_id, false);
    column->visit(*this, context);
    */
    threads.at(chunk_id) = std::thread(&SortMergeJoin::SortMergeJoinImpl<T>::sort_right_partition, *this, chunk_id);
  }

  for (ChunkID chunk_id = 0; chunk_id < _sort_merge_join._input_right->chunk_count(); ++chunk_id) {
    threads.at(chunk_id).join();
  }

  if (_partition_count == 1) {
    std::vector<std::pair<T, RowID>> partition_values;
    for (auto& s_chunk : _sorted_right_table->_partition) {
      for (auto& entry : s_chunk._values) {
        partition_values.push_back(entry);
      }
    }

    _sorted_right_table->_partition.clear();
    _sorted_right_table->_partition.resize(1);

    for (auto& entry : partition_values) {
      _sorted_right_table->_partition[0]._values.push_back(entry);
    }
  } else {
    // Do radix-partitioning here for _partition_count>1 partitions
  }
  // Sort partitions (right now std:sort -> but maybe can be replaced with
  // an algorithm more efficient, if subparts are already sorted [InsertionSort?])
  for (auto& partition : _sorted_right_table->_partition) {
    std::sort(partition._values.begin(), partition._values.end(),
              [](auto& value_left, auto& value_right) { return value_left.first < value_right.first; });
  }
}

template <typename T>
void SortMergeJoin::SortMergeJoinImpl<T>::perform_join() {
  _sort_merge_join._pos_list_left = std::make_shared<PosList>();
  _sort_merge_join._pos_list_right = std::make_shared<PosList>();
  // For now only equi-join is implemented
  // That means we only have to join partitions who are the same from both sides

  for (uint32_t partition_number = 0; partition_number < _sorted_left_table->_partition.size(); ++partition_number) {
    uint32_t left_index = 0;
    uint32_t right_index = 0;

    auto& left_current_partition = _sorted_left_table->_partition[partition_number];
    auto& right_current_partition = _sorted_right_table->_partition[partition_number];

    const size_t left_size = left_current_partition._values.size();
    const size_t right_size = right_current_partition._values.size();

    while (left_index < left_size && right_index < right_size) {
      T left_value = left_current_partition._values[left_index].first;
      T right_value = right_current_partition._values[right_index].first;
      uint32_t left_index_offset = 0;
      uint32_t right_index_offset = 0;

      // determine offset up to which all values are the same
      // left side
      for (; left_index_offset < left_size - left_index; ++left_index_offset) {
        if (left_index + left_index_offset + 1 == left_size ||
            left_value != left_current_partition._values[left_index + left_index_offset + 1].first) {
          break;
        }
      }
      // right side
      for (; right_index_offset < right_size - right_index; ++right_index_offset) {
        if (right_index_offset + right_index + 1 == right_size ||
            right_value != right_current_partition._values[right_index + right_index_offset + 1].first) {
          break;
        }
      }

      // search for matching values of both partitions
      if (left_value == right_value) {
        /* match found */
        // find all same values in each table then add product to _output
        uint32_t max_index_left = left_index + left_index_offset;
        uint32_t max_index_right = right_index_offset + right_index;
        RowID left_row_id;
        RowID right_row_id;

        for (uint32_t l_index = left_index; l_index <= max_index_left; ++l_index) {
          left_row_id = left_current_partition._values[l_index].second;

          for (uint32_t r_index = right_index; r_index <= max_index_right; ++r_index) {
            right_row_id = right_current_partition._values[r_index].second;
            _sort_merge_join._pos_list_left->push_back(left_row_id);
            _sort_merge_join._pos_list_right->push_back(right_row_id);
          }
        }

        // afterwards set index for both tables to next new value
        left_index += left_index_offset + 1u;
        right_index += right_index_offset + 1u;
      } else {
        if (left_value < right_value) {
          left_index += left_index_offset + 1u;
        } else {
          right_index += right_index_offset + 1u;
        }
      }
    }
  }
}

template <typename T>
void SortMergeJoin::SortMergeJoinImpl<T>::build_output() {
  _sort_merge_join._output = std::make_shared<Table>(0, false);
  // left output
  for (size_t column_id = 0; column_id < _sort_merge_join._input_left->col_count(); column_id++) {
    // Add the column meta data
    _sort_merge_join._output->add_column(_sort_merge_join._input_left->column_name(column_id),
                                         _sort_merge_join._input_left->column_type(column_id), false);

    // Check whether the column consists of reference columns
    /* const auto r_column =
    std::dynamic_pointer_cast<ReferenceColumn>(input_table->get_chunk(0).get_column(column_id));
    if (r_column) {
      // Create a pos_list referencing the original column
      auto new_pos_list = dereference_pos_list(input_table, column_id, pos_list);
      auto ref_column = std::make_shared<ReferenceColumn>(r_column->referenced_table(),
                                                          r_column->referenced_column_id(), new_pos_list);
      _output->get_chunk(0).add_column(ref_column);
    } else {
    */
    auto ref_column =
        std::make_shared<ReferenceColumn>(_sort_merge_join._input_left, column_id, _sort_merge_join._pos_list_left);
    _sort_merge_join._output->get_chunk(0).add_column(ref_column);
    // }
  }
  // right_output
  for (size_t column_id = 0; column_id < _sort_merge_join._input_right->col_count(); column_id++) {
    // Add the column meta data
    _sort_merge_join._output->add_column(_sort_merge_join._input_right->column_name(column_id),
                                         _sort_merge_join._input_right->column_type(column_id), false);

    // Check whether the column consists of reference columns
    /* const auto r_column =
    std::dynamic_pointer_cast<ReferenceColumn>(_sort_merge_join->_input_right->get_chunk(0).get_column(column_id));
    if (r_column) {
      // Create a pos_list referencing the original column
      auto new_pos_list = dereference_pos_list(input_table, column_id, pos_list);
      auto ref_column = std::make_shared<ReferenceColumn>(r_column->referenced_table(),
                                                          r_column->referenced_column_id(), new_pos_list);
      _output->get_chunk(0).add_column(ref_column);
    } else {
    */
    auto ref_column =
        std::make_shared<ReferenceColumn>(_sort_merge_join._input_right, column_id, _sort_merge_join._pos_list_right);
    _sort_merge_join._output->get_chunk(0).add_column(ref_column);
    // }
  }
}

template <typename T>
void SortMergeJoin::SortMergeJoinImpl<T>::execute() {
  sort_left_table();
  sort_right_table();
  perform_join();
  build_output();
}

template <typename T>
std::shared_ptr<Table> SortMergeJoin::SortMergeJoinImpl<T>::get_output() const {
  return _sort_merge_join._output;
}

template <typename T>
void SortMergeJoin::SortMergeJoinImpl<T>::handle_value_column(BaseColumn& column,
                                                              std::shared_ptr<ColumnVisitableContext> context) {
  auto& value_column = dynamic_cast<ValueColumn<T>&>(column);
  auto sort_context = std::static_pointer_cast<SortContext>(context);
  auto& sorted_table = sort_context->_write_to_sorted_left_table ? _sorted_left_table : _sorted_right_table;
  // SortedChunk chunk;

  for (ChunkOffset chunk_offset = 0; chunk_offset < value_column.values().size(); chunk_offset++) {
    RowID row_id{sort_context->_chunk_id, chunk_offset};
    sorted_table->_partition[sort_context->_chunk_id]._values[chunk_offset] =
        std::pair<T, RowID>(value_column.values()[chunk_offset], row_id);
  }

  std::sort(sorted_table->_partition[sort_context->_chunk_id]._values.begin(),
            sorted_table->_partition[sort_context->_chunk_id]._values.end(),
            [](auto& value_left, auto& value_right) { return value_left.first < value_right.first; });
  // sorted_table->_partition[sort_context->_chunk_id] = std::move(chunk);
}

template <typename T>
void SortMergeJoin::SortMergeJoinImpl<T>::handle_dictionary_column(BaseColumn& column,
                                                                   std::shared_ptr<ColumnVisitableContext> context) {
  auto& dictionary_column = dynamic_cast<DictionaryColumn<T>&>(column);
  auto sort_context = std::static_pointer_cast<SortContext>(context);
  auto& sorted_table = sort_context->_write_to_sorted_left_table ? _sorted_left_table : _sorted_right_table;
  SortedChunk chunk;

  auto value_ids = dictionary_column.attribute_vector();
  auto dict = dictionary_column.dictionary();

  std::vector<std::vector<RowID>> value_count = std::vector<std::vector<RowID>>(dict->size());

  // Collect the rows for each value id
  for (ChunkOffset chunk_offset = 0; chunk_offset < value_ids->size(); chunk_offset++) {
    value_count[value_ids->get(chunk_offset)].push_back(RowID{sort_context->_chunk_id, chunk_offset});
  }

  // Append the rows to the sorted chunk
  for (ValueID value_id = 0; value_id < dict->size(); value_id++) {
    for (auto& row_id : value_count[value_id]) {
      chunk._values.push_back(std::pair<T, RowID>(dict->at(value_id), row_id));
    }
  }

  // Chunk is already sorted now because the dictionary is sorted
  sorted_table->_partition[sort_context->_chunk_id] = std::move(chunk);
}

template <typename T>
void SortMergeJoin::SortMergeJoinImpl<T>::handle_reference_column(ReferenceColumn& reference_column_left,
                                                                  std::shared_ptr<ColumnVisitableContext> context) {
  /*
auto join_context = std::static_pointer_cast<JoinContext>(context);

auto value_column_right = std::dynamic_pointer_cast<ValueColumn<T>>(join_context->_column_right);
if (value_column_right) {
join_value_reference(*value_column_right, reference_column_left, join_context, true);
return;
}
auto dictionary_column_right = std::dynamic_pointer_cast<DictionaryColumn<T>>(join_context->_column_right);
if (dictionary_column_right) {
join_dictionary_reference(*dictionary_column_right, reference_column_left, join_context, true);
return;
}
auto reference_column_right = std::dynamic_pointer_cast<ReferenceColumn>(join_context->_column_right);
if (reference_column_right) {
join_reference_reference(reference_column_left, *reference_column_right, join_context);
return;
} */
}

}  // namespace opossum
