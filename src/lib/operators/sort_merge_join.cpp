#include "sort_merge_join.hpp"

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace opossum {
// TODO(Fabian): Comment everything!!

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

template <typename T>
void SortMergeJoin::SortMergeJoinImpl<T>::sort_left_table() {
  _sorted_left_table = std::make_shared<SortMergeJoin::SortMergeJoinImpl<T>::SortedTable>();
  _sorted_left_table->_chunks.resize(_sort_merge_join._input_left->chunk_count());
  for (ChunkID chunk_id = 0; chunk_id < _sort_merge_join._input_left->chunk_count(); ++chunk_id) {
    auto& chunk = _sort_merge_join._input_left->get_chunk(chunk_id);
    auto column = chunk.get_column(_sort_merge_join._input_left->column_id_by_name(_sort_merge_join._left_column_name));
    auto context = std::make_shared<SortContext>(chunk_id, true);
    column->visit(*this, context);
  }
  if (_partition_count == 1) {
    std::vector<std::pair<T, RowID>> partition_values;
    for (auto& s_chunk : _sorted_left_table->_chunks) {
      for (auto& entry : s_chunk._values) {
        partition_values.push_back(entry);
      }
    }
    _sorted_left_table->_chunks.clear();
    for (auto& entry : partition_values) {
      _sorted_left_table->_chunks[0]._values.push_back(entry);
    }
  } else {
    // Do radix-partitioning here for _partition_count partitions
  }
  // Sort partitions (right now std:sort -> but maybe can be replaced with
  // an algorithm that is more efficient, if subparts are already sorted [InsertionSort?])
  for (auto& partition : _sorted_left_table->_chunks) {
    std::sort(partition._values.begin(), partition._values.end(),
              [](auto& value_left, auto& value_right) { return value_left.first < value_right.first; });
  }
}

template <typename T>
void SortMergeJoin::SortMergeJoinImpl<T>::sort_right_table() {
  _sorted_right_table = std::make_shared<SortedTable>();
  _sorted_right_table->_chunks.resize(_sort_merge_join._input_right->chunk_count());
  for (ChunkID chunk_id = 0; chunk_id < _sort_merge_join._input_right->chunk_count(); ++chunk_id) {
    auto& chunk = _sort_merge_join._input_right->get_chunk(chunk_id);
    auto column =
        chunk.get_column(_sort_merge_join._input_right->column_id_by_name(_sort_merge_join._right_column_name));
    auto context = std::make_shared<SortContext>(chunk_id, false);
    column->visit(*this, context);
  }
  if (_partition_count == 1) {
    std::vector<std::pair<T, RowID>> partition_values;
    for (auto& s_chunk : _sorted_right_table->_chunks) {
      for (auto& entry : s_chunk._values) {
        partition_values.push_back(entry);
      }
    }
    _sorted_right_table->_chunks.clear();
    for (auto& entry : partition_values) {
      _sorted_right_table->_chunks[0]._values.push_back(entry);
    }
  } else {
    // Do radix-partitioning here for _partition_count>1 partitions
  }
  // Sort partitions (right now std:sort -> but maybe can be replaced with
  // an algorithm more efficient, if subparts are already sorted [InsertionSort?])
  for (auto& partition : _sorted_left_table->_chunks) {
    std::sort(partition._values.begin(), partition._values.end(),
              [](auto& value_left, auto& value_right) { return value_left.first < value_right.first; });
  }
}

template <typename T>
void SortMergeJoin::SortMergeJoinImpl<T>::perform_join() {
  // For now only equi-join is implemented
  // That means we only have to join partitions who are the same from both sides
  for (uint32_t partition_number = 0; partition_number < _sorted_left_table->_chunks.size(); ++partition_number) {
    // join partition
    uint32_t left_index = 0;
    uint32_t right_index = 0;
    while (left_index < _sorted_left_table->_chunks[partition_number]._values.size() &&
           right_index < _sorted_right_table->_chunks[partition_number]._values.size()) {
      T left_value = _sorted_left_table->_chunks[partition_number]._values[left_index].first;
      T right_value = _sorted_right_table->_chunks[partition_number]._values[right_index].first;
      if (left_value != right_value) {
        if (left_value < right_value) {
          ++left_index;
        } else {
          ++right_index;
        }
      } else /* match found */ {
        // find all same values in each table then add product to _output
        // afterwards set index for both tables to next new value
      }
    }
  }
}

template <typename T>
void SortMergeJoin::SortMergeJoinImpl<T>::execute() {
  sort_left_table();
  sort_right_table();
  perform_join();
}

template <typename T>
std::shared_ptr<Table> SortMergeJoin::SortMergeJoinImpl<T>::get_output() const {
  return _sort_merge_join._output;
}

template <typename T>
void SortMergeJoin::SortMergeJoinImpl<T>::join_value_value(ValueColumn<T>& left, ValueColumn<T>& right,
                                                           std::shared_ptr<SortContext> context, bool reverse_order) {}

template <typename T>
void SortMergeJoin::SortMergeJoinImpl<T>::join_value_dictionary(ValueColumn<T>& left, DictionaryColumn<T>& right,
                                                                std::shared_ptr<SortContext> context,
                                                                bool reverse_order) {}

template <typename T>
void SortMergeJoin::SortMergeJoinImpl<T>::join_value_reference(ValueColumn<T>& left, ReferenceColumn& right,
                                                               std::shared_ptr<SortContext> context,
                                                               bool reverse_order) {}

template <typename T>
void SortMergeJoin::SortMergeJoinImpl<T>::join_dictionary_dictionary(DictionaryColumn<T>& left,
                                                                     DictionaryColumn<T>& right,
                                                                     std::shared_ptr<SortContext> context,
                                                                     bool reverse_order) {}

template <typename T>
void SortMergeJoin::SortMergeJoinImpl<T>::join_dictionary_reference(DictionaryColumn<T>& left, ReferenceColumn& right,
                                                                    std::shared_ptr<SortContext> context,
                                                                    bool reverse_order) {}

template <typename T>
void SortMergeJoin::SortMergeJoinImpl<T>::join_reference_reference(ReferenceColumn& left, ReferenceColumn& right,
                                                                   std::shared_ptr<SortContext> context,
                                                                   bool reverse_order) {}

template <typename T>
void SortMergeJoin::SortMergeJoinImpl<T>::handle_value_column(BaseColumn& column,
                                                              std::shared_ptr<ColumnVisitableContext> context) {
  auto& value_column = dynamic_cast<ValueColumn<T>&>(column);
  auto sort_context = std::static_pointer_cast<SortContext>(context);
  auto& sorted_table = sort_context->_write_to_sorted_left_table ? _sorted_left_table : _sorted_right_table;
  SortedChunk chunk;

  for (ChunkOffset chunk_offset = 0; chunk_offset < value_column.values().size(); chunk_offset++) {
    RowID row_id{sort_context->_chunk_id, chunk_offset};
    chunk._values.push_back(std::pair<T, RowID>(value_column.values()[chunk_offset], row_id));
  }

  std::sort(chunk._values.begin(), chunk._values.end(),
            [](auto& value_left, auto& value_right) { return value_left.first < value_right.first; });
  sorted_table->_chunks[sort_context->_chunk_id] = std::move(chunk);
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
  sorted_table->_chunks[sort_context->_chunk_id] = std::move(chunk);
}

template <typename T>
void SortMergeJoin::SortMergeJoinImpl<T>::handle_reference_column(ReferenceColumn& ref_column,
                                                                  std::shared_ptr<ColumnVisitableContext> context) {
  auto referenced_table = ref_column.referenced_table();
  auto referenced_column_id = ref_column.referenced_column_id();
  auto sort_context = std::static_pointer_cast<SortContext>(context);
  auto sorted_table = sort_context->_write_to_sorted_left_table ? _sorted_left_table : _sorted_right_table;
  auto pos_list = ref_column.pos_list();
  SortedChunk chunk;

  // Retrieve the columns from the referenced table so they only have to be casted once
  auto v_columns = std::vector<std::shared_ptr<ValueColumn<T>>>(referenced_table->chunk_count());
  auto d_columns = std::vector<std::shared_ptr<DictionaryColumn<T>>>(referenced_table->chunk_count());
  for (ChunkID chunk_id = 0; chunk_id < referenced_table->chunk_count(); chunk_id++) {
    v_columns[chunk_id] = std::dynamic_pointer_cast<ValueColumn<T>>(
        referenced_table->get_chunk(chunk_id).get_column(referenced_column_id));
    d_columns[chunk_id] = std::dynamic_pointer_cast<DictionaryColumn<T>>(
        referenced_table->get_chunk(chunk_id).get_column(referenced_column_id));
  }

  // Retrieve the values from the referenced columns
  for (ChunkOffset chunk_offset = 0; chunk_offset < pos_list->size(); chunk_offset++) {
    const auto& row_id = pos_list->at(chunk_offset);

    // Dereference the value
    T value;
    if (v_columns[chunk_offset]) {
      value = v_columns[row_id.chunk_id]->values()[row_id.chunk_offset];
    } else if (d_columns[chunk_offset]) {
      ValueID value_id = d_columns[row_id.chunk_id]->attribute_vector()->get(row_id.chunk_offset);
      value = d_columns[row_id.chunk_id]->dictionary()->at(value_id);
    } else {
      throw std::runtime_error(
          "SortMergeJoinImpl::handle_reference_column: referenced column is neither value nor dictionary column!");
    }

    chunk._values.push_back(std::pair<T, RowID>(value, RowID{sort_context->_chunk_id, chunk_offset}));
  }

  // Sort the values and append the chunk to the sorted table
  std::sort(chunk._values.begin(), chunk._values.end(),
            [](auto& value_left, auto& value_right) { return value_left.first < value_right.first; });
  sorted_table->_chunks[sort_context->_chunk_id] = std::move(chunk);
}

}  // namespace opossum
