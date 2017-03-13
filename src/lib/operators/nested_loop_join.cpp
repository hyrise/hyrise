#include "nested_loop_join.hpp"

#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace opossum {

NestedLoopJoin::NestedLoopJoin(const std::shared_ptr<AbstractOperator> left,
                               const std::shared_ptr<AbstractOperator> right,
                               optional<std::pair<const std::string&, const std::string&>> column_names,
                               const std::string& op, const JoinMode mode)
    : AbstractOperator(left, right), _op{op}, _mode{mode} {
  // Check optional column names
  // Per definition either two names are specified or none
  if (column_names) {
    _left_column_name = column_names->first;
    _right_column_name = column_names->second;
  } else {
    // No names specified --> this is only valid if we want to cross-join
    if (_mode != JoinMode::Cross) {
      std::string message = "NestedLoopJoin::NestedLoopJoin: No columns specified for join operator";
      throw std::runtime_error(message);
    }
  }
  if (left == nullptr) {
    std::string message = "NestedLoopJoin::NestedLoopJoin: left input operator is null";
    throw std::runtime_error(message);
  }

  if (right == nullptr) {
    std::string message = "NestedLoopJoin::NestedLoopJoin: right input operator is null";
    throw std::runtime_error(message);
  }
  // If cross-join, we use the functionality already provided by product to compute the result
  if (_mode == JoinMode::Cross) {
    _product = std::make_shared<Product>(left, right);
  }

  _output = std::make_shared<Table>(0, false);
  _pos_list_left = std::make_shared<PosList>();
  _left_match = std::vector<bool>(_input_left->row_count());
  _pos_list_right = std::make_shared<PosList>();
  _right_match = std::vector<bool>(_input_right->row_count());
}

std::shared_ptr<PosList> NestedLoopJoin::dereference_pos_list(std::shared_ptr<const Table> input_table,
                                                              size_t column_id,
                                                              std::shared_ptr<const PosList> pos_list) {
  // Get all the input pos lists so that we only have to pointer cast the columns once
  auto input_pos_lists = std::vector<std::shared_ptr<const PosList>>();
  for (ChunkID chunk_id = 0; chunk_id < input_table->chunk_count(); chunk_id++) {
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

void NestedLoopJoin::append_columns_to_output(std::shared_ptr<const Table> input_table,
                                              std::shared_ptr<PosList> pos_list) {
  // Append each column of the input column to the output
  for (size_t column_id = 0; column_id < input_table->col_count(); column_id++) {
    // Add the column meta data
    _output->add_column(input_table->column_name(column_id), input_table->column_type(column_id), false);

    // Check whether the column consists of reference columns
    const auto r_column = std::dynamic_pointer_cast<ReferenceColumn>(input_table->get_chunk(0).get_column(column_id));
    if (r_column) {
      // Create a pos_list referencing the original column
      auto new_pos_list = dereference_pos_list(input_table, column_id, pos_list);
      auto ref_column = std::make_shared<ReferenceColumn>(r_column->referenced_table(),
                                                          r_column->referenced_column_id(), new_pos_list);
      _output->get_chunk(0).add_column(ref_column);
    } else {
      auto ref_column = std::make_shared<ReferenceColumn>(input_table, column_id, pos_list);
      _output->get_chunk(0).add_column(ref_column);
    }
  }
}

// Join two columns of the input tables
void NestedLoopJoin::join_columns(size_t left_column_id, size_t right_column_id, std::string left_column_type) {
  auto impl = make_shared_by_column_type<ColumnVisitable, NestedLoopJoinImpl>(left_column_type, *this);
  // For each combination of chunks from both input tables call visitor pattern to actually perform the join.
  for (ChunkID chunk_id_left = 0; chunk_id_left < _input_left->chunk_count(); ++chunk_id_left) {
    for (ChunkID chunk_id_right = 0; chunk_id_right < _input_right->chunk_count(); ++chunk_id_right) {
      auto& chunk_left = _input_left->get_chunk(chunk_id_left);
      auto column_left = chunk_left.get_column(left_column_id);
      auto& chunk_right = _input_right->get_chunk(chunk_id_right);
      auto column_right = chunk_right.get_column(right_column_id);

      auto context = std::make_shared<JoinContext>(column_left, column_right, chunk_id_left, chunk_id_right, _mode);
      column_left->visit(*impl, context);
    }
  }
}

void NestedLoopJoin::add_outer_join_rows() {
  // Fill the table with non matching rows for outer join modes
  if (_mode == JoinMode::Left_outer || _mode == JoinMode::Full_outer) {
    for (size_t i = 0; i < _input_left->row_count(); i++) {
      if (!_left_match.at(i)) {
        RowID row_id = RowID{static_cast<ChunkID>(i / _input_left->chunk_size()),
                             static_cast<ChunkOffset>(i % _input_left->chunk_size())};
        _pos_list_left->push_back(row_id);
        _pos_list_right->push_back(NULL_ROW);
      }
    }
  }

  if (_mode == JoinMode::Right_outer || _mode == JoinMode::Full_outer) {
    for (size_t i = 0; i < _input_right->row_count(); i++) {
      if (!_right_match.at(i)) {
        RowID row_id = RowID{static_cast<ChunkID>(i / _input_right->chunk_size()),
                             static_cast<ChunkOffset>(i % _input_right->chunk_size())};
        _pos_list_right->push_back(row_id);
        _pos_list_left->push_back(NULL_ROW);
      }
    }
  }
}

void NestedLoopJoin::execute() {
  // output is equal to result of product in case of croos-join
  if (_mode == JoinMode::Cross) {
    _product->execute();
    return;
  }

  // Get types and ids of the input columns
  auto left_column_id = _input_left->column_id_by_name(_left_column_name);
  auto right_column_id = _input_right->column_id_by_name(_right_column_name);
  auto left_column_type = _input_left->column_type(left_column_id);
  auto right_column_type = _input_right->column_type(right_column_id);

  // Ensure matching column types
  // We suggest to only join matching types, as for different types there exists no basic/standard rules to provide a
  // valid result.
  // This can be later extended if we have those.
  if (left_column_type != right_column_type) {
    std::string message = "NestedLoopJoin::execute: column type \"" + left_column_type + "\" of left column \"" +
                          _left_column_name + "\" does not match colum type \"" + right_column_type +
                          "\" of right column \"" + _right_column_name + "\"!";
    throw std::runtime_error(message);
  }

  join_columns(left_column_id, right_column_id, left_column_type);

  if (_mode != JoinMode::Inner) {
    add_outer_join_rows();
  }

  append_columns_to_output(_input_left, _pos_list_left);
  append_columns_to_output(_input_right, _pos_list_right);
}

std::shared_ptr<const Table> NestedLoopJoin::get_output() const {
  return (_mode == JoinMode::Cross) ? _product->get_output() : _output;
}

const std::string NestedLoopJoin::name() const { return "NestedLoopJoin"; }

uint8_t NestedLoopJoin::num_in_tables() const { return 2u; }

uint8_t NestedLoopJoin::num_out_tables() const { return 1u; }

template <typename T>
NestedLoopJoin::NestedLoopJoinImpl<T>::NestedLoopJoinImpl(NestedLoopJoin& nested_loop_join)
    : _nested_loop_join{nested_loop_join} {
  // No compare function is necessary for the cross join
  if (_nested_loop_join._mode == JoinMode::Cross) {
    return;
  }

  if (_nested_loop_join._op == "=") {
    _compare = [](const T& value_left, const T& value_right) -> bool { return value_left == value_right; };
  } else if (_nested_loop_join._op == "<") {
    _compare = [](const T& value_left, const T& value_right) -> bool { return value_left < value_right; };
  } else if (_nested_loop_join._op == ">") {
    _compare = [](const T& value_left, const T& value_right) -> bool { return value_left > value_right; };
  } else if (_nested_loop_join._op == ">=") {
    _compare = [](const T& value_left, const T& value_right) -> bool { return value_left >= value_right; };
  } else if (_nested_loop_join._op == "<=") {
    _compare = [](const T& value_left, const T& value_right) -> bool { return value_left <= value_right; };
  } else if (_nested_loop_join._op == "!=") {
    _compare = [](const T& value_left, const T& value_right) -> bool { return value_left != value_right; };
  } else {
    std::string message = "NestedLoopJoinImpl::NestedLoopJoinImpl: Unknown operator " + _nested_loop_join._op;
    throw std::runtime_error(message);
  }
}

template <typename T>
void NestedLoopJoin::NestedLoopJoinImpl<T>::execute() {}

template <typename T>
std::shared_ptr<Table> NestedLoopJoin::NestedLoopJoinImpl<T>::get_output() const {
  std::string message = "NestedLoopJoinImpl::get_output() not implemented";
  throw std::runtime_error(message);
  return nullptr;
}

/*
** All join functions only consider the combination of types of columns that can be joined.
** The ordering is mostly not important (equi-join, etc.) but for other compare function like "<" we use
** an adiditional flag 'reverse_order' to compute the right result.
*/

template <typename T>
void NestedLoopJoin::NestedLoopJoinImpl<T>::join_value_value(ValueColumn<T>& left, ValueColumn<T>& right,
                                                             std::shared_ptr<JoinContext> context, bool reverse_order) {
  auto& values_left = left.values();
  auto& values_right = right.values();

  for (ChunkOffset left_chunk_offset = 0; left_chunk_offset < values_left.size(); left_chunk_offset++) {
    const auto& value_left = values_left[left_chunk_offset];

    for (ChunkOffset right_chunk_offset = 0; right_chunk_offset < values_right.size(); right_chunk_offset++) {
      const auto& value_right = values_right[right_chunk_offset];

      if (reverse_order ? _compare(value_right, value_left) : _compare(value_left, value_right)) {
        RowID left_row_id = _nested_loop_join._input_left->calculate_row_id(
            context->_left_chunk_id, reverse_order ? right_chunk_offset : left_chunk_offset);
        RowID right_row_id = _nested_loop_join._input_right->calculate_row_id(
            context->_right_chunk_id, reverse_order ? left_chunk_offset : right_chunk_offset);
        if (context->_mode == JoinMode::Left_outer) {
          _nested_loop_join._left_match.at(left_row_id.chunk_id * _nested_loop_join._input_left->chunk_size() +
                                           left_row_id.chunk_offset) = true;
        } else if (context->_mode == JoinMode::Right_outer) {
          _nested_loop_join._right_match.at(right_row_id.chunk_id * _nested_loop_join._input_right->chunk_size() +
                                            right_row_id.chunk_offset) = true;
        } else if (context->_mode == JoinMode::Full_outer) {
          _nested_loop_join._left_match.at(left_row_id.chunk_id * _nested_loop_join._input_left->chunk_size() +
                                           left_row_id.chunk_offset) = true;
          _nested_loop_join._right_match.at(right_row_id.chunk_id * _nested_loop_join._input_right->chunk_size() +
                                            right_row_id.chunk_offset) = true;
        }
        _nested_loop_join._pos_list_left->push_back(left_row_id);
        _nested_loop_join._pos_list_right->push_back(right_row_id);
      }
    }
  }
}

template <typename T>
void NestedLoopJoin::NestedLoopJoinImpl<T>::join_value_dictionary(ValueColumn<T>& left, DictionaryColumn<T>& right,
                                                                  std::shared_ptr<JoinContext> context,
                                                                  bool reverse_order) {
  auto& values = left.values();
  const auto& att = right.attribute_vector();

  for (ChunkOffset left_chunk_offset = 0; left_chunk_offset < values.size(); left_chunk_offset++) {
    const auto& value_left = values[left_chunk_offset];

    for (ChunkOffset right_chunk_offset = 0; right_chunk_offset < att->size(); right_chunk_offset++) {
      const auto& value_right = right.value_by_value_id(att->get(right_chunk_offset));

      if (reverse_order ? _compare(value_right, value_left) : _compare(value_left, value_right)) {
        RowID left_row_id = _nested_loop_join._input_left->calculate_row_id(
            context->_left_chunk_id, reverse_order ? right_chunk_offset : left_chunk_offset);
        RowID right_row_id = _nested_loop_join._input_right->calculate_row_id(
            context->_right_chunk_id, reverse_order ? left_chunk_offset : right_chunk_offset);
        _nested_loop_join._pos_list_left->push_back(left_row_id);
        _nested_loop_join._pos_list_right->push_back(right_row_id);
      }
    }
  }
}

template <typename T>
void NestedLoopJoin::NestedLoopJoinImpl<T>::join_value_reference(ValueColumn<T>& left, ReferenceColumn& right,
                                                                 std::shared_ptr<JoinContext> context,
                                                                 bool reverse_order) {
  auto& values = left.values();
  auto& ref_table = right.referenced_table();
  auto& pos_list = right.pos_list();

  for (ChunkOffset left_chunk_offset = 0; left_chunk_offset < values.size(); left_chunk_offset++) {
    const auto& value_left = values[left_chunk_offset];

    // Since it isn't ensured that the poslist isn't ordered according to the chunk distribution,
    // we have to check the column type for each row
    for (ChunkOffset right_chunk_offset = 0; right_chunk_offset < pos_list->size(); right_chunk_offset++) {
      const auto& row_location = ref_table->locate_row(pos_list->at(right_chunk_offset));
      const auto& referenced_chunk_id = row_location.first;
      const auto& referenced_chunk_offset = row_location.second;
      const auto& referenced_chunk = ref_table->get_chunk(referenced_chunk_id);
      const auto& referenced_column = referenced_chunk.get_column(right.referenced_column_id());

      const auto& d_column = std::dynamic_pointer_cast<DictionaryColumn<T>>(referenced_column);
      const auto& v_column = std::dynamic_pointer_cast<ValueColumn<T>>(referenced_column);
      T value_right;
      if (d_column) {
        value_right = d_column->value_by_value_id(d_column->attribute_vector()->get(referenced_chunk_offset));
      } else if (v_column) {
        value_right = v_column->values()[referenced_chunk_offset];
      } else {
        std::string message = "NestedLoopJoinImpl::join_value_reference: can't figure out referenced column type";
        throw std::runtime_error(message);
      }

      if (reverse_order ? _compare(value_right, value_left) : _compare(value_left, value_right)) {
        RowID left_row_id = _nested_loop_join._input_left->calculate_row_id(
            context->_left_chunk_id, reverse_order ? right_chunk_offset : left_chunk_offset);
        RowID right_row_id = _nested_loop_join._input_right->calculate_row_id(
            context->_right_chunk_id, reverse_order ? left_chunk_offset : right_chunk_offset);
        _nested_loop_join._pos_list_left->push_back(left_row_id);
        _nested_loop_join._pos_list_right->push_back(right_row_id);
      }
    }
  }
}

template <typename T>
void NestedLoopJoin::NestedLoopJoinImpl<T>::join_dictionary_dictionary(DictionaryColumn<T>& left,
                                                                       DictionaryColumn<T>& right,
                                                                       std::shared_ptr<JoinContext> context,
                                                                       bool reverse_order) {
  const auto& att_left = left.attribute_vector();
  const auto& att_right = right.attribute_vector();

  for (ChunkOffset left_chunk_offset = 0; left_chunk_offset < att_left->size(); left_chunk_offset++) {
    const auto& value_left = left.value_by_value_id(att_left->get(left_chunk_offset));

    for (ChunkOffset right_chunk_offset = 0; right_chunk_offset < att_right->size(); right_chunk_offset++) {
      const auto& value_right = right.value_by_value_id(att_right->get(right_chunk_offset));

      if (reverse_order ? _compare(value_right, value_left) : _compare(value_left, value_right)) {
        RowID left_row_id = _nested_loop_join._input_left->calculate_row_id(
            context->_left_chunk_id, reverse_order ? right_chunk_offset : left_chunk_offset);
        RowID right_row_id = _nested_loop_join._input_right->calculate_row_id(
            context->_right_chunk_id, reverse_order ? left_chunk_offset : right_chunk_offset);
        _nested_loop_join._pos_list_left->push_back(left_row_id);
        _nested_loop_join._pos_list_right->push_back(right_row_id);
      }
    }
  }
}

template <typename T>
void NestedLoopJoin::NestedLoopJoinImpl<T>::join_dictionary_reference(DictionaryColumn<T>& left, ReferenceColumn& right,
                                                                      std::shared_ptr<JoinContext> context,
                                                                      bool reverse_order) {
  const auto& att_left = left.attribute_vector();
  auto& ref_table = right.referenced_table();
  auto& pos_list = right.pos_list();

  for (ChunkOffset left_chunk_offset = 0; left_chunk_offset < att_left->size(); left_chunk_offset++) {
    const auto& value_left = left.value_by_value_id(att_left->get(left_chunk_offset));

    // Since it isn't ensured that the poslist isn't ordered according to the chunk distribution,
    // we have to check the column type for each row
    for (ChunkOffset right_chunk_offset = 0; right_chunk_offset < pos_list->size(); right_chunk_offset++) {
      const auto& row_location = ref_table->locate_row(pos_list->at(right_chunk_offset));
      const auto& referenced_chunk_id = row_location.first;
      const auto& referenced_chunk_offset = row_location.second;
      const auto& referenced_chunk = ref_table->get_chunk(referenced_chunk_id);
      const auto& referenced_column = referenced_chunk.get_column(right.referenced_column_id());

      const auto& d_column = std::dynamic_pointer_cast<DictionaryColumn<T>>(referenced_column);
      const auto& v_column = std::dynamic_pointer_cast<ValueColumn<T>>(referenced_column);
      T value_right;
      if (d_column) {
        value_right = d_column->value_by_value_id(d_column->attribute_vector()->get(referenced_chunk_offset));
      } else if (v_column) {
        value_right = v_column->values()[referenced_chunk_offset];
      } else {
        throw std::exception(
            std::runtime_error("NestedLoopJoinImpl::join_value_reference: can't figure out referenced column type"));
      }

      if (reverse_order ? _compare(value_right, value_left) : _compare(value_left, value_right)) {
        RowID left_row_id = _nested_loop_join._input_left->calculate_row_id(
            context->_left_chunk_id, reverse_order ? right_chunk_offset : left_chunk_offset);
        RowID right_row_id = _nested_loop_join._input_right->calculate_row_id(
            context->_right_chunk_id, reverse_order ? left_chunk_offset : right_chunk_offset);
        _nested_loop_join._pos_list_left->push_back(left_row_id);
        _nested_loop_join._pos_list_right->push_back(right_row_id);
      }
    }
  }
}

template <typename T>
void NestedLoopJoin::NestedLoopJoinImpl<T>::join_reference_reference(ReferenceColumn& left, ReferenceColumn& right,
                                                                     std::shared_ptr<JoinContext> context,
                                                                     bool reverse_order) {
  auto& ref_table_left = left.referenced_table();
  auto& pos_list_left = left.pos_list();
  auto& ref_table_right = right.referenced_table();
  auto& pos_list_right = right.pos_list();

  // Since it isn't ensured that the poslist isn't ordered according to the chunk distribution,
  // we have to check the column type for each row
  for (ChunkOffset left_chunk_offset = 0; left_chunk_offset < pos_list_left->size(); left_chunk_offset++) {
    const auto& row_location = ref_table_left->locate_row(pos_list_left->at(left_chunk_offset));
    const auto& referenced_chunk_id = row_location.first;
    const auto& referenced_chunk_offset = row_location.second;
    const auto& referenced_chunk = ref_table_left->get_chunk(referenced_chunk_id);
    const auto& referenced_column = referenced_chunk.get_column(left.referenced_column_id());

    const auto& d_column = std::dynamic_pointer_cast<DictionaryColumn<T>>(referenced_column);
    const auto& v_column = std::dynamic_pointer_cast<ValueColumn<T>>(referenced_column);
    T value_left;
    if (d_column) {
      value_left = d_column->value_by_value_id(d_column->attribute_vector()->get(referenced_chunk_offset));
    } else if (v_column) {
      value_left = v_column->values()[referenced_chunk_offset];
    } else {
      throw std::exception(
          std::runtime_error("NestedLoopJoinImpl::join_value_reference: can't figure out referenced column type"));
    }

    for (ChunkOffset right_chunk_offset = 0; right_chunk_offset < pos_list_right->size(); right_chunk_offset++) {
      const auto& row_location_r = ref_table_right->locate_row(pos_list_right->at(right_chunk_offset));
      const auto& referenced_chunk_id_r = row_location_r.first;
      const auto& referenced_chunk_offset_r = row_location_r.second;
      const auto& referenced_chunk_r = ref_table_right->get_chunk(referenced_chunk_id_r);
      const auto& referenced_column_r = referenced_chunk_r.get_column(right.referenced_column_id());

      const auto& d_column = std::dynamic_pointer_cast<DictionaryColumn<T>>(referenced_column_r);
      const auto& v_column = std::dynamic_pointer_cast<ValueColumn<T>>(referenced_column_r);
      T value_right;
      if (d_column) {
        value_right = d_column->value_by_value_id(d_column->attribute_vector()->get(referenced_chunk_offset_r));
      } else if (v_column) {
        value_right = v_column->values()[referenced_chunk_offset_r];
      } else {
        throw std::exception(
            std::runtime_error("NestedLoopJoinImpl::join_value_reference: can't figure out referenced column type"));
      }

      if (reverse_order ? _compare(value_right, value_left) : _compare(value_left, value_right)) {
        RowID left_row_id = _nested_loop_join._input_left->calculate_row_id(
            context->_left_chunk_id, reverse_order ? right_chunk_offset : left_chunk_offset);
        RowID right_row_id = _nested_loop_join._input_right->calculate_row_id(
            context->_right_chunk_id, reverse_order ? left_chunk_offset : right_chunk_offset);
        _nested_loop_join._pos_list_left->push_back(left_row_id);
        _nested_loop_join._pos_list_right->push_back(right_row_id);
      }
    }
  }
}

/*
** This functions get called by the visitor and check for the type of the second column to actually call the
** join function.
** May be later replaced by left_builder/right_builder pattern.
*/

template <typename T>
void NestedLoopJoin::NestedLoopJoinImpl<T>::handle_value_column(BaseColumn& column,
                                                                std::shared_ptr<ColumnVisitableContext> context) {
  auto join_context = std::static_pointer_cast<JoinContext>(context);
  auto& value_column_left = dynamic_cast<ValueColumn<T>&>(column);
  auto value_column_right = std::dynamic_pointer_cast<ValueColumn<T>>(join_context->_column_right);
  if (value_column_right) {
    join_value_value(value_column_left, *value_column_right, join_context);
    return;
  }
  auto dictionary_column_right = std::dynamic_pointer_cast<DictionaryColumn<T>>(join_context->_column_right);
  if (dictionary_column_right) {
    join_value_dictionary(value_column_left, *dictionary_column_right, join_context);
    return;
  }
  auto reference_column_right = std::dynamic_pointer_cast<ReferenceColumn>(join_context->_column_right);
  if (reference_column_right) {
    join_value_reference(value_column_left, *reference_column_right, join_context);
    return;
  }
}

template <typename T>
void NestedLoopJoin::NestedLoopJoinImpl<T>::handle_dictionary_column(BaseColumn& column,
                                                                     std::shared_ptr<ColumnVisitableContext> context) {
  auto join_context = std::static_pointer_cast<JoinContext>(context);
  auto& dictionary_column_left = dynamic_cast<DictionaryColumn<T>&>(column);

  auto value_column_right = std::dynamic_pointer_cast<ValueColumn<T>>(join_context->_column_right);
  if (value_column_right) {
    join_value_dictionary(*value_column_right, dictionary_column_left, join_context, true);
    return;
  }
  auto dictionary_column_right = std::dynamic_pointer_cast<DictionaryColumn<T>>(join_context->_column_right);
  if (dictionary_column_right) {
    join_dictionary_dictionary(dictionary_column_left, *dictionary_column_right, join_context);
    return;
  }
  auto reference_column_right = std::dynamic_pointer_cast<ReferenceColumn>(join_context->_column_right);
  if (reference_column_right) {
    join_dictionary_reference(dictionary_column_left, *reference_column_right, join_context);
    return;
  }
}

template <typename T>
void NestedLoopJoin::NestedLoopJoinImpl<T>::handle_reference_column(ReferenceColumn& reference_column_left,
                                                                    std::shared_ptr<ColumnVisitableContext> context) {
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
  }
}
}  // namespace opossum
