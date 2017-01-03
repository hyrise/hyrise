#include "nested_loop_join.hpp"

#include <exception>
#include <memory>
#include <stdexcept>
#include <string>

namespace opossum {

NestedLoopJoin::NestedLoopJoin(std::shared_ptr<AbstractOperator> left, std::shared_ptr<AbstractOperator> right,
                               std::string& left_column_name, std::string& right_column_name, std::string& op)
    : AbstractOperator(left, right),
      _left_column_name{left_column_name},
      _right_column_name{right_column_name},
      _op{op} {
  _pos_list_left = std::make_shared<PosList>();
  _pos_list_right = std::make_shared<PosList>();
}

void NestedLoopJoin::execute() {
  auto left_column_id = _input_left->column_id_by_name(_left_column_name);
  auto right_column_id = _input_right->column_id_by_name(_right_column_name);
  auto left_column_type = _input_left->column_type(left_column_id);
  auto right_column_type = _input_right->column_type(right_column_id);

  if (left_column_type != right_column_type) {
    throw std::exception(
        std::runtime_error("NestedLoopJoin::execute: left column type does not match right column type!"));
  }

  for (ChunkID chunk_id_left = 0; chunk_id_left < _input_left->chunk_count(); ++chunk_id_left) {
    for (ChunkID chunk_id_right = 0; chunk_id_right < _input_right->chunk_count(); ++chunk_id_right) {
      auto& chunk_left = _input_left->get_chunk(chunk_id_left);
      auto column_left = chunk_left.get_column(left_column_id);
      auto& chunk_right = _input_right->get_chunk(chunk_id_right);
      auto column_right = chunk_right.get_column(right_column_id);

      auto impl = make_shared_by_column_type<ColumnVisitable, NestedLoopJoinImpl>(left_column_type, *this);
      auto context = std::make_shared<JoinContext>(column_left, column_right, chunk_id_left, chunk_id_right);
      column_left->visit(*impl, context);
    }
  }

  // std::cout << left_column_id << right_column_id << std::endl;
}

std::shared_ptr<const Table> NestedLoopJoin::get_output() const { return _output; }

const std::string NestedLoopJoin::name() const { return "NestedLoopJoin"; }

uint8_t NestedLoopJoin::num_in_tables() const { return 2u; }

uint8_t NestedLoopJoin::num_out_tables() const { return 1u; }

template <typename T>
NestedLoopJoin::NestedLoopJoinImpl<T>::NestedLoopJoinImpl(NestedLoopJoin& nested_loop_join)
    : _nested_loop_join{nested_loop_join} {
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
    throw std::exception(
        std::runtime_error("NestedLoopJoinImpl::NestedLoopJoinImpl: Unknown operator " + _nested_loop_join._op));
  }
}

template <typename T>
void NestedLoopJoin::NestedLoopJoinImpl<T>::execute() {}

template <typename T>
std::shared_ptr<Table> NestedLoopJoin::NestedLoopJoinImpl<T>::get_output() const {
  return nullptr;
}

template <typename T>
void NestedLoopJoin::NestedLoopJoinImpl<T>::join_value_value(ValueColumn<T>& left, ValueColumn<T>& right,
                                                             std::shared_ptr<JoinContext> context, bool reverse_order) {
  auto& values_left = reverse_order ? right.values() : left.values();
  auto& values_right = reverse_order ? left.values() : right.values();

  for (ChunkOffset left_chunk_offset = 0; left_chunk_offset < values_left.size(); left_chunk_offset++) {
    auto& value_left = values_left[left_chunk_offset];

    for (ChunkOffset right_chunk_offset = 0; right_chunk_offset < values_right.size(); right_chunk_offset++) {
      auto& value_right = values_right[right_chunk_offset];

      if (_compare(value_left, value_right)) {
        RowID left_row_id = _nested_loop_join._input_left->calculate_row_id(context->_left_chunk_id, left_chunk_offset);
        RowID right_row_id =
            _nested_loop_join._input_left->calculate_row_id(context->_right_chunk_id, right_chunk_offset);
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
    auto& value_left = values[left_chunk_offset];

    for (ChunkOffset right_chunk_offset = 0; right_chunk_offset < att->size(); right_chunk_offset++) {
      auto& value_right = right.value_by_value_id(att->get(right_chunk_offset));

      if (reverse_order ? _compare(value_right, value_left) : _compare(value_left, value_right)) {
        RowID left_row_id = _nested_loop_join._input_left->calculate_row_id(context->_left_chunk_id, left_chunk_offset);
        RowID right_row_id =
            _nested_loop_join._input_left->calculate_row_id(context->_right_chunk_id, right_chunk_offset);
        _nested_loop_join._pos_list_left->push_back(left_row_id);
        _nested_loop_join._pos_list_right->push_back(right_row_id);
      }
    }
  }
}

template <typename T>
void NestedLoopJoin::NestedLoopJoinImpl<T>::join_value_reference(ValueColumn<T>& left, ReferenceColumn& right,
                                                                 std::shared_ptr<JoinContext> context,
                                                                 bool reverse_order) {}
template <typename T>
void NestedLoopJoin::NestedLoopJoinImpl<T>::join_dictionary_dictionary(DictionaryColumn<T>& left,
                                                                       DictionaryColumn<T>& right,
                                                                       std::shared_ptr<JoinContext> context,
                                                                       bool reverse_order) {}

template <typename T>
void NestedLoopJoin::NestedLoopJoinImpl<T>::join_dictionary_reference(DictionaryColumn<T>& left, ReferenceColumn& right,
                                                                      std::shared_ptr<JoinContext> context,
                                                                      bool reverse_order) {}

template <typename T>
void NestedLoopJoin::NestedLoopJoinImpl<T>::join_reference_reference(ReferenceColumn& left, ReferenceColumn& right,
                                                                     std::shared_ptr<JoinContext> context,
                                                                     bool reverse_order) {}

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
void NestedLoopJoin::NestedLoopJoinImpl<T>::handle_reference_column(ReferenceColumn& column,
                                                                    std::shared_ptr<ColumnVisitableContext> context) {}
}  // namespace opossum
