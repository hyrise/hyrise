#include "nested_loop_join.hpp"

#include <memory>
#include <string>

namespace opossum {

NestedLoopJoin::NestedLoopJoin(std::shared_ptr<AbstractOperator> left, std::shared_ptr<AbstractOperator> right,
                               std::string& left_column_name, std::string& right_column_name, std::string& op)
    : AbstractOperator(left, right),
      _left_column_name{left_column_name},
      _right_column_name{right_column_name},
      _op{op} {}

void NestedLoopJoin::execute() {
  auto left_column_id = _input_left->column_id_by_name(_left_column_name);
  auto right_column_id = _input_right->column_id_by_name(_right_column_name);

  std::cout << left_column_id << right_column_id << std::endl;
}

std::shared_ptr<const Table> NestedLoopJoin::get_output() const { return _output; }

const std::string NestedLoopJoin::name() const { return "NestedLoopJoin"; }

uint8_t NestedLoopJoin::num_in_tables() const { return 2u; }

uint8_t NestedLoopJoin::num_out_tables() const { return 1u; }

template <typename T>
NestedLoopJoin::NestedLoopJoinImpl<T>::NestedLoopJoinImpl(NestedLoopJoin& nested_loop_join)
    : _nested_loop_join{nested_loop_join} {
  if (_nested_loop_join._op == "=") {
    _compare = [](T& value_left, T& value_right) -> bool { return value_left == value_right; };
  } else if (_nested_loop_join._op == "<") {
    _compare = [](T& value_left, T& value_right) -> bool { return value_left < value_right; };
  } else if (_nested_loop_join._op == ">") {
    _compare = [](T& value_left, T& value_right) -> bool { return value_left < value_right; };
  } else if (_nested_loop_join._op == ">=") {
    _compare = [](T& value_left, T& value_right) -> bool { return value_left >= value_right; };
  } else if (_nested_loop_join._op == "<=") {
    _compare = [](T& value_left, T& value_right) -> bool { return value_left <= value_right; };
  } else if (_nested_loop_join._op == "!=") {
    _compare = [](T& value_left, T& value_right) -> bool { return value_left != value_right; };
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
void NestedLoopJoin::NestedLoopJoinImpl<T>::handle_value_column(BaseColumn& column,
                                                                std::shared_ptr<ColumnVisitableContext> context) {}

template <typename T>
void NestedLoopJoin::NestedLoopJoinImpl<T>::handle_dictionary_column(BaseColumn& column,
                                                                     std::shared_ptr<ColumnVisitableContext> context) {}

template <typename T>
void NestedLoopJoin::NestedLoopJoinImpl<T>::handle_reference_column(ReferenceColumn& column,
                                                                    std::shared_ptr<ColumnVisitableContext> context) {}
}  // namespace opossum
