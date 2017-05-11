#include "join_nested_loop_b.hpp"

#include <exception>
#include <memory>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace opossum {

JoinNestedLoopB::JoinNestedLoopB(const std::shared_ptr<const AbstractOperator> left,
                                 const std::shared_ptr<const AbstractOperator> right,
                                 optional<std::pair<std::string, std::string>> column_names, const std::string& op,
                                 const JoinMode mode, const std::string& prefix_left, const std::string& prefix_right)
    : AbstractJoinOperator(left, right, column_names, op, mode, prefix_left, prefix_right), _op{op}, _mode{mode} {
  if (mode == Cross) {
    throw std::runtime_error(
        "JoinNestedLoopA: this operator does not support Cross Joins, the optimizer should use Product operator.");
  }

  // Check optional column names
  // Per definition either two names are specified or none
  if (column_names) {
    _left_column_name = column_names->first;
    _right_column_name = column_names->second;
  } else {
    throw std::runtime_error("JoinNestedLoopB::JoinNestedLoopB: No columns specified for join operator");
  }

  if (left == nullptr) {
    throw std::runtime_error("JoinNestedLoopB::JoinNestedLoopB: left input operator is null");
  }
  if (right == nullptr) {
    throw std::runtime_error("JoinNestedLoopB::JoinNestedLoopB: right input operator is null");
  }

  _output = std::make_shared<Table>(0);
  _pos_list_left = std::make_shared<PosList>();
  _pos_list_right = std::make_shared<PosList>();
}

// This funtion turns a pos list with references to a reference column into a pos list with references
// to the original columns.
// It is assumed that either non or all chunks of a table contain reference columns.
std::shared_ptr<PosList> JoinNestedLoopB::_dereference_pos_list(std::shared_ptr<const Table> input_table,
                                                                size_t column_id,
                                                                std::shared_ptr<const PosList> pos_list) {
  // Get all the input pos lists so that we only have to pointer cast the columns once
  auto input_pos_lists = std::vector<std::shared_ptr<const PosList>>();
  for (ChunkID chunk_id = 0; chunk_id < input_table->chunk_count(); chunk_id++) {
    auto base_column = input_table->get_chunk(chunk_id).get_column(column_id);
    auto reference_column = std::dynamic_pointer_cast<ReferenceColumn>(base_column);
    input_pos_lists.push_back(reference_column->pos_list());
  }

  // Get the row ids that are referenced
  auto new_pos_list = std::make_shared<PosList>();
  for (const auto& row : *pos_list) {
    new_pos_list->push_back(input_pos_lists.at(row.chunk_id)->at(row.chunk_offset));
  }

  return new_pos_list;
}

void JoinNestedLoopB::_append_columns_to_output(std::shared_ptr<const Table> input_table,
                                                std::shared_ptr<PosList> pos_list, std::string prefix) {
  // Append each column of the input column to the output
  for (size_t column_id = 0; column_id < input_table->col_count(); column_id++) {
    // Add the column meta data
    _output->add_column(prefix + input_table->column_name(column_id), input_table->column_type(column_id), false);

    // Check whether the column consists of reference columns
    const auto r_column = std::dynamic_pointer_cast<ReferenceColumn>(input_table->get_chunk(0).get_column(column_id));
    if (r_column) {
      // Create a pos_list referencing the original column
      auto new_pos_list = _dereference_pos_list(input_table, column_id, pos_list);
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
void JoinNestedLoopB::_join_columns(size_t left_column_id, size_t right_column_id, std::string left_column_type) {
  auto impl = make_shared_by_column_type<ColumnVisitable, JoinNestedLoopBImpl>(left_column_type, *this);
  // For each combination of chunks from both input tables call visitor pattern to actually perform the join.
  for (ChunkID chunk_id_left = 0; chunk_id_left < input_table_left()->chunk_count(); ++chunk_id_left) {
    for (ChunkID chunk_id_right = 0; chunk_id_right < input_table_right()->chunk_count(); ++chunk_id_right) {
      auto& chunk_left = input_table_left()->get_chunk(chunk_id_left);
      auto column_left = chunk_left.get_column(left_column_id);
      auto& chunk_right = input_table_right()->get_chunk(chunk_id_right);
      auto column_right = chunk_right.get_column(right_column_id);

      auto context = std::make_shared<JoinContext>(column_left, column_right, chunk_id_left, chunk_id_right, _mode);
      column_left->visit(*impl, context);
    }
  }
}

// Adds the rows to the output that didn't match to any other rows in the join phase and
// fills those rows with null values
void JoinNestedLoopB::_add_outer_join_rows(std::shared_ptr<const Table> outer_side_table,
                                           std::shared_ptr<PosList> outer_side_pos_list,
                                           std::set<RowID>& outer_side_matches,
                                           std::shared_ptr<PosList> null_side_pos_list) {
  for (ChunkID chunk_id = 0; chunk_id < outer_side_table->chunk_count(); chunk_id++) {
    for (ChunkOffset chunk_offset = 0; chunk_offset < outer_side_table->get_chunk(chunk_id).size(); chunk_offset++) {
      RowID row_id = RowID{chunk_id, chunk_offset};

      // if there was no match during the join phase
      if (outer_side_matches.find(row_id) == outer_side_matches.end()) {
        outer_side_pos_list->push_back(row_id);
        null_side_pos_list->push_back(RowID{0u, INVALID_CHUNK_OFFSET});
      }
    }
  }
}

std::shared_ptr<const Table> JoinNestedLoopB::on_execute() {
  // Get types and ids of the input columns
  auto left_column_id = input_table_left()->column_id_by_name(_left_column_name);
  auto right_column_id = input_table_right()->column_id_by_name(_right_column_name);
  auto left_column_type = input_table_left()->column_type(left_column_id);
  auto right_column_type = input_table_right()->column_type(right_column_id);

  // Ensure matching column types for simplicity
  // Joins on non-matching types can be added later.
  if (left_column_type != right_column_type) {
    std::string message = "JoinNestedLoopB::execute: column type \"" + left_column_type + "\" of left column \"" +
                          _left_column_name + "\" does not match colum type \"" + right_column_type +
                          "\" of right column \"" + _right_column_name + "\"!";
    throw std::runtime_error(message);
  }

  _join_columns(left_column_id, right_column_id, left_column_type);

  if (_mode == Left || _mode == Outer) {
    _add_outer_join_rows(input_table_left(), _pos_list_left, _left_match, _pos_list_right);
  }

  if (_mode == Right || _mode == Outer) {
    _add_outer_join_rows(input_table_right(), _pos_list_right, _right_match, _pos_list_left);
  }

  _append_columns_to_output(input_table_left(), _pos_list_left, _prefix_left);
  _append_columns_to_output(input_table_right(), _pos_list_right, _prefix_right);

  return _output;
}

const std::string JoinNestedLoopB::name() const { return "JoinNestedLoopB"; }

uint8_t JoinNestedLoopB::num_in_tables() const { return 2u; }

uint8_t JoinNestedLoopB::num_out_tables() const { return 1u; }

template <typename T>
JoinNestedLoopB::JoinNestedLoopBImpl<T>::JoinNestedLoopBImpl(JoinNestedLoopB& join_nested_loop_b)
    : _join_nested_loop_b{join_nested_loop_b} {
  // No compare function is necessary for the cross join
  if (_join_nested_loop_b._mode == Cross) {
    return;
  }

  if (_join_nested_loop_b._op == "=") {
    _compare = [](const T& value_left, const T& value_right) -> bool { return value_left == value_right; };
  } else if (_join_nested_loop_b._op == "<") {
    _compare = [](const T& value_left, const T& value_right) -> bool { return value_left < value_right; };
  } else if (_join_nested_loop_b._op == ">") {
    _compare = [](const T& value_left, const T& value_right) -> bool { return value_left > value_right; };
  } else if (_join_nested_loop_b._op == ">=") {
    _compare = [](const T& value_left, const T& value_right) -> bool { return value_left >= value_right; };
  } else if (_join_nested_loop_b._op == "<=") {
    _compare = [](const T& value_left, const T& value_right) -> bool { return value_left <= value_right; };
  } else if (_join_nested_loop_b._op == "!=") {
    _compare = [](const T& value_left, const T& value_right) -> bool { return value_left != value_right; };
  } else {
    throw std::runtime_error("JoinNestedLoopBImpl::JoinNestedLoopBImpl: Unknown operator " + _join_nested_loop_b._op);
  }
}

template <typename T>
std::shared_ptr<const Table> JoinNestedLoopB::JoinNestedLoopBImpl<T>::on_execute() {
  return _join_nested_loop_b._output;
}

/*
** All join functions only consider the combination of types of columns that can be joined.
** The ordering is mostly not important (equi-join, etc.) but for other compare function like "<" we use
** an additional flag 'reverse_order' to compute the right result.
*/

template <typename T>
void JoinNestedLoopB::JoinNestedLoopBImpl<T>::_match_values(const T& value_left, ChunkOffset left_chunk_offset,
                                                            const T& value_right, ChunkOffset right_chunk_offset,
                                                            std::shared_ptr<JoinContext> context, bool reverse_order) {
  bool values_match = reverse_order ? _compare(value_right, value_left) : _compare(value_left, value_right);
  if (values_match) {
    RowID left_row_id = _join_nested_loop_b.input_table_left()->calculate_row_id(
        context->_left_chunk_id, reverse_order ? right_chunk_offset : left_chunk_offset);
    RowID right_row_id = _join_nested_loop_b.input_table_right()->calculate_row_id(
        context->_right_chunk_id, reverse_order ? left_chunk_offset : right_chunk_offset);

    if (context->_mode == Left || context->_mode == Outer) {
      // For inner joins, the list of matched values is not needed and is not maintained
      _join_nested_loop_b._left_match.insert(left_row_id);
    }

    if (context->_mode == Right || context->_mode == Outer) {
      _join_nested_loop_b._right_match.insert(right_row_id);
    }

    _join_nested_loop_b._pos_list_left->push_back(left_row_id);
    _join_nested_loop_b._pos_list_right->push_back(right_row_id);
  }
}

template <typename T>
void JoinNestedLoopB::JoinNestedLoopBImpl<T>::join_value_value(ValueColumn<T>& left, ValueColumn<T>& right,
                                                               std::shared_ptr<JoinContext> context,
                                                               bool reverse_order) {
  const auto& values_left = left.values();
  const auto& values_right = right.values();

  for (ChunkOffset left_chunk_offset = 0; left_chunk_offset < values_left.size(); left_chunk_offset++) {
    const auto& value_left = values_left[left_chunk_offset];

    for (ChunkOffset right_chunk_offset = 0; right_chunk_offset < values_right.size(); right_chunk_offset++) {
      const auto& value_right = values_right[right_chunk_offset];
      _match_values(value_left, left_chunk_offset, value_right, right_chunk_offset, context, reverse_order);
    }
  }
}

template <typename T>
void JoinNestedLoopB::JoinNestedLoopBImpl<T>::join_value_dictionary(ValueColumn<T>& left, DictionaryColumn<T>& right,
                                                                    std::shared_ptr<JoinContext> context,
                                                                    bool reverse_order) {
  const auto& values = left.values();
  const auto& att = right.attribute_vector();

  for (ChunkOffset left_chunk_offset = 0; left_chunk_offset < values.size(); left_chunk_offset++) {
    const auto& value_left = values[left_chunk_offset];

    for (ChunkOffset right_chunk_offset = 0; right_chunk_offset < att->size(); right_chunk_offset++) {
      const auto& value_right = right.value_by_value_id(att->get(right_chunk_offset));
      _match_values(value_left, left_chunk_offset, value_right, right_chunk_offset, context, reverse_order);
    }
  }
}

// Resolves a reference in a reference column and returns the original value
template <typename T>
const T& JoinNestedLoopB::JoinNestedLoopBImpl<T>::_resolve_reference(ReferenceColumn& ref_column,
                                                                     ChunkOffset chunk_offset) {
  // TODO(anyone): This can be replaced by operator[] once gcc optimizes properly
  auto& ref_table = ref_column.referenced_table();
  auto& pos_list = ref_column.pos_list();
  const auto& row_location = ref_table->locate_row(pos_list->at(chunk_offset));
  const auto& referenced_chunk_id = row_location.first;
  const auto& referenced_chunk_offset = row_location.second;
  const auto& referenced_chunk = ref_table->get_chunk(referenced_chunk_id);
  const auto& referenced_column = referenced_chunk.get_column(ref_column.referenced_column_id());

  const auto& d_column = std::dynamic_pointer_cast<DictionaryColumn<T>>(referenced_column);
  const auto& v_column = std::dynamic_pointer_cast<ValueColumn<T>>(referenced_column);

  // Since it isn't ensured that the poslist isn't ordered according to the chunk distribution,
  // we have to check the column type for each row
  if (d_column) {
    return d_column->value_by_value_id(d_column->attribute_vector()->get(referenced_chunk_offset));
  } else if (v_column) {
    return v_column->values()[referenced_chunk_offset];
  } else {
    throw std::runtime_error("JoinNestedLoopBImpl::_resolve_reference: can't figure out referenced column type");
  }
}

template <typename T>
void JoinNestedLoopB::JoinNestedLoopBImpl<T>::join_value_reference(ValueColumn<T>& left, ReferenceColumn& right,
                                                                   std::shared_ptr<JoinContext> context,
                                                                   bool reverse_order) {
  auto& values = left.values();
  auto& pos_list = right.pos_list();

  for (ChunkOffset left_chunk_offset = 0; left_chunk_offset < values.size(); left_chunk_offset++) {
    const auto& value_left = values[left_chunk_offset];

    for (ChunkOffset right_chunk_offset = 0; right_chunk_offset < pos_list->size(); right_chunk_offset++) {
      const auto& value_right = _resolve_reference(right, right_chunk_offset);
      _match_values(value_left, left_chunk_offset, value_right, right_chunk_offset, context, reverse_order);
    }
  }
}

template <typename T>
void JoinNestedLoopB::JoinNestedLoopBImpl<T>::join_dictionary_dictionary(DictionaryColumn<T>& left,
                                                                         DictionaryColumn<T>& right,
                                                                         std::shared_ptr<JoinContext> context,
                                                                         bool reverse_order) {
  const auto& att_left = left.attribute_vector();
  const auto& att_right = right.attribute_vector();

  for (ChunkOffset left_chunk_offset = 0; left_chunk_offset < att_left->size(); left_chunk_offset++) {
    const auto& value_left = left.value_by_value_id(att_left->get(left_chunk_offset));

    for (ChunkOffset right_chunk_offset = 0; right_chunk_offset < att_right->size(); right_chunk_offset++) {
      const auto& value_right = right.value_by_value_id(att_right->get(right_chunk_offset));
      _match_values(value_left, left_chunk_offset, value_right, right_chunk_offset, context, reverse_order);
    }
  }
}

template <typename T>
void JoinNestedLoopB::JoinNestedLoopBImpl<T>::join_dictionary_reference(DictionaryColumn<T>& left,
                                                                        ReferenceColumn& right,
                                                                        std::shared_ptr<JoinContext> context,
                                                                        bool reverse_order) {
  const auto& att_left = left.attribute_vector();
  auto& pos_list = right.pos_list();

  for (ChunkOffset left_chunk_offset = 0; left_chunk_offset < att_left->size(); left_chunk_offset++) {
    const auto& value_left = left.value_by_value_id(att_left->get(left_chunk_offset));

    for (ChunkOffset right_chunk_offset = 0; right_chunk_offset < pos_list->size(); right_chunk_offset++) {
      const auto& value_right = _resolve_reference(right, right_chunk_offset);
      _match_values(value_left, left_chunk_offset, value_right, right_chunk_offset, context, reverse_order);
    }
  }
}

template <typename T>
void JoinNestedLoopB::JoinNestedLoopBImpl<T>::join_reference_reference(ReferenceColumn& left, ReferenceColumn& right,
                                                                       std::shared_ptr<JoinContext> context,
                                                                       bool reverse_order) {
  auto& pos_list_left = left.pos_list();
  auto& pos_list_right = right.pos_list();

  for (ChunkOffset left_chunk_offset = 0; left_chunk_offset < pos_list_left->size(); left_chunk_offset++) {
    const auto& value_left = _resolve_reference(left, left_chunk_offset);

    for (ChunkOffset right_chunk_offset = 0; right_chunk_offset < pos_list_right->size(); right_chunk_offset++) {
      const auto& value_right = _resolve_reference(right, right_chunk_offset);
      _match_values(value_left, left_chunk_offset, value_right, right_chunk_offset, context, reverse_order);
    }
  }
}

/*
** This functions get called by the visitor and check for the type of the second column to actually call the
** join function.
** May be later replaced by left_builder/right_builder pattern.
*/

template <typename T>
void JoinNestedLoopB::JoinNestedLoopBImpl<T>::handle_value_column(BaseColumn& column,
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
void JoinNestedLoopB::JoinNestedLoopBImpl<T>::handle_dictionary_column(
    BaseColumn& column, std::shared_ptr<ColumnVisitableContext> context) {
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
void JoinNestedLoopB::JoinNestedLoopBImpl<T>::handle_reference_column(ReferenceColumn& reference_column_left,
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
