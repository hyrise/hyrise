#include "jit_read_tuples.hpp"

#include "../jit_types.hpp"
#include "all_type_variant.hpp"
#include "expression/evaluation/expression_evaluator.hpp"
#include "resolve_type.hpp"
#include "storage/segment_iterables/create_iterable_from_attribute_vector.hpp"
#include "storage/segment_iterate.hpp"
#include "jit_expression.hpp"

namespace opossum {

namespace {
struct CastedDictionary {
  std::shared_ptr<const BaseDictionarySegment> dictionary_segment = nullptr;
  std::shared_ptr<const PosList> pos_list = nullptr;
};
CastedDictionary get_dictionary_segment(const std::shared_ptr<const BaseSegment> segment) {
  if (const auto dict_segment = std::dynamic_pointer_cast<const BaseDictionarySegment>(segment)) {
    return {dict_segment};
  }
  if (const auto ref_segment = std::dynamic_pointer_cast<const ReferenceSegment>(segment)) {
    const auto pos_list = ref_segment->pos_list();
    if (pos_list->references_single_chunk() && !pos_list->empty()) {
      const auto referenced_chunk = ref_segment->referenced_table()->get_chunk(pos_list->common_chunk_id());
      auto referenced_segment = referenced_chunk->get_segment(ref_segment->referenced_column_id());
      if (const auto dict_segment = std::dynamic_pointer_cast<const BaseDictionarySegment>(referenced_segment)) {
        return {dict_segment, pos_list};
      }
    }
  }
  return {};
}

AllTypeVariant cast_all_type_variant_to_type(const AllTypeVariant& variant, const DataType requested_type) {
  if (requested_type == DataType::Null) return NULL_VALUE;

  const auto current_data_type = data_type_from_all_type_variant(variant);

  if (current_data_type == requested_type) return variant;

  if (variant_is_null(variant)) Fail("Cannot convert null variant");

  // optinal as all type variants cannot be assigned to other variants
  AllTypeVariant casted_variant;

  resolve_data_type(current_data_type, [&](const auto current_data_type_t) {
    using CurrentType = typename decltype(current_data_type_t)::type;
    resolve_data_type(requested_type, [&](const auto new_data_type_t) {
      using NewType = typename decltype(new_data_type_t)::type;
      if constexpr (std::is_scalar_v<NewType> == std::is_scalar_v<CurrentType>) {
        casted_variant = static_cast<NewType>(get<CurrentType>(variant));
      } else {
        Fail("Strings and numbers cannot be converted");
      }
    });
  });

  return casted_variant;
}
}  // namespace

JitReadTuples::JitReadTuples(const bool has_validate, const std::shared_ptr<AbstractExpression>& row_count_expression)
    : _has_validate(has_validate), _row_count_expression(row_count_expression) {}

std::string JitReadTuples::description() const {
  std::stringstream desc;
  desc << "[ReadTuple] ";
  for (const auto& input_column : _input_columns) {
    if (input_column.value_id_access_count) desc << "(ValueID) ";
    desc << "x" << input_column.tuple_entry.tuple_index() << " = Column#" << input_column.column_id << ", ";
  }
  for (const auto& input_literal : _input_literals) {
    if (input_literal.use_value_id) desc << "(ValueID) ";
    desc << "x" << input_literal.tuple_entry.tuple_index() << " = " << input_literal.value << ", ";
  }
  for (const auto& input_parameter : _input_parameters) {
    if (input_parameter.use_value_id) desc << "(ValueID) ";
    desc << "x" << input_parameter.tuple_entry.tuple_index() << " = Parameter#" << input_parameter.parameter_id << ", ";
  }
  return desc.str();
}

void JitReadTuples::before_query(const Table& in_table, const std::vector<AllTypeVariant>& parameter_values,
                                 JitRuntimeContext& context) const {
  // Create a runtime tuple of the appropriate size
  context.tuple.resize(_num_tuple_values);

  const auto set_value_in_tuple = [&](const JitTupleEntry& tuple_entry, const AllTypeVariant& value) {
    auto data_type = tuple_entry.data_type();
    if (data_type == DataType::Null) {
      tuple_entry.set_is_null(true, context);
    } else {
      resolve_data_type(data_type, [&](auto type) {
        using LiteralDataType = typename decltype(type)::type;
        tuple_entry.set<LiteralDataType>(boost::get<LiteralDataType>(value), context);
        // Non-jit operators store bool values as int values
        if constexpr (std::is_same_v<LiteralDataType, Bool>) {
          tuple_entry.set<bool>(boost::get<LiteralDataType>(value), context);
        }
      });
    }
  };

  // Copy all input literals to the runtime tuple
  for (const auto& input_literal : _input_literals) {
    set_value_in_tuple(input_literal.tuple_entry, input_literal.value);
  }

  // Copy all parameter values to the runtime tuple
  DebugAssert(_input_parameters.size() == parameter_values.size(), "Wrong number of parameter values");
  auto parameter_value_itr = parameter_values.cbegin();
  for (const auto& input_parameter : _input_parameters) {
    set_value_in_tuple(input_parameter.tuple_entry, *parameter_value_itr++);
  }

  // Not related to reading tuples - evaluate the limit expression if JitLimit operator is used.
  if (_row_count_expression) {
    const auto num_rows_expression_result =
        ExpressionEvaluator{}.evaluate_expression_to_result<int64_t>(*_row_count_expression);
    Assert(num_rows_expression_result->size() == 1, "Expected exactly one row for Limit");
    Assert(!num_rows_expression_result->is_null(0), "Expected non-null for Limit");

    const auto signed_num_rows = num_rows_expression_result->value(0);
    Assert(signed_num_rows >= 0, "Can't Limit to a negative number of Rows");

    context.limit_rows = static_cast<size_t>(signed_num_rows);
  } else {
    context.limit_rows = std::numeric_limits<size_t>::max();
  }
}

bool JitReadTuples::before_chunk(const Table& in_table, const ChunkID chunk_id, const std::vector<AllTypeVariant>& parameter_values, JitRuntimeContext& context) {
  const auto& in_chunk = *in_table.get_chunk(chunk_id);

  context.inputs.clear();
  context.chunk_offset = 0;
  context.chunk_size = in_chunk.size();
  context.chunk_id = chunk_id;

  // Do not prepare empty chunks
  if (context.chunk_size == 0) return true;

  // Not related to reading tuples - set MVCC in context if JitValidate operator is used.
  if (_has_validate) {
    if (in_chunk.has_mvcc_data()) {
      // materialize atomic transaction ids as specialization cannot handle atomics
      context.row_tids.resize(in_chunk.mvcc_data()->tids.size());
      auto itr = context.row_tids.begin();
      for (const auto& tid : in_chunk.mvcc_data()->tids) {
        *itr++ = tid.load();
      }
      // Lock MVCC data before accessing it.
      context.mvcc_data_lock = std::make_unique<SharedScopedLockingPtr<MvccData>>(in_chunk.get_scoped_mvcc_data_lock());
      context.mvcc_data = in_chunk.mvcc_data();
    } else {
      DebugAssert(in_chunk.references_exactly_one_table(),
                  "Input to Validate contains a Chunk referencing more than one table.");
      const auto& ref_col_in = std::dynamic_pointer_cast<const ReferenceSegment>(in_chunk.get_segment(ColumnID{0}));
      context.referenced_table = ref_col_in->referenced_table();
      context.pos_list = ref_col_in->pos_list();
    }
  }

  const auto add_iterator = [&](auto it, auto type, const JitInputColumn& input_column, const bool is_nullalbe) {
    using IteratorType = decltype(it);
    using Type = decltype(type);;
    if (is_nullalbe) {
      context.inputs.push_back(std::make_shared<JitReadTuples::JitSegmentReader<IteratorType, Type, true>>(it, input_column.tuple_entry.tuple_index()));
    } else {
      context.inputs.push_back(std::make_shared<JitReadTuples::JitSegmentReader<IteratorType, Type, false>>(it, input_column.tuple_entry.tuple_index()));
    }
  };

  bool use_specialization = true;

  std::vector<bool> use_value_id_for_segment(_input_columns.size(), false);
  for (const auto& value_id_expression : _value_id_expressions) {
    const auto& jit_input_column = _input_columns[value_id_expression.input_column_index];
    const auto segment = in_chunk.get_segment(jit_input_column.column_id);
    const auto dictionary = get_dictionary_segment(segment).dictionary_segment;
    use_value_id_for_segment[value_id_expression.input_column_index] = dictionary != nullptr;
    if (dictionary) {
      _enable_value_id_in_expression(value_id_expression);
      if (jit_expression_is_binary(value_id_expression.expression_type)) {
        AllTypeVariant value;
        size_t tuple_index;
        if (const auto literal_index = value_id_expression.input_literal_index) {
          value = _input_literals[*literal_index].value;
          tuple_index = _input_literals[*literal_index].tuple_entry.tuple_index();
        } else {
          const auto parameter_index = value_id_expression.input_parameter_index;
          value = parameter_values[*parameter_index];
          tuple_index = _input_parameters[*parameter_index].tuple_entry.tuple_index();
        }
        const auto casted_value = cast_all_type_variant_to_type(value, jit_input_column.tuple_entry.data_type());

        ValueID value_id;
        switch (value_id_expression.expression_type) {
          case JitExpressionType::Equals:
          case JitExpressionType::NotEquals:
            // check if value exists in segment
            if (dictionary->value_of_value_id(dictionary->lower_bound(casted_value)) == casted_value) {
              value_id = INVALID_VALUE_ID;
              break;
            }
            [[fallthrough]];
          case JitExpressionType::LessThan:
          case JitExpressionType::GreaterThanEquals:
            value_id = dictionary->lower_bound(casted_value);
            break;
          case JitExpressionType::LessThanEquals:
          case JitExpressionType::GreaterThan:
            value_id = dictionary->upper_bound(casted_value);
            break;
          default:
            Fail("Unsupported expression type for binary value id predicate");
        }
        context.tuple.set<ValueID::base_type>(tuple_index, value_id);
      }
    } else {
      _disable_value_id_in_expression(value_id_expression);
      use_specialization = false;
    }
  }

  // Create the segment iterator for each input segment and store them to the runtime context
  for (size_t i = 0; i < _input_columns.size(); ++i) {
    const auto& input_column = _input_columns[i];
    const auto column_id = input_column.column_id;
    const auto segment = in_chunk.get_segment(column_id);
    const auto is_nullalbe = in_table.column_is_nullable(column_id);

    const bool use_value_id = use_value_id_for_segment[i];

    bool use_normal_access = input_column.total_access_count > input_column.value_id_access_count || !use_value_id;

    if (use_value_id) {
      const auto [dict_segment, pos_list] = get_dictionary_segment(segment);
      DebugAssert(dict_segment, "Segment is not a dictionary or reference segment");
      if (pos_list) {
        create_iterable_from_attribute_vector(*dict_segment).with_iterators(pos_list, [&](auto it, auto end) {
          add_iterator(it, ValueID::base_type{}, input_column, is_nullalbe);
        });
      } else {
        create_iterable_from_attribute_vector(*dict_segment).with_iterators([&](auto it, auto end) {
          add_iterator(it, ValueID::base_type{}, input_column, is_nullalbe);
        });
      }
    }
    if (use_normal_access) {
      // We need the actual values of a segment
      segment_with_iterators(*segment, [&](auto it, const auto end) {
        using Type = typename decltype(it)::ValueType;
        add_iterator(it, Type{}, input_column, is_nullalbe);
      });
    }
  }

  return use_specialization;
}

void JitReadTuples::execute(JitRuntimeContext& context) const {
  for (; context.chunk_offset < context.chunk_size; ++context.chunk_offset) {
    // We read from and advance all segment iterators, before passing the tuple on to the next operator.
    for (const auto& input : context.inputs) {
      input->read_value(context);
    }
    _emit(context);
  }
}

bool JitReadTuples::_value_id_usable_in_chunk(const JitValueIdExpression value_id_expression, const Chunk& chunk) const {
  const auto column_id = _input_columns[value_id_expression.input_column_index].column_id;
  const auto segment = chunk.get_segment(column_id);
  return get_dictionary_segment(segment).dictionary_segment != nullptr;
}

void JitReadTuples::before_specialization(const Table& in_table) {
  if (in_table.chunk_count() == 0) return;

  const auto& chunk = *in_table.get_chunk(ChunkID{0});
  _value_id_expressions.erase(std::remove_if(_value_id_expressions.begin(),
                                             _value_id_expressions.end(),
                                             [&](const JitValueIdExpression value_id_expression) {
    return !_value_id_usable_in_chunk(value_id_expression, chunk);
  }), _value_id_expressions.end());

  if (!_value_id_expressions.empty()) {
    std::cout << "using value ids" << std::endl;
  }
  for (const auto& value_id_expression : _value_id_expressions) {
    ++_input_columns[value_id_expression.input_column_index].value_id_access_count;
    _enable_value_id_in_expression(value_id_expression);
  }
}

JitTupleEntry JitReadTuples::add_input_column(const DataType data_type, const bool is_nullable,
                                              const ColumnID column_id, const bool use_value_id) {
  // There is no need to add the same input column twice.
  // If the same column is requested for the second time, we return the JitTupleEntry created previously.
  const auto it = std::find_if(_input_columns.begin(), _input_columns.end(),
                               [&column_id](const auto& input_column) { return input_column.column_id == column_id; });
  if (it != _input_columns.end()) {
    ++it->total_access_count;
    return it->tuple_entry;
  }

  const auto tuple_entry = JitTupleEntry(data_type, is_nullable, _num_tuple_values++);
  _input_columns.push_back({column_id, tuple_entry, 1, 0});
  return tuple_entry;
}

JitTupleEntry JitReadTuples::add_literal_value(const AllTypeVariant& value, const bool use_value_id) {
  // Somebody needs a literal value. We assign it a position in the runtime tuple and store the literal value,
  // so we can initialize the corresponding tuple entry to the correct literal value later.
  const auto data_type = data_type_from_all_type_variant(value);
  const bool nullable = variant_is_null(value);
  const auto tuple_entry = JitTupleEntry(data_type, nullable, _num_tuple_values++);
  _input_literals.push_back({value, tuple_entry, use_value_id});
  return tuple_entry;
}

JitTupleEntry JitReadTuples::add_parameter(const DataType data_type, const ParameterID parameter_id, const bool use_value_id) {
  if (!use_value_id) {
    // Check if parameter was already added. A subquery uses the same parameter_id for all references to the same column.
    // The query "SELECT * FROM T1 WHERE EXISTS (SELECT * FROM T2 WHERE T1.a > T2.a AND T1.a < T2.b)" contains the
    // following subquery "SELECT * FROM T2 WHERE Parameter#0 > a AND Parameter#0 < b".
    const auto it =
        std::find_if(_input_parameters.begin(), _input_parameters.end(),
                     [parameter_id](const auto& parameter) { return parameter.parameter_id == parameter_id; });
    if (it != _input_parameters.end()) {
      return it->tuple_entry;
    }
  }

  const auto tuple_entry = JitTupleEntry(data_type, true, _num_tuple_values++);
  _input_parameters.push_back({parameter_id, tuple_entry, use_value_id});
  return tuple_entry;
}

size_t JitReadTuples::add_temporary_value() {
  // Somebody wants to store a temporary value in the runtime tuple. We don't really care about the value itself,
  // but have to remember to make some space for it when we create the runtime tuple.
  return _num_tuple_values++;
}

void JitReadTuples::_disable_value_id_in_expression(const JitValueIdExpression value_id_expression) {
  const auto expression = value_id_expression.jit_expression;

  // Check if expression does not use value id
  if (expression->left_child()->result_entry().data_type() != DataType::ValueID) return;

  // value_id_expression.input_column_index;
  const auto left_data_type = _input_columns[value_id_expression.input_column_index].tuple_entry.data_type();
  expression->left_child()->set_result_entry_type(left_data_type);
  if (jit_expression_is_binary(value_id_expression.expression_type)) {
    if (const auto literal_index = value_id_expression.input_literal_index) {
      expression->right_child()->set_result_entry_type(_input_literals[*literal_index].tuple_entry.data_type());
    } else {
      const auto parameter_index = value_id_expression.input_parameter_index;
      expression->right_child()->set_result_entry_type(_input_parameters[*parameter_index].tuple_entry.data_type());
    }

    expression->set_expression_type(value_id_expression.expression_type);
  }
}

void JitReadTuples::_enable_value_id_in_expression(const JitValueIdExpression value_id_expression) {
  // Check if expression uses value id
  if (value_id_expression.jit_expression->left_child()->result_entry().data_type() == DataType::ValueID) return;

  value_id_expression.jit_expression->left_child()->set_result_entry_type(DataType::ValueID);
  if (jit_expression_is_binary(value_id_expression.expression_type)) {
    value_id_expression.jit_expression->right_child()->set_result_entry_type(DataType::ValueID);

    // update expression types for > and <=
    if (value_id_expression.expression_type == JitExpressionType::GreaterThan) {
      value_id_expression.jit_expression->set_expression_type(JitExpressionType::GreaterThanEquals);
    } else if (value_id_expression.expression_type == JitExpressionType::LessThanEquals) {
      value_id_expression.jit_expression->set_expression_type(JitExpressionType::LessThan);
    }
  }
}

void JitReadTuples::add_value_id_expression(const std::shared_ptr<JitExpression>& jit_expression) {
  const auto find = [](const auto& vector, const JitTupleEntry& tuple_entry) -> std::optional<size_t> {
    // iterate backwards as the to be found items should have been inserted last
    const auto itr = std::find_if(vector.crbegin(), vector.crend(), [&tuple_entry](const auto& item) {
      return item.tuple_entry == tuple_entry;
    });
    if (itr != vector.crend()) {
      return std::distance(itr, vector.crend()) - 1;
    } else {
      return {};
    }
  };
  const auto column_id = find(_input_columns, jit_expression->left_child()->result_entry());
  DebugAssert(column_id, "Column id must be set.");

  const auto expression_type = jit_expression->expression_type();

  const auto right_child_result = jit_expression->right_child()->result_entry();
  std::optional<size_t> literal_id, parameter_id;
  if (jit_expression_is_binary(expression_type)) {
    literal_id = find(_input_literals, right_child_result);
    if (!literal_id) {
      parameter_id = find(_input_parameters, right_child_result);
    }
    DebugAssert(literal_id || parameter_id, "Neither input literal nor parameter index have been set.");
  }

  _value_id_expressions.push_back({jit_expression, expression_type, *column_id, literal_id, parameter_id});
}

const std::vector<JitInputColumn>& JitReadTuples::input_columns() const { return _input_columns; }

const std::vector<JitInputLiteral>& JitReadTuples::input_literals() const { return _input_literals; }

const std::vector<JitInputParameter>& JitReadTuples::input_parameters() const { return _input_parameters; }

const std::vector<JitValueIdExpression>& JitReadTuples::value_id_expressions() const { return _value_id_expressions; }

std::optional<ColumnID> JitReadTuples::find_input_column(const JitTupleEntry& tuple_entry) const {
  const auto it = std::find_if(_input_columns.begin(), _input_columns.end(), [&tuple_entry](const auto& input_column) {
    return input_column.tuple_entry == tuple_entry;
  });

  if (it != _input_columns.end()) {
    return it->column_id;
  } else {
    return {};
  }
}

std::optional<AllTypeVariant> JitReadTuples::find_literal_value(const JitTupleEntry& tuple_entry) const {
  const auto it = std::find_if(_input_literals.begin(), _input_literals.end(),
                               [&tuple_entry](const auto& literal) { return literal.tuple_entry == tuple_entry; });

  if (it != _input_literals.end()) {
    return it->value;
  } else {
    return {};
  }
}

std::shared_ptr<AbstractExpression> JitReadTuples::row_count_expression() const { return _row_count_expression; }

}  // namespace opossum
