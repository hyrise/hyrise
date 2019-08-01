#include "jit_read_tuples.hpp"

#include "../jit_types.hpp"
#include "all_type_variant.hpp"
#include "expression/evaluation/expression_evaluator.hpp"
#include "jit_expression.hpp"
#include "resolve_type.hpp"
#include "storage/segment_iterables/create_iterable_from_attribute_vector.hpp"
#include "storage/segment_iterate.hpp"

namespace opossum {

namespace {

/*
 * AttributeIterableData holds all data to create a (filtered) iterable from an attribute vector of a dictionary
 * segment.
 *
 * If dictionary_segment is a nullptr, the iterable cannot be created.
 * If pos_list is a nullptr, no pos list is required to create the iterable.
 */
struct AttributeIterableData {
  std::shared_ptr<const BaseDictionarySegment> dictionary_segment = nullptr;
  std::shared_ptr<const PosList> pos_list = nullptr;
};

AttributeIterableData get_attribute_iterable_data(const std::shared_ptr<const BaseSegment>& segment) {
  // Only returns the attribute iterable data instead of the iterable itself as this method is also used to check
  // whether an iterable can be created (dictionary_segment != nullptr).
  // If this is not the case, a segment iterable is used.

  // Check if the input segment is a value segment that is dictionary-encoded
  if (const auto dict_segment = std::dynamic_pointer_cast<const BaseDictionarySegment>(segment)) {
    return {dict_segment};
  }

  // Check if the input segment is a reference segment that references a dictionary-encoded value segment
  const auto reference_segment = std::dynamic_pointer_cast<const ReferenceSegment>(segment);
  if (!reference_segment) return {};

  const auto pos_list = reference_segment->pos_list();
  if (!pos_list->references_single_chunk() || pos_list->empty()) return {};

  const auto referenced_chunk = reference_segment->referenced_table()->get_chunk(pos_list->common_chunk_id());
  const auto referenced_segment = referenced_chunk->get_segment(reference_segment->referenced_column_id());
  const auto dict_segment = std::dynamic_pointer_cast<const BaseDictionarySegment>(referenced_segment);
  return {dict_segment, pos_list};
}

ValueID get_search_value_id(const JitExpressionType expression_type,
                            const std::shared_ptr<const BaseDictionarySegment>& dict_segment,
                            const AllTypeVariant& value) {
  // Lookup the value id according to the comparison operator
  // See operators/table_scan/column_vs_value_table_scan_impl.cpp for details
  switch (expression_type) {
    case JitExpressionType::Equals:
    case JitExpressionType::NotEquals: {
      const auto value_id = dict_segment->lower_bound(value);
      // Check if value exists in dictionary
      if (value_id < dict_segment->unique_values_count() && dict_segment->value_of_value_id(value_id) != value) {
        return INVALID_VALUE_ID;
      }
      return value_id;
    }
    case JitExpressionType::LessThan:
    case JitExpressionType::GreaterThanEquals:
      return dict_segment->lower_bound(value);
    case JitExpressionType::LessThanEquals:
    case JitExpressionType::GreaterThan:
      return dict_segment->upper_bound(value);
    default:
      Fail("Unsupported expression type for binary value id predicate");
  }
}

}  // namespace

JitReadTuples::JitReadTuples(const bool has_validate, const std::shared_ptr<AbstractExpression>& row_count_expression)
    : row_count_expression(row_count_expression), _has_validate(has_validate) {}

std::string JitReadTuples::description() const {
  std::stringstream desc;
  desc << "[ReadTuple] ";
  for (const auto& input_column : _input_columns) {
    desc << "x" << input_column.tuple_entry.tuple_index << " = Column#" << input_column.column_id << ", ";
  }
  for (const auto& input_literal : _input_literals) {
    desc << "x" << input_literal.tuple_entry.tuple_index << " = " << input_literal.value << ", ";
  }
  for (const auto& input_parameter : _input_parameters) {
    desc << "x" << input_parameter.tuple_entry.tuple_index << " = Parameter#" << input_parameter.parameter_id << ", ";
  }
  return desc.str();
}

void JitReadTuples::before_specialization(const Table& in_table, std::vector<bool>& tuple_non_nullable_information) {
  // Update the nullable information in the JitExpressions accessing input table values
  tuple_non_nullable_information.resize(_num_tuple_values);
  for (auto& input_column : _input_columns) {
    input_column.tuple_entry.guaranteed_non_null = !in_table.column_is_nullable(input_column.column_id);
    tuple_non_nullable_information[input_column.tuple_entry.tuple_index] = input_column.tuple_entry.guaranteed_non_null;
  }

  // Check for each JitValueIdExpression whether its referenced expression can be actually used with value ids.
  // If it can not be used, the JitValueIdExpression is removed from the vector of JitValueIdExpressions.
  // It it can be used, the corresponding expression is updated to use value ids.

  if (in_table.chunk_count() == 0) return;

  const auto chunk = in_table.get_chunk(ChunkID{0});
  Assert(chunk, "Did not expect deleted chunk here.");  // see #1686

  // Remove expressions that use a column where the first segment is not dictionary-encoded
  _value_id_expressions.erase(
      std::remove_if(_value_id_expressions.begin(), _value_id_expressions.end(),
                     [&](const JitValueIdExpression& value_id_expression) {
                       const auto column_id = _input_columns[value_id_expression.input_column_index].column_id;
                       const auto segment = chunk->get_segment(column_id);
                       const auto casted_dictionary = get_attribute_iterable_data(segment);
                       const bool remove = casted_dictionary.dictionary_segment == nullptr;
                       if (remove) {
                         // Ensure that the actual values are loaded as this expression cannot use value ids
                         _input_columns[value_id_expression.input_column_index].use_actual_value = true;
                       }
                       return remove;
                     }),
      _value_id_expressions.end());

  // Update the remaining value id expressions
  for (const auto& value_id_expression : _value_id_expressions) {
    value_id_expression.jit_expression->use_value_ids = true;
  }
}

void JitReadTuples::before_query(const Table& in_table, const std::vector<AllTypeVariant>& parameter_values,
                                 JitRuntimeContext& context) const {
  // Create a runtime tuple of the appropriate size
  context.tuple.resize(_num_tuple_values);

  const auto set_value_in_tuple = [&](const JitTupleEntry& tuple_entry, const AllTypeVariant& value) {
    auto data_type = tuple_entry.data_type;
    if (data_type == DataType::Null || variant_is_null(value)) {
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
  if (row_count_expression) {
    resolve_data_type(row_count_expression->data_type(), [&](const auto data_type_t) {
      using LimitDataType = typename decltype(data_type_t)::type;

      if constexpr (std::is_integral_v<LimitDataType>) {
        const auto num_rows_expression_result =
            ExpressionEvaluator{}.evaluate_expression_to_result<LimitDataType>(*row_count_expression);
        Assert(num_rows_expression_result->size() == 1, "Expected exactly one row for Limit");
        Assert(!num_rows_expression_result->is_null(0), "Expected non-null for Limit");

        const auto signed_num_rows = num_rows_expression_result->value(0);
        Assert(signed_num_rows >= 0, "Can't Limit to a negative number of Rows");

        context.limit_rows = static_cast<size_t>(signed_num_rows);
      } else {
        Fail("Non-integral types not allowed in Limit");
      }
    });
  } else {
    context.limit_rows = std::numeric_limits<size_t>::max();
  }
}

bool JitReadTuples::before_chunk(const Table& in_table, const ChunkID chunk_id,
                                 const std::vector<AllTypeVariant>& parameter_values, JitRuntimeContext& context) {
  const auto& in_chunk = *in_table.get_chunk(chunk_id);

  context.inputs.clear();
  context.chunk_offset = 0;
  context.chunk_size = in_chunk.size();
  context.chunk_id = chunk_id;

  // Further preparation for current chunk can be skipped if it is empty.
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

  const auto add_iterator = [&](auto it, auto type, const JitInputColumn& input_column, const bool is_nullable) {
    using IteratorType = decltype(it);
    using Type = decltype(type);
    if (is_nullable) {
      context.inputs.push_back(std::make_shared<JitReadTuples::JitSegmentReader<IteratorType, Type, true>>(
          it, input_column.tuple_entry.tuple_index));
    } else {
      context.inputs.push_back(std::make_shared<JitReadTuples::JitSegmentReader<IteratorType, Type, false>>(
          it, input_column.tuple_entry.tuple_index));
    }
  };

  bool use_specialization = true;

  std::vector<bool> segments_are_dictionaries(_input_columns.size(), false);
  for (const auto& value_id_expression : _value_id_expressions) {
    // Check for each expression using value ids whether the corresponding segment is dictionary-encoded.
    const auto& jit_input_column = _input_columns[value_id_expression.input_column_index];
    const auto segment = in_chunk.get_segment(jit_input_column.column_id);
    const auto dict_segment = get_attribute_iterable_data(segment).dictionary_segment;
    segments_are_dictionaries[value_id_expression.input_column_index] = dict_segment != nullptr;

    if (dict_segment) {
      const auto expression_type = value_id_expression.jit_expression->expression_type;
      if (jit_expression_is_binary(expression_type)) {
        // Set the searched value id for each expression according to the segment's dictionary in the runtime tuple.

        // Retrieve the searched value
        AllTypeVariant value;
        size_t tuple_index;
        if (const auto literal_index = value_id_expression.input_literal_index) {
          value = _input_literals[*literal_index].value;
          tuple_index = _input_literals[*literal_index].tuple_entry.tuple_index;
        } else {
          const auto parameter_index = value_id_expression.input_parameter_index;
          value = parameter_values[*parameter_index];
          tuple_index = _input_parameters[*parameter_index].tuple_entry.tuple_index;
        }

        // Null values are set in before_query() function
        if (variant_is_null(value)) continue;

        // Convert the value to the column data type
        AllTypeVariant casted_value;
        resolve_data_type(jit_input_column.tuple_entry.data_type, [&](const auto column_data_type_t) {
          using ColumnDataType = typename decltype(column_data_type_t)::type;

          resolve_data_type(data_type_from_all_type_variant(value), [&](const auto value_data_type_t) {
            using ValueDataType = typename decltype(value_data_type_t)::type;

            if constexpr (std::is_same_v<ColumnDataType, pmr_string> == std::is_same_v<ValueDataType, pmr_string>) {
              casted_value = static_cast<ColumnDataType>(boost::get<ValueDataType>(value));
            } else {
              Fail("Cannot compare string type with non-string type");
            }
          });
        });

        // Lookup the value id according to the comparison operator
        const auto value_id = get_search_value_id(expression_type, dict_segment, casted_value);
        context.tuple.set<ValueID>(tuple_index, value_id);
      }
    } else {
      use_specialization = false;
    }
  }

  // If the specialized function cannot be used, the jit expressions must be updated according to the encoding of the
  // current chunk
  if (!use_specialization) {
    for (const auto& value_id_expression : _value_id_expressions) {
      const bool use_value_ids = segments_are_dictionaries[value_id_expression.input_column_index];
      value_id_expression.jit_expression->use_value_ids = use_value_ids;
    }
  }

  // Create the segment iterator for each input segment and store them to the runtime context
  for (size_t input_column_index{0}; input_column_index < _input_columns.size(); ++input_column_index) {
    const auto& input_column = _input_columns[input_column_index];
    const auto column_id = input_column.column_id;
    const auto segment = in_chunk.get_segment(column_id);
    const auto is_nullable = in_table.column_is_nullable(column_id);

    if (segments_are_dictionaries[input_column_index]) {
      // We need the value ids from a dictionary segment
      const auto [dict_segment, pos_list] = get_attribute_iterable_data(segment);
      DebugAssert(dict_segment, "Segment is not a dictionary or a reference segment referencing a dictionary");
      if (pos_list) {
        create_iterable_from_attribute_vector(*dict_segment).with_iterators(pos_list, [&](auto it, auto end) {
          add_iterator(it, ValueID{}, input_column, is_nullable);
        });
      } else {
        create_iterable_from_attribute_vector(*dict_segment).with_iterators([&](auto it, auto end) {
          add_iterator(it, ValueID{}, input_column, is_nullable);
        });
      }
    }

    // A query can require both value ids (for predicates) and actual values (for math operations) of a column
    if (input_column.use_actual_value || !segments_are_dictionaries[input_column_index]) {
      // We need the actual values of a segment
      segment_with_iterators(*segment, [&](auto it, const auto end) {
        using Type = typename decltype(it)::ValueType;
        add_iterator(it, Type{}, input_column, is_nullable);
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

JitTupleEntry JitReadTuples::add_input_column(const DataType data_type, const bool guaranteed_non_null,
                                              const ColumnID column_id, const bool use_actual_value) {
  // There is no need to add the same input column twice.
  // If the same column is requested for the second time, we return the JitTupleEntry created previously.
  const auto it = std::find_if(_input_columns.begin(), _input_columns.end(),
                               [&column_id](const auto& input_column) { return input_column.column_id == column_id; });
  if (it != _input_columns.end()) {
    it->use_actual_value |= use_actual_value;
    return it->tuple_entry;
  }

  const auto tuple_entry = JitTupleEntry(data_type, guaranteed_non_null, _num_tuple_values++);
  _input_columns.push_back({column_id, tuple_entry, use_actual_value});
  return tuple_entry;
}

JitTupleEntry JitReadTuples::add_literal_value(const AllTypeVariant& value) {
  // Somebody needs a literal value. We assign it a position in the runtime tuple and store the literal value,
  // so we can initialize the corresponding tuple entry to the correct literal value later.
  const auto data_type = data_type_from_all_type_variant(value);
  const bool guaranteed_non_null = !variant_is_null(value);
  const auto tuple_entry = JitTupleEntry(data_type, guaranteed_non_null, _num_tuple_values++);
  _input_literals.push_back({value, tuple_entry});
  return tuple_entry;
}

JitTupleEntry JitReadTuples::add_parameter(const DataType data_type, const ParameterID parameter_id) {
  const auto tuple_entry = JitTupleEntry(data_type, false, _num_tuple_values++);
  _input_parameters.push_back({parameter_id, tuple_entry});
  return tuple_entry;
}

size_t JitReadTuples::add_temporary_value() {
  // Somebody wants to store a temporary value in the runtime tuple. We don't really care about the value itself,
  // but have to remember to make some space for it when we create the runtime tuple.
  return _num_tuple_values++;
}

void JitReadTuples::add_value_id_expression(const std::shared_ptr<JitExpression>& jit_expression) {
  // Function ensures that the expression operands were added as input columns, values or parameters.
  // If this is the case, a reference to the expression is stored with the indices to the corresponding vector entries
  // which hold the information for one operand.

  const auto find_vector_entry = [](const auto& vector, const JitTupleEntry& tuple_entry) -> std::optional<size_t> {
    // Iterate backwards as the to be found items should have been inserted last
    const auto it = std::find_if(vector.crbegin(), vector.crend(),
                                 [&tuple_entry](const auto& item) { return item.tuple_entry == tuple_entry; });
    if (it != vector.crend()) {
      return std::distance(it, vector.crend()) - 1;  // -1 required due to backwards iterators
    }
    return std::nullopt;
  };
  const auto column_index = find_vector_entry(_input_columns, jit_expression->left_child->result_entry);
  Assert(column_index, "Column index must be set.");

  std::optional<size_t> literal_index, parameter_index;
  if (jit_expression_is_binary(jit_expression->expression_type)) {
    const auto right_child_result = jit_expression->right_child->result_entry;
    literal_index = find_vector_entry(_input_literals, right_child_result);
    if (!literal_index) {
      parameter_index = find_vector_entry(_input_parameters, right_child_result);
      Assert(parameter_index, "Neither input literal nor parameter index have been set.");
    }
  }

  _value_id_expressions.push_back({jit_expression, *column_index, literal_index, parameter_index});
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
    return std::nullopt;
  }
}

std::optional<AllTypeVariant> JitReadTuples::find_literal_value(const JitTupleEntry& tuple_entry) const {
  const auto it = std::find_if(_input_literals.begin(), _input_literals.end(),
                               [&tuple_entry](const auto& literal) { return literal.tuple_entry == tuple_entry; });

  if (it != _input_literals.end()) {
    return it->value;
  } else {
    return std::nullopt;
  }
}

}  // namespace opossum
