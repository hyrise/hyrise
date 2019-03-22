#include "jit_read_tuples.hpp"

#include "../jit_types.hpp"
#include "expression/evaluation/expression_evaluator.hpp"
#include "resolve_type.hpp"
#include "storage/segment_iterate.hpp"

namespace opossum {

JitReadTuples::JitReadTuples(const bool has_validate, const std::shared_ptr<AbstractExpression>& row_count_expression)
    : _has_validate(has_validate), _row_count_expression(row_count_expression) {}

std::string JitReadTuples::description() const {
  std::stringstream desc;
  desc << "[ReadTuple] ";
  for (const auto& input_column : _input_columns) {
    desc << "x" << input_column.tuple_entry.tuple_index() << " = Column#" << input_column.column_id << ", ";
  }
  for (const auto& input_literal : _input_literals) {
    desc << "x" << input_literal.tuple_entry.tuple_index() << " = " << input_literal.value << ", ";
  }
  for (const auto& input_parameter : _input_parameters) {
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

void JitReadTuples::before_chunk(const Table& in_table, const ChunkID chunk_id, JitRuntimeContext& context) const {
  const auto& in_chunk = *in_table.get_chunk(chunk_id);

  context.inputs.clear();
  context.chunk_offset = 0;
  context.chunk_size = in_chunk.size();
  context.chunk_id = chunk_id;

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

  // Create the segment iterator for each input segment and store them to the runtime context
  for (const auto& input_column : _input_columns) {
    const auto column_id = input_column.column_id;
    const auto segment = in_chunk.get_segment(column_id);
    const auto is_nullable = in_table.column_is_nullable(column_id);

    if (is_nullable) {
      segment_with_iterators(*segment, [&](auto it, const auto end) {
        using IteratorType = decltype(it);
        using Type = typename IteratorType::ValueType;

        context.inputs.push_back(
            std::make_shared<JitSegmentReader<IteratorType, Type, true>>(it, input_column.tuple_entry));
      });
    } else {
      segment_with_iterators(*segment, [&](auto it, const auto end) {
        using IteratorType = decltype(it);
        using Type = typename IteratorType::ValueType;
        context.inputs.push_back(
            std::make_shared<JitSegmentReader<IteratorType, Type, false>>(it, input_column.tuple_entry));
      });
    }
  }
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

JitTupleEntry JitReadTuples::add_input_column(const DataType data_type, const bool is_nullable,
                                              const ColumnID column_id) {
  // There is no need to add the same input column twice.
  // If the same column is requested for the second time, we return the JitTupleEntry created previously.
  const auto it = std::find_if(_input_columns.begin(), _input_columns.end(),
                               [&column_id](const auto& input_column) { return input_column.column_id == column_id; });
  if (it != _input_columns.end()) {
    return it->tuple_entry;
  }

  const auto tuple_entry = JitTupleEntry(data_type, is_nullable, _num_tuple_values++);
  _input_columns.push_back({column_id, tuple_entry});
  return tuple_entry;
}

JitTupleEntry JitReadTuples::add_literal_value(const AllTypeVariant& value) {
  // Somebody needs a literal value. We assign it a position in the runtime tuple and store the literal value,
  // so we can initialize the corresponding tuple entry to the correct literal value later.
  const auto data_type = data_type_from_all_type_variant(value);
  const bool nullable = variant_is_null(value);
  const auto tuple_entry = JitTupleEntry(data_type, nullable, _num_tuple_values++);
  _input_literals.push_back({value, tuple_entry});
  return tuple_entry;
}

JitTupleEntry JitReadTuples::add_parameter(const DataType data_type, const ParameterID parameter_id) {
  // Check if parameter was already added. A subquery uses the same parameter_id for all references to the same column.
  // The query "SELECT * FROM T1 WHERE EXISTS (SELECT * FROM T2 WHERE T1.a > T2.a AND T1.a < T2.b)" contains the
  // following subquery "SELECT * FROM T2 WHERE Parameter#0 > a AND Parameter#0 < b".
  const auto it =
      std::find_if(_input_parameters.begin(), _input_parameters.end(),
                   [parameter_id](const auto& parameter) { return parameter.parameter_id == parameter_id; });
  if (it != _input_parameters.end()) {
    return it->tuple_entry;
  }

  const auto tuple_entry = JitTupleEntry(data_type, true, _num_tuple_values++);
  _input_parameters.push_back({parameter_id, tuple_entry});
  return tuple_entry;
}

size_t JitReadTuples::add_temporary_value() {
  // Somebody wants to store a temporary value in the runtime tuple. We don't really care about the value itself,
  // but have to remember to make some space for it when we create the runtime tuple.
  return _num_tuple_values++;
}

const std::vector<JitInputColumn>& JitReadTuples::input_columns() const { return _input_columns; }

const std::vector<JitInputLiteral>& JitReadTuples::input_literals() const { return _input_literals; }

const std::vector<JitInputParameter>& JitReadTuples::input_parameters() const { return _input_parameters; }

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
