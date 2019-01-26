#include "jit_read_tuples.hpp"

#include "../jit_types.hpp"
#include "resolve_type.hpp"
#include "storage/segment_iterate.hpp"

namespace opossum {

JitReadTuples::JitReadTuples(const bool has_validate) : _has_validate(has_validate) {}

std::string JitReadTuples::description() const {
  std::stringstream desc;
  desc << "[ReadTuple] ";
  for (const auto& input_column : _input_columns) {
    desc << "x" << input_column.tuple_value.tuple_index() << " = Column#" << input_column.column_id << ", ";
  }
  for (const auto& input_literal : _input_literals) {
    desc << "x" << input_literal.tuple_value.tuple_index() << " = " << input_literal.value << ", ";
  }
  return desc.str();
}

void JitReadTuples::before_query(const Table& in_table, JitRuntimeContext& context) const {
  // Create a runtime tuple of the appropriate size
  context.tuple.resize(_num_tuple_values);

  // Copy all input literals to the runtime tuple
  for (const auto& input_literal : _input_literals) {
    auto data_type = input_literal.tuple_value.data_type();
    if (data_type == DataType::Null) {
      input_literal.tuple_value.set_is_null(true, context);
    } else {
      resolve_data_type(data_type, [&](auto type) {
        using LiteralDataType = typename decltype(type)::type;
        input_literal.tuple_value.set<LiteralDataType>(boost::get<LiteralDataType>(input_literal.value), context);
        // Non-jit operators store bool values as int values
        if constexpr (std::is_same_v<LiteralDataType, Bool>) {
          input_literal.tuple_value.set<bool>(boost::get<LiteralDataType>(input_literal.value), context);
        }
      });
    }
  }
}

void JitReadTuples::before_chunk(const Table& in_table, const Chunk& in_chunk, JitRuntimeContext& context) const {
  context.inputs.clear();
  context.chunk_offset = 0;
  context.chunk_size = in_chunk.size();

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
            std::make_shared<JitSegmentReader<IteratorType, Type, true>>(it, input_column.tuple_value));
      });
    } else {
      segment_with_iterators(*segment, [&](auto it, const auto end) {
        using IteratorType = decltype(it);
        using Type = typename IteratorType::ValueType;
        context.inputs.push_back(
            std::make_shared<JitSegmentReader<IteratorType, Type, false>>(it, input_column.tuple_value));
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

JitTupleValue JitReadTuples::add_input_column(const DataType data_type, const bool is_nullable,
                                              const ColumnID column_id) {
  // There is no need to add the same input column twice.
  // If the same column is requested for the second time, we return the JitTupleValue created previously.
  const auto it = std::find_if(_input_columns.begin(), _input_columns.end(),
                               [&column_id](const auto& input_column) { return input_column.column_id == column_id; });
  if (it != _input_columns.end()) {
    return it->tuple_value;
  }

  const auto tuple_value = JitTupleValue(data_type, is_nullable, _num_tuple_values++);
  _input_columns.push_back({column_id, tuple_value});
  return tuple_value;
}

JitTupleValue JitReadTuples::add_literal_value(const AllTypeVariant& value) {
  // Somebody needs a literal value. We assign it a position in the runtime tuple and store the literal value,
  // so we can initialize the corresponding tuple value to the correct literal value later.
  const auto data_type = data_type_from_all_type_variant(value);
  const bool nullable = variant_is_null(value);
  const auto tuple_value = JitTupleValue(data_type, nullable, _num_tuple_values++);
  _input_literals.push_back({value, tuple_value});
  return tuple_value;
}

size_t JitReadTuples::add_temporary_value() {
  // Somebody wants to store a temporary value in the runtime tuple. We don't really care about the value itself,
  // but have to remember to make some space for it when we create the runtime tuple.
  return _num_tuple_values++;
}

std::vector<JitInputColumn> JitReadTuples::input_columns() const { return _input_columns; }

std::vector<JitInputLiteral> JitReadTuples::input_literals() const { return _input_literals; }

std::optional<ColumnID> JitReadTuples::find_input_column(const JitTupleValue& tuple_value) const {
  const auto it = std::find_if(_input_columns.begin(), _input_columns.end(), [&tuple_value](const auto& input_column) {
    return input_column.tuple_value == tuple_value;
  });

  if (it != _input_columns.end()) {
    return it->column_id;
  } else {
    return {};
  }
}

std::optional<AllTypeVariant> JitReadTuples::find_literal_value(const JitTupleValue& tuple_value) const {
  const auto it = std::find_if(_input_literals.begin(), _input_literals.end(),
                               [&tuple_value](const auto& literal) { return literal.tuple_value == tuple_value; });

  if (it != _input_literals.end()) {
    return it->value;
  } else {
    return {};
  }
}

}  // namespace opossum
