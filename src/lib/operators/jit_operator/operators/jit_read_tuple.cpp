#include "jit_read_tuple.hpp"

#include "constant_mappings.hpp"
#include "resolve_type.hpp"
#include "storage/create_iterable_from_column.hpp"

namespace opossum {

std::string JitReadTuple::description() const {
  std::stringstream desc;
  desc << "[ReadTuple] ";
  for (const auto& input_column : _input_columns) {
    desc << "x" << input_column.tuple_value.tuple_index() << " = Col#" << input_column.column_id << ", ";
  }
  for (const auto& input_literal : _input_literals) {
    desc << "x" << input_literal.tuple_value.tuple_index() << " = " << input_literal.value << ", ";
  }
  return desc.str();
}

void JitReadTuple::before_query(const Table& in_table, JitRuntimeContext& ctx) {
  // Create a runtime tuple of the appropriate size
  ctx.tuple.resize(_num_tuple_values);

  // Copy all input literals to the runtime tuple
  for (const auto& input_literal : _input_literals) {
    auto data_type = input_literal.tuple_value.data_type();
    resolve_data_type(data_type, [&](auto type) {
      using DataType = typename decltype(type)::type;
      input_literal.tuple_value.materialize(ctx).set<DataType>(boost::get<DataType>(input_literal.value));
    });
  }

  // Create a JitColumnReader for each input column. We use the first chunk in the table to determine the column type.
  for (const auto& input_column : _input_columns) {
    const auto column_id = input_column.column_id;
    const auto column = in_table.get_chunk(opossum::ChunkID{0})->get_column(column_id);
    const auto data_type = in_table.column_type(column_id);
    const auto is_nullable = in_table.column_is_nullable(column_id);

    opossum::resolve_data_and_column_type(data_type, *column, [&](auto type, auto& typed_column) {
      using ColumnDataType = typename decltype(type)::type;
      auto iterable = opossum::create_iterable_from_column<ColumnDataType>(typed_column);
      iterable.with_iterators([&](auto it, auto end) {
        using IteratorType = decltype(it);
        if (is_nullable) {
          _column_readers.push_back(std::make_shared<JitColumnReader<IteratorType, ColumnDataType, true>>(
              _column_readers.size(), input_column.tuple_value));
        } else {
          _column_readers.push_back(std::make_shared<JitColumnReader<IteratorType, ColumnDataType, false>>(
              _column_readers.size(), input_column.tuple_value));
        }
      });
    });
  }
}

void JitReadTuple::before_chunk(const Table& in_table, const Chunk& in_chunk, JitRuntimeContext& ctx) const {
  ctx.inputs.clear();

  // Create the column iterator for each input column and store them to the runtime context
  for (const auto& input_column : _input_columns) {
    const auto column_id = input_column.column_id;
    const auto column = in_chunk.get_column(column_id);
    const auto column_data_type = in_table.column_type(column_id);
    resolve_data_and_column_type(column_data_type, *column, [&](auto type, auto& typed_column) {
      using ColumnDataType = typename decltype(type)::type;
      create_iterable_from_column<ColumnDataType>(typed_column).with_iterators([&](auto it, auto end) {
        using IteratorType = decltype(it);
        ctx.inputs.push_back(std::make_shared<IteratorType>(it));
      });
    });
  }
}

void JitReadTuple::execute(JitRuntimeContext& ctx) const {
  for (; ctx.chunk_offset < ctx.chunk_size; ++ctx.chunk_offset) {
    // We read from and advance all column iterators, before passing the tuple on to the next operator.
    for (const auto& column_reader : _column_readers) {
      column_reader->read_value(ctx);
      column_reader->increment(ctx);
    }
    emit(ctx);
  }
}

JitTupleValue JitReadTuple::add_input_column(const DataType data_type, const bool is_nullable,
                                             const ColumnID column_id) {
  // There is no need to add the same input column twice.
  const auto it = std::find_if(_input_columns.begin(), _input_columns.end(),
                               [&column_id](const auto& input_column) { return input_column.column_id == column_id; });
  if (it != _input_columns.end()) {
    return it->tuple_value;
  }

  const auto tuple_value = JitTupleValue(data_type, is_nullable, _num_tuple_values++);
  _input_columns.push_back({column_id, tuple_value});
  return tuple_value;
}

JitTupleValue JitReadTuple::add_literal_value(const AllTypeVariant& value) {
  // Somebody needs a literal value. We assign it a position in the runtime tuple and store the literal value,
  // so we can initialize the corresponding tuple value to the correct literal value later.
  const auto data_type = data_type_from_all_type_variant(value);
  const auto tuple_value = JitTupleValue(data_type, false, _num_tuple_values++);
  _input_literals.push_back({value, tuple_value});
  return tuple_value;
}

size_t JitReadTuple::add_temorary_value() {
  // Somebody wahts to store a temporary value in the runtime tuple. We don't really care about the value itself,
  // but have to remember to make some space for it when we create the runtime tuple.
  return _num_tuple_values++;
}

}  // namespace opossum
