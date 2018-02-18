#include "jit_read_tuple.hpp"

#include "constant_mappings.hpp"
#include "resolve_type.hpp"
#include "storage/create_iterable_from_column.hpp"

namespace opossum {

std::string JitReadTuple::description() const {
  std::stringstream desc;
  desc << "[ReadTuple] ";
  for (const auto& input_column : _input_columns) {
    desc << "x" << input_column.second.tuple_index() << " = Col#" << input_column.first << ", ";
  }
  for (const auto& input_literal : _input_literals) {
    desc << "x" << input_literal.second.tuple_index() << " = " << input_literal.first << ", ";
  }
  return desc.str();
}

void JitReadTuple::before_query(const Table& in_table, JitRuntimeContext& ctx) {
  ctx.tuple.resize(_num_tuple_values);

  for (const auto& input_literal : _input_literals) {
    auto data_type = jit_data_type_to_data_type.at(input_literal.second.data_type());
    resolve_data_type(data_type, [&](auto type) {
      using DataType = typename decltype(type)::type;
      input_literal.second.materialize(ctx).as<DataType>() = boost::get<DataType>(input_literal.first);
    });
  }

  for (const auto& input_column : _input_columns) {
    const auto column_id = input_column.first;
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
              _column_readers.size(), input_column.second));
        } else {
          _column_readers.push_back(std::make_shared<JitColumnReader<IteratorType, ColumnDataType, false>>(
              _column_readers.size(), input_column.second));
        }
      });
    });
  }
}

void JitReadTuple::before_chunk(const Table& in_table, const Chunk& in_chunk, JitRuntimeContext& ctx) const {
  ctx.inputs.clear();

  for (const auto& input_column : _input_columns) {
    const auto column = in_chunk.get_column(input_column.first);
    const auto column_data_type = in_table.column_type(input_column.first);
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
    for (const auto& column_reader : _column_readers) {
      column_reader->read_value(ctx);
      column_reader->increment(ctx);
    }
    emit(ctx);
  }
}

JitTupleValue JitReadTuple::add_input_column(const Table& table, const ColumnID column_id) {
  const auto it = std::find_if(_input_columns.begin(), _input_columns.end(),
                               [&column_id](const auto& input_column) { return input_column.first == column_id; });
  if (it != _input_columns.end()) {
    return it->second;
  }

  const auto data_type = data_type_to_jit_data_type.at(table.column_type(column_id));
  const auto is_nullable = table.column_is_nullable(column_id);
  const auto tuple_value = JitTupleValue(data_type, is_nullable, _num_tuple_values++);
  _input_columns.push_back(std::make_pair(column_id, tuple_value));
  return tuple_value;
}

JitTupleValue JitReadTuple::add_literal_value(const AllTypeVariant& value) {
  const auto data_type = data_type_to_jit_data_type.at(data_type_from_all_type_variant(value));
  const auto tuple_value = JitTupleValue(data_type, false, _num_tuple_values++);
  _input_literals.push_back(std::make_pair(value, tuple_value));
  return tuple_value;
}

size_t JitReadTuple::add_temorary_value() { return _num_tuple_values++; }

}  // namespace opossum
