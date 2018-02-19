#include "jit_write_tuple.hpp"

#include "constant_mappings.hpp"
#include "resolve_type.hpp"
#include "storage/base_value_column.hpp"
#include "storage/value_column.hpp"

namespace opossum {

std::string JitWriteTuple::description() const {
  std::stringstream desc;
  desc << "[WriteTuple] ";
  for (const auto& output_column : _output_columns) {
    desc << output_column.column_name << " = x" << output_column.tuple_value.tuple_index() << ", ";
  }
  return desc.str();
}

void JitWriteTuple::before_query(Table& out_table, JitRuntimeContext& ctx) {
  for (const auto& output_column : _output_columns) {
    // Add a column definition for each output column
    const auto data_type = jit_data_type_to_data_type.at(output_column.tuple_value.data_type());
    const auto is_nullable = output_column.tuple_value.is_nullable();
    out_table.add_column_definition(output_column.column_name, data_type, is_nullable);

    // Create the appropriate column writer for each output column
    resolve_data_type(data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      if (is_nullable) {
        _column_writers.push_back(std::make_shared<JitColumnWriter<ValueColumn<ColumnDataType>, ColumnDataType, true>>(
            _column_writers.size(), output_column.tuple_value));
      } else {
        _column_writers.push_back(std::make_shared<JitColumnWriter<ValueColumn<ColumnDataType>, ColumnDataType, false>>(
            _column_writers.size(), output_column.tuple_value));
      }
    });
  }

  _create_output_chunk(ctx);
}

void JitWriteTuple::after_chunk(Table& out_table, JitRuntimeContext& ctx) const {
  if (ctx.out_chunk->size() > 0) {
    out_table.emplace_chunk(ctx.out_chunk);
    _create_output_chunk(ctx);
  }
}

void JitWriteTuple::add_output_column(const std::string& column_name, const JitTupleValue& value) {
  _output_columns.push_back({column_name, value});
}

void JitWriteTuple::next(JitRuntimeContext& ctx) const {
  for (const auto& column_writer : _column_writers) {
    column_writer->write_value(ctx);
  }
}

void JitWriteTuple::_create_output_chunk(JitRuntimeContext& ctx) const {
  ctx.out_chunk = std::make_shared<Chunk>();
  ctx.outputs.clear();

  // Create new value columns and add them to the runtime context to make them accessible by the column writers
  for (const auto& output_column : _output_columns) {
    const auto data_type = jit_data_type_to_data_type.at(output_column.tuple_value.data_type());
    resolve_data_type(data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      auto column = std::make_shared<ValueColumn<ColumnDataType>>(output_column.tuple_value.is_nullable());
      ctx.outputs.push_back(column);
      ctx.out_chunk->add_column(column);
    });
  }
}

}  // namespace opossum
