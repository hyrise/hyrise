#include "jit_save_table.hpp"

#include "constant_mappings.hpp"
#include "resolve_type.hpp"
#include "storage/base_value_column.hpp"
#include "storage/value_column.hpp"

namespace opossum {

std::string JitSaveTable::description() const {
  std::stringstream desc;
  desc << "[SaveTable] ";
  for (const auto& output_column : _output_columns) {
    desc << output_column.first << " = x" << output_column.second.tuple_index() << ", ";
  }
  return desc.str();
}

void JitSaveTable::before_query(Table& out_table, JitRuntimeContext& ctx) {
  for (const auto& output_column : _output_columns) {
    const auto data_type = jit_data_type_to_data_type.at(output_column.second.data_type());
    const auto is_nullable = output_column.second.is_nullable();
    out_table.add_column_definition(output_column.first, data_type, is_nullable);

    resolve_data_type(data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      if (is_nullable) {
        _column_writers.push_back(std::make_shared<JitColumnWriter<ValueColumn<ColumnDataType>, ColumnDataType, true>>(
            _column_writers.size(), output_column.second));
      } else {
        _column_writers.push_back(std::make_shared<JitColumnWriter<ValueColumn<ColumnDataType>, ColumnDataType, false>>(
            _column_writers.size(), output_column.second));
      }
    });
  }

  _create_output_chunk(ctx);
}

void JitSaveTable::after_chunk(Table& out_table, JitRuntimeContext& ctx) const {
  if (ctx.out_chunk->size() > 0) {
    out_table.emplace_chunk(ctx.out_chunk);
    _create_output_chunk(ctx);
  }
}

void JitSaveTable::add_output_column(const std::string& column_name, const JitTupleValue& value) {
  _output_columns.push_back(std::make_pair(column_name, value));
}

void JitSaveTable::next(JitRuntimeContext& ctx) const {
  for (const auto& column_writer : _column_writers) {
    column_writer->write_value(ctx);
  }
}

void JitSaveTable::_create_output_chunk(JitRuntimeContext& ctx) const {
  ctx.out_chunk = std::make_shared<Chunk>();
  ctx.outputs.clear();

  for (const auto& output_column : _output_columns) {
    const auto data_type = jit_data_type_to_data_type.at(output_column.second.data_type());
    resolve_data_type(data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      auto column = std::make_shared<ValueColumn<ColumnDataType>>(output_column.second.is_nullable());
      ctx.outputs.push_back(column);
      ctx.out_chunk->add_column(column);
    });
  }
}

}  // namespace opossum
