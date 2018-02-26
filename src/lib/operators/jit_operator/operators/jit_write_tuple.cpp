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

void JitWriteTuple::before_query(Table& out_table, JitRuntimeContext& context) {
  for (const auto& output_column : _output_columns) {
    // Add a column definition for each output column
    const auto data_type = output_column.tuple_value.data_type();
    const auto is_nullable = output_column.tuple_value.is_nullable();
    out_table.add_column_definition(output_column.column_name, data_type, is_nullable);
  }

  _create_output_chunk(context);
}

void JitWriteTuple::after_chunk(Table& out_table, JitRuntimeContext& context) const {
  if (context.out_chunk->size() > 0) {
    out_table.emplace_chunk(context.out_chunk);
    _create_output_chunk(context);
  }
}

void JitWriteTuple::add_output_column(const std::string& column_name, const JitTupleValue& value) {
  _output_columns.push_back({column_name, value});
}

void JitWriteTuple::_consume(JitRuntimeContext& context) const {
  for (const auto& output : context.outputs) {
    output->write_value();
  }
}

void JitWriteTuple::_create_output_chunk(JitRuntimeContext& context) const {
  context.out_chunk = std::make_shared<Chunk>();
  context.outputs.clear();

  // Create new value columns and add them to the runtime context to make them accessible by the column writers
  for (const auto& output_column : _output_columns) {
    const auto data_type = output_column.tuple_value.data_type();
    const auto is_nullable = output_column.tuple_value.is_nullable();

    // Create the appropriate column writer for the output column
    resolve_data_type(data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      auto column = std::make_shared<ValueColumn<ColumnDataType>>(output_column.tuple_value.is_nullable());
      context.out_chunk->add_column(column);

      if (is_nullable) {
        context.outputs.push_back(std::make_shared<JitColumnWriter<ValueColumn<ColumnDataType>, ColumnDataType, true>>(
                column, output_column.tuple_value.materialize(context)));
      } else {
        context.outputs.push_back(std::make_shared<JitColumnWriter<ValueColumn<ColumnDataType>, ColumnDataType, false>>(
                column, output_column.tuple_value.materialize(context)));
      }
    });
  }
}

}  // namespace opossum
