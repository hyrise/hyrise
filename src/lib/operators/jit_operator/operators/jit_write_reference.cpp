#include "jit_write_reference.hpp"

#include "../jit_types.hpp"
#include "constant_mappings.hpp"
#include "resolve_type.hpp"
#include "storage/base_value_segment.hpp"
#include "storage/value_segment.hpp"

namespace opossum {

std::string JitWriteReference::description() const {
  std::stringstream desc;
  desc << "[WriteReference] ";
  for (const auto& output_column : _output_columns) {
    desc << output_column.column_name << " = Col#" << output_column.referenced_column_id << ", ";
  }
  return desc.str();
}

std::shared_ptr<Table> JitWriteReference::create_output_table(const Table& in_table) const {
  TableColumnDefinitions column_definitions;

  const auto& input_column_definitions = in_table.column_definitions();

  for (const auto& output_column : _output_columns) {
    auto column_definition = input_column_definitions[output_column.referenced_column_id];
    column_definition.name = output_column.column_name;
    column_definitions.push_back(column_definition);
  }

  return std::make_shared<Table>(column_definitions, TableType::References);
}

void JitWriteReference::before_query(Table& out_table, JitRuntimeContext& context) const {
  context.output_pos_list = std::make_shared<PosList>();
}

void JitWriteReference::after_chunk(const std::shared_ptr<const Table>& in_table, Table& out_table,
                                    JitRuntimeContext& context) const {
  if (context.output_pos_list->size() > 0) {
    Segments out_segments;

    out_segments.reserve(_output_columns.size());

    const auto chunk_in = in_table->get_chunk(context.chunk_id);
    if (in_table->type() == TableType::References) {
      auto filtered_pos_lists = std::map<std::shared_ptr<const PosList>, std::shared_ptr<PosList>>{};

      for (const auto& output_column : _output_columns) {
        auto segment_in = chunk_in->get_segment(output_column.referenced_column_id);

        auto ref_segment_in = std::dynamic_pointer_cast<const ReferenceSegment>(segment_in);
        DebugAssert(ref_segment_in != nullptr, "All columns should be of type ReferenceColumn.");

        const auto pos_list_in = ref_segment_in->pos_list();

        const auto table_out = ref_segment_in->referenced_table();
        const auto column_id_out = ref_segment_in->referenced_column_id();

        auto& filtered_pos_list = filtered_pos_lists[pos_list_in];

        if (!filtered_pos_list) {
          filtered_pos_list = std::make_shared<PosList>();
          if (pos_list_in->references_single_chunk()) {
            filtered_pos_list->guarantee_single_chunk();
          }
          filtered_pos_list->reserve(context.output_pos_list->size());

          for (const auto& match : *context.output_pos_list) {
            const auto row_id = (*pos_list_in)[match.chunk_offset];
            filtered_pos_list->push_back(row_id);
          }
        }

        const auto ref_segment_out = std::make_shared<ReferenceSegment>(table_out, column_id_out, filtered_pos_list);
        out_segments.push_back(ref_segment_out);
      }
    } else {
      context.output_pos_list->guarantee_single_chunk();
      for (const auto& output_column : _output_columns) {
        auto ref_segment_out =
            std::make_shared<ReferenceSegment>(in_table, output_column.referenced_column_id, context.output_pos_list);
        out_segments.push_back(ref_segment_out);
      }
    }
    out_table.append_chunk(out_segments);
    // Check if current chunk is last
    if (context.chunk_id + 1 < in_table->chunk_count()) {
      context.output_pos_list = std::make_shared<PosList>();
    }
  }
}

void JitWriteReference::add_output_column(const std::string& column_name, const ColumnID referenced_column_id) {
  _output_columns.push_back(OutputColumn{column_name, referenced_column_id});
}

const std::vector<JitWriteReference::OutputColumn>& JitWriteReference::output_columns() const {
  return _output_columns;
}

void JitWriteReference::_consume(JitRuntimeContext& context) const {
  context.output_pos_list->emplace_back(context.chunk_id, context.chunk_offset);
}

}  // namespace opossum
