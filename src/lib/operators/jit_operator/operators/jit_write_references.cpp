#include "jit_write_references.hpp"

#include "../jit_types.hpp"
#include "constant_mappings.hpp"
#include "resolve_type.hpp"
#include "storage/base_value_segment.hpp"
#include "storage/value_segment.hpp"

namespace opossum {

std::string JitWriteReferences::description() const {
  std::stringstream desc;
  desc << "[WriteReference] ";
  for (const auto& output_column : _output_columns) {
    desc << output_column.column_name << " = Col#" << output_column.referenced_column_id << ", ";
  }
  return desc.str();
}

std::shared_ptr<Table> JitWriteReferences::create_output_table(const Table& in_table) const {
  TableColumnDefinitions column_definitions;

  const auto& input_column_definitions = in_table.column_definitions();

  for (const auto& output_column : _output_columns) {
    auto column_definition = input_column_definitions[output_column.referenced_column_id];
    column_definition.name = output_column.column_name;
    column_definitions.push_back(column_definition);
  }

  return std::make_shared<Table>(column_definitions, TableType::References);
}

void JitWriteReferences::before_query(Table& out_table, JitRuntimeContext& context) const {
  context.output_pos_list = std::make_shared<PosList>();
}

void JitWriteReferences::after_chunk(const std::shared_ptr<const Table>& in_table, Table& out_table,
                                     JitRuntimeContext& context) const {
  const auto matches_out = context.output_pos_list;
  if (!matches_out->empty()) {
    Segments out_segments;
    out_segments.reserve(_output_columns.size());

    /**
     * matches_out contains a list of row IDs into this chunk. If this is not a reference table, we can
     * directly use the matches to construct the reference segments of the output. If it is a reference segment,
     * we need to resolve the row IDs so that they reference the physical data segments (value, dictionary) instead,
     * since we don’t allow multi-level referencing. To save time and space, we want to share position lists
     * between segments as much as possible. Position lists can be shared between two segments iff
     * (a) they point to the same table and
     * (b) the reference segments of the input table point to the same positions in the same order
     *     (i.e. they share their position list).
     */
    if (in_table->type() == TableType::References) {
      auto filtered_pos_lists = std::map<std::shared_ptr<const PosList>, std::shared_ptr<PosList>>{};

      const auto chunk_in = in_table->get_chunk(context.chunk_id);
      for (const auto& output_column : _output_columns) {
        auto segment_in = chunk_in->get_segment(output_column.referenced_column_id);

        auto ref_segment_in = std::dynamic_pointer_cast<const ReferenceSegment>(segment_in);
        DebugAssert(ref_segment_in, "All segments should be of type ReferenceSegment.");

        const auto pos_list_in = ref_segment_in->pos_list();

        const auto table_out = ref_segment_in->referenced_table();
        const auto column_id_out = ref_segment_in->referenced_column_id();

        auto& filtered_pos_list = filtered_pos_lists[pos_list_in];

        if (!filtered_pos_list) {
          filtered_pos_list = std::make_shared<PosList>();
          if (pos_list_in->references_single_chunk()) {
            filtered_pos_list->guarantee_single_chunk();
          }
          filtered_pos_list->reserve(matches_out->size());

          for (const auto& match : *matches_out) {
            const auto row_id = (*pos_list_in)[match.chunk_offset];
            filtered_pos_list->push_back(row_id);
          }
        }

        const auto ref_segment_out = std::make_shared<ReferenceSegment>(table_out, column_id_out, filtered_pos_list);
        out_segments.push_back(ref_segment_out);
      }
    } else {
      matches_out->guarantee_single_chunk();
      for (const auto& output_column : _output_columns) {
        auto ref_segment_out =
            std::make_shared<ReferenceSegment>(in_table, output_column.referenced_column_id, matches_out);
        out_segments.push_back(ref_segment_out);
      }
    }
    out_table.append_chunk(out_segments);

    // Prepare output_pos_list for next chunk
    context.output_pos_list = std::make_shared<PosList>();
  }
}

void JitWriteReferences::add_output_column_definition(const std::string& column_name,
                                                      const ColumnID referenced_column_id) {
  _output_columns.push_back(OutputColumn{column_name, referenced_column_id});
}

const std::vector<JitWriteReferences::OutputColumn>& JitWriteReferences::output_columns() const {
  return _output_columns;
}

void JitWriteReferences::_consume(JitRuntimeContext& context) const {
  context.output_pos_list->emplace_back(RowID{context.chunk_id, context.chunk_offset});
}

}  // namespace opossum
