#include "limit.hpp"

#include <algorithm>
#include <memory>
#include <string>
#include <utility>

#include "../storage/reference_column.hpp"
#include "../storage/table.hpp"

namespace opossum {

Limit::Limit(const std::shared_ptr<const AbstractOperator> in, size_t num_rows)
    : AbstractReadOnlyOperator(in), _num_rows(num_rows) {}

const std::string Limit::name() const { return "Limit"; }

uint8_t Limit::num_in_tables() const { return 1; }

uint8_t Limit::num_out_tables() const { return 1; }

std::shared_ptr<const Table> Limit::on_execute() {
  const auto input_table = input_table_left();

  // Create output table and column layout.
  auto output_table = std::make_shared<Table>(input_table->chunk_size());
  for (ColumnID c = 0; c < input_table->col_count(); c++) {
    output_table->add_column(input_table->column_name(c), input_table->column_type(c), false);
  }

  ChunkID chunk_id = 0;
  for (size_t i = 0; i < _num_rows && chunk_id < input_table->chunk_count(); chunk_id++) {
    const auto &input_chunk = input_table->get_chunk(chunk_id);
    Chunk output_chunk;

    size_t output_chunk_row_count = std::min<size_t>(input_chunk.size(), _num_rows - i);

    for (ColumnID c = 0; c < input_table->col_count(); c++) {
      const auto input_base_column = input_chunk.get_column(c);
      auto output_pos_list = std::make_shared<PosList>(output_chunk_row_count);
      std::shared_ptr<const Table> referenced_table;

      if (auto input_ref_column = std::dynamic_pointer_cast<ReferenceColumn>(input_base_column)) {
        referenced_table = input_ref_column->referenced_table();
        // TODO(all): optimize using whole chunk whenever possible
        auto begin = input_ref_column->pos_list()->begin();
        std::copy(begin, begin + output_chunk_row_count, output_pos_list->begin());
      } else {
        referenced_table = input_table;
        for (ChunkOffset r = 0; r < output_chunk_row_count; r++) {
          (*output_pos_list)[r] = RowID{chunk_id, r};
        }
      }

      output_chunk.add_column(std::make_shared<ReferenceColumn>(referenced_table, c, output_pos_list));
    }

    i += output_chunk_row_count;
    output_table->add_chunk(std::move(output_chunk));
  }

  return output_table;
}

}  // namespace opossum
