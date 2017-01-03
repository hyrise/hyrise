
#include "delete.hpp"

Delete::Delete(const std::shared_ptr<const TableScan>& table_scan) : AbstractOperator{table_scan} {}

void Delete::execute() {
  const auto reference_table = const_pointer_cast<Table>(_left->get_output());

  // assumption: table contains only referenced columns that only reference one table
  // assert(col_count > 0)
  // assert(chunk_size == 0)

  const auto chunk = &reference_table->get_chunk(0);

  const auto first_column = static_pointer_cast<ReferenceColumn>(chunk.get_column(0));

  auto table = const_pointer_cast<Table>(first_column->referenced_table());

  for (const auto& row_id : *first_column->pos_list()) {
    const auto chunk = &table->get_chunk(row_id.chunk_id);
    chunk->remove(row_id.chunk_offset);
  }
}

std::shared_ptr<const Table> Delete::get_output() const { return nullptr; }

const std::string Delete::name() const { return "Delete"; }

uint8_t Delete::num_in_tables() const { return 1u; }

uint8_t Delete::num_out_tables() const { return 0u; }
