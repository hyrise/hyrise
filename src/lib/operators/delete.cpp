#include "delete.hpp"

#include <memory>
#include <string>

namespace opossum {

Delete::Delete(const std::shared_ptr<const TableScan>& table_scan) : AbstractModifyingOperator{table_scan} {}

std::shared_ptr<const Table> Delete::on_execute(const TransactionContext* context) {
  const auto reference_table = std::const_pointer_cast<Table>(_input_left->get_output());

  // assumption: table contains only referenced columns that only reference one table
  // assert(col_count > 0)
  // assert(chunk_size == 0)

  const auto chunk = &reference_table->get_chunk(0);

  const auto first_column = std::static_pointer_cast<ReferenceColumn>(chunk->get_column(0));

  auto table = std::const_pointer_cast<Table>(first_column->referenced_table());

  for (const auto& row_id : *first_column->pos_list()) {
    const auto chunk = &table->get_chunk(row_id.chunk_id);
    chunk->remove(row_id.chunk_offset);
  }

  return nullptr;
}

const std::string Delete::name() const { return "Delete"; }

uint8_t Delete::num_in_tables() const { return 1u; }

uint8_t Delete::num_out_tables() const { return 0u; }
}  // namespace opossum
