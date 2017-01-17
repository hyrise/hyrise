#include "update.hpp"

#include <memory>
#include <string>
#include <vector>
#include <utility>

#include "concurrency/transaction_context.hpp"
#include "storage/reference_column.hpp"

namespace opossum {

Update::Update(std::shared_ptr<AbstractOperator> input_table, std::shared_ptr<AbstractOperator> update_values)
    : AbstractModifyingOperator(input_table, update_values) {}

const std::string Update::name() const { return "Update"; }

uint8_t Update::num_in_tables() const { return 1; }

std::shared_ptr<const Table> Update::on_execute(const TransactionContext* context) {
  // Create table to use for insert operator.
  auto casted_ref_col = std::dynamic_pointer_cast<ReferenceColumn>(input_table_left()->get_chunk(0).get_column(0));
  auto pos_list = casted_ref_col->pos_list();
  auto insert_table = std::make_shared<Table>();

  for (size_t column_id = 0; column_id < input_table_left()->col_count(); ++column_id) {
    insert_table->add_column(input_table_left()->column_name(column_id), input_table_left()->column_type(column_id),
                             false);
  }

  Chunk chunk_out;
  for (size_t column_id = 0; column_id < input_table_left()->col_count(); ++column_id) {
    // TODO(everyone): because of projection, column_id might be wrong. need to look it up in case of referencing
    // tables.
    auto ref_col_out = std::make_shared<ReferenceColumn>(casted_ref_col->referenced_table(), column_id, pos_list);
    chunk_out.add_column(ref_col_out);
  }

  insert_table->add_chunk(std::move(chunk_out));

  // TODO(david): implement UPDATE:
  // 1. move unchanged data into input table
  // 2. move data to update into input table
  // 3. call delete on table.
  // 4. call insert on table. (moves happen twice??)

  return nullptr;
}

void Update::commit(const uint32_t cid) {
  auto _table = std::const_pointer_cast<Table>(input_table_left());
  for (auto row_id : _updated_rows) {
    auto& chunk = _table->get_chunk(row_id.chunk_id);

    chunk.mvcc_columns().begin_cids[row_id.chunk_offset] = cid;
    chunk.mvcc_columns().tids[row_id.chunk_offset] = 0u;
  }
}

void Update::abort() {
  auto _table = std::const_pointer_cast<Table>(input_table_left());
  for (auto row_id : _updated_rows) {
    auto& chunk = _table->get_chunk(row_id.chunk_id);
    chunk.mvcc_columns().tids[row_id.chunk_offset] = 0u;
  }
}

}  // namespace opossum
