#include "update.hpp"

#include <memory>
#include <string>
#include <vector>

#include "concurrency/transaction_context.hpp"

namespace opossum {

Update::Update(std::shared_ptr<GetTable> get_table, std::vector<AllTypeVariant>&& values)
    : AbstractModifyingOperator(get_table), _values(values) {}

const std::string Update::name() const { return "Update"; }

uint8_t Update::num_in_tables() const { return 1; }

std::shared_ptr<const Table> Update::on_execute(const TransactionContext* context) {
  // Create table to use for insert operator.
  auto pos_list = input_table_left()->get_chunk(0).get_column(0)->pos_list();
  auto insert_table = std::make_shared<Table>();

  for (size_t column_id = 0; column_id < input_table_left()->col_count(); ++column_id) {
    insert_table->add_column(input_table_left()->column_name(column_id), input_table_left()->column_type(column_id),
                             false);
  }

  Chunk chunk_out;
  for (size_t column_id = 0; column_id < _in_table->col_count(); ++column_id) {
    // TODO(everyone): because of projection, column_id might be wrong. need to look it up in case of referencing
    // tables.
    auto ref_col_out = std::make_shared<ReferenceColumn>(referenced_table, column_id, pos_list);
    chunk_out.add_column(ref_col_out);
  }

  insert_table->add_chunk(std::move(chunk_out));

  // const auto last_CID = 5;  // from TransactionManager
  auto _table = std::const_pointer_cast<Table>(input_table_left());
  size_t row_id;
  size_t chunk_id;

  // Lock to get actual Table row.
  {
    std::lock_guard<std::mutex> lock(*_table->append_mtx);

    // Append row. Might create new chunk with new MVCC columns
    _table->append(_values);

    // get atomic chunk_id and row_id
    chunk_id = _table->chunk_count() - 1;
    row_id = _table->get_chunk(chunk_id)._TIDs.size() - 1;
  }

  // Set the mvcc column values for chunk:
  auto& chunk = _table->get_chunk(chunk_id);

  // Uncommitted:
  chunk._TIDs[row_id] = context->tid();
  _inserted_rows.push_back(_table->calculate_row_id(chunk_id, row_id));

  return nullptr;
}

void Update::commit(const uint32_t cid) {
  auto _table = std::const_pointer_cast<Table>(input_table_left());
  for (auto row_id : _inserted_rows) {
    _table->get_chunk(row_id.chunk_id)._begin_CIDs[row_id.chunk_offset] = cid;
    _table->get_chunk(row_id.chunk_id)._TIDs[row_id.chunk_offset] = 0;
  }
}

void Update::abort() {
  auto _table = std::const_pointer_cast<Table>(input_table_left());
  for (auto row_id : _inserted_rows) {
    _table->get_chunk(row_id.chunk_id)._TIDs[row_id.chunk_offset] = 0;
  }
}

}  // namespace opossum
