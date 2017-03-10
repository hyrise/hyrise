#include "update.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "concurrency/transaction_context.hpp"
#include "storage/reference_column.hpp"
#include "table_wrapper.hpp"

namespace opossum {

Update::Update(std::shared_ptr<AbstractOperator> table_to_update_op, std::shared_ptr<AbstractOperator> update_values_op)
    : AbstractReadWriteOperator(table_to_update_op, update_values_op) {}

const std::string Update::name() const { return "Update"; }

uint8_t Update::num_in_tables() const { return 1; }

std::shared_ptr<const Table> Update::on_execute(TransactionContext* context) {
#ifdef IS_DEBUG
  if (!_execution_input_valid(context)) {
    throw std::runtime_error("Input to Update isn't valid");
  }
#endif

  auto casted_ref_col = std::dynamic_pointer_cast<ReferenceColumn>(input_table_left()->get_chunk(0).get_column(0));
  auto original_table = casted_ref_col->referenced_table();

  // 1. Create insert_table with ReferenceColumns that contain all rows that should be updated
  // We assume, that all Reference Columns in this table reference the same original table
  auto insert_table = std::make_shared<Table>();

  for (size_t column_id = 0u; column_id < original_table->col_count(); ++column_id) {
    insert_table->add_column(original_table->column_name(column_id), original_table->column_type(column_id), false);
  }

  auto current_row_in_left_chunk = 0u;
  auto current_left_chunk_id = 0u;
  auto current_pos_list = casted_ref_col->pos_list();

  for (auto chunk_id = 0u; chunk_id < input_table_right()->chunk_count(); ++chunk_id) {
    // Build poslists for mixed chunk numbers and sizes.
    auto pos_list = std::make_shared<PosList>();
    for (auto i = 0u; i < input_table_right()->get_chunk(chunk_id).size(); ++i) {
      if (current_row_in_left_chunk == current_pos_list->size()) {
        current_row_in_left_chunk = 0u;
        current_left_chunk_id++;
        current_pos_list = std::dynamic_pointer_cast<ReferenceColumn>(
                               input_table_left()->get_chunk(current_left_chunk_id).get_column(0))
                               ->pos_list();
      }

      pos_list->emplace_back((*current_pos_list)[current_row_in_left_chunk]);
      current_row_in_left_chunk++;
    }

    // Add ReferenceColumns with built poslist.
    Chunk chunk{false};
    for (size_t column_id = 0u; column_id < original_table->col_count(); ++column_id) {
      chunk.add_column(std::make_shared<ReferenceColumn>(original_table, column_id, pos_list));
    }

    insert_table->add_chunk(std::move(chunk));
  }

  // 2. Replace the columns to update in insert_table with the updated data from input_table_right
  auto& left_chunk = input_table_left()->get_chunk(0);
  for (auto chunk_id = 0u; chunk_id < insert_table->chunk_count(); ++chunk_id) {
    auto& insert_chunk = insert_table->get_chunk(chunk_id);
    auto& right_chunk = input_table_right()->get_chunk(chunk_id);

    for (size_t column_id = 0u; column_id < input_table_left()->col_count(); ++column_id) {
      auto right_col = right_chunk.get_column(column_id);

      auto left_col = std::dynamic_pointer_cast<ReferenceColumn>(left_chunk.get_column(column_id));
      insert_chunk.columns()[left_col->referenced_column_id()] = right_col;
    }
  }

  // 3. call delete on old data.
  _delete = std::make_unique<Delete>(original_table->name(), _input_left);

  _delete->execute(context);

  _execute_failed |= _delete->execute_failed();
  if (_execute_failed) return nullptr;

  // 4. call insert using insert_table.
  auto helper_op = std::make_shared<TableWrapper>(insert_table);
  helper_op->execute();
  _insert = std::make_unique<Insert>(original_table->name(), helper_op);

  _insert->execute(context);

  return nullptr;
}

void Update::commit(const CommitID cid) {
  _delete->commit(cid);
  _insert->commit(cid);
}

void Update::abort() {
  _delete->abort();
  _insert->abort();
}

bool Update::_execution_input_valid(const TransactionContext* context) const {
  if (context == nullptr) return false;

  if (input_table_left()->col_count() != input_table_right()->col_count()) return false;

  for (auto chunk_id = 0u; chunk_id < input_table_left()->chunk_count(); ++chunk_id)
    if (!input_table_left()->get_chunk(chunk_id).references_only_one_table()) return false;

  return true;
}

}  // namespace opossum
