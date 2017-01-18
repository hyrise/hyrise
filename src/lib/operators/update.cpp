#include "update.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "concurrency/transaction_context.hpp"
#include "fake_operator.hpp"
#include "storage/reference_column.hpp"

namespace opossum {

Update::Update(std::shared_ptr<AbstractOperator> table_to_update_op, std::shared_ptr<AbstractOperator> update_values_op)
    : AbstractReadWriteOperator(table_to_update_op, update_values_op) {}

const std::string Update::name() const { return "Update"; }

uint8_t Update::num_in_tables() const { return 1; }

std::shared_ptr<const Table> Update::on_execute(const TransactionContext* context) {
  // The table to update should always be referenced. Updating all values of all rows is not allowed (TODO(all):
  // discuss)
  auto casted_ref_col = std::dynamic_pointer_cast<ReferenceColumn>(input_table_left()->get_chunk(0).get_column(0));
  auto original_table = casted_ref_col->referenced_table();

  // 1. Create insert_table with ReferenceColumns that contain all rows that should be updated
  auto pos_list = casted_ref_col->pos_list();
  auto insert_table = std::make_shared<Table>();
  Chunk& chunk_out = insert_table->get_chunk(0);

  for (size_t column_id = 0; column_id < original_table->col_count(); ++column_id) {
    insert_table->add_column(original_table->column_name(column_id), original_table->column_type(column_id), false);
    chunk_out.add_column(std::make_shared<ReferenceColumn>(original_table, column_id, pos_list));
  }

  // 2. Replace the columns to update in insert_table with the updated data from input_table_right
  // TODO(all): here we assume that both left and right table have only one chunk
  auto& columns = insert_table->get_chunk(0).columns();
  for (size_t column_id = 0; column_id < input_table_left()->col_count(); ++column_id) {
    auto left_col = std::dynamic_pointer_cast<ReferenceColumn>(input_table_left()->get_chunk(0).get_column(column_id));
    auto right_col = input_table_right()->get_chunk(0).get_column(column_id);

    columns[left_col->referenced_column_id()] = right_col;
  }

  // 3. call delete on old data.
  _delete = std::make_unique<Delete>(_input_left);

  _delete->execute(context);

  // 4. call insert on insert_table. (moves might happen twice??)
  auto get_table = std::make_shared<GetTable>(original_table->name());
  auto fake_op = std::make_shared<FakeOperator>(insert_table);
  get_table->execute();
  fake_op->execute();
  _insert = std::make_unique<Insert>(get_table, fake_op);

  _insert->execute(context);

  return nullptr;
}

void Update::commit(const uint32_t cid) {
  _delete->commit(cid);
  _insert->commit(cid);
}

void Update::abort() {
  _delete->abort();
  _insert->abort();
}

}  // namespace opossum
