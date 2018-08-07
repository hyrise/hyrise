#include "update.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "concurrency/transaction_context.hpp"
#include "delete.hpp"
#include "insert.hpp"
#include "storage/reference_column.hpp"
#include "storage/storage_manager.hpp"
#include "table_wrapper.hpp"
#include "utils/assert.hpp"

namespace opossum {

Update::Update(const std::string& table_to_update_name, const std::shared_ptr<AbstractOperator>& fields_to_update_op,
               const std::shared_ptr<AbstractOperator>& update_values_op)
    : AbstractReadWriteOperator(OperatorType::Update, fields_to_update_op, update_values_op),
      _table_to_update_name{table_to_update_name} {}

Update::~Update() = default;

const std::string Update::name() const { return "Update"; }

std::shared_ptr<const Table> Update::_on_execute(std::shared_ptr<TransactionContext> context) {
  if (_input_left->get_output()->empty()) return nullptr;  // Subsequent code relies on there being at least one chunk

  DebugAssert((_execution_input_valid(context)), "Input to Update isn't valid");

  const auto table_to_update = StorageManager::get().get_table(_table_to_update_name);

  // 1. Create insert_table with ReferenceColumns that contain all rows that should be updated
  TableColumnDefinitions insert_table_column_definitions;
  for (ColumnID column_id{0}; column_id < table_to_update->column_count(); ++column_id) {
    insert_table_column_definitions.emplace_back(table_to_update->column_name(column_id),
                                                 table_to_update->column_data_type(column_id));
  }

  auto insert_table = std::make_shared<Table>(insert_table_column_definitions, TableType::References);

  auto current_row_in_left_chunk = 0u;
  auto current_pos_list = std::shared_ptr<const PosList>();
  auto current_left_chunk_id = ChunkID{0};

  for (ChunkID chunk_id{0}; chunk_id < input_table_right()->chunk_count(); ++chunk_id) {
    // Build poslists for mixed chunk numbers and sizes.
    auto pos_list = std::make_shared<PosList>();
    for (auto i = 0u; i < input_table_right()->get_chunk(chunk_id)->size(); ++i) {
      if (current_pos_list == nullptr || current_row_in_left_chunk == current_pos_list->size()) {
        current_row_in_left_chunk = 0u;
        current_pos_list = std::static_pointer_cast<const ReferenceColumn>(
                               input_table_left()->get_chunk(current_left_chunk_id)->get_column(ColumnID{0}))
                               ->pos_list();
        current_left_chunk_id++;
      }

      pos_list->emplace_back((*current_pos_list)[current_row_in_left_chunk]);
      current_row_in_left_chunk++;
    }

    // Add ReferenceColumns with built poslist.
    ChunkColumns insert_table_columns;
    for (ColumnID column_id{0}; column_id < table_to_update->column_count(); ++column_id) {
      insert_table_columns.push_back(std::make_shared<ReferenceColumn>(table_to_update, column_id, pos_list));
    }

    insert_table->append_chunk(insert_table_columns);
  }

  // 2. Replace the columns to update in insert_table with the updated data from input_table_right
  const auto left_chunk = input_table_left()->get_chunk(ChunkID{0});
  for (ChunkID chunk_id{0}; chunk_id < insert_table->chunk_count(); ++chunk_id) {
    auto insert_chunk = insert_table->get_chunk(chunk_id);
    auto right_chunk = input_table_right()->get_chunk(chunk_id);

    for (ColumnID column_id{0}; column_id < input_table_left()->column_count(); ++column_id) {
      auto right_col = right_chunk->get_column(column_id);

      auto left_col = std::dynamic_pointer_cast<const ReferenceColumn>(left_chunk->get_column(column_id));

      insert_chunk->replace_column(left_col->referenced_column_id(), right_col);
    }
  }

  // 3. call delete on old data.
  _delete = std::make_shared<Delete>(_table_to_update_name, _input_left);

  _delete->set_transaction_context(context);

  _delete->execute();

  if (_delete->execute_failed()) {
    _mark_as_failed();
    return nullptr;
  }

  // 4. call insert using insert_table.
  auto helper_operator = std::make_shared<TableWrapper>(insert_table);
  helper_operator->execute();

  _insert = std::make_shared<Insert>(_table_to_update_name, helper_operator);
  _insert->set_transaction_context(context);

  _insert->execute();

  return nullptr;
}

/**
 * input_table_left must be a table with at least one chunk, containing at least one ReferenceColumn
 * that all reference the table specified by table_to_update_name. The column count and types in input_table_left
 * must match the count and types in input_table_right.
 */
bool Update::_execution_input_valid(const std::shared_ptr<TransactionContext>& context) const {
  if (context == nullptr) return false;

  if (input_table_left()->column_count() != input_table_right()->column_count()) return false;

  if (input_table_left()->chunk_count() == 0) return false;

  const auto table_to_update = StorageManager::get().get_table(_table_to_update_name);

  for (ChunkID chunk_id{0}; chunk_id < input_table_left()->chunk_count(); ++chunk_id) {
    const auto chunk = input_table_left()->get_chunk(chunk_id);

    if (!chunk->references_exactly_one_table()) return false;

    const auto first_column = std::static_pointer_cast<const ReferenceColumn>(chunk->get_column(ColumnID{0}));
    if (table_to_update != first_column->referenced_table()) return false;
  }

  return true;
}

std::shared_ptr<AbstractOperator> Update::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<Update>(_table_to_update_name, copied_input_left, copied_input_right);
}

void Update::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

}  // namespace opossum
