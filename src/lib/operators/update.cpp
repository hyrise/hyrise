#include "update.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "concurrency/transaction_context.hpp"
#include "delete.hpp"
#include "insert.hpp"
#include "storage/reference_segment.hpp"
#include "storage/storage_manager.hpp"
#include "table_wrapper.hpp"
#include "utils/assert.hpp"

namespace opossum {

Update::Update(const std::string& target_table_name, const std::shared_ptr<AbstractOperator>& fields_to_update_op,
               const std::shared_ptr<AbstractOperator>& update_values_op)
    : AbstractReadWriteOperator(OperatorType::Update, fields_to_update_op, update_values_op, target_table_name) {}

const std::string Update::name() const { return "Update"; }

std::shared_ptr<const Table> Update::_on_execute(std::shared_ptr<TransactionContext> context) {
  const auto table_to_update = StorageManager::get().get_table(_target_table_name);

  // 0. Validate input
  DebugAssert(context != nullptr, "Update needs a transaction context");
  DebugAssert(input_table_left()->row_count() == input_table_right()->row_count(),
              "Update required identical layouts from its input tables: row_count unequal.");
  DebugAssert(input_table_left()->column_data_types() == input_table_right()->column_data_types(),
              "Update required identical layouts from its input tables: column_data_types unequal.");

  for (const auto& chunk : input_table_left()->chunks()) {
    DebugAssert(chunk->references_exactly_one_table(),
                "Update left input must be a reference table referencing exactly one table");

    const auto first_segment = std::static_pointer_cast<const ReferenceSegment>(chunk->get_segment(ColumnID{0}));
    DebugAssert(table_to_update == first_segment->referenced_table(),
                "Update left input must reference the table that is supposed to be updated");
  }

  // 1. Delete obsolete data with the Delete operator.
  //    Delete doesn't accept empty input data
  if (input_table_left()->row_count() > 0) {
    _delete = std::make_shared<Delete>(_target_table_name, _input_left);
    _delete->set_transaction_context(context);
    _delete->execute();

    if (_delete->execute_failed()) {
      _mark_as_failed();
      return nullptr;
    }
  }

  // 2. Insert new data with the Insert operator.
  _insert = std::make_shared<Insert>(_target_table_name, _input_right);
  _insert->set_transaction_context(context);
  _insert->execute();

  if (_insert->execute_failed()) {
    _mark_as_failed();
    return nullptr;
  }

  return nullptr;
}

std::shared_ptr<AbstractOperator> Update::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<Update>(_target_table_name, copied_input_left, copied_input_right);
}

void Update::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

}  // namespace opossum
