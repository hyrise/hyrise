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

Update::Update(const std::string& table_to_update_name, const std::shared_ptr<AbstractOperator>& fields_to_update_op,
               const std::shared_ptr<AbstractOperator>& update_values_op)
    : AbstractReadWriteOperator(OperatorType::Update, fields_to_update_op, update_values_op),
      _table_to_update_name{table_to_update_name} {}

const std::string Update::name() const { return "Update"; }

std::shared_ptr<const Table> Update::_on_execute(std::shared_ptr<TransactionContext> context) {
  const auto table_to_update = StorageManager::get().get_table(_table_to_update_name);

  // 0. Validate input
  DebugAssert(context != nullptr, "Update needs a transaction context");
  DebugAssert(input_table_left()->row_count() == input_table_right()->row_count(),
              "Update required identical layouts from its input tables");
  DebugAssert(input_table_left()->column_data_types() == input_table_right()->column_data_types(),
              "Update required identical layouts from its input tables");

  // 1. Delete obsolete data with the Delete operator.
  //    Delete doesn't accept empty input data
  if (input_table_left()->row_count() > 0) {
    _delete = std::make_shared<Delete>(_input_left);
    _delete->set_transaction_context(context);
    _delete->execute();

    if (_delete->execute_failed()) {
      _mark_as_failed();
      return nullptr;
    }
  }

  // 2. Insert new data with the Insert operator.
  _insert = std::make_shared<Insert>(_table_to_update_name, _input_right);
  _insert->set_transaction_context(context);
  _insert->execute();
  // Insert cannot fail in the MVCC sense, no check necessary

  return nullptr;
}

std::shared_ptr<AbstractOperator> Update::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<Update>(_table_to_update_name, copied_input_left, copied_input_right);
}

void Update::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

}  // namespace opossum
