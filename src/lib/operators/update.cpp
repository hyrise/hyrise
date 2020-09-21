#include "update.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "concurrency/transaction_context.hpp"
#include "delete.hpp"
#include "hyrise.hpp"
#include "insert.hpp"
#include "storage/reference_segment.hpp"
#include "table_wrapper.hpp"
#include "utils/assert.hpp"

namespace opossum {

Update::Update(const std::string& table_to_update_name, const std::shared_ptr<AbstractOperator>& fields_to_update_op,
               const std::shared_ptr<AbstractOperator>& update_values_op)
    : AbstractReadWriteOperator(OperatorType::Update, fields_to_update_op, update_values_op),
      _table_to_update_name{table_to_update_name} {}

const std::string& Update::name() const {
  static const auto name = std::string{"Update"};
  return name;
}

std::shared_ptr<const Table> Update::_on_execute(std::shared_ptr<TransactionContext> context) {
  const auto table_to_update = Hyrise::get().storage_manager.get_table(_table_to_update_name);

  // 0. Validate input
  DebugAssert(context, "Update needs a transaction context");
  DebugAssert(left_input_table()->row_count() == right_input_table()->row_count(),
              "Update required identical layouts from its input tables");
  DebugAssert(left_input_table()->column_data_types() == right_input_table()->column_data_types(),
              "Update required identical layouts from its input tables");

  // 1. Delete obsolete data with the Delete operator.
  //    Delete doesn't accept empty input data
  if (left_input_table()->row_count() > 0) {
    _delete = std::make_shared<Delete>(_left_input);
    _delete->set_transaction_context(context);
    _delete->execute();

    if (_delete->execute_failed()) {
      _mark_as_failed();
      return nullptr;
    }
  }

  // 2. Insert new data with the Insert operator.
  _insert = std::make_shared<Insert>(_table_to_update_name, _right_input);
  _insert->set_transaction_context(context);
  _insert->execute();
  // Insert cannot fail in the MVCC sense, no check necessary

  return nullptr;
}

std::shared_ptr<AbstractOperator> Update::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& copied_right_input) const {
  return std::make_shared<Update>(_table_to_update_name, copied_left_input, copied_right_input);
}

void Update::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

}  // namespace opossum
