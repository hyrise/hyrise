#include "update.hpp"

#include <memory>
#include <string>
#include <unordered_map>

#include "all_type_variant.hpp"
#include "concurrency/transaction_context.hpp"
#include "delete.hpp"
#include "hyrise.hpp"
#include "insert.hpp"
#include "operators/abstract_operator.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

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
    const auto delete_operator = std::make_shared<Delete>(_left_input);
    delete_operator->set_transaction_context(context);
    delete_operator->execute();

    if (delete_operator->execute_failed()) {
      _mark_as_failed();
      return nullptr;
    }
  }

  // A table must not have an empty chunk, because the Import operator would crash when making the empty chunk
  // immutable to append to the table. We, therefore, only create the Insert operator if there are rows in the input.
  if (_right_input->get_output()->row_count() > 0) {
    const auto insert = std::make_shared<Insert>(_table_to_update_name, _right_input);
    insert->set_transaction_context(context);
    insert->execute();
    // Insert cannot fail in the MVCC sense, no check necessary.
  }

  return nullptr;
}

std::shared_ptr<AbstractOperator> Update::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& copied_right_input,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& /*copied_ops*/) const {
  return std::make_shared<Update>(_table_to_update_name, copied_left_input, copied_right_input);
}

void Update::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

}  // namespace hyrise
