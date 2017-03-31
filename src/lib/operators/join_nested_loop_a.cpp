#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "join_nested_loop_a.hpp"
#include "product.hpp"

namespace opossum {
JoinNestedLoopA::JoinNestedLoopA(const std::shared_ptr<const AbstractOperator> left,
                                 const std::shared_ptr<const AbstractOperator> right,
                                 optional<std::pair<std::string, std::string>> column_names, const std::string &op,
                                 const JoinMode mode, const std::string &prefix_left, const std::string &prefix_right)
    : AbstractJoinOperator(left, right, column_names, op, mode, prefix_left, prefix_right) {
  if (IS_DEBUG) {
    if (mode == Cross) {
      throw std::runtime_error(
          "JoinNestedLoopA: this operator does not support Cross Joins, the optimizer should use Product operator.");
    }

    if (_mode == Natural) {
      throw std::runtime_error("NestedLoopJoin: this operator currently does not support Natural Joins.");
    }

    if (!column_names) {
      throw std::runtime_error("NestedLoopJoin: optional column names are only supported for Cross and Natural Joins.");
    }
  }
}

const std::string JoinNestedLoopA::name() const { return "JoinNestedLoopA"; }

uint8_t JoinNestedLoopA::num_in_tables() const { return 2; }

uint8_t JoinNestedLoopA::num_out_tables() const { return 1; }

std::shared_ptr<const Table> JoinNestedLoopA::on_execute() {
  const auto first_column = _column_names->first;
  const auto second_column = _column_names->second;

  _impl = make_unique_by_column_types<AbstractReadOnlyOperatorImpl, JoinNestedLoopAImpl>(
      input_table_left()->column_type(input_table_left()->column_id_by_name(first_column)),
      input_table_right()->column_type(input_table_right()->column_id_by_name(second_column)), _input_left,
      _input_right, *_column_names, _op, _mode, _prefix_left, _prefix_right);

  return _impl->on_execute();
}

}  // namespace opossum
