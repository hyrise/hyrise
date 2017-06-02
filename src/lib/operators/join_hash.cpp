#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "join_hash.hpp"
#include "product.hpp"
#include "utils/assert.hpp"

#include "resolve_type.hpp"

namespace opossum {

JoinHash::JoinHash(const std::shared_ptr<const AbstractOperator> left,
                   const std::shared_ptr<const AbstractOperator> right,
                   optional<std::pair<std::string, std::string>> column_names, const std::string &op,
                   const JoinMode mode, const std::string &prefix_left, const std::string &prefix_right)
    : AbstractJoinOperator(left, right, column_names, op, mode, prefix_left, prefix_right) {
  DebugAssert((op == "="), (std::string("Operator not supported by Hash Join: ") + op));
  DebugAssert((_mode != Cross),
              "JoinHash: this operator does not support Cross Joins, the optimizer should use Product operator.");
  DebugAssert((_mode != Natural), "JoinHash: this operator currently does not support Natural Joins.");
  DebugAssert(static_cast<bool>(column_names),
              "JoinHash: optional column names are only supported for Cross and Natural Joins.");
}

const std::string JoinHash::name() const { return "JoinHash"; }

uint8_t JoinHash::num_in_tables() const { return 2; }

uint8_t JoinHash::num_out_tables() const { return 1; }

std::shared_ptr<const Table> JoinHash::on_execute() {
  std::shared_ptr<const AbstractOperator> build_operator;
  std::shared_ptr<const AbstractOperator> probe_operator;
  bool inputs_swapped;
  std::string build_column_name;
  std::string probe_column_name;

  /*
  This is the expected implementation for swapping tables:
  (1) if left or right outer join, outer relation becomes probe relation (we have to swap only for left outer)
  (2) else the smaller relation will become build relation, the larger probe relation
  (3) for full outer joins we currently don't have an implementation.
  */
  if (_mode == Left ||
      (_mode != Right && (_input_left->get_output()->row_count() > _input_right->get_output()->row_count()))) {
    // luckily we don't have to swap the operation itself here, because we only support the commutative Equi Join.
    inputs_swapped = true;
    build_operator = _input_right;
    probe_operator = _input_left;
    build_column_name = _column_names->second;
    probe_column_name = _column_names->first;
  } else {
    inputs_swapped = false;
    build_operator = _input_left;
    probe_operator = _input_right;
    build_column_name = _column_names->first;
    probe_column_name = _column_names->second;
  }

  auto adjusted_column_names = std::make_pair(build_column_name, probe_column_name);

  auto build_input = build_operator->get_output();
  auto probe_input = probe_operator->get_output();

  _impl = make_unique_by_column_types<AbstractReadOnlyOperatorImpl, JoinHashImpl>(
      build_input->column_type(build_input->column_id_by_name(build_column_name)),
      probe_input->column_type(probe_input->column_id_by_name(probe_column_name)), build_operator, probe_operator,
      adjusted_column_names, _op, _mode, _prefix_left, _prefix_right, inputs_swapped);
  return _impl->on_execute();
}

}  // namespace opossum
