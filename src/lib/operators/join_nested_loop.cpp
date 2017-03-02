#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "join_nested_loop.hpp"
#include "product.hpp"

namespace opossum {
JoinNestedLoop::JoinNestedLoop(const std::shared_ptr<AbstractOperator> left,
                               const std::shared_ptr<AbstractOperator> right,
                               optional<std::pair<const std::string &, const std::string &>> column_names,
                               const std::string &op, const JoinMode mode, const std::string &prefix_left,
                               const std::string &prefix_right)
    : AbstractJoinOperator(left, right, column_names, op, mode, prefix_left, prefix_right) {
  if (column_names) {
    // only use the templated JoinNestedLoopImpl if we have column names
    // otherwise, it is a cross-join
    const auto first_column = column_names->first;
    const auto second_column = column_names->second;

    _impl = make_unique_by_column_types<AbstractReadOnlyOperatorImpl, JoinNestedLoopImpl>(
        input_table_left()->column_type(input_table_left()->column_id_by_name(first_column)),
        input_table_right()->column_type(input_table_right()->column_id_by_name(second_column)), left, right,
        *column_names, op, mode, prefix_left, prefix_right);
  } else {
    if (mode != Cross) {
      throw std::runtime_error("JoinNestedLoop: missing column names for non-cross-join!");
    }
  }
}

const std::string JoinNestedLoop::name() const { return "JoinNestedLoop"; }

uint8_t JoinNestedLoop::num_in_tables() const { return 2; }

uint8_t JoinNestedLoop::num_out_tables() const { return 1; }

std::shared_ptr<const Table> JoinNestedLoop::on_execute() {
  if (_mode != Cross) {
    return _impl->on_execute();
  } else {
    // We do not implement a new Cross Join operator, but instead rely on the existing Product Operator.
    auto product = std::make_shared<Product>(_input_left, _input_right, "left", "right");
    product->execute();
    return product->get_output();
  }
}

}  // namespace opossum
