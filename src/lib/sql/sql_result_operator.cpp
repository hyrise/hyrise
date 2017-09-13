#include "sql_result_operator.hpp"

#include <memory>
#include <string>
#include <vector>

#include "SQLParser.h"

namespace opossum {

SQLResultOperator::SQLResultOperator() {}

const std::string SQLResultOperator::name() const { return "SQLResultOperator"; }

uint8_t SQLResultOperator::num_in_tables() const { return 1; }

uint8_t SQLResultOperator::num_out_tables() const { return 1; }

void SQLResultOperator::set_input_operator(const std::shared_ptr<const AbstractOperator> input) { _input_left = input; }

std::shared_ptr<const Table> SQLResultOperator::_on_execute() {
  if (!_input_left) {
    return std::make_shared<Table>();
  }
  return _input_left->get_output();
}

std::shared_ptr<AbstractOperator> SQLResultOperator::recreate(const std::vector<AllParameterVariant> &args) const {
  auto op = std::make_shared<SQLResultOperator>();
  op->set_input_operator(_input_left->recreate(args));
  return op;
}

}  // namespace opossum
