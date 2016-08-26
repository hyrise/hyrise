#include "print.hpp"

#include <memory>
#include <string>

namespace opossum {

Print::Print(const std::shared_ptr<AbstractOperator> in) : AbstractOperator(in) {}

const std::string Print::get_name() const { return "Print"; }

uint8_t Print::get_num_in_tables() const { return 1; }

uint8_t Print::get_num_out_tables() const { return 1; }

void Print::execute() {
  // TODO(Anyone): move print method(s) from table/chunk to here
  _input_left->print();
}

std::shared_ptr<Table> Print::get_output() const { return _input_left; }
}  // namespace opossum
