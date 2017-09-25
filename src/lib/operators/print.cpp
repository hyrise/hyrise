#include "print.hpp"

#include <algorithm>
#include <iomanip>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "operators/table_wrapper.hpp"
#include "storage/base_column.hpp"
#include "type_cast.hpp"
#include "utils/performance_warning.hpp"
#include "utils/table_printer.hpp"

namespace opossum {

Print::Print(const std::shared_ptr<const AbstractOperator> in, std::ostream& out, bool ignore_empty_chunks)
    : AbstractReadOnlyOperator(in), _out(out), _ignore_empty_chunks(ignore_empty_chunks) {}

const std::string Print::name() const { return "Print"; }

uint8_t Print::num_in_tables() const { return 1; }

uint8_t Print::num_out_tables() const { return 1; }

std::shared_ptr<AbstractOperator> Print::recreate(const std::vector<AllParameterVariant>& args) const {
  return std::make_shared<Print>(_input_left->recreate(args), _out);
}

void Print::print(std::shared_ptr<const Table> table, bool ignore_empty_chunks, std::ostream& out) {
  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();
  Print(table_wrapper, out, ignore_empty_chunks).execute();
}


std::shared_ptr<const Table> Print::_on_execute() {
  TablePrinter printer(_input_table_left(), _out, _ignore_empty_chunks);
  printer.print();
  return _input_table_left();
}

}  // namespace opossum
