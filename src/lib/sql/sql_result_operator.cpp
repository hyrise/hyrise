#include "sql_query_operator.hpp"

#include <memory>
#include <string>

#include "sql_query_translator.hpp"

#include "SQLParser.h"

namespace opossum {

SQLResultOperator::SQLResultOperator() {}

const std::string SQLResultOperator::name() const { return "SQLResultOperator"; }

uint8_t SQLResultOperator::num_in_tables() const { return 1; }

uint8_t SQLResultOperator::num_out_tables() const { return 1; }

void SQLResultOperator::set_input_operator(const std::shared_ptr<const AbstractOperator> input) { _input = input; }

std::shared_ptr<const Table> SQLResultOperator::on_execute() { return _input->get_output(); }

}  // namespace opossum
