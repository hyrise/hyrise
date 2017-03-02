#include "update_helper_operator.hpp"

#include <memory>
#include <string>

namespace opossum {

UpdateHelperOperator::UpdateHelperOperator(const std::shared_ptr<Table> table) : _table(table) {}

const std::string UpdateHelperOperator::name() const { return "Fake Operator"; }

uint8_t UpdateHelperOperator::num_in_tables() const { return 1; }

uint8_t UpdateHelperOperator::num_out_tables() const { return 1; }

std::shared_ptr<const Table> UpdateHelperOperator::on_execute() { return _table; }
}  // namespace opossum
