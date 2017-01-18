#include "fake_operator.hpp"

#include <memory>
#include <string>

namespace opossum {

FakeOperator::FakeOperator(const std::shared_ptr<Table> table) : _table(table) {}

const std::string FakeOperator::name() const { return "Fake Operator"; }

uint8_t FakeOperator::num_in_tables() const { return 1; }

uint8_t FakeOperator::num_out_tables() const { return 1; }

std::shared_ptr<const Table> FakeOperator::on_execute() { return _table; }
}  // namespace opossum
