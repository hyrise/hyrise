#include "table_wrapper.hpp"

#include <memory>
#include <string>

namespace opossum {

TableWrapper::TableWrapper(const std::shared_ptr<const Table> table) : _table(table) {}

const std::string TableWrapper::name() const { return "Update Helper Operator"; }

uint8_t TableWrapper::num_in_tables() const { return 1; }

uint8_t TableWrapper::num_out_tables() const { return 1; }

std::shared_ptr<const Table> TableWrapper::_on_execute() { return _table; }
}  // namespace opossum
