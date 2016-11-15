#include "table_scan.hpp"

#include <memory>
#include <string>

namespace opossum {

TableScan::TableScan(const std::shared_ptr<AbstractOperator> in, const std::string &column_name, const std::string &op,
                     const AllTypeVariant value, const optional<AllTypeVariant> value2)
    : AbstractOperator(in),
      _impl(make_unique_by_column_type<AbstractOperatorImpl, TableScanImpl>(
          _input_left->column_type(_input_left->column_id_by_name(column_name)), in, column_name, op, value, value2)) {}

const std::string TableScan::name() const { return "TableScan"; }

uint8_t TableScan::num_in_tables() const { return 1; }

uint8_t TableScan::num_out_tables() const { return 1; }

void TableScan::execute() { _impl->execute(); }

std::shared_ptr<const Table> TableScan::get_output() const { return _impl->get_output(); }
}  // namespace opossum
