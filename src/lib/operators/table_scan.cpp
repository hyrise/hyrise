#include "table_scan.hpp"

#include <memory>
#include <string>

namespace opossum {

TableScan::TableScan(const std::shared_ptr<AbstractOperator> in, const std::string &column_name,
                     /* const std::string &op*/ const AllTypeVariant value)
    : AbstractOperator(in),
      _impl(make_unique_templated<AbstractOperatorImpl, TableScanImpl>(
          _input_left->get_column_type(_input_left->get_column_id_by_name(column_name)), in, column_name,
          /*op,*/ value)) {}

const std::string TableScan::get_name() const { return "TableScan"; }

uint8_t TableScan::get_num_in_tables() const { return 1; }

uint8_t TableScan::get_num_out_tables() const { return 1; }

void TableScan::execute() { _impl->execute(); }

std::shared_ptr<Table> TableScan::get_output() const { return _impl->get_output(); }
}  // namespace opossum
