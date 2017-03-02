#include "index_column_scan.hpp"

#include <memory>
#include <string>

namespace opossum {

IndexColumnScan::IndexColumnScan(const std::shared_ptr<AbstractOperator> in, const std::string &column_name,
                                 const std::string &op, const AllTypeVariant value,
                                 const optional<AllTypeVariant> value2)
    : AbstractReadOnlyOperator(in), _column_name(column_name), _op(op), _value(value), _value2(value2) {}

const std::string IndexColumnScan::name() const { return "IndexColumnScan"; }

uint8_t IndexColumnScan::num_in_tables() const { return 1; }

uint8_t IndexColumnScan::num_out_tables() const { return 1; }

std::shared_ptr<const Table> IndexColumnScan::on_execute() {
  _impl = make_unique_by_column_type<AbstractReadOnlyOperatorImpl, IndexColumnScanImpl>(
      input_table_left()->column_type(input_table_left()->column_id_by_name(_column_name)), _input_left, _column_name,
      _op, _value, _value2);
  return _impl->on_execute();
}

}  // namespace opossum
