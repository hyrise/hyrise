#include "table_scan_node.hpp"

#include <memory>
#include <string>

#include "common.hpp"

namespace opossum {

TableScanNode::TableScanNode(const std::string& column_name, const ScanType& scan_type, const AllParameterVariant value,
                             const optional<AllTypeVariant> value2)
    : AbstractNode(NodeType::TableScan),
      _column_name(column_name),
      _scan_type(scan_type),
      _value(value),
      _value2(value2) {}

const std::string TableScanNode::description() const {
  std::ostringstream desc;

  desc << "TableScan: [" << _column_name << "] [" << scan_type_to_string(_scan_type) << "]";
  //  desc << "TableScan: [" << _column_name << "] [" << _scan_type << "]";
  //    desc << "[" << boost::get<std::string>(_value) << "]";
  //    if (_value2) {
  //      desc << " [" << boost::get<std::string>(*_value2) << "]";
  //    }

  return desc.str();
}

const std::string& TableScanNode::column_name() const { return _column_name; }

const ScanType& TableScanNode::scan_type() const { return _scan_type; }

const AllParameterVariant& TableScanNode::value() const { return _value; }

const optional<AllTypeVariant>& TableScanNode::value2() const { return _value2; }

std::shared_ptr<TableStatistics> TableScanNode::create_statistics() const {
  Assert(static_cast<bool>(_left), "Table scan needs left input");

  return _left->get_or_create_statistics()->predicate_statistics(_column_name, _scan_type, _value, _value2);
}

}  // namespace opossum
