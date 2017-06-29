#include "table_scan_node.hpp"

#include <string>

#include "common.hpp"

namespace opossum {

TableScanNode::TableScanNode(const std::string &column_name, const ScanType &scan_type, const AllParameterVariant value,
                             const optional<AllTypeVariant> value2)
    : AbstractNode(NodeType::TableScan), _column_name(column_name), _scan_type(scan_type), _value(value), _value2(value2) {}

const std::string TableScanNode::description() const {
  std::ostringstream desc;

  desc << "TableScan: [" << _column_name << "] [" << _scan_type << "]";
  //    desc << "[" << boost::get<std::string>(_value) << "]";
  //    if (_value2) {
  //      desc << " [" << boost::get<std::string>(*_value2) << "]";
  //    }

  return desc.str();
}

}  // namespace opossum
