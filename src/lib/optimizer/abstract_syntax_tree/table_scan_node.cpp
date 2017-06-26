#include "table_scan_node.hpp"

#include <string>

#include "common.hpp"

namespace opossum {

const std::string TableScanNode::description() const {
  std::ostringstream desc;

  desc << "TableScan: [" << _column_name << "] [" << _op << "]";
  //    desc << "[" << boost::get<std::string>(_value) << "]";
  //    if (_value2) {
  //      desc << " [" << boost::get<std::string>(*_value2) << "]";
  //    }

  return desc.str();
}

}  // namespace opossum
