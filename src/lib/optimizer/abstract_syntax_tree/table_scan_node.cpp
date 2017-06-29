#include "table_scan_node.hpp"

#include <memory>
#include <string>

#include "common.hpp"

namespace opossum {

const std::string TableScanNode::description() const {
  std::ostringstream desc;

  desc << "TableScan: [" << _column_name << "] [" << scan_type_to_string(_scan_type) << "]";
  //    desc << "[" << boost::get<std::string>(_value) << "]";
  //    if (_value2) {
  //      desc << " [" << boost::get<std::string>(*_value2) << "]";
  //    }

  return desc.str();
}

std::shared_ptr<TableStatistics> TableScanNode::create_statistics() const {
  Assert(static_cast<bool>(_left), "Table scan needs left input");

  return _left->get_or_create_statistics()->predicate_statistics(_column_name, scan_type_to_string(_scan_type), _value,
                                                                 _value2);
}

}  // namespace opossum
