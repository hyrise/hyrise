#include "table_scan_node.hpp"

#include <string>

#include "common.hpp"
#include "optimizer/abstract_syntax_tree/expression_node.hpp"

namespace opossum {

TableScanNode::TableScanNode(const std::string& column_name, const std::shared_ptr<ExpressionNode> predicate,
                             const ScanType& scan_type, const AllParameterVariant value,
                             const optional<AllTypeVariant> value2)
    : AbstractNode(NodeType::TableScan),
      _column_name(column_name),
      _predicate(predicate),
      _scan_type(scan_type),
      _value(value),
      _value2(value2) {}

const std::string TableScanNode::description() const {
  std::ostringstream desc;

  desc << "TableScan: [" << _column_name << "]";
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

const std::shared_ptr<ExpressionNode> TableScanNode::predicate() const { return _predicate; }

}  // namespace opossum
