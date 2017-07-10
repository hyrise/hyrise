#include "predicate_node.hpp"

#include <string>

#include "common.hpp"
#include "constant_mappings.hpp"

namespace opossum {

PredicateNode::PredicateNode(const std::string& column_name, ScanType scan_type, const AllParameterVariant value,
                             const optional<AllTypeVariant> value2)
    : AbstractAstNode(AstNodeType::Predicate),
      _column_name(column_name),
      _scan_type(scan_type),
      _value(value),
      _value2(value2) {}

std::string PredicateNode::description() const {
  std::ostringstream desc;

  desc << "TableScan: [" << _column_name << "] [" << scan_type_to_string.at(_scan_type) << "]";
  desc << "[" << boost::get<std::string>(boost::get<AllTypeVariant>(_value)) << "]";
  if (_value2) {
    desc << " [" << boost::get<std::string>(_value2.value()) << "]";
  }

  return desc.str();
}

const std::string& PredicateNode::column_name() const { return _column_name; }

ScanType PredicateNode::scan_type() const { return _scan_type; }

const AllParameterVariant& PredicateNode::value() const { return _value; }

const optional<AllTypeVariant>& PredicateNode::value2() const { return _value2; }

}  // namespace opossum
