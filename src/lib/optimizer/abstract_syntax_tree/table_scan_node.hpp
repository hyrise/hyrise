#pragma once

#include <sstream>
#include <unordered_map>
#include <vector>
#include <sql/Expr.h>

#include "common.hpp"
#include "all_parameter_variant.hpp"
#include "all_type_variant.hpp"
#include "optimizer/abstract_syntax_tree/abstract_node.hpp"

namespace opossum {

//std::unordered_map scan_type_mapping = {
//        hsql::Expr::OperatorType::
//};

class TableScanNode : public AbstractNode {
 public:
  TableScanNode(const std::string &column_name, const std::string &op, const AllParameterVariant value,
                const optional<AllTypeVariant> value2 = nullopt) : _column_name(column_name), _op(op), _value(value),
                                                                   _value2(value2) {};

  const std::string description() const override {
    std::ostringstream desc;

    desc << "TableScan: [" << _column_name << "] [" << _op << "]";
//    desc << "[" << boost::get<std::string>(_value) << "]";
//    if (_value2) {
//      desc << " [" << boost::get<std::string>(*_value2) << "]";
//    }

    return desc.str();
  }

 private:
  const std::string _column_name;
  const std::string _op;
  const AllParameterVariant _value;
  const optional<AllTypeVariant> _value2;
};

}  // namespace opossum
