#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "all_parameter_variant.hpp"
#include "all_type_variant.hpp"
#include "common.hpp"
#include "operators/table_scan.hpp"
#include "optimizer/abstract_syntax_tree/abstract_node.hpp"

namespace opossum {

class TableScanNode : public AbstractNode {
 public:
  TableScanNode(const std::string &column_name, const std::string &op, const AllParameterVariant value,
                const optional<AllTypeVariant> value2 = nullopt)
      : _column_name(column_name), _op(op), _value(value), _value2(value2) {
    _type = TableScanNodeType;
  }

  const std::string description() const override;
  const std::string &column_name() const { return _column_name; };
  const std::string &op() const { return _op; };
  const AllParameterVariant &value() const { return _value; };
  const optional<AllTypeVariant> &value2() const { return _value2; };

 private:
  const std::string _column_name;
  const std::string& _op;
  const AllParameterVariant _value;
  const optional<AllTypeVariant> _value2;
};

}  // namespace opossum
