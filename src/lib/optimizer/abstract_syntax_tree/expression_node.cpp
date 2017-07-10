#include "expression_node.hpp"

#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "all_type_variant.hpp"
#include "common.hpp"
#include "constant_mappings.hpp"
#include "utils/assert.hpp"

namespace opossum {

ExpressionNode::ExpressionNode(const ExpressionType type)
    : AbstractExpressionNode(type),
      _value(NULL_VALUE) /*, _value2(NULL_VALUE)*/,
      _expression_list({}),
      _name("-"),
      _table("-") {}

ExpressionNode::ExpressionNode(const ExpressionType type, const std::string &table_name, const std::string &column_name)
    : AbstractExpressionNode(type),
      _value(NULL_VALUE) /*, _value2(NULL_VALUE)*/,
      _expression_list({}),
      _name(column_name),
      _table(table_name) {}

ExpressionNode::ExpressionNode(const ExpressionType type, const AllTypeVariant value /*, const AllTypeVariant value2*/)
    : AbstractExpressionNode(type),
      _value(value) /*, _value2(value2)*/,
      _expression_list({}),
      _name("-"),
      _table("-") {}

ExpressionNode::ExpressionNode(const ExpressionType type, const std::string &function_name,
                               std::shared_ptr<std::vector<std::shared_ptr<ExpressionNode>>> expression_list)
    : AbstractExpressionNode(type),
      _value(NULL_VALUE) /*, _value2(NULL_VALUE)*/,
      _expression_list(expression_list),
      _name(function_name),
      _table("-") {}

const std::string ExpressionNode::description() const {
  std::ostringstream desc;

  desc << "Expression (" << expression_type_to_string.at(_type) << ", " << value() /*<< ", " << value2()*/ << ", "
       << table_name() << ", " << column_name() << ")";

  return desc.str();
}

const std::string &ExpressionNode::table_name() const { return _table; }

const std::string &ExpressionNode::column_name() const { return _name; }

const AllTypeVariant ExpressionNode::value() const { return _value; }

// const AllTypeVariant ExpressionNode::value2() const {
//  return _value2;
//}

const std::shared_ptr<std::vector<std::shared_ptr<ExpressionNode>>> &ExpressionNode::expression_list() const {
  return _expression_list;
}

}  // namespace opossum
