#pragma once

#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "all_type_variant.hpp"
#include "common.hpp"
#include "types.hpp"

namespace opossum {

class ExpressionNode : public std::enable_shared_from_this<ExpressionNode> {
 public:
  explicit ExpressionNode(const ExpressionType type);

  const std::weak_ptr<ExpressionNode>& parent() const;
  void set_parent(const std::weak_ptr<ExpressionNode>& parent);

  const std::shared_ptr<ExpressionNode>& left_child() const;
  void set_left_child(const std::shared_ptr<ExpressionNode>& left);

  const std::shared_ptr<ExpressionNode>& right_child() const;
  void set_right_child(const std::shared_ptr<ExpressionNode>& right);

  const ExpressionType type() const;

  void print(const uint8_t level = 0) const;

  // Is +, -, *, /
  bool is_arithmetic() const;

  // Is literal or column-ref
  bool is_operand() const;

  // ColumnReferences
  ExpressionNode(const ExpressionType type, const std::string& table_name, const std::string& column_name);
  // Literals
  ExpressionNode(const ExpressionType type, const AllTypeVariant value /*, const AllTypeVariant value2*/);
  // FunctionReferences
  ExpressionNode(const ExpressionType type, const std::string& function_name,
                 std::shared_ptr<std::vector<std::shared_ptr<ExpressionNode>>> expression_list);

  // Convert expression_type to AggregateFunction, if possible
  AggregateFunction as_aggregate_function() const;

  const std::string description() const;

  const std::string& table_name() const;

  const std::string& name() const;
  const std::string& column_name() const;

  const AllTypeVariant value() const;

  // There is currently no need for value2
  //  const AllTypeVariant value2() const;

  const std::shared_ptr<std::vector<std::shared_ptr<ExpressionNode>>>& expression_list() const;

  // Expression as string, parse-able by Projection
  std::string to_expression_string() const;

 protected:
  const ExpressionType _type;

 private:
  const AllTypeVariant _value;
  //  const AllTypeVariant _value2;
  const std::shared_ptr<std::vector<std::shared_ptr<ExpressionNode>>> _expression_list;

  const std::string _name;
  const std::string _table;
  //  char* alias;

  std::weak_ptr<ExpressionNode> _parent;
  std::shared_ptr<ExpressionNode> _left_child;
  std::shared_ptr<ExpressionNode> _right_child;
};

}  // namespace opossum
