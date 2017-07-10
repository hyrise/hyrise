#pragma once

#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "all_type_variant.hpp"
#include "common.hpp"
#include "optimizer/abstract_syntax_tree/abstract_expression_node.hpp"
#include "types.hpp"

namespace opossum {

class ExpressionNode : public AbstractExpressionNode {
 public:
  explicit ExpressionNode(const ExpressionType type);
  // ColumnReferences
  ExpressionNode(const ExpressionType type, const std::string& table_name, const std::string& column_name);
  // Literals
  ExpressionNode(const ExpressionType type, const AllTypeVariant value /*, const AllTypeVariant value2*/);
  // FunctionReferences
  ExpressionNode(const ExpressionType type, const std::string& function_name,
                 std::shared_ptr<std::vector<std::shared_ptr<ExpressionNode>>> expression_list);

  const std::string description() const override;

  const std::string& table_name() const;

  const std::string& column_name() const;

  const AllTypeVariant value() const;

  // There is currently no need for value2
  //  const AllTypeVariant value2() const;

  const std::shared_ptr<std::vector<std::shared_ptr<ExpressionNode>>>& expression_list() const;

 private:
  const AllTypeVariant _value;
  //  const AllTypeVariant _value2;
  const std::shared_ptr<std::vector<std::shared_ptr<ExpressionNode>>> _expression_list;

  const std::string _name;
  const std::string _table;
  //  char* alias;
};

}  // namespace opossum
