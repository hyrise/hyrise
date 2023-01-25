#pragma once

#include "abstract_expression.hpp"
#include "utils/make_bimap.hpp"

namespace hyrise {

// For a possible list of functions, see https://www.w3schools.com/sql/sql_ref_sqlserver.asp
enum class FunctionType {
  Substring,   // SUBSTR()
  Concatenate  // CONCAT()
};

const boost::bimap<FunctionType, std::string> function_type_to_string =
    make_bimap<FunctionType, std::string>({{FunctionType::Substring, "SUBSTR"}, {FunctionType::Concatenate, "CONCAT"}});

class FunctionExpression : public AbstractExpression {
 public:
  FunctionExpression(const FunctionType init_function_type,
                     const std::vector<std::shared_ptr<AbstractExpression>>& init_arguments);

  std::shared_ptr<AbstractExpression> _on_deep_copy(
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const override;
  std::string description(const DescriptionMode mode) const override;
  DataType data_type() const override;

  FunctionType function_type;

 protected:
  bool _shallow_equals(const AbstractExpression& expression) const override;
  size_t _shallow_hash() const override;
};

}  // namespace hyrise
