#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_read_only_operator.hpp"
#include "expression/abstract_expression.hpp"

namespace opossum {
// operator to limit the input to n rows
class Limit : public AbstractReadOnlyOperator {
 public:
  Limit(const std::shared_ptr<const AbstractOperator> in,
        const std::shared_ptr<AbstractExpression>& row_count_expression);

  const std::string name() const override;

  std::shared_ptr<AbstractExpression> row_count_expression() const;

 protected:
  std::shared_ptr<const Table> _on_execute() override;
  std::shared_ptr<AbstractOperator> _on_recreate(
      const std::vector<AllParameterVariant>& args, const std::shared_ptr<AbstractOperator>& recreated_input_left,
      const std::shared_ptr<AbstractOperator>& recreated_input_right) const override;

 private:
  std::shared_ptr<AbstractExpression> _row_count_expression;
};
}  // namespace opossum
