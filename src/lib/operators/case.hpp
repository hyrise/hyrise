#pragma once

#include <memory>

#include "abstract_read_only_operator.hpp"
#include "case_expression.hpp"

namespace opossum {

template<typename DataType> using PhysicalCaseClause = CaseClause<ColumnID, DataType>;
template<typename DataType> using PhysicalCaseExpression = CaseExpression<ColumnID, DataType>;
template<typename DataType> using PhysicalCaseResult = CaseResult<ColumnID, DataType>;

class Case : public AbstractReadOnlyOperator {
 public:
  using Expressions = std::vector<std::shared_ptr<const AbstractCaseExpression>>;

  Case(const std::shared_ptr<AbstractOperator>& input, const Expressions& case_expressions);

  // TODO(moritz) description()

  const std::string name() const override;

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  std::shared_ptr<AbstractOperator> _on_recreate(
  const std::vector<AllParameterVariant>& args, const std::shared_ptr<AbstractOperator>& recreated_input_left,
  const std::shared_ptr<AbstractOperator>& recreated_input_right) const override;

 private:
  std::vector<std::shared_ptr<const AbstractCaseExpression>> _case_expressions;
};

}  // namespace opossum
