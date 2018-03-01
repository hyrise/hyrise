#pragma once

#include <memory>

#include "abstract_read_only_operator.hpp"
#include "case_expression.hpp"

namespace opossum {

template<typename Result> using PhysicalCaseClause = CaseClause<ColumnID, Result>;
template<typename Result> using PhysicalCaseExpression = CaseExpression<ColumnID, Result>;

class Case : public AbstractReadOnlyOperator {
 public:
  Case(const std::shared_ptr<AbstractOperator>& input, std::vector<std::unique_ptr<AbstractCaseExpression>> case_expressions);

  // TODO(moritz) description()

  const std::string name() const override;

 protected:
  std::shared_ptr<const Table> _on_execute() override;

 private:
  std::vector<std::unique_ptr<AbstractCaseExpression>> _case_expressions;
};

}  // namespace opossum
