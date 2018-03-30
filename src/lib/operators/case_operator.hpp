#pragma once

#include <memory>
#include <sstream>

#include "abstract_read_only_operator.hpp"
#include "case_expression.hpp"

namespace opossum {

template <typename DataType>
using PhysicalCaseWhenClause = CaseWhenClause<ColumnID, DataType>;
template <typename DataType>
using PhysicalCaseExpression = CaseExpression<ColumnID, DataType>;
template <typename DataType>
using PhysicalCaseExpressionResult = CaseExpressionResult<ColumnID, DataType>;

/**
 * Computes CaseExpressions, forwards the input columns and stores the result of each CaseExpression in an appended
 * Column.
 * For general comments on CASE support in Hyrise, see case_expression.hpp
 */
class CaseOperator : public AbstractReadOnlyOperator {
 public:
  // const and wrapped in shared_ptr, so this vector can simply be copied in _on_recreate()
  using Expressions = std::vector<std::shared_ptr<const AbstractCaseExpression>>;

  CaseOperator(const std::shared_ptr<AbstractOperator>& input, const Expressions& case_expressions);

  /**
   * @defgroup AbstractOperator interface
   * @{
   */
  const std::string name() const override;
  const std::string description(DescriptionMode description_mode) const override;
  /**@}*/

 protected:
  /**
   * @defgroup AbstractOperator interface
   * @{
   */

  std::shared_ptr<const Table> _on_execute() override;

  std::shared_ptr<AbstractOperator> _on_recreate(
      const std::vector<AllParameterVariant>& args, const std::shared_ptr<AbstractOperator>& recreated_input_left,
      const std::shared_ptr<AbstractOperator>& recreated_input_right) const override;

  /**@}*/

 private:
  std::vector<std::shared_ptr<const AbstractCaseExpression>> _case_expressions;
};

}  // namespace opossum
