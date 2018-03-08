#pragma once

#include <optional>

#include "boost/variant.hpp"

#include "resolve_type.hpp"
#include "types.hpp"

namespace opossum {

template<typename ColumnReference, typename Result> using CaseResult = boost::variant<Null, ColumnReference, Result>;

template<typename ColumnReference, typename Result>
struct CaseClause final {
  using ThenType = CaseResult<ColumnReference, Result>;

  CaseClause(const ColumnReference& when, const ThenType& then): when(when), then(then) {}

  ColumnReference when;
  ThenType then;
};

struct AbstractCaseExpression {
  AbstractCaseExpression(const DataType result_data_type, const std::optional<std::string>& alias): result_data_type(result_data_type), alias(alias) {}
  virtual ~AbstractCaseExpression() = default;

  const DataType result_data_type;
  std::optional<std::string> alias;
};

template<typename ColumnReference, typename Result>
struct CaseExpression : public AbstractCaseExpression {
  using ElseType = CaseResult<ColumnReference, Result>;
  using ClauseType = CaseClause<ColumnReference, Result>;

  explicit CaseExpression(const ClauseType& clause, const ElseType& else_ = Null{}, const std::optional<std::string>& alias = std::nullopt): CaseExpression(std::vector<ClauseType>{clause}, else_, alias) {}
  explicit CaseExpression(const std::vector<ClauseType>& clauses, const ElseType& else_ = Null{}, const std::optional<std::string>& alias  = std::nullopt): AbstractCaseExpression(data_type_from_type<Result>(), alias), clauses(clauses), else_(else_) {}

  std::vector<ClauseType> clauses;
  ElseType else_;
};

}  // namespace opossum
