#include "case_expression.hpp"

namespace opossum {

CaseExpression::CaseExpression(const CaseWhenClause& clause, const std::shared_ptr<AbstractExpression>& else_):
  CaseExpression({clause}, else_) {

}

CaseExpression::CaseExpression(const std::vector<CaseWhenClause>& clauses, const std::shared_ptr<AbstractExpression>& else_):
  AbstractExpression(ExpressionType::Case), clauses(clauses), else_(else_) {}

bool CaseExpression::deep_equals(const AbstractExpression& expression) const {
  if (type != expression.type) return false;

  const auto& case_expression = static_cast<const CaseExpression&>(expression);

  if (!else_->deep_equals(*case_expression.else_)) return false;
  if (clauses.size() != case_expression.clauses.size()) return false;

  for (auto clause_idx = size_t{0}; clause_idx < clauses.size(); ++clause_idx) {
    if (!clauses[clause_idx].when->deep_equals(*case_expression.clauses[clause_idx].when)) return false;
    if (!clauses[clause_idx].then->deep_equals(*case_expression.clauses[clause_idx].then)) return false;
  }

  return true;
}

std::shared_ptr<AbstractExpression> CaseExpression::deep_copy() const {
  std::vector<CaseWhenClause> deep_copy_clauses;
  deep_copy_clauses.reserve(clauses.size());

  for (const auto& clause : clauses) {
    deep_copy_clauses.emplace_back(clause.when->deep_copy(), clause.then->deep_copy());
  }

  return std::make_shared<CaseExpression>(deep_copy_clauses, else_->deep_copy());
}

std::shared_ptr<AbstractExpression> CaseExpression::deep_resolve_column_expressions() {
  for (auto& clause : clauses) {
    clause.when = clause.when->deep_resolve_column_expressions();
    clause.then = clause.then->deep_resolve_column_expressions();
  }

  else_ = else_->deep_resolve_column_expressions();
}

}  // namespace opossum
