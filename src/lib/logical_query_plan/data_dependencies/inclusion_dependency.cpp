#include "inclusion_dependency.hpp"

#include "expression/lqp_column_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"

namespace hyrise {

InclusionDependency::InclusionDependency(const std::vector<std::shared_ptr<AbstractExpression>>& init_expressions,
                                         const std::vector<ColumnID>& init_included_column_ids,
                                         const std::shared_ptr<Table>& init_included_table)
    : expressions{init_expressions},
      included_column_ids{init_included_column_ids},
      included_table{init_included_table} {
  Assert(!expressions.empty(), "InclusionDependency cannot be empty.");
  Assert(expressions.size() == included_column_ids.size(),
         "InclusionDependency expects same amount of including and included columns.");
  Assert(included_table, "InclusionDependency must reference a table.");
}

bool InclusionDependency::operator==(const InclusionDependency& rhs) const {
  const auto expression_count = expressions.size();
  if (included_table != rhs.included_table || expression_count != rhs.expressions.size()) {
    return false;
  }

  // For INDs with the same columns, the order of the expressions is relevant. For instance, [A.a, A.b] in [B.x, B.y] is
  // equals to [A.b, A.a] in [B.y, B.x], but not equals to [A.a, A.b] in [B.y, B.x]. To address this property, we sort
  // the columns and use the permutation to order the included columns in the TableKeyConstraint constructor (see
  // foreign_key_constraint.cpp). Thus, we do not have to handle these cases here and we can assume that equal INDs
  // have their expressions ordered in the same way.
  for (auto expression_idx = size_t{0}; expression_idx < expression_count; ++expression_idx) {
    if (*expressions[expression_idx] != *rhs.expressions[expression_idx] ||
        included_column_ids[expression_idx] != rhs.included_column_ids[expression_idx]) {
      return false;
    }
  }

  return true;
}

bool InclusionDependency::operator!=(const InclusionDependency& rhs) const {
  return !(rhs == *this);
}

size_t InclusionDependency::hash() const {
  auto hash = boost::hash_value(included_table);
  boost::hash_combine(hash, expressions.size());
  for (const auto& expression : expressions) {
    boost::hash_combine(hash, expression->hash());
  }

  for (const auto column_id : included_column_ids) {
    boost::hash_combine(hash, column_id);
  }

  return hash;
}

std::ostream& operator<<(std::ostream& stream, const InclusionDependency& ind) {
  stream << "[";
  stream << ind.included_table << "." << ind.included_column_ids[0];
  for (auto column_id = size_t{1}; column_id < ind.included_column_ids.size(); ++column_id) {
    stream << ", " << ind.included_table << "." << ind.included_column_ids[column_id];
  }
  stream << "] in [";
  stream << ind.expressions[0]->as_column_name();
  for (auto expression_idx = size_t{1}; expression_idx < ind.expressions.size(); ++expression_idx) {
    stream << ", " << ind.expressions[expression_idx]->as_column_name();
  }
  stream << "]";
  return stream;
}

}  // namespace hyrise

namespace std {

size_t hash<hyrise::InclusionDependency>::operator()(const hyrise::InclusionDependency& ind) const {
  return ind.hash();
}

}  // namespace std
