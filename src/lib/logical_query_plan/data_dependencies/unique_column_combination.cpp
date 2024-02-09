#include "unique_column_combination.hpp"

#include <algorithm>
#include <cstddef>
#include <functional>
#include <memory>
#include <ostream>
#include <utility>
#include <vector>

#include "expression/abstract_expression.hpp"
#include "utils/assert.hpp"

namespace hyrise {

UniqueColumnCombination::UniqueColumnCombination(ExpressionUnorderedSet init_expressions)
    : expressions(std::move(init_expressions)) {
  Assert(!expressions.empty(), "UniqueColumnCombination cannot be empty.");
}

bool UniqueColumnCombination::operator==(const UniqueColumnCombination& rhs) const {
  if (expressions.size() != rhs.expressions.size()) {
    return false;
  }
  return std::all_of(expressions.cbegin(), expressions.cend(),
                     [&rhs](const auto column_expression) { return rhs.expressions.contains(column_expression); });
}

bool UniqueColumnCombination::operator!=(const UniqueColumnCombination& rhs) const {
  return !(rhs == *this);
}

size_t UniqueColumnCombination::hash() const {
  auto hash = size_t{0};
  for (const auto& expression : expressions) {
    // To make the hash independent of the expressions' order, we have to use a commutative operator like XOR.
    hash = hash ^ expression->hash();
  }

  return boost::hash_value(hash - expressions.size());
}

std::ostream& operator<<(std::ostream& stream, const UniqueColumnCombination& ucc) {
  stream << "{";
  auto expressions_vector =
      std::vector<std::shared_ptr<AbstractExpression>>{ucc.expressions.begin(), ucc.expressions.end()};
  stream << expressions_vector.at(0)->as_column_name();
  for (auto expression_idx = size_t{1}; expression_idx < expressions_vector.size(); ++expression_idx) {
    stream << ", " << expressions_vector[expression_idx]->as_column_name();
  }
  stream << "}";

  return stream;
}

}  // namespace hyrise

namespace std {

size_t hash<hyrise::UniqueColumnCombination>::operator()(const hyrise::UniqueColumnCombination& ucc) const {
  return ucc.hash();
}

}  // namespace std
