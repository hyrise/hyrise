#include "order_dependency.hpp"

#include <algorithm>
#include <cstddef>
#include <functional>
#include <memory>
#include <ostream>
#include <utility>
#include <vector>

#include <boost/container_hash/hash.hpp>

#include "expression/abstract_expression.hpp"
#include "expression/expression_utils.hpp"
#include "utils/assert.hpp"
#include "utils/print_utils.hpp"

namespace hyrise {

OrderDependency::OrderDependency(std::vector<std::shared_ptr<AbstractExpression>>&& init_ordering_expressions,
                                 std::vector<std::shared_ptr<AbstractExpression>>&& init_ordered_expessions)
    : ordering_expressions{std::move(init_ordering_expressions)},
      ordered_expressions{std::move(init_ordered_expessions)} {
  Assert(!ordering_expressions.empty() && !ordered_expressions.empty(), "OrderDependency cannot be empty.");
  if constexpr (HYRISE_DEBUG) {
    // Do not allow trivial, reflexive ODs.
    auto lhs_expressions = ordering_expressions;
    const auto expression_count = std::min(ordering_expressions.size(), ordered_expressions.size());

    if (lhs_expressions.size() > expression_count) {
      lhs_expressions.erase(
          lhs_expressions.begin() +
              static_cast<std::vector<std::shared_ptr<AbstractExpression>>::difference_type>(expression_count),
          lhs_expressions.end());
    }

    Assert(!first_expressions_match(lhs_expressions, ordered_expressions), "Trivial reflexive ODs are not permitted.");
  }
}

bool OrderDependency::operator==(const OrderDependency& rhs) const {
  const auto ordering_expression_count = ordering_expressions.size();
  const auto ordered_expression_count = ordered_expressions.size();
  if (ordering_expression_count != rhs.ordering_expressions.size() ||
      ordered_expression_count != rhs.ordered_expressions.size()) {
    return false;
  }

  for (auto expression_idx = size_t{0}; expression_idx < ordering_expression_count; ++expression_idx) {
    if (*ordering_expressions[expression_idx] != *rhs.ordering_expressions[expression_idx]) {
      return false;
    }
  }

  for (auto expression_idx = size_t{0}; expression_idx < ordered_expression_count; ++expression_idx) {
    if (*ordered_expressions[expression_idx] != *rhs.ordered_expressions[expression_idx]) {
      return false;
    }
  }

  return true;
}

bool OrderDependency::operator!=(const OrderDependency& rhs) const {
  return !(rhs == *this);
}

size_t OrderDependency::hash() const {
  auto hash = size_t{0};
  boost::hash_combine(hash, ordering_expressions.size());
  for (const auto& expression : ordering_expressions) {
    boost::hash_combine(hash, expression->hash());
  }

  boost::hash_combine(hash, ordered_expressions.size());
  for (const auto& expression : ordered_expressions) {
    boost::hash_combine(hash, expression->hash());
  }

  return hash;
}

std::ostream& operator<<(std::ostream& stream, const OrderDependency& od) {
  stream << "[";
  print_expressions(od.ordering_expressions, stream);
  stream << "] |-> [";
  print_expressions(od.ordered_expressions, stream);
  stream << "]";
  return stream;
}

void build_transitive_od_closure(OrderDependencies& order_dependencies) {
  // Usually, we do not expect to have many ODs per table with even more transitive relationships. Thus, we chose a
  // simple implementation to build the closure.
  while (true) {
    auto transitive_ods = std::vector<OrderDependency>{};
    for (const auto& od : order_dependencies) {
      const auto& ordered_expressions = od.ordered_expressions;
      for (const auto& candidate_od : order_dependencies) {
        // Given od [a] |-> [b, c], check if candidate_od looks like [b] |-> [d].
        const auto& candidate_expressions = candidate_od.ordering_expressions;
        if (ordered_expressions.size() < candidate_expressions.size() ||
            !first_expressions_match(candidate_expressions, ordered_expressions)) {
          continue;
        }

        // Skip if OD would contain an expression both in LHS and RHS or OD is already known.
        auto expression_on_both_sides = false;
        for (const auto& expression : od.ordering_expressions) {
          if (find_expression_idx(*expression, candidate_od.ordered_expressions)) {
            expression_on_both_sides = true;
            break;
          }
        }

        if (expression_on_both_sides) {
          continue;
        }

        auto ordering_expressions = od.ordering_expressions;
        auto ordered_expressions = candidate_od.ordered_expressions;

        const auto& transitive_od = OrderDependency(std::move(ordering_expressions), std::move(ordered_expressions));
        if (order_dependencies.contains(transitive_od)) {
          continue;
        }

        // We cannot insert directly into order_dependencies since we still iterate over it and might invalidate the
        // iterator.
        transitive_ods.emplace_back(transitive_od);
      }
    }

    if (transitive_ods.empty()) {
      return;
    }

    order_dependencies.insert(transitive_ods.cbegin(), transitive_ods.cend());
  }
}

}  // namespace hyrise

namespace std {

size_t hash<hyrise::OrderDependency>::operator()(const hyrise::OrderDependency& od) const {
  return od.hash();
}

}  // namespace std
