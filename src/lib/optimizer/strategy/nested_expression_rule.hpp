#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_rule.hpp"
#include "all_type_variant.hpp"
#include "types.hpp"

namespace opossum {

class AbstractLQPNode;
class PredicateNode;
class LQPExpression;

/**
 * 
 */
class NestedExpressionRule : public AbstractRule {
 public:
  std::string name() const override;
  bool apply_to(const std::shared_ptr<AbstractLQPNode>& node) override;

 private:
  // bool _reorder_predicates(std::vector<std::shared_ptr<PredicateNode>>& predicates) const;
  DataType _get_type_of_expression(const std::shared_ptr<LQPExpression>& expression) const;

  template <typename T>
  AllTypeVariant _evaluate_expression(boost::hana::basic_type<T> type,
                                      const std::shared_ptr<LQPExpression>& expression) const;

  /**
   * Operators that all numerical types support.
   */
  template <typename T>
  static std::function<T(const T&, const T&)> _get_base_operator_function(ExpressionType type);

  /**
   * Operators that integral types support.
   */
  template <typename T>
  static std::function<T(const T&, const T&)> _get_operator_function(ExpressionType type);
};

}  // namespace opossum
