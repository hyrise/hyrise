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
class LQPColumnReference;

/**
 * 
 */
class NestedExpressionRule : public AbstractRule {
 public:
  std::string name() const override;
  bool apply_to(const std::shared_ptr<AbstractLQPNode>& node) override;

 private:
  bool _replace_expression_in_parents(const std::shared_ptr<AbstractLQPNode>& node,
                                      const LQPColumnReference& column_reference, const AllTypeVariant& value);
  std::optional<DataType> _get_type_of_expression(const std::shared_ptr<LQPExpression>& expression) const;

  template <typename T>
  AllTypeVariant _evaluate_expression(boost::hana::basic_type<T> type,
                                      const std::shared_ptr<LQPExpression>& expression) const;
};

}  // namespace opossum
