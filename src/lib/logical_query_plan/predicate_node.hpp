#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "abstract_lqp_node.hpp"
#include "all_parameter_variant.hpp"
#include "all_type_variant.hpp"
#include "lqp_column_reference.hpp"

namespace opossum {

class TableStatistics;

/**
 * This node type represents a filter.
 * The most common use case is to represent a regular TableScan,
 * but this node is also supposed to be used for IndexScans, for example.
 *
 * HAVING clauses of GROUP BY clauses will be translated to this node type as well.
 */
class PredicateNode : public AbstractLQPNode {
 public:
  PredicateNode(const LQPColumnReference& column_reference, const PredicateCondition predicate_condition,
                const AllParameterVariant& value, const std::optional<AllTypeVariant>& value2 = std::nullopt);

  std::string description() const override;

  const LQPColumnReference& column_reference() const;
  PredicateCondition predicate_condition() const;
  const AllParameterVariant& value() const;
  const std::optional<AllTypeVariant>& value2() const;

  std::shared_ptr<TableStatistics> derive_statistics_from(
      const std::shared_ptr<AbstractLQPNode>& left_child,
      const std::shared_ptr<AbstractLQPNode>& right_child = nullptr) const override;

 protected:
  std::shared_ptr<AbstractLQPNode> _deep_copy_impl(
      const std::shared_ptr<AbstractLQPNode>& copied_left_child,
      const std::shared_ptr<AbstractLQPNode>& copied_right_child) const override;

 private:
  const LQPColumnReference _column_reference;
  const PredicateCondition _predicate_condition;
  const AllParameterVariant _value;
  const std::optional<AllTypeVariant> _value2;
};

}  // namespace opossum
