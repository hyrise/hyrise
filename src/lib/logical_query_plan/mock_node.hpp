#pragma once

#include <memory>
#include <optional>
#include <string>
#include "boost/variant.hpp"

#include "abstract_lqp_node.hpp"
#include "all_type_variant.hpp"
#include "lqp_column_reference.hpp"

namespace opossum {

class TableStatistics;

/**
 * Node that represents a table that has no data backing it, but may provide
 *  - (mocked) statistics
 *  - or just a column layout. It will pretend it created the columns.
 * It is useful in tests (e.g. general LQP tests, optimizer tests that just rely on statistics and not actual data) and
 * the playground
 */
class MockNode : public EnableMakeForLQPNode<MockNode>, public AbstractLQPNode {
 public:
  using ColumnDefinitions = std::vector<std::pair<DataType, std::string>>;

  explicit MockNode(const ColumnDefinitions& column_definitions, const std::optional<std::string>& name = {});
  explicit MockNode(const std::shared_ptr<TableStatistics>& statistics);

  LQPColumnReference get_column(const std::string& name) const;

  const ColumnDefinitions& column_definitions() const;
  const boost::variant<ColumnDefinitions, std::shared_ptr<TableStatistics>>& constructor_arguments() const;

  const std::vector<std::shared_ptr<AbstractExpression>>& column_expressions() const override;

  std::string description() const override;

  std::shared_ptr<TableStatistics> derive_statistics_from(
      const std::shared_ptr<AbstractLQPNode>& left_input,
      const std::shared_ptr<AbstractLQPNode>& right_input = nullptr) const override;

 protected:
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;

 private:
  std::optional<std::string> _name;
  mutable std::optional<std::vector<std::shared_ptr<AbstractExpression>>> _column_expressions;

  // Constructor args to keep around for deep_copy()
  boost::variant<ColumnDefinitions, std::shared_ptr<TableStatistics>> _constructor_arguments;
};
}  // namespace opossum
