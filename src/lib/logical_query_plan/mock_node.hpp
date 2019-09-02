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

  LQPColumnReference get_column(const std::string& column_name) const;

  const ColumnDefinitions& column_definitions() const;

  const std::vector<std::shared_ptr<AbstractExpression>>& column_expressions() const override;
  bool is_column_nullable(const ColumnID column_id) const override;

  /**
   * @defgroup ColumnIDs to be pruned from the mocked Table.
   * Vector passed to `set_pruned_column_ids()` needs to be sorted and unique
   * @{
   */
  void set_pruned_column_ids(const std::vector<ColumnID>& pruned_column_ids);
  const std::vector<ColumnID>& pruned_column_ids() const;
  /** @} */

  std::string description() const override;

  const std::shared_ptr<TableStatistics>& table_statistics() const;
  void set_table_statistics(const std::shared_ptr<TableStatistics>& table_statistics);

  std::optional<std::string> name;

 protected:
  size_t _shallow_hash() const override;
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;

 private:
  mutable std::optional<std::vector<std::shared_ptr<AbstractExpression>>> _column_expressions;

  // Constructor args to keep around for deep_copy()
  ColumnDefinitions _column_definitions;
  std::shared_ptr<TableStatistics> _table_statistics;
  std::vector<ColumnID> _pruned_column_ids;
};
}  // namespace opossum
