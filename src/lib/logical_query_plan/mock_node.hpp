#pragma once

#include <memory>
#include <optional>
#include <string>
#include "boost/variant.hpp"

#include "abstract_lqp_node.hpp"
#include "all_type_variant.hpp"
#include "storage/constraints/expressions_constraint_definition.hpp"
#include "storage/constraints/table_constraint_definition.hpp"

namespace opossum {

class LQPColumnExpression;
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

  explicit MockNode(const ColumnDefinitions& column_definitions, const std::optional<std::string>& init_name = {},
                    const TableConstraintDefinitions& constraints = {});

  std::shared_ptr<LQPColumnExpression> get_column(const std::string& column_name) const;

  const ColumnDefinitions& column_definitions() const;

  std::vector<std::shared_ptr<AbstractExpression>> column_expressions() const override;
  bool is_column_nullable(const ColumnID column_id) const override;

  void set_functional_dependencies(const std::vector<FunctionalDependency>& fds);
  std::vector<FunctionalDependency> functional_dependencies() const override;

  /**
   * @defgroup ColumnIDs to be pruned from the mocked Table.
   * Vector passed to `set_pruned_column_ids()` needs to be sorted and unique
   * @{
   */
  void set_pruned_column_ids(const std::vector<ColumnID>& pruned_column_ids);
  const std::vector<ColumnID>& pruned_column_ids() const;
  /** @} */

  std::string description(const DescriptionMode mode = DescriptionMode::Short) const override;

  const std::shared_ptr<TableStatistics>& table_statistics() const;
  void set_table_statistics(const std::shared_ptr<TableStatistics>& table_statistics);

  void set_table_constraints(const TableConstraintDefinitions& table_constraints);

  std::optional<std::string> name;

 protected:
  size_t _on_shallow_hash() const override;
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;

 private:
  mutable std::optional<std::vector<std::shared_ptr<AbstractExpression>>> _column_expressions;

  // Constructor args to keep around for deep_copy()
  ColumnDefinitions _column_definitions;
  TableConstraintDefinitions _table_constraints;
  std::shared_ptr<TableStatistics> _table_statistics;
  std::vector<ColumnID> _pruned_column_ids;
  std::vector<FunctionalDependency> _functional_dependencies;
};
}  // namespace opossum
