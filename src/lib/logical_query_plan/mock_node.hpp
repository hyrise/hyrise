#pragma once

#include <memory>
#include <optional>
#include <string>
#include "boost/variant.hpp"

#include "abstract_lqp_node.hpp"
#include "all_type_variant.hpp"

namespace opossum {

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

  explicit MockNode(const ColumnDefinitions& column_definitions,
                    const std::optional<std::string>& alias = std::nullopt);
  explicit MockNode(const std::shared_ptr<TableStatistics>& statistics,
                    const std::optional<std::string>& alias = std::nullopt);

  const std::vector<std::string>& output_column_names() const override;

  const boost::variant<ColumnDefinitions, std::shared_ptr<TableStatistics>>& constructor_arguments() const;

  std::string description() const override;
  std::string get_verbose_column_name(ColumnID column_id) const override;

  bool shallow_equals(const AbstractLQPNode& rhs) const override;

 protected:
  std::shared_ptr<AbstractLQPNode> _deep_copy_impl(
      const std::shared_ptr<AbstractLQPNode>& copied_left_input,
      const std::shared_ptr<AbstractLQPNode>& copied_right_input) const override;

 private:
  std::vector<std::string> _output_column_names;

  // Constructor args to keep around for deep_copy()
  boost::variant<ColumnDefinitions, std::shared_ptr<TableStatistics>> _constructor_arguments;
};
}  // namespace opossum
