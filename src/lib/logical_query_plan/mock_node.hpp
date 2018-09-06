#pragma once

#include <memory>
#include <optional>
#include <string>
#include "boost/variant.hpp"

#include "abstract_lqp_node.hpp"
#include "all_type_variant.hpp"
#include "lqp_cxlumn_reference.hpp"

namespace opossum {

class TableStatistics;

/**
 * Node that represents a table that has no data backing it, but may provide
 *  - (mocked) statistics
 *  - or just a cxlumn layout. It will pretend it created the cxlumns.
 * It is useful in tests (e.g. general LQP tests, optimizer tests that just rely on statistics and not actual data) and
 * the playground
 */
class MockNode : public EnableMakeForLQPNode<MockNode>, public AbstractLQPNode {
 public:
  using CxlumnDefinitions = std::vector<std::pair<DataType, std::string>>;

  explicit MockNode(const CxlumnDefinitions& cxlumn_definitions, const std::optional<std::string>& name = {});

  LQPCxlumnReference get_cxlumn(const std::string& name) const;

  const CxlumnDefinitions& cxlumn_definitions() const;

  const std::vector<std::shared_ptr<AbstractExpression>>& cxlumn_expressions() const override;

  std::string description() const override;

  std::shared_ptr<TableStatistics> derive_statistics_from(
      const std::shared_ptr<AbstractLQPNode>& left_input,
      const std::shared_ptr<AbstractLQPNode>& right_input = nullptr) const override;

  void set_statistics(const std::shared_ptr<TableStatistics>& statistics);

 protected:
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;

 private:
  std::optional<std::string> _name;
  mutable std::optional<std::vector<std::shared_ptr<AbstractExpression>>> _cxlumn_expressions;

  // Constructor args to keep around for deep_copy()
  CxlumnDefinitions _cxlumn_definitions;
  std::shared_ptr<TableStatistics> _table_statistics;
};
}  // namespace opossum
