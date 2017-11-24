#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "abstract_lqp_node.hpp"

namespace opossum {

/**
 * Node that represents a table that has no data backing it, but may provide (mocked) statistics. Intended for usage in
 * tests (e.g. general LQP tests, optimizer tests that just rely on statistics and not actual data) and the playground
 */
class MockNode : public AbstractLQPNode {
 public:
  MockNode();
  explicit MockNode(const std::shared_ptr<TableStatistics>& statistics);

  const std::vector<ColumnID>& output_column_ids_to_input_column_ids() const override;
  const std::vector<std::string>& output_column_names() const override;

  std::string description() const override;
  std::string get_verbose_column_name(const ColumnID column_id) const override;

 private:
  std::vector<std::string> _output_column_names;
};
}  // namespace opossum
