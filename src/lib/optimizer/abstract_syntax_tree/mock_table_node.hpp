#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"

namespace opossum {

/**
 * Node that represents a table that has no data backing it, but provides (mocked) statistics. Intended for usage in
 * tests (e.g. optimizer tests that just rely on statistics and not actual data) and the playground
 */
class MockTableNode : public AbstractASTNode {
 public:
  explicit MockTableNode(const std::shared_ptr<TableStatistics>& statistics);

  const std::vector<ColumnID>& output_column_id_to_input_column_id() const override;
  const std::vector<std::string>& output_column_names() const override;

  std::string description() const override;
  std::string get_verbose_column_name(ColumnID column_id) const override;

 protected:
  void _on_child_changed() override;

 private:
  std::vector<ColumnID> _output_column_id_to_input_column_id;
  std::vector<std::string> _output_column_names;
};
}  // namespace opossum
