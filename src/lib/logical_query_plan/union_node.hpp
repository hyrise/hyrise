#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_logical_query_plan_node.hpp"
#include "types.hpp"

namespace opossum {

class UnionNode : public AbstractLogicalQueryPlanNode {
 public:
  explicit UnionNode(UnionMode union_mode);

  UnionMode union_mode() const;

  std::string description() const override;

  std::string get_verbose_column_name(ColumnID column_id) const override;

  const std::vector<std::string>& output_column_names() const override;
  const std::vector<ColumnID>& output_column_ids_to_input_column_ids() const override;

  std::shared_ptr<TableStatistics> derive_statistics_from(
      const std::shared_ptr<AbstractLogicalQueryPlanNode>& left_child,
      const std::shared_ptr<AbstractLogicalQueryPlanNode>& right_child) const override;

  std::optional<ColumnID> find_column_id_by_named_column_reference(
      const NamedColumnReference& named_column_reference) const override;

  bool knows_table(const std::string& table_name) const override;

  std::vector<ColumnID> get_output_column_ids_for_table(const std::string& table_name) const override;

 private:
  UnionMode _union_mode;
};
}  // namespace opossum
