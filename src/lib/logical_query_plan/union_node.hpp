#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_lqp_node.hpp"
#include "types.hpp"

namespace opossum {

class UnionNode : public AbstractLQPNode {
 public:
  explicit UnionNode(UnionMode union_mode);

  UnionMode union_mode() const;

  std::string description() const override;

  std::string get_verbose_column_name(ColumnID column_id) const override;

  const std::vector<std::string>& output_column_names() const override;
  const std::vector<ColumnOrigin>& output_column_origins() const override;

  std::shared_ptr<TableStatistics> derive_statistics_from(
      const std::shared_ptr<AbstractLQPNode>& left_child,
      const std::shared_ptr<AbstractLQPNode>& right_child) const override;

 private:
  UnionMode _union_mode;
};
}  // namespace opossum
