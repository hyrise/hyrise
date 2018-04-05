#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_lqp_node.hpp"
#include "types.hpp"

namespace opossum {

class UnionNode : public EnableMakeForLQPNode<UnionNode>, public AbstractLQPNode {
 public:
  explicit UnionNode(UnionMode union_mode);

  UnionMode union_mode() const;

  std::string description() const override;

  std::string get_verbose_column_name(ColumnID column_id) const override;

  const std::vector<std::string>& output_column_names() const override;
  const std::vector<LQPColumnReference>& output_column_references() const override;

  TableStatisticsSPtr derive_statistics_from(
      const AbstractLQPNodeSPtr& left_input,
      const AbstractLQPNodeSPtr& right_input) const override;

  bool shallow_equals(const AbstractLQPNode& rhs) const override;

 protected:
  AbstractLQPNodeSPtr _deep_copy_impl(
      const AbstractLQPNodeSPtr& copied_left_input,
      const AbstractLQPNodeSPtr& copied_right_input) const override;

 private:
  UnionMode _union_mode;
};
}  // namespace opossum
