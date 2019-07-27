#pragma once

#include <memory>
#include <string>

#include "abstract_rule.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "storage/index/index_statistics.hpp"
#include "storage/table.hpp"
#include "types.hpp"

enum class JoinInputSide { Left, Right };

struct JoinIndexApplicabilityResult {
  std::optional<opossum::IndexSide> index_side{std::nullopt};
  bool pull_up_left_predicates{false};
  bool pull_up_right_predicates{false};
};

namespace opossum {

class AbstractLQPNode;
class JoinNode;
class PredicateNode;
class StoredTableNode;

class JoinIndexPlacementRule : public AbstractRule {
 public:
  std::string name() const override;
  void apply_to(const std::shared_ptr<AbstractLQPNode>& node) const override;

 protected:
  // return values indicate whether the left input tree contains a JoinNode
  bool _place_join_node_recursively(const std::shared_ptr<AbstractLQPNode>& node, const LQPInputSide input_side,
                                    std::vector<std::shared_ptr<PredicateNode>>& left_predicates_to_pull_up,
                                    std::vector<std::shared_ptr<PredicateNode>>& right_predicates_to_pull_up,
                                    const std::optional<JoinInputSide> join_input_side = std::nullopt) const;
  JoinIndexApplicabilityResult _is_index_join_applicable_locally(const std::shared_ptr<JoinNode>& join_node) const;
  bool _is_index_on_join_column(const std::shared_ptr<const AbstractLQPNode>& larger_join_input_node,
                                const ColumnID join_column_id) const;
};

}  // namespace opossum
