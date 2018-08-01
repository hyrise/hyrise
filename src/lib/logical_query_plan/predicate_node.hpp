#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "abstract_lqp_node.hpp"
#include "all_parameter_variant.hpp"
#include "all_type_variant.hpp"
#include "lqp_column_reference.hpp"

namespace opossum {

class AbstractExpression;
class TableStatistics;

enum class ScanType : uint8_t { TableScan, IndexScan };

/**
 * This node type represents a filter.
 * The most common use case is to represent a regular TableScan,
 * but this node is also supposed to be used for IndexScans, for example.
 *
 * HAVING clauses of GROUP BY clauses will be translated to this node type as well.
 */
class PredicateNode : public EnableMakeForLQPNode<PredicateNode>, public AbstractLQPNode {
 public:
  explicit PredicateNode(const std::shared_ptr<AbstractExpression>& predicate);

  std::string description() const override;
  std::vector<std::shared_ptr<AbstractExpression>> node_expressions() const override;
  std::shared_ptr<TableStatistics> derive_statistics_from(
      const std::shared_ptr<AbstractLQPNode>& left_input,
      const std::shared_ptr<AbstractLQPNode>& right_input = nullptr) const override;

  const std::shared_ptr<AbstractExpression> predicate;
  ScanType scan_type{ScanType::TableScan};

 protected:
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;
  //
};

}  // namespace opossum
