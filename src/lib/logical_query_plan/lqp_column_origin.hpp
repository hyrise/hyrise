#pragma once

#include <memory>

#include "types.hpp"

namespace opossum {

class AbstractLQPNode;

/**
 * Used for identifying a Column in an LQP by the Node and the ColumnID in that node in which it was created.
 * Currently this happens in StoredTableNode (which creates all of its columns), AggregateNode (which creates all
 * aggregate columns) and ProjectionNode (which creates all columns containing arithmetics)
 */
class LQPColumnOrigin final {
 public:
  LQPColumnOrigin() = default;
  LQPColumnOrigin(const std::shared_ptr<const AbstractLQPNode>& node, ColumnID column_id);

  std::shared_ptr<const AbstractLQPNode> node() const;
  ColumnID column_id() const;

  std::string description() const;

  bool operator==(const LQPColumnOrigin& rhs) const;

 private:
  // Needs to be weak since Nodes can hold ColumnOrigins referring to themselves
  std::weak_ptr<const AbstractLQPNode> _node;
  ColumnID _column_id{INVALID_COLUMN_ID};
};

std::ostream& operator<<(std::ostream& os, const LQPColumnOrigin& column_origin);
}  // namespace opossum
