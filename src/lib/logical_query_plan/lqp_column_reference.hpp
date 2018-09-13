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
class LQPColumnReference final {
 public:
  LQPColumnReference() = default;
  LQPColumnReference(const std::shared_ptr<const AbstractLQPNode>& original_node, ColumnID original_column_id);

  std::shared_ptr<const AbstractLQPNode> original_node() const;
  ColumnID original_column_id() const;

  bool operator==(const LQPColumnReference& rhs) const;

 private:
  // Needs to be weak since Nodes can hold ColumnReferences referring to themselves
  std::weak_ptr<const AbstractLQPNode> _original_node;
  ColumnID _original_column_id{INVALID_COLUMN_ID};
};

std::ostream& operator<<(std::ostream& os, const LQPColumnReference& column_reference);
}  // namespace opossum

namespace std {

template <>
struct hash<opossum::LQPColumnReference> {
  size_t operator()(const opossum::LQPColumnReference& column_reference) const;
};

}  // namespace std
