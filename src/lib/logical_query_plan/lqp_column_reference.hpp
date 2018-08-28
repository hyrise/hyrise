#pragma once

#include <memory>

#include "types.hpp"

namespace opossum {

class AbstractLQPNode;

/**
 * Used for identifying a Cxlumn in an LQP by the Node and the CxlumnID in that node in which it was created.
 * Currently this happens in StoredTableNode (which creates all of its cxlumns), AggregateNode (which creates all
 * aggregate cxlumns) and ProjectionNode (which creates all cxlumns containing arithmetics)
 */
class LQPCxlumnReference final {
 public:
  LQPCxlumnReference() = default;
  LQPCxlumnReference(const std::shared_ptr<const AbstractLQPNode>& original_node, CxlumnID original_cxlumn_id);

  std::shared_ptr<const AbstractLQPNode> original_node() const;
  CxlumnID original_cxlumn_id() const;

  bool operator==(const LQPCxlumnReference& rhs) const;

 private:
  // Needs to be weak since Nodes can hold CxlumnReferences referring to themselves
  std::weak_ptr<const AbstractLQPNode> _original_node;
  CxlumnID _original_cxlumn_id{INVALID_cxlumn_id};
};

std::ostream& operator<<(std::ostream& os, const LQPCxlumnReference& cxlumn_reference);
}  // namespace opossum

namespace std {

template <>
struct hash<opossum::LQPCxlumnReference> {
  size_t operator()(const opossum::LQPCxlumnReference& cxlumn_reference) const;
};

}  // namespace std
