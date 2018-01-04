#pragma once

#include <memory>

#include "types.hpp"

namespace opossum {

class AbstractLQPNode;

class LQPColumnOrigin final {
 public:
  LQPColumnOrigin() = default;
  LQPColumnOrigin(const std::shared_ptr<const AbstractLQPNode>& node, ColumnID column_id);

  std::shared_ptr<const AbstractLQPNode> node() const;
  ColumnID column_id() const;

  std::string get_verbose_name() const;

  bool operator==(const LQPColumnOrigin& rhs) const;

 private:
  // Needs to be weak since Nodes can hold ColumnOrigins referring to themselves
  std::weak_ptr<const AbstractLQPNode> _node;
  ColumnID _column_id{INVALID_COLUMN_ID};
};

std::ostream& operator<<(std::ostream& os, const LQPColumnOrigin& column_origin);
}  // namespace opossum
