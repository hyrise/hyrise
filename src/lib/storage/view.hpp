#pragma once

#include <memory>
#include <unordered_map>

#include "types.hpp"

namespace opossum {

class AbstractLQPNode;

class View {
 public:
  View(const std::shared_ptr<AbstractLQPNode>& lqp, const std::unordered_map<ColumnID, std::string>& column_names);

  std::shared_ptr<View> deep_copy() const;
  bool deep_equals(const View& other) const;

  const std::shared_ptr<AbstractLQPNode> lqp;
  const std::unordered_map<ColumnID, std::string> column_names;
};

}  // namespace opossum
