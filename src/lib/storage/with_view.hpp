#pragma once

#include <memory>
#include <unordered_map>

#include "types.hpp"

namespace opossum {

class AbstractLQPNode;

class WithView {
 public:
  WithView(const std::shared_ptr<AbstractLQPNode>& lqp,
           const std::unordered_multimap<ColumnID, std::string>& column_names);

  std::shared_ptr<WithView> deep_copy() const;
  bool deep_equals(const WithView& other) const;

  const std::shared_ptr<AbstractLQPNode> lqp;
  const std::unordered_multimap<ColumnID, std::string> column_names;
};

}  // namespace opossum
