#pragma once

#include <memory>
#include <string>

#include "abstract_lqp_node.hpp"

namespace opossum {

/**
 * Node type to represent deletion (more specifically, invalidation) in a table.
 */
class DeleteNode : public AbstractLQPNode {
 public:
  explicit DeleteNode(const std::string& table_name);

  std::string description() const override;

  const std::string& table_name() const;

 protected:
  const std::string _table_name;
};

}  // namespace opossum
