#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_lqp_node.hpp"

namespace opossum {

/**
 * Node type to represent insertion of rows into a table.
 */
class InsertNode : public AbstractLQPNode {
 public:
  explicit InsertNode(const std::string& table_name);

  std::string description() const override;

  const std::string& table_name() const;

 protected:
  const std::string _table_name;
};

}  // namespace opossum
