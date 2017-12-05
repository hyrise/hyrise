#pragma once

#include <string>

#include "abstract_lqp_node.hpp"

namespace opossum {

/**
 * This node type represents limiting a result to a certain number of rows (LIMIT operator).
 */
class LimitNode : public AbstractLQPNode {
 public:
  explicit LimitNode(const size_t num_rows);

  std::string description() const override;

  size_t num_rows() const;

 protected:
  std::shared_ptr<AbstractLQPNode> _deep_copy_impl() const override;

 private:
  const size_t _num_rows;
};

}  // namespace opossum
