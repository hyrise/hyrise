#pragma once

#include <memory>
#include <string>
#include <vector>

#include "types.hpp"

namespace opossum {

class AbstractExpressionNode : public std::enable_shared_from_this<AbstractExpressionNode> {
 public:
  explicit AbstractExpressionNode(ExpressionType type);

  const std::weak_ptr<AbstractExpressionNode> &parent() const;
  void set_parent(const std::weak_ptr<AbstractExpressionNode> &parent);

  const std::shared_ptr<AbstractExpressionNode> &left_child() const;
  void set_left_child(const std::shared_ptr<AbstractExpressionNode> &left);

  const std::shared_ptr<AbstractExpressionNode> &right_child() const;
  void set_right_child(const std::shared_ptr<AbstractExpressionNode> &right);

  const ExpressionType type() const;

  void print(const uint8_t level = 0) const;
  virtual std::string description() const = 0;

 protected:
  ExpressionType _type;

 private:
  std::weak_ptr<AbstractExpressionNode> _parent;
  std::shared_ptr<AbstractExpressionNode> _left_child;
  std::shared_ptr<AbstractExpressionNode> _right_child;
};

}  // namespace opossum
