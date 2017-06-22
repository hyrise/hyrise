#pragma once

#include <memory>

namespace opossum {

class AbstractNode {
 public:
  template <typename Node, typename... ConstructorArgs>
  static const std::shared_ptr<AbstractNode> make_shared_from_args(ConstructorArgs &&... args) {
    return std::make_shared<Node>(std::forward<ConstructorArgs>(args)...);
  }

  template <typename Node, typename... ConstructorArgs>
  static const std::shared_ptr<AbstractNode> make_shared_from_args(const std::shared_ptr<AbstractNode> &left,
                                                                   ConstructorArgs &&... args) {
    auto node = AbstractNode::make_shared_from_args<Node>(std::forward<ConstructorArgs>(args)...);
    left->set_parent(node);
    node->set_left(left);
    return node;
  }

  template <typename Node, typename... ConstructorArgs>
  static const std::shared_ptr<AbstractNode> make_shared_from_args(const std::shared_ptr<AbstractNode> &left,
                                                                   const std::shared_ptr<AbstractNode> &right,
                                                                   ConstructorArgs &&... args) {
    auto node = AbstractNode::make_shared_from_args<Node>(left, std::forward<ConstructorArgs>(args)...);
    right->set_parent(node);
    node->set_right(right);
    return node;
  }

  const std::weak_ptr<AbstractNode> &get_parent() const;
  void set_parent(const std::weak_ptr<AbstractNode> &parent);

  const std::shared_ptr<AbstractNode> &get_left() const;
  void set_left(const std::shared_ptr<AbstractNode> &left);

  const std::shared_ptr<AbstractNode> &get_right() const;
  void set_right(const std::shared_ptr<AbstractNode> &right);

  void print(const uint8_t indent = 0) const;
  virtual const std::string description() const = 0;

 private:
  std::weak_ptr<AbstractNode> _parent;
  std::shared_ptr<AbstractNode> _left;
  std::shared_ptr<AbstractNode> _right;
};

}  // namespace opossum
