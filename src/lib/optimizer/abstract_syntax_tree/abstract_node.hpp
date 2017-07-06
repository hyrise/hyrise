#pragma once

#include <memory>
#include <string>
#include <vector>

namespace opossum {

enum class NodeType { Aggregate, Projection, Table, TableScan, Sort };

class AbstractNode : public std::enable_shared_from_this<AbstractNode> {
 public:
  explicit AbstractNode(NodeType node_type);

  const std::weak_ptr<AbstractNode> &parent() const;
  void set_parent(const std::weak_ptr<AbstractNode> &parent);

  const std::shared_ptr<AbstractNode> &left() const;
  void set_left(const std::shared_ptr<AbstractNode> &left);

  const std::shared_ptr<AbstractNode> &right() const;
  void set_right(const std::shared_ptr<AbstractNode> &right);

  const NodeType type() const;

  virtual const std::vector<std::string> output_columns();

  void print(const uint8_t indent = 0) const;
  virtual const std::string description() const = 0;

 protected:
  // Used to easily differentiate between node types without pointer casts.
  NodeType _type;

 private:
  std::weak_ptr<AbstractNode> _parent;
  std::shared_ptr<AbstractNode> _left;
  std::shared_ptr<AbstractNode> _right;
};

}  // namespace opossum
