#pragma once

#include <iostream>
#include <memory>
#include <string>
#include <vector>

namespace opossum {

enum class AstNodeType { Aggregate, Expression, Projection, Table, Predicate, Sort };

class AbstractAstNode : public std::enable_shared_from_this<AbstractAstNode> {
 public:
  explicit AbstractAstNode(AstNodeType node_type);

  /**
   * The _parent is implicitly set in set_left/set_right
   * for un-setting _parent use clear_parent()
   */
  std::shared_ptr<AbstractAstNode> parent() const;
  void clear_parent();

  const std::shared_ptr<AbstractAstNode> &left() const;
  void set_left(const std::shared_ptr<AbstractAstNode> &left);

  const std::shared_ptr<AbstractAstNode> &right() const;
  void set_right(const std::shared_ptr<AbstractAstNode> &right);

  AstNodeType type() const;

  virtual const std::vector<std::string> &output_columns() const;

  void print(const uint32_t indent = 0, std::ostream &out = std::cout) const;
  virtual std::string description() const = 0;

 protected:
  // Used to easily differentiate between node types without pointer casts.
  AstNodeType _type;
  mutable std::vector<std::string> _output_columns;

 private:
  std::weak_ptr<AbstractAstNode> _parent;
  std::shared_ptr<AbstractAstNode> _left;
  std::shared_ptr<AbstractAstNode> _right;
};

}  // namespace opossum
