#pragma once

#include <iostream>
#include <memory>
#include <string>
#include <vector>

namespace opossum {

enum class ASTNodeType { Join, Predicate, Projection, Sort, StoredTable };

/**
 * Abstract element in an Abstract Syntax Tree.
 * This tree is the base structure used by the optimizer to change the query plan.
 *
 *
 * Design decision:
 * We decided to have mutable Nodes for now.
 * By that we can apply rules without creating new nodes for every optimization rule.
 */
class AbstractASTNode : public std::enable_shared_from_this<AbstractASTNode> {
 public:
  explicit AbstractASTNode(ASTNodeType node_type);

  /**
   * The _parent is implicitly set in set_left_child/set_right_child
   * for un-setting _parent use clear_parent()
   */
  std::shared_ptr<AbstractASTNode> parent() const;
  void clear_parent();

  const std::shared_ptr<AbstractASTNode> &left_child() const;
  void set_left_child(const std::shared_ptr<AbstractASTNode> &left);

  const std::shared_ptr<AbstractASTNode> &right_child() const;
  void set_right_child(const std::shared_ptr<AbstractASTNode> &right);

  ASTNodeType type() const;

  virtual std::vector<std::string> output_column_names() const;

  void print(const uint32_t level = 0, std::ostream &out = std::cout) const;
  virtual std::string description() const = 0;

 protected:
  // Used to easily differentiate between node types without pointer casts.
  ASTNodeType _type;
  mutable std::vector<std::string> _output_column_names;

 private:
  std::weak_ptr<AbstractASTNode> _parent;
  std::shared_ptr<AbstractASTNode> _left_child;
  std::shared_ptr<AbstractASTNode> _right_child;
};

}  // namespace opossum
