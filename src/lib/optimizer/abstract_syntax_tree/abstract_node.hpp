#pragma once

#include <memory>
#include <string>
#include <vector>

#include "optimizer/table_statistics.hpp"

namespace opossum {

enum NodeType { ProjectionNodeType, TableNodeType, TableScanNodeType };

class AbstractNode : public std::enable_shared_from_this<AbstractNode> {
 public:
  const std::weak_ptr<AbstractNode> &get_parent() const;
  void set_parent(const std::weak_ptr<AbstractNode> parent);

  const std::shared_ptr<AbstractNode> &get_left() const;
  void set_left(const std::shared_ptr<AbstractNode> left);

  const std::shared_ptr<AbstractNode> &get_right() const;
  void set_right(const std::shared_ptr<AbstractNode> right);

  const NodeType get_type() const;
  void set_type(const NodeType _type);

  const std::shared_ptr<TableStatistics> get_statistics() const;
  void set_statistics(const std::shared_ptr<TableStatistics> statistics);

  virtual const std::vector<std::string> output_columns();

  void print(const uint8_t indent = 0) const;
  virtual const std::string description() const = 0;

 protected:
  NodeType _type;

 private:
  std::weak_ptr<AbstractNode> _parent;
  std::shared_ptr<AbstractNode> _left;
  std::shared_ptr<AbstractNode> _right;

  std::shared_ptr<TableStatistics> _statistics;
};

}  // namespace opossum
