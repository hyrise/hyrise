#pragma once

#include <memory>
#include <string>
#include <vector>

#include "optimizer/table_statistics.hpp"

namespace opossum {

enum class NodeType { Projection, Table, TableScan, Sort };

class AbstractNode : public std::enable_shared_from_this<AbstractNode> {
 public:
  explicit AbstractNode(NodeType node_type);

  /**
   * set_parent() is implicitly included in set_left()/set_right()
   */
  const std::weak_ptr<AbstractNode> parent() const;
  void clear_parent();

  const std::shared_ptr<AbstractNode> left() const;
  void set_left(const std::shared_ptr<AbstractNode> &left);

  const std::shared_ptr<AbstractNode> right() const;
  void set_right(const std::shared_ptr<AbstractNode> &right);

  const NodeType type() const;

  const std::shared_ptr<TableStatistics> statistics() const;
  void set_statistics(const std::shared_ptr<TableStatistics> statistics);

  virtual const std::vector<std::string> output_columns();
  const std::shared_ptr<TableStatistics> get_or_create_statistics();

  void print(const uint8_t indent = 0) const;
  virtual const std::string description() const = 0;

 protected:
  virtual std::shared_ptr<TableStatistics> create_statistics() const;

 protected:
  // Used to easily differentiate between node types without pointer casts.
  NodeType _type;

 protected:
  std::weak_ptr<AbstractNode> _parent;
  std::shared_ptr<AbstractNode> _left;
  std::shared_ptr<AbstractNode> _right;

  std::shared_ptr<TableStatistics> _statistics;
};

}  // namespace opossum
