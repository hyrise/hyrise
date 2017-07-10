#pragma once

#include <string>
#include <vector>

#include "common.hpp"
#include "optimizer/abstract_syntax_tree/abstract_node.hpp"

namespace opossum {

class SortNode : public AbstractNode {
 public:
  explicit SortNode(const std::string column_name, const bool asc);

  const std::string description() const override;

  const std::string column_name() const;
  const bool asc() const;

 private:
  const std::string _column_name;
  const bool _asc;
};

}  // namespace opossum
