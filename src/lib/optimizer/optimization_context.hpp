#pragma once

#include <iostream>
#include <map>
#include <memory>
#include <unordered_map>

#include <boost/dynamic_bitset.hpp>
#include "expression/abstract_expression.hpp"

namespace opossum {

class AbstractLQPNode;
class AbstractExpression;
class TableStatistics2;

class OptimizationContext {
 public:
  void print(std::ostream& stream = std::cout) const;

  std::unordered_map<std::shared_ptr<AbstractLQPNode>, size_t> plan_leaf_indices;
  std::unordered_map<std::shared_ptr<AbstractExpression>, size_t> predicate_indices;

  struct TableStatisticsCacheEntry {
    std::shared_ptr<TableStatistics2> table_statistics;
    ExpressionUnorderedMap<ColumnID> column_expressions;
  };

  std::map<boost::dynamic_bitset<>, TableStatisticsCacheEntry> cache;
};

};  // namespace opossum