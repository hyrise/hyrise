#pragma once

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"

namespace opossum {

using JoinGeneratorFunctor = std::function<const std::shared_ptr<AbstractExpression>(
    const std::shared_ptr<StoredTableNode>&, const std::shared_ptr<StoredTableNode>&)>;

class CalibrationQueryGeneratorJoin {
 public:
  static const std::vector<std::shared_ptr<AbstractLQPNode>> generate_join(
      const JoinGeneratorFunctor& join_predicate_generator, const std::shared_ptr<StoredTableNode>& left_table,
      const std::shared_ptr<StoredTableNode>& right_table);

  /*
     * Functors to generate joins.
     * They all implement 'JoinGeneratorFunctor'
     */
  static const std::shared_ptr<AbstractExpression> generate_join_predicate(
      const std::shared_ptr<StoredTableNode>& left_table, const std::shared_ptr<StoredTableNode>& right_table);

 private:
  CalibrationQueryGeneratorJoin() = default;
};

}  // namespace opossum
