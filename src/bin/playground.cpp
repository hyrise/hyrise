#include <iostream>

#include "types.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/limit.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "expression/expression_functional.hpp"
#include "statistics/cardinality_estimator.hpp"
#include "tpch/tpch_db_generator.hpp"
#include "visualization/lqp_visualizer.hpp"

using namespace opossum;  // NOLINT
using namespace opossum::expression_functional;

int main() {
  TpchDbGenerator{0.02, 100'000}.generate_and_store();

  const auto part_node = StoredTableNode::make("part");
  const auto lineitem_node = StoredTableNode::make("lineitem");

  const auto p_type = part_node->get_column("p_type");
  const auto p_partkey = part_node->get_column("p_partkey");
  const auto l_partkey = lineitem_node->get_column("l_partkey");

  const auto lqp =
  LimitNode::make(value_(1),
  JoinNode::make(JoinMode::Inner, equals_(p_partkey, l_partkey),
    PredicateNode::make(equals_(p_type, "ECONOMY ANODIZED STEEL"),
      part_node),
    lineitem_node
  ));

  LQPVisualizer{}.visualize({lqp}, "lqp.dot", "lqp.svg");

  std::cout << CardinalityEstimator{}.estimate_cardinality(lqp) << std::endl;
  CardinalityEstimator{}.estimate_statistics(lqp);

  return 0;
}
