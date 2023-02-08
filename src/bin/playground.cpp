#include <iostream>

#include "expression/expression_functional.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "operators/print.hpp"
#include "optimizer/optimizer.hpp"
#include "optimizer/strategy/chunk_pruning_rule.hpp"
#include "types.hpp"
#include "utils/load_table.hpp"

using namespace hyrise;  // NOLINT

using namespace hyrise::expression_functional;

int main() {
  const auto& table_a = load_table("resources/test_data/tbl/int_sorted_value_clustered.tbl", ChunkOffset{2});
  const auto& table_b = load_table("resources/test_data/tbl/int_int_int2.tbl", ChunkOffset{2});
  Hyrise::get().storage_manager.add_table("table_a", table_a);
  Hyrise::get().storage_manager.add_table("table_b", table_b);

  const auto stored_table_a = StoredTableNode::make("table_a");
  const auto stored_table_b = StoredTableNode::make("table_b");

  const auto a_a = stored_table_a->get_column("a");
  const auto a_b = stored_table_a->get_column("b");
  const auto b_a = stored_table_b->get_column("a");
  const auto b_b = stored_table_b->get_column("b");

  Print::print(table_a);

  std::cout << std::endl << std::endl;

  // clang-format off
// Maximal prossible pruning with b > 6 and < = 4.
auto lqp_1 =
PredicateNode::make(greater_than_(a_b, value_(6)),
  PredicateNode::make(equals_(a_a, value_(4)),
    stored_table_a))->deep_copy();

// Pruning with b > 6.
auto lqp_2 =
PredicateNode::make(greater_than_(a_b, value_(6)),
  stored_table_a)->deep_copy();

auto subquery =
ProjectionNode::make(expression_vector(b_a),
  PredicateNode::make(equals_(b_b, value_(5)),
    stored_table_b));

// Pruning with b > 6 and subquery.
auto lqp_3 =
PredicateNode::make(greater_than_(a_b, value_(6)),
  PredicateNode::make(equals_(a_a, lqp_subquery_(subquery)),
   stored_table_a))->deep_copy();
  // clang-format on

  const auto optimizer = std::make_shared<Optimizer>();
  optimizer->add_rule(std::make_unique<ChunkPruningRule>());

  std::cout << *optimizer->optimize(std::move(lqp_1)) << std::endl << std::endl;
  std::cout << *optimizer->optimize(std::move(lqp_2)) << std::endl << std::endl;
  std::cout << *optimizer->optimize(std::move(lqp_3)) << std::endl << std::endl;
  return 0;
}
