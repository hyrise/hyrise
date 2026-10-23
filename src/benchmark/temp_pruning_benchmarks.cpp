#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "benchmark/benchmark.h"

#include "all_type_variant.hpp"
#include "expression/expression_functional.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/logical_plan_root_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "optimizer/optimization_context.hpp"
#include "optimizer/strategy/chunk_pruning_rule.hpp"
#include "statistics/generate_pruning_statistics.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/encoding_type.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace hyrise {

using namespace expression_functional;  // NOLINT(build/namespaces)

/**
 * Micro-benchmark for ChunkPruningRule (see chunk_pruning_rule.cpp), motivated by TPC-C pipeline metrics: the
 * optimizer takes a large share of TPC-C query runtime, and ChunkPruningRule a large share of the optimizer's.
 * TPC-C tables hold few chunks, so the rule's cost there is per-invocation overhead (container allocations, the
 * per-PredicateNode deep_copy of the StoredTableNode, StorageManager lookups), not work proportional to the number
 * of chunks.
 *
 * Args({chunk_count, predicate_count, branch_count, column_count}):
 *   chunk_count      -- chunks of the stored table
 *   predicate_count  -- predicates per pruning chain
 *   branch_count     -- number of predicate pruning chains reaching the StoredTableNode. 1 means a linear plan.
 *                       Values > 1 branch the plan and re-merge it with a UnionNode, which is the ONLY way to make
 *                       ChunkPruningRule::_intersect_chunk_ids do any work: a linear plan yields a single chain and
 *                       hits the single-list shortcut. Branching also exercises the shared-prefix cache hit in
 *                       compute_chunk_exclude_list.
 *   column_count     -- columns of the stored table. Deliberately a dimension: the rule deep-copies the
 *                       StoredTableNode and re-derives its output expressions once per PredicateNode, so cost grows
 *                       with column count even though no extra pruning work is done. TPC-C tables have ~8-21 columns.
 *
 * Rows per chunk are held constant and small: the rule only reads per-chunk pruning statistics, so the number of
 * rows behind each chunk does not affect its runtime, only the fixture's setup cost.
 */
class ChunkPruningRuleBenchmarkFixture : public benchmark::Fixture {
 public:
  void SetUp(::benchmark::State& state) override {
    _chunk_count = static_cast<size_t>(state.range(0));
    _predicate_count = static_cast<size_t>(state.range(1));
    _branch_count = static_cast<size_t>(state.range(2));
    _column_count = static_cast<size_t>(state.range(3));

    auto column_definitions = TableColumnDefinitions{};
    column_definitions.reserve(_column_count);
    for (auto column_idx = size_t{0}; column_idx < _column_count; ++column_idx) {
      column_definitions.emplace_back("column_" + std::to_string(column_idx), DataType::Int, false);
    }

    auto table = std::make_shared<Table>(column_definitions, TableType::Data, ROWS_PER_CHUNK, UseMvcc::Yes);

    const auto row_count = _chunk_count * ROWS_PER_CHUNK;
    auto row = std::vector<AllTypeVariant>(_column_count, AllTypeVariant{int32_t{0}});
    for (auto row_idx = size_t{0}; row_idx < row_count; ++row_idx) {
      const auto value = static_cast<int32_t>(row_idx);
      for (auto& field : row) {
        field = value;
      }
      table->append(row);
    }

    // encode_all_chunks requires immutable chunks.
    const auto chunk_count = table->chunk_count();
    if (chunk_count > 0) {
      const auto& last_chunk = table->get_chunk(ChunkID{chunk_count - 1});
      if (last_chunk && last_chunk->is_mutable()) {
        last_chunk->set_immutable();
      }
    }
    ChunkEncoder::encode_all_chunks(table, SegmentEncodingSpec{EncodingType::Dictionary});
    generate_chunk_pruning_statistics(table);

    Hyrise::get().storage_manager.add_table(TABLE_NAME, table);
    _max_value = static_cast<int32_t>(row_count);
  }

  void TearDown(::benchmark::State& /*state*/) override {
    Hyrise::get().storage_manager.drop_table(TABLE_NAME);
  }

 protected:
  static constexpr auto ROWS_PER_CHUNK = ChunkOffset{8};
  static constexpr auto TABLE_NAME = "pruning_benchmark_table";

  // Builds either
  //   (branch_count == 1) StoredTable -> Predicate* -> Root, a single pruning chain, or
  //   (branch_count >  1) StoredTable -> Predicate -> {Predicate*} x branch_count -> Union(s) -> Root.
  // In the branching case the shared PredicateNode has multiple outputs, which is what makes
  // find_predicate_pruning_chains_by_stored_table_node_recursively emit one chain per branch. Each branch uses
  // different thresholds, so the exclusion lists differ and the intersection is not a no-op.
  std::shared_ptr<LogicalPlanRootNode> _build_lqp() const {
    const auto stored_table_node = StoredTableNode::make(TABLE_NAME);
    const auto column = stored_table_node->get_column("column_0");

    auto append_predicates = [&](std::shared_ptr<AbstractLQPNode> input, const size_t first_divisor) {
      for (auto predicate_idx = size_t{0}; predicate_idx < _predicate_count; ++predicate_idx) {
        const auto divisor = static_cast<int32_t>(first_divisor + predicate_idx);
        input = PredicateNode::make(greater_than_(column, _max_value / divisor), input);
      }
      return input;
    };

    auto top_node = std::shared_ptr<AbstractLQPNode>{};
    if (_branch_count <= 1) {
      top_node = append_predicates(stored_table_node, 2);
    } else {
      // One shared predicate so that the branch point is a PredicateNode rather than the StoredTableNode itself.
      const auto shared_node = PredicateNode::make(greater_than_(column, _max_value / 2), stored_table_node);

      auto branches = std::vector<std::shared_ptr<AbstractLQPNode>>{};
      branches.reserve(_branch_count);
      for (auto branch_idx = size_t{0}; branch_idx < _branch_count; ++branch_idx) {
        branches.emplace_back(append_predicates(shared_node, 3 + branch_idx * _predicate_count));
      }

      // Fold the branches back together. Chains end at the Union, so the plan above it does not matter.
      top_node = branches[0];
      for (auto branch_idx = size_t{1}; branch_idx < _branch_count; ++branch_idx) {
        top_node = UnionNode::make(SetOperationMode::Positions, top_node, branches[branch_idx]);
      }
    }

    const auto root_node = LogicalPlanRootNode::make();
    root_node->set_left_input(top_node);
    return root_node;
  }

  size_t _chunk_count{0};
  size_t _predicate_count{0};
  size_t _branch_count{0};
  size_t _column_count{0};
  int32_t _max_value{0};
};

BENCHMARK_DEFINE_F(ChunkPruningRuleBenchmarkFixture, BM_ChunkPruningRule)(benchmark::State& state) {
  for (auto _ : state) {
    // Fresh LQP per iteration: the rule stores pruned ChunkIDs on the StoredTableNode, so a plan cannot be reused.
    // Fresh rule per iteration: its exclusion cache is keyed on (StoredTableNode, PredicateNode) shared_ptrs, which
    // are new for every statement anyway, so a long-lived rule would not produce cache hits across iterations -- it
    // would only accumulate entries. Within one iteration the cache is still exercised by branch_count > 1.
    auto root_node = _build_lqp();
    const auto rule = ChunkPruningRule{};
    auto context = OptimizationContext{};
    rule.apply_to_plan(root_node, context);
    benchmark::DoNotOptimize(root_node);
    benchmark::ClobberMemory();
  }
}

// Baseline: LQP construction only, to subtract from the rule benchmark.
BENCHMARK_DEFINE_F(ChunkPruningRuleBenchmarkFixture, BM_ChunkPruningRule_LQPOnly)
(benchmark::State& state) {
  for (auto _ : state) {
    auto root_node = _build_lqp();
    benchmark::DoNotOptimize(root_node);
    benchmark::ClobberMemory();
  }
}

// TPC-C-like shapes: few chunks, short chains, realistic column counts. These are the numbers the optimization is
// supposed to move. The column_count pair (1 vs. 16 at otherwise identical shape) isolates the per-PredicateNode
// StoredTableNode deep_copy. The last group is a scaling sanity check that the rule has not become asymptotically
// worse; it is not representative of TPC-C.
#define PRUNING_BENCHMARK_ARGS                                                                        \
  ->ArgNames({"chunks", "predicates", "branches", "columns"})                                         \
      ->Args({1, 1, 1, 16})                                                                           \
      ->Args({2, 1, 1, 16})                                                                           \
      ->Args({4, 1, 1, 16})                                                                           \
      ->Args({4, 3, 1, 1})                                                                            \
      ->Args({4, 3, 1, 16})                                                                           \
      ->Args({8, 3, 1, 16})                                                                           \
      ->Args({8, 3, 2, 16})                                                                           \
      ->Args({8, 3, 4, 16})                                                                           \
      ->Args({256, 3, 1, 16})                                                                         \
      ->Args({4096, 3, 1, 16})                                                                        \
      ->Args({4096, 3, 2, 16})                                                                        \
      ->Unit(benchmark::kNanosecond)

BENCHMARK_REGISTER_F(ChunkPruningRuleBenchmarkFixture, BM_ChunkPruningRule) PRUNING_BENCHMARK_ARGS;
BENCHMARK_REGISTER_F(ChunkPruningRuleBenchmarkFixture, BM_ChunkPruningRule_LQPOnly) PRUNING_BENCHMARK_ARGS;

}  // namespace hyrise
