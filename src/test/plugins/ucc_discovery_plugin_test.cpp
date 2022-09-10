#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "base_test.hpp"
#include "lib/utils/plugin_test_utils.hpp"

#include "../../plugins/ucc_discovery_plugin.hpp"
#include "concurrency/transaction_manager.hpp"
#include "expression/expression_functional.hpp"
#include "expression/pqp_column_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "operators/get_table.hpp"
#include "operators/projection.hpp"
#include "operators/table_scan.hpp"
#include "operators/update.hpp"
#include "operators/validate.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "utils/load_table.hpp"
#include "utils/plugin_manager.hpp"

namespace hyrise {

class UccDiscoveryPluginTest : public BaseTest {
 public:
  void SetUp() override {
    const auto& table_A = load_table("resources/test_data/tbl/uniqueness_test_A.tbl", _chunk_size);
    ChunkEncoder::encode_all_chunks(table_A);
    const auto& table_B = load_table("resources/test_data/tbl/uniqueness_test_B.tbl", _chunk_size);
    Hyrise::get().storage_manager.add_table(_table_name_A, table_A);
    Hyrise::get().storage_manager.add_table(_table_name_B, table_B);

    const auto table_node_A = StoredTableNode::make(_table_name_A);
    const auto table_node_B = StoredTableNode::make(_table_name_B);

    _join_col_A = table_node_A->get_column("A_a");
    _join_col_B = table_node_B->get_column("B_a");
    _predicate_col_A = table_node_A->get_column("c");
    _predicate_col_B = table_node_B->get_column("d");

    _lqp = JoinNode::make(JoinMode::Inner, equals_(_join_col_A, _join_col_B),
                          PredicateNode::make(equals_(_predicate_col_A, "unique"), table_node_A),
                          PredicateNode::make(equals_(_predicate_col_B, "not"), table_node_B));
  }

  void TearDown() override {
    Hyrise::reset();
  }

 protected:
  UCCCandidates _identify_ucc_candidates() {
    return _uccPlugin.identify_ucc_candidates();
  }

  void _discover_uccs() {
    return _uccPlugin.discover_uccs();
  }

  const std::string _table_name_A{"uniquenessTestTableA"};
  const std::string _table_name_B{"uniquenessTestTableB"};
  static constexpr auto _chunk_size = ChunkOffset{2};
  const UccDiscoveryPlugin _uccPlugin{};
  std::shared_ptr<JoinNode> _lqp{};
  std::shared_ptr<AggregateNode> _agg_lqp{};

  std::shared_ptr<LQPColumnExpression> _join_col_A{};
  std::shared_ptr<LQPColumnExpression> _join_col_B{};
  std::shared_ptr<LQPColumnExpression> _predicate_col_A{};
  std::shared_ptr<LQPColumnExpression> _predicate_col_B{};
};

TEST_F(UccDiscoveryPluginTest, LoadUnloadPlugin) {
  auto& pm = Hyrise::get().plugin_manager;
  pm.load_plugin(build_dylib_path("libhyriseUccDiscoveryPlugin"));
  pm.unload_plugin("hyriseUccDiscoveryPlugin");
}

TEST_F(UccDiscoveryPluginTest, CorrectCandidatesGeneratedForJoin) {
  Hyrise::get().default_lqp_cache = std::make_shared<SQLLogicalPlanCache>();
  Hyrise::get().default_pqp_cache = std::make_shared<SQLPhysicalPlanCache>();

  Hyrise::get().default_lqp_cache->set("TestLQP", _lqp);

  auto ucc_candidates = _identify_ucc_candidates();

  EXPECT_EQ(ucc_candidates.size(), 4);

  const auto join_col_A_candidate = UCCCandidate{_table_name_A, _join_col_A->original_column_id};
  const auto predicate_col_A_candidate = UCCCandidate{_table_name_A, _predicate_col_A->original_column_id};
  const auto join_col_B_candidate = UCCCandidate{_table_name_B, _join_col_B->original_column_id};
  const auto predicate_col_B_candidate = UCCCandidate{_table_name_B, _predicate_col_B->original_column_id};

  EXPECT_TRUE(ucc_candidates.contains(join_col_A_candidate));
  EXPECT_TRUE(ucc_candidates.contains(predicate_col_A_candidate));
  EXPECT_TRUE(ucc_candidates.contains(join_col_B_candidate));
  EXPECT_TRUE(ucc_candidates.contains(predicate_col_B_candidate));
}

TEST_F(UccDiscoveryPluginTest, CorrectCandidatesGeneratedForAggregate) {
  const auto table_node_A = StoredTableNode::make(_table_name_A);

  const auto lqp = AggregateNode::make(expression_vector(_join_col_A), expression_vector(),
                                       PredicateNode::make(equals_(_predicate_col_A, "unique"), table_node_A));

  Hyrise::get().default_lqp_cache = std::make_shared<SQLLogicalPlanCache>();
  Hyrise::get().default_pqp_cache = std::make_shared<SQLPhysicalPlanCache>();

  Hyrise::get().default_lqp_cache->set("TestLQP", lqp);

  auto ucc_candidates = _identify_ucc_candidates();

  const auto join_col_A_candidate = UCCCandidate{_table_name_A, _join_col_A->original_column_id};
  const auto predicate_col_A_candidate = UCCCandidate{_table_name_A, _predicate_col_A->original_column_id};

  EXPECT_EQ(ucc_candidates.size(), 1);
  EXPECT_TRUE(ucc_candidates.contains(join_col_A_candidate));
  EXPECT_TRUE(!ucc_candidates.contains(predicate_col_A_candidate));
}

TEST_F(UccDiscoveryPluginTest, NoCandidatesGenerated) {
  const auto table_node_A = StoredTableNode::make(_table_name_A);
  const auto table_node_B = StoredTableNode::make(_table_name_B);

  const auto lqp =
      UnionNode::make(SetOperationMode::All, PredicateNode::make(equals_(_predicate_col_A, "unique"), table_node_A),
                      PredicateNode::make(equals_(_predicate_col_B, "not"), table_node_B));

  Hyrise::get().default_lqp_cache = std::make_shared<SQLLogicalPlanCache>();
  Hyrise::get().default_pqp_cache = std::make_shared<SQLPhysicalPlanCache>();

  Hyrise::get().default_lqp_cache->set("TestLQP", lqp);

  auto ucc_candidates = _identify_ucc_candidates();

  EXPECT_EQ(ucc_candidates.size(), 0);
}

TEST_F(UccDiscoveryPluginTest, PluginFullRun) {
  Hyrise::get().default_lqp_cache = std::make_shared<SQLLogicalPlanCache>();
  Hyrise::get().default_pqp_cache = std::make_shared<SQLPhysicalPlanCache>();

  Hyrise::get().default_lqp_cache->set("TestLQP", _lqp);

  // run UCC discovery; we expect both join columns and the predicate on table A to be identified as unique
  _discover_uccs();

  // collect constraints known for the tables
  const auto& constraints_A = Hyrise::get().storage_manager.get_table(_table_name_A)->soft_key_constraints();
  const auto& constraints_B = Hyrise::get().storage_manager.get_table(_table_name_B)->soft_key_constraints();

  EXPECT_EQ(constraints_A.size(), 2);
  EXPECT_EQ(constraints_B.size(), 1);

  auto constrained_columns_A = std::unordered_set<ColumnID>{};
  auto constrained_columns_B = std::unordered_set<ColumnID>{};

  for (const auto& constraint : constraints_A) {
    const auto& columns = constraint.columns();
    EXPECT_EQ(columns.size(), 1);
    constrained_columns_A.insert(*begin(columns));
  }

  for (const auto& constraint : constraints_B) {
    const auto& columns = constraint.columns();
    EXPECT_EQ(columns.size(), 1);
    constrained_columns_B.insert(*begin(columns));
  }

  // ensure the correct columns are identified as unique
  EXPECT_TRUE(constrained_columns_A.contains(_join_col_A->original_column_id));
  EXPECT_TRUE(constrained_columns_A.contains(_predicate_col_A->original_column_id));
  EXPECT_TRUE(constrained_columns_B.contains(_join_col_B->original_column_id));
  EXPECT_TRUE(!constrained_columns_B.contains(_predicate_col_B->original_column_id));

  // ensure we clear the LQP again
  EXPECT_EQ(Hyrise::get().default_lqp_cache->size(), 0);
}

}  // namespace hyrise
