#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "base_test.hpp"
#include "lib/storage/encoding_test.hpp"
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
    _table_A = load_table("resources/test_data/tbl/uniqueness_test_A.tbl", _chunk_size);
    _table_B = load_table("resources/test_data/tbl/uniqueness_test_B.tbl", _chunk_size);
    Hyrise::get().storage_manager.add_table(_table_name_A, _table_A);
    Hyrise::get().storage_manager.add_table(_table_name_B, _table_B);

    _table_node_A = StoredTableNode::make(_table_name_A);
    _table_node_B = StoredTableNode::make(_table_name_B);

    _join_columnA = _table_node_A->get_column("A_a");
    _join_columnB = _table_node_B->get_column("B_a");
    _predicate_column_A = _table_node_A->get_column("c");
    _predicate_column_B = _table_node_B->get_column("d");

    Hyrise::get().default_lqp_cache = std::make_shared<SQLLogicalPlanCache>();
    Hyrise::get().default_pqp_cache = std::make_shared<SQLPhysicalPlanCache>();
  }

 protected:
  UccCandidates _identify_ucc_candidates() {
    return UccDiscoveryPlugin::_identify_ucc_candidates();
  }

  void _discover_uccs() {
    UccDiscoveryPlugin::_validate_ucc_candidates(UccDiscoveryPlugin::_identify_ucc_candidates());
  }

  void _validate_ucc_candidates(const UccCandidates& candidates) {
    UccDiscoveryPlugin::_validate_ucc_candidates(candidates);
  }

  void _encode_table(const std::shared_ptr<Table>& table, const SegmentEncodingSpec& encoding_spec) {
    auto chunk_encoding_spec = ChunkEncodingSpec{table->column_count(), SegmentEncodingSpec{EncodingType::Unencoded}};

    for (auto column_id = ColumnID{0}; column_id < table->column_count(); ++column_id) {
      if (encoding_supports_data_type(encoding_spec.encoding_type, table->column_data_type(column_id))) {
        chunk_encoding_spec[column_id] = encoding_spec;
      }
    }
    ChunkEncoder::encode_all_chunks(table, chunk_encoding_spec);
  }

  const std::string _table_name_A{"uniquenessTestTableA"};
  const std::string _table_name_B{"uniquenessTestTableB"};
  const ChunkOffset _chunk_size{2};

  std::shared_ptr<Table> _table_A{};
  std::shared_ptr<Table> _table_B{};
  std::shared_ptr<StoredTableNode> _table_node_A{};
  std::shared_ptr<StoredTableNode> _table_node_B{};
  std::shared_ptr<LQPColumnExpression> _join_columnA{};
  std::shared_ptr<LQPColumnExpression> _join_columnB{};
  std::shared_ptr<LQPColumnExpression> _predicate_column_A{};
  std::shared_ptr<LQPColumnExpression> _predicate_column_B{};
};

class UccDiscoveryPluginMultiEncodingTest : public UccDiscoveryPluginTest,
                                            public ::testing::WithParamInterface<SegmentEncodingSpec> {};

TEST_F(UccDiscoveryPluginTest, LoadUnloadPlugin) {
  auto& plugin_manager = Hyrise::get().plugin_manager;
  plugin_manager.load_plugin(build_dylib_path("libhyriseUccDiscoveryPlugin"));
  plugin_manager.unload_plugin("hyriseUccDiscoveryPlugin");
}

TEST_F(UccDiscoveryPluginTest, CorrectCandidatesGeneratedForJoin) {
  for (const auto join_mode : {JoinMode::Inner, JoinMode::Semi}) {
    // clang-format off
    const auto lqp =
    JoinNode::make(join_mode, equals_(_join_columnA, _join_columnB),
      PredicateNode::make(equals_(_predicate_column_A, "unique"), _table_node_A),
      PredicateNode::make(equals_(_predicate_column_B, "not"), _table_node_B));
    // clang-format on

    Hyrise::get().default_lqp_cache->set("TestLQP", lqp);

    const auto& ucc_candidates = _identify_ucc_candidates();

    // For semi joins, the plugin should only look at the right join input.
    const auto expected_candidate_count = join_mode == JoinMode::Inner ? 4 : 2;
    EXPECT_EQ(ucc_candidates.size(), expected_candidate_count) << "for JoinMode::" << join_mode;

    const auto join_column_A_candidate = UccCandidate{_table_name_A, _join_columnA->original_column_id};
    const auto predicate_column_A_candidate = UccCandidate{_table_name_A, _predicate_column_A->original_column_id};
    const auto join_column_B_candidate = UccCandidate{_table_name_B, _join_columnB->original_column_id};
    const auto predicate_column_B_candidate = UccCandidate{_table_name_B, _predicate_column_B->original_column_id};

    EXPECT_TRUE(ucc_candidates.contains(join_column_B_candidate)) << "for JoinMode::" << join_mode;
    EXPECT_TRUE(ucc_candidates.contains(predicate_column_B_candidate)) << "for JoinMode::" << join_mode;

    if (join_mode != JoinMode::Semi) {
      EXPECT_TRUE(ucc_candidates.contains(join_column_A_candidate)) << "for JoinMode::" << join_mode;
      EXPECT_TRUE(ucc_candidates.contains(predicate_column_A_candidate)) << "for JoinMode::" << join_mode;
    }
  }
}

TEST_F(UccDiscoveryPluginTest, NoCandidatesGeneratedForUnsupportedJoinModes) {
  const auto join_modes = {JoinMode::Left,           JoinMode::Right,           JoinMode::FullOuter,
                           JoinMode::AntiNullAsTrue, JoinMode::AntiNullAsFalse, JoinMode::Cross};
  for (const auto join_mode : join_modes) {
    const auto lqp = join_mode == JoinMode::Cross ? JoinNode::make(JoinMode::Cross)
                                                  : JoinNode::make(join_mode, equals_(_join_columnA, _join_columnB));
    lqp->set_left_input(PredicateNode::make(equals_(_predicate_column_A, "unique"), _table_node_A));
    lqp->set_right_input(PredicateNode::make(equals_(_predicate_column_B, "not"), _table_node_B));

    Hyrise::get().default_lqp_cache->set("TestLQP", lqp);

    const auto& ucc_candidates = _identify_ucc_candidates();
    EXPECT_TRUE(ucc_candidates.empty()) << "for JoinMode::" << join_mode;
  }
}

TEST_F(UccDiscoveryPluginTest, CorrectCandidatesGeneratedForAggregate) {
  const auto table_node_A = StoredTableNode::make(_table_name_A);

  // clang-format off
  const auto lqp =
  AggregateNode::make(expression_vector(_join_columnA), expression_vector(),
    PredicateNode::make(equals_(_predicate_column_A, "unique"), table_node_A));
  // clang-format on

  Hyrise::get().default_lqp_cache->set("TestLQP", lqp);

  auto ucc_candidates = _identify_ucc_candidates();

  const auto join_column_A_candidate = UccCandidate{_table_name_A, _join_columnA->original_column_id};
  const auto predicate_column_A_candidate = UccCandidate{_table_name_A, _predicate_column_A->original_column_id};

  EXPECT_EQ(ucc_candidates.size(), 1);
  EXPECT_TRUE(ucc_candidates.contains(join_column_A_candidate));
  EXPECT_TRUE(!ucc_candidates.contains(predicate_column_A_candidate));
}

TEST_F(UccDiscoveryPluginTest, NoCandidatesGeneratedForUnionNode) {
  const auto table_node_A = StoredTableNode::make(_table_name_A);
  const auto table_node_B = StoredTableNode::make(_table_name_B);

  // clang-format off
  const auto lqp =
  UnionNode::make(SetOperationMode::All,
    PredicateNode::make(equals_(_predicate_column_A, "unique"), table_node_A),
    PredicateNode::make(equals_(_predicate_column_B, "not"), table_node_B));
  // clang-format on

  Hyrise::get().default_lqp_cache->set("TestLQP", lqp);

  auto ucc_candidates = _identify_ucc_candidates();

  EXPECT_TRUE(ucc_candidates.empty());
}

TEST_P(UccDiscoveryPluginMultiEncodingTest, ValidateCandidates) {
  _encode_table(_table_A, GetParam());
  _encode_table(_table_B, GetParam());

  // Insert all columns as candidates
  auto ucc_candidates = UccCandidates{{"uniquenessTestTableA", ColumnID{0}},
                                      {"uniquenessTestTableA", ColumnID{1}},
                                      {"uniquenessTestTableA", ColumnID{2}},
                                      {"uniquenessTestTableB", ColumnID{0}},
                                      {"uniquenessTestTableB", ColumnID{1}}};

  _validate_ucc_candidates(ucc_candidates);

  // Collect constraints known for the tables
  const auto& constraints_A = _table_A->soft_key_constraints();
  const auto& constraints_B = _table_B->soft_key_constraints();

  EXPECT_EQ(constraints_A.size(), 2);
  EXPECT_EQ(constraints_B.size(), 1);

  EXPECT_TRUE(constraints_A.contains({{ColumnID{0}}, KeyConstraintType::UNIQUE}));
  EXPECT_TRUE(constraints_A.contains({{ColumnID{2}}, KeyConstraintType::UNIQUE}));
  EXPECT_FALSE(constraints_A.contains({{ColumnID{1}}, KeyConstraintType::UNIQUE}));

  EXPECT_TRUE(constraints_B.contains({{ColumnID{0}}, KeyConstraintType::UNIQUE}));
  EXPECT_FALSE(constraints_B.contains({{ColumnID{1}}, KeyConstraintType::UNIQUE}));
}

TEST_P(UccDiscoveryPluginMultiEncodingTest, PluginFullRun) {
  // clang-format off
  const auto lqp =
  JoinNode::make(JoinMode::Inner, equals_(_join_columnA, _join_columnB),
    PredicateNode::make(equals_(_predicate_column_A, "unique"), _table_node_A),
    PredicateNode::make(equals_(_predicate_column_B, "not"), _table_node_B));
  // clang-format on
  Hyrise::get().default_lqp_cache->set("TestLQP", lqp);

  _encode_table(_table_A, GetParam());
  _encode_table(_table_B, GetParam());

  // Run UCC discovery: we expect both join columns and the predicate on table A to be identified as unique.
  _discover_uccs();

  // Collect constraints known for the tables.
  const auto& constraints_A = _table_A->soft_key_constraints();
  const auto& constraints_B = _table_B->soft_key_constraints();

  EXPECT_EQ(constraints_A.size(), 2);
  EXPECT_EQ(constraints_B.size(), 1);

  EXPECT_TRUE(constraints_A.contains({{_join_columnA->original_column_id}, KeyConstraintType::UNIQUE}));
  EXPECT_TRUE(constraints_A.contains({{_predicate_column_A->original_column_id}, KeyConstraintType::UNIQUE}));

  EXPECT_TRUE(constraints_B.contains({{_join_columnB->original_column_id}, KeyConstraintType::UNIQUE}));
  EXPECT_FALSE(constraints_B.contains({{_predicate_column_B->original_column_id}, KeyConstraintType::UNIQUE}));

  // Ensure we clear the plan caches.
  EXPECT_EQ(Hyrise::get().default_lqp_cache->size(), 0);
  EXPECT_EQ(Hyrise::get().default_pqp_cache->size(), 0);
}

INSTANTIATE_TEST_SUITE_P(UccDiscoveryPluginMultiEncodingTestInstances, UccDiscoveryPluginMultiEncodingTest,
                         ::testing::ValuesIn(all_segment_encoding_specs), all_segment_encoding_specs_formatter);

}  // namespace hyrise
