#include <functional>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "nlohmann/json.hpp"

#include "../../plugins/ucc_discovery_plugin.hpp"
#include "all_type_variant.hpp"
#include "base_test.hpp"
#include "concurrency/transaction_context.hpp"
#include "concurrency/transaction_manager.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/pqp_column_expression.hpp"
#include "lib/storage/encoding_test.hpp"
#include "lib/utils/plugin_test_utils.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "operators/delete.hpp"
#include "operators/get_table.hpp"
#include "operators/insert.hpp"
#include "operators/projection.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/update.hpp"
#include "operators/validate.hpp"
#include "storage/constraints/constraint_utils.hpp"
#include "storage/constraints/table_key_constraint.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "storage/table_column_definition.hpp"
#include "types.hpp"
#include "utils/load_table.hpp"
#include "utils/plugin_manager.hpp"
#include "utils/template_type.hpp"

namespace {
using namespace hyrise;  // NOLINT(build/namespaces)

void encode_table(const std::shared_ptr<Table>& table, const SegmentEncodingSpec& encoding_spec) {
  auto chunk_encoding_spec = ChunkEncodingSpec{table->column_count(), SegmentEncodingSpec{EncodingType::Unencoded}};

  for (auto column_id = ColumnID{0}; column_id < table->column_count(); ++column_id) {
    if (encoding_supports_data_type(encoding_spec.encoding_type, table->column_data_type(column_id))) {
      chunk_encoding_spec[column_id] = encoding_spec;
    }
  }
  ChunkEncoder::encode_all_chunks(table, chunk_encoding_spec);
}

std::shared_ptr<TransactionContext> insert_row(const std::string& table_name,
                                               const TableColumnDefinitions& column_definitions,
                                               const std::vector<AllTypeVariant>& values,
                                               const bool commit_transaction = true) {
  auto transaction_context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);

  const auto table = Table::create_dummy_table(column_definitions);
  table->append(values);

  const auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  const auto insert_op = std::make_shared<Insert>(table_name, table_wrapper);
  insert_op->set_transaction_context(transaction_context);
  insert_op->execute();

  if (commit_transaction) {
    transaction_context->commit();
  }

  return transaction_context;
}

std::shared_ptr<TransactionContext> delete_row(const std::shared_ptr<Table> table, const size_t row_index,
                                               const bool commit_transaction = true) {
  auto transaction_context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
  const auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  const auto table_scan =
      create_table_scan(table_wrapper, ColumnID{0}, PredicateCondition::Equals, table->get_row(row_index).at(0));
  table_scan->execute();

  const auto delete_op = std::make_shared<Delete>(table_scan);
  delete_op->set_transaction_context(transaction_context);
  delete_op->execute();

  if (commit_transaction) {
    transaction_context->commit();
  }

  return transaction_context;
}

}  // namespace

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
  EXPECT_NO_THROW(plugin_manager.load_plugin(build_dylib_path("libhyriseUccDiscoveryPlugin")));
  EXPECT_NO_THROW(plugin_manager.unload_plugin("hyriseUccDiscoveryPlugin"));
}

TEST_F(UccDiscoveryPluginTest, DescriptionAndProvidedFunction) {
  auto plugin = UccDiscoveryPlugin{};
  EXPECT_EQ(plugin.description(), "Unary Unique Column Combination Discovery Plugin");
  const auto& provided_functions = plugin.provided_user_executable_functions();
  ASSERT_EQ(provided_functions.size(), 1);
  EXPECT_EQ(provided_functions.front().first, "DiscoverUCCs");
}

TEST_F(UccDiscoveryPluginTest, UserCallableFunction) {
  auto& plugin_manager = Hyrise::get().plugin_manager;
  EXPECT_NO_THROW(plugin_manager.load_plugin(build_dylib_path("libhyriseUccDiscoveryPlugin")));
  EXPECT_NO_THROW(plugin_manager.exec_user_function("hyriseUccDiscoveryPlugin", "DiscoverUCCs"));
  EXPECT_NO_THROW(plugin_manager.unload_plugin("hyriseUccDiscoveryPlugin"));
}

TEST_F(UccDiscoveryPluginTest, BenchmarkHooks) {
  auto& plugin_manager = Hyrise::get().plugin_manager;

  EXPECT_NO_THROW(plugin_manager.load_plugin(build_dylib_path("libhyriseUccDiscoveryPlugin")));
  // We only check if the plugin has a pre-benchmark hook. Actually executing it requires benchmark items and tables.
  EXPECT_TRUE(plugin_manager.has_pre_benchmark_hook("hyriseUccDiscoveryPlugin"));
  EXPECT_FALSE(plugin_manager.has_post_benchmark_hook("hyriseUccDiscoveryPlugin"));
  auto report = nlohmann::json{};
  EXPECT_THROW(plugin_manager.exec_post_benchmark_hook("hyriseUccDiscoveryPlugin", report), std::logic_error);
  EXPECT_NO_THROW(plugin_manager.unload_plugin("hyriseUccDiscoveryPlugin"));
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
    SCOPED_TRACE("for JoinMode::" + std::string{magic_enum::enum_name(join_mode)});

    // For semi joins, the plugin should only look at the right join input.
    const auto expected_candidate_count = join_mode == JoinMode::Inner ? 4 : 2;
    EXPECT_EQ(ucc_candidates.size(), expected_candidate_count);

    const auto join_column_A_candidate = UccCandidate{_table_name_A, _join_columnA->original_column_id};
    const auto predicate_column_A_candidate = UccCandidate{_table_name_A, _predicate_column_A->original_column_id};
    const auto join_column_B_candidate = UccCandidate{_table_name_B, _join_columnB->original_column_id};
    const auto predicate_column_B_candidate = UccCandidate{_table_name_B, _predicate_column_B->original_column_id};

    EXPECT_TRUE(ucc_candidates.contains(join_column_B_candidate));
    EXPECT_TRUE(ucc_candidates.contains(predicate_column_B_candidate));

    if (join_mode != JoinMode::Semi) {
      EXPECT_TRUE(ucc_candidates.contains(join_column_A_candidate));
      EXPECT_TRUE(ucc_candidates.contains(predicate_column_A_candidate));
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

TEST_F(UccDiscoveryPluginTest, NoCandidatesGeneratedForComplexPredicates) {
  const auto join_modes = {JoinMode::Inner, JoinMode::Semi};
  for (const auto join_mode : join_modes) {
    // clang-format off
    const auto lqp =
    JoinNode::make(join_mode, equals_(add_(_join_columnA, 1), add_(_join_columnB, 1)),
      PredicateNode::make(equals_(_predicate_column_A, "unique"), _table_node_A),
      PredicateNode::make(equals_(_predicate_column_B, "not"), _table_node_B));
    // clang-format on

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
  encode_table(_table_A, GetParam());
  encode_table(_table_B, GetParam());

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

  EXPECT_EQ(constraints_A.size(), 3);
  EXPECT_EQ(constraints_B.size(), 2);

  const auto& table_key_constraint_A_0 =
      constraints_A.find(TableKeyConstraint{{ColumnID{0}}, KeyConstraintType::UNIQUE});
  ASSERT_NE(table_key_constraint_A_0, constraints_A.end());
  EXPECT_EQ(table_key_constraint_A_0->last_validation_result(), ValidationResultType::VALID);

  const auto& table_key_constraint_A_2 =
      constraints_A.find(TableKeyConstraint{{ColumnID{2}}, KeyConstraintType::UNIQUE});
  EXPECT_NE(table_key_constraint_A_2, constraints_A.end());
  EXPECT_EQ(table_key_constraint_A_2->last_validation_result(), ValidationResultType::VALID);

  const auto& table_key_constraint_A_1 =
      constraints_A.find(TableKeyConstraint{{ColumnID{1}}, KeyConstraintType::UNIQUE});
  ASSERT_NE(table_key_constraint_A_1, constraints_A.end());
  EXPECT_EQ(table_key_constraint_A_1->last_validation_result(), ValidationResultType::INVALID);

  const auto& table_key_constraint_B_0 =
      constraints_B.find(TableKeyConstraint{{ColumnID{0}}, KeyConstraintType::UNIQUE});
  ASSERT_NE(table_key_constraint_B_0, constraints_B.end());
  EXPECT_EQ(table_key_constraint_B_0->last_validation_result(), ValidationResultType::VALID);

  const auto& table_key_constraint_B_1 =
      constraints_B.find(TableKeyConstraint{{ColumnID{1}}, KeyConstraintType::UNIQUE});
  ASSERT_NE(table_key_constraint_B_1, constraints_B.end());
  EXPECT_EQ(table_key_constraint_B_1->last_validation_result(), ValidationResultType::INVALID);
}

TEST_P(UccDiscoveryPluginMultiEncodingTest, InvalidateCandidatesAfterDuplicateInsertion) {
  encode_table(_table_A, GetParam());
  const auto& constraints_A = _table_A->soft_key_constraints();
  EXPECT_EQ(constraints_A.size(), 0);  // No constraints known for the table yet.

  // Delete row of _table_A that had a duplicate value regarding column 1 such that column 1 is unique afterwards.
  delete_row(_table_A, 3);

  // We are only interested in column 1, since it was not unique before the deletion but should be now.
  const auto ucc_candidates = UccCandidates{{"uniquenessTestTableA", ColumnID{1}}};

  _validate_ucc_candidates(ucc_candidates);

  EXPECT_EQ(constraints_A.size(), 1);
  const auto& constraint_A_1 = constraints_A.find(TableKeyConstraint{{ColumnID{1}}, KeyConstraintType::UNIQUE});
  ASSERT_NE(constraint_A_1, constraints_A.end());
  EXPECT_EQ(constraint_A_1->last_validation_result(), ValidationResultType::VALID);

  // Insert single row into table A that adds a duplicate value in column 1. Do not commit to first test that UCC
  // stays valid.
  auto transaction_context = insert_row(_table_name_A, _table_A->column_definitions(), {2, 3, "duplicate"}, false);

  // Re-validate UCCs.
  _validate_ucc_candidates(ucc_candidates);

  EXPECT_EQ(constraints_A.size(), 1);
  const auto& constraint_A_1_after_insert =
      constraints_A.find(TableKeyConstraint{{ColumnID{1}}, KeyConstraintType::UNIQUE});
  ASSERT_NE(constraint_A_1_after_insert, constraints_A.end());
  EXPECT_EQ(constraint_A_1_after_insert->last_validation_result(), ValidationResultType::VALID);

  // Commit transaction and re-validate UCCs.
  transaction_context->commit();

  // Validate for a second time. This time, the UCC should be invalid.
  _validate_ucc_candidates(ucc_candidates);

  EXPECT_EQ(constraints_A.size(), 1);
  const auto& constraint_A_1_after_commit =
      constraints_A.find(TableKeyConstraint{{ColumnID{1}}, KeyConstraintType::UNIQUE});
  ASSERT_NE(constraint_A_1_after_commit, constraints_A.end());
  EXPECT_EQ(constraint_A_1_after_commit->last_validation_result(), ValidationResultType::INVALID);
}

TEST_P(UccDiscoveryPluginMultiEncodingTest, InvalidateCandidatesAfterUpdate) {
  encode_table(_table_A, GetParam());
  const auto& constraints_A = _table_A->soft_key_constraints();
  EXPECT_EQ(constraints_A.size(), 0);  // No constraints known for the table yet.

  // Delete row of _table_A that had a duplicate value in column 1. Column 1 is unique afterwards.
  delete_row(_table_A, 3);

  // We are only interested in column 1, since it was not unique before the deletion but should be now.
  const auto ucc_candidates = UccCandidates{{"uniquenessTestTableA", ColumnID{1}}};

  _validate_ucc_candidates(ucc_candidates);

  EXPECT_EQ(constraints_A.size(), 1);
  const auto& constraint_A_1 = constraints_A.find(TableKeyConstraint{{ColumnID{1}}, KeyConstraintType::UNIQUE});
  ASSERT_NE(constraint_A_1, constraints_A.end());
  EXPECT_EQ(constraint_A_1->last_validation_result(), ValidationResultType::VALID);

  // Delete and then insert a row. After the delete was committed, the UCC should still be valid but after the insert
  // it should not.
  delete_row(_table_A, 2, true);
  auto transaction_context = insert_row(_table_name_A, _table_A->column_definitions(), {2, 3, "duplicate"}, false);

  // Re-validate UCCs. The UCC should remain valid since only a delete was committed.
  _validate_ucc_candidates(ucc_candidates);

  EXPECT_EQ(constraints_A.size(), 1);
  const auto& constraint_A_1_after_delete =
      constraints_A.find(TableKeyConstraint{{ColumnID{1}}, KeyConstraintType::UNIQUE});
  ASSERT_NE(constraint_A_1_after_delete, constraints_A.end());
  EXPECT_EQ(constraint_A_1_after_delete->last_validation_result(), ValidationResultType::VALID);

  // Commit transaction and re-validate UCCs.
  transaction_context->commit();

  // Validate for a second time. This time the UCC should be invalid.
  _validate_ucc_candidates(ucc_candidates);

  EXPECT_EQ(constraints_A.size(), 1);
  const auto& constraint_A_1_after_insert =
      constraints_A.find(TableKeyConstraint{{ColumnID{1}}, KeyConstraintType::UNIQUE});
  ASSERT_NE(constraint_A_1_after_insert, constraints_A.end());
  EXPECT_EQ(constraint_A_1_after_insert->last_validation_result(), ValidationResultType::INVALID);
}

TEST_P(UccDiscoveryPluginMultiEncodingTest, RevalidationUpdatesValidationTimestamp) {
  encode_table(_table_A, GetParam());
  const auto& constraints_A = _table_A->soft_key_constraints();
  EXPECT_EQ(constraints_A.size(), 0);  // No constraints known for the table yet.

  // Add schema-given UCC to table A.
  _table_A->add_soft_constraint(TableKeyConstraint{{ColumnID{0}}, KeyConstraintType::UNIQUE});
  delete_row(_table_A, 3);

  const auto ucc_candidates =
      UccCandidates{{"uniquenessTestTableA", ColumnID{0}}, {"uniquenessTestTableA", ColumnID{1}}};
  _validate_ucc_candidates(ucc_candidates);

  // Perform a transaction that does not affect table A but increments the global CommitID.
  delete_row(_table_B, 0);

  EXPECT_EQ(constraints_A.size(), 2);  // One permanent UCC and one non-permanent UCC.

  const auto search_constraint_A = TableKeyConstraint{{ColumnID{0}}, KeyConstraintType::UNIQUE};
  const auto column_0_constraint = constraints_A.find(search_constraint_A);
  ASSERT_NE(column_0_constraint, constraints_A.end());
  // The permanent UCC should remain permanent.
  EXPECT_EQ(column_0_constraint->last_validated_on(), MAX_COMMIT_ID);

  const auto search_constraint_B = TableKeyConstraint{{ColumnID{1}}, KeyConstraintType::UNIQUE};
  const auto column_1_constraint = constraints_A.find(search_constraint_B);
  ASSERT_NE(column_1_constraint, constraints_A.end());
  // The non-permanent UCC should have been validated.
  const auto first_validation_timestamp = column_1_constraint->last_validated_on();
  EXPECT_NE(first_validation_timestamp, MAX_COMMIT_ID);

  // The following validation should change the timestamp as the validity of the UCC is visible by examining the
  // MVCC data of the tables chunks (see `key_constraint_is_confidently_valid`).
  _validate_ucc_candidates(ucc_candidates);
  EXPECT_EQ(column_1_constraint->last_validated_on(), first_validation_timestamp);

  const auto& constraint_A_0_after_revalidation =
      constraints_A.find(TableKeyConstraint{{ColumnID{0}}, KeyConstraintType::UNIQUE});
  EXPECT_EQ(constraint_A_0_after_revalidation->last_validation_result(), ValidationResultType::VALID);

  const auto& constraint_A_1_after_revalidation =
      constraints_A.find(TableKeyConstraint{{ColumnID{1}}, KeyConstraintType::UNIQUE});
  EXPECT_EQ(constraint_A_1_after_revalidation->last_validation_result(), ValidationResultType::VALID);
}

TEST_P(UccDiscoveryPluginMultiEncodingTest, PluginFullRun) {
  // clang-format off
  const auto lqp =
  JoinNode::make(JoinMode::Inner, equals_(_join_columnA, _join_columnB),
    PredicateNode::make(equals_(_predicate_column_A, "unique"), _table_node_A),
    PredicateNode::make(equals_(_predicate_column_B, "not"), _table_node_B));
  // clang-format on
  Hyrise::get().default_lqp_cache->set("TestLQP", lqp);

  encode_table(_table_A, GetParam());
  encode_table(_table_B, GetParam());

  // Run UCC discovery: we expect both join columns and the predicate on table A to be identified as unique.
  _discover_uccs();

  // Collect constraints known for the tables.
  const auto& constraints_A = _table_A->soft_key_constraints();
  const auto& constraints_B = _table_B->soft_key_constraints();

  EXPECT_EQ(constraints_A.size(), 2);
  EXPECT_EQ(constraints_B.size(), 2);

  const auto join_column_A_constraint =
      constraints_A.find(TableKeyConstraint{{_join_columnA->original_column_id}, KeyConstraintType::UNIQUE});
  ASSERT_NE(join_column_A_constraint, constraints_A.end());
  EXPECT_EQ(join_column_A_constraint->last_validation_result(), ValidationResultType::VALID);

  const auto predicate_column_A_constraint =
      constraints_A.find(TableKeyConstraint{{_predicate_column_A->original_column_id}, KeyConstraintType::UNIQUE});
  ASSERT_NE(predicate_column_A_constraint, constraints_A.end());
  EXPECT_EQ(predicate_column_A_constraint->last_validation_result(), ValidationResultType::VALID);

  const auto join_column_B_constraint =
      constraints_B.find(TableKeyConstraint{{_join_columnB->original_column_id}, KeyConstraintType::UNIQUE});
  ASSERT_NE(join_column_B_constraint, constraints_B.end());
  EXPECT_EQ(join_column_B_constraint->last_validation_result(), ValidationResultType::VALID);

  const auto predicate_column_B_constraint =
      constraints_B.find(TableKeyConstraint{{_predicate_column_B->original_column_id}, KeyConstraintType::UNIQUE});
  ASSERT_NE(predicate_column_B_constraint, constraints_B.end());
  EXPECT_EQ(predicate_column_B_constraint->last_validation_result(), ValidationResultType::INVALID);

  // Ensure we clear the plan caches.
  EXPECT_EQ(Hyrise::get().default_lqp_cache->size(), 0);
  EXPECT_EQ(Hyrise::get().default_pqp_cache->size(), 0);
}

TEST_F(UccDiscoveryPluginMultiEncodingTest, PluginIntegrationTestWithInvalidation) {
  Hyrise::get().default_pqp_cache = std::make_shared<SQLPhysicalPlanCache>();
  Hyrise::get().default_lqp_cache = std::make_shared<SQLLogicalPlanCache>();

  const auto exec_groupby_statement = [&]() {
    auto pipeline = SQLPipelineBuilder{"SELECT b,c FROM " + _table_name_A + " GROUP BY b,c;"}.create_pipeline();
    const auto& [pipeline_status, _] = pipeline.get_result_table();
    EXPECT_EQ(pipeline_status, SQLPipelineStatus::Success);
  };

  const auto exec_insert_statement = [&]() {
    auto transaction_context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
    auto pipeline = SQLPipelineBuilder{"INSERT INTO " + _table_name_A + " (A_a, b, c) VALUES (14, 12, 'duplicate_b');"}
                        .with_transaction_context(transaction_context)
                        .create_pipeline();
    const auto& [pipeline_status, _] = pipeline.get_result_table();
    EXPECT_EQ(pipeline_status, SQLPipelineStatus::Success);
    transaction_context->commit();
  };

  // Perform a single query to populate the LQPCache for `identify_ucc_candidates`.
  exec_groupby_statement();

  EXPECT_EQ(Hyrise::get().default_lqp_cache->size(), 1);

  // Discover UCCs used in previous query.
  auto& plugin_manager = Hyrise::get().plugin_manager;
  plugin_manager.load_plugin(build_dylib_path("libhyriseUccDiscoveryPlugin"));
  plugin_manager.exec_user_function("hyriseUccDiscoveryPlugin", "DiscoverUCCs");

  // Check that key constraint was added to the table.
  const auto constraint_A_c = TableKeyConstraint{{ColumnID{2}}, KeyConstraintType::UNIQUE};

  EXPECT_EQ(_table_A->soft_key_constraints().size(), 2);

  const auto constraint_A_c_after_discovery = _table_A->soft_key_constraints().find(constraint_A_c);
  EXPECT_NE(constraint_A_c_after_discovery, _table_A->soft_key_constraints().end());
  EXPECT_EQ(constraint_A_c_after_discovery->last_validation_result(), ValidationResultType::VALID);

  exec_groupby_statement();
  exec_insert_statement();
  // The second insert leads to duplicate values in all columns. None of the constraints should be valid anymore.
  exec_insert_statement();

  // Check that validity of constraint is not guaranteed anymore.
  const auto constraint_A = _table_A->soft_key_constraints().find(constraint_A_c);
  ASSERT_NE(constraint_A, _table_A->soft_key_constraints().end());
  EXPECT_FALSE(key_constraint_is_confidently_valid(_table_A, *constraint_A));

  // Flush and repopulate the cache.
  Hyrise::get().default_pqp_cache->clear();
  Hyrise::get().default_lqp_cache->clear();
  exec_groupby_statement();

  // Check that key constraint is properly invalidated.
  plugin_manager.exec_user_function("hyriseUccDiscoveryPlugin", "DiscoverUCCs");

  const auto constraint_A_c_after_invalidation = _table_A->soft_key_constraints().find(constraint_A_c);
  ASSERT_NE(constraint_A_c_after_invalidation, _table_A->soft_key_constraints().end());
  EXPECT_EQ(constraint_A_c_after_invalidation->last_validation_result(), ValidationResultType::INVALID);

  // Remove duplicates.
  {
    auto pipeline = SQLPipelineBuilder{"DELETE FROM " + _table_name_A + " WHERE b > 10;"}.create_pipeline();
    pipeline.get_result_table();
  }

  // Repopulate the cache.
  exec_groupby_statement();
  plugin_manager.exec_user_function("hyriseUccDiscoveryPlugin", "DiscoverUCCs");

  // Check that the key constraint is valid again.
  const auto constraint_A_c_after_revalidation = _table_A->soft_key_constraints().find(constraint_A_c);
  ASSERT_NE(constraint_A_c_after_revalidation, _table_A->soft_key_constraints().end());
  EXPECT_EQ(constraint_A_c_after_revalidation->last_validation_result(), ValidationResultType::VALID);
}

INSTANTIATE_TEST_SUITE_P(UccDiscoveryPluginMultiEncodingTestInstances, UccDiscoveryPluginMultiEncodingTest,
                         ::testing::ValuesIn(all_segment_encoding_specs), all_segment_encoding_specs_formatter);

}  // namespace hyrise
