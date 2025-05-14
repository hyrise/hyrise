#include <gtest/gtest.h>

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

  std::shared_ptr<TransactionContext> _insert_row(const std::string& table_name,
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

  std::shared_ptr<TransactionContext> _delete_row(const std::shared_ptr<Table> table, const size_t row_index,
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
  const auto constraints_A = _table_A->soft_key_constraints();
  const auto constraints_B = _table_B->soft_key_constraints();

  EXPECT_EQ(constraints_A.size(), 3);
  EXPECT_EQ(constraints_B.size(), 2);

  EXPECT_TRUE(constraints_A.contains(TableKeyConstraint{{ColumnID{0}}, KeyConstraintType::UNIQUE}));
  EXPECT_TRUE(constraints_A.find(TableKeyConstraint{{ColumnID{0}}, KeyConstraintType::UNIQUE})->is_valid());

  EXPECT_TRUE(constraints_A.contains(TableKeyConstraint{{ColumnID{2}}, KeyConstraintType::UNIQUE}));
  EXPECT_TRUE(constraints_A.find(TableKeyConstraint{{ColumnID{2}}, KeyConstraintType::UNIQUE})->is_valid());

  EXPECT_TRUE(constraints_A.contains(TableKeyConstraint{{ColumnID{1}}, KeyConstraintType::UNIQUE}));
  EXPECT_FALSE(constraints_A.find(TableKeyConstraint{{ColumnID{1}}, KeyConstraintType::UNIQUE})->is_valid());

  EXPECT_TRUE(constraints_B.contains(TableKeyConstraint{{ColumnID{0}}, KeyConstraintType::UNIQUE}));
  EXPECT_TRUE(constraints_B.find(TableKeyConstraint{{ColumnID{0}}, KeyConstraintType::UNIQUE})->is_valid());

  EXPECT_TRUE(constraints_B.contains(TableKeyConstraint{{ColumnID{1}}, KeyConstraintType::UNIQUE}));
  EXPECT_FALSE(constraints_B.find(TableKeyConstraint{{ColumnID{1}}, KeyConstraintType::UNIQUE})->is_valid());
}

TEST_P(UccDiscoveryPluginMultiEncodingTest, ValidateCandidatesAfterDeletion) {
  _encode_table(_table_A, GetParam());

  // Delete row of _table_A that had a duplicate value regarding column 1 such that column 1 is unique afterwards.
  _delete_row(_table_A, 3);

  // We are only interested in column 1, since it was not unique before the deletion but should be now.
  const auto ucc_candidates = UccCandidates{{"uniquenessTestTableA", ColumnID{1}}};

  _validate_ucc_candidates(ucc_candidates);

  // Collect constraints known for the tables.
  const auto& constraints_A = _table_A->soft_key_constraints();
  EXPECT_EQ(constraints_A.size(), 1);
  EXPECT_TRUE(constraints_A.contains({{ColumnID{1}}, KeyConstraintType::UNIQUE}));
  EXPECT_TRUE(constraints_A.find(TableKeyConstraint{{ColumnID{1}}, KeyConstraintType::UNIQUE})->is_valid());

  // Delete another row. This should have no effect on the previously discovered UCC.
  auto transaction_context = _delete_row(_table_A, 2, false);

  const auto& constraints_A_2 = _table_A->soft_key_constraints();
  EXPECT_EQ(constraints_A_2.size(), 1);
  EXPECT_TRUE(constraints_A_2.contains({{ColumnID{1}}, KeyConstraintType::UNIQUE}));
  EXPECT_TRUE(constraints_A_2.find(TableKeyConstraint{{ColumnID{1}}, KeyConstraintType::UNIQUE})->is_valid());

  transaction_context->commit();

  const auto& constraints_A_3 = _table_A->soft_key_constraints();
  EXPECT_EQ(constraints_A_3.size(), 1);
  EXPECT_TRUE(constraints_A_3.contains({{ColumnID{1}}, KeyConstraintType::UNIQUE}));
  EXPECT_TRUE(constraints_A_3.find(TableKeyConstraint{{ColumnID{1}}, KeyConstraintType::UNIQUE})->is_valid());
}

TEST_P(UccDiscoveryPluginMultiEncodingTest, ValidateCandidatesAfterInsertion) {
  _encode_table(_table_A, GetParam());

  // Delete row of _table_A that had a duplicate value regarding column 1 such that column 1 is unique afterwards.
  _delete_row(_table_A, 3);

  // We are only interested in column 1, since it was not unique before the deletion but should be now.
  const auto ucc_candidates = UccCandidates{{"uniquenessTestTableA", ColumnID{1}}};

  _validate_ucc_candidates(ucc_candidates);

  // Collect constraints known for the tables.
  const auto& constraints_A = _table_A->soft_key_constraints();
  EXPECT_EQ(constraints_A.size(), 1);
  EXPECT_TRUE(constraints_A.contains({{ColumnID{1}}, KeyConstraintType::UNIQUE}));
  EXPECT_TRUE(constraints_A.find(TableKeyConstraint{{ColumnID{1}}, KeyConstraintType::UNIQUE})->is_valid());

  // Insert a row that does not affect UCC.
  auto transaction_context =
      _insert_row(_table_name_A,
                  TableColumnDefinitions{
                      {"A_a", DataType::Int, false}, {"b", DataType::Int, false}, {"c", DataType::String, false}},
                  {2, 4, "fine"}, false);

  // Re-validate UCCs.
  _validate_ucc_candidates(ucc_candidates);
  const auto& constraints_A_2 = _table_A->soft_key_constraints();

  EXPECT_EQ(constraints_A_2.size(), 1);
  EXPECT_TRUE(constraints_A_2.contains({{ColumnID{1}}, KeyConstraintType::UNIQUE}));
  EXPECT_TRUE(constraints_A_2.find(TableKeyConstraint{{ColumnID{1}}, KeyConstraintType::UNIQUE})->is_valid());

  // Commit transaction and re-validate UCCs.
  transaction_context->commit();

  // Validate for a second time after the commit.
  _validate_ucc_candidates(ucc_candidates);
  const auto& constraints_A_3 = _table_A->soft_key_constraints();

  EXPECT_EQ(constraints_A_3.size(), 1);
  EXPECT_TRUE(constraints_A_3.contains({{ColumnID{1}}, KeyConstraintType::UNIQUE}));
  EXPECT_TRUE(constraints_A_3.find(TableKeyConstraint{{ColumnID{1}}, KeyConstraintType::UNIQUE})->is_valid());
}

TEST_P(UccDiscoveryPluginMultiEncodingTest, InvalidateCandidatesAfterDuplicateInsertion) {
  _encode_table(_table_A, GetParam());

  // Delete row of _table_A that had a duplicate value regarding column 1 such that column 1 is unique afterwards.
  _delete_row(_table_A, 3);

  // We are only interested in column 1, since it was not unique before the deletion but should be now.
  const auto ucc_candidates = UccCandidates{{"uniquenessTestTableA", ColumnID{1}}};

  _validate_ucc_candidates(ucc_candidates);

  // Collect constraints known for the tables.
  const auto& constraints_A = _table_A->soft_key_constraints();
  EXPECT_EQ(constraints_A.size(), 1);
  EXPECT_TRUE(constraints_A.contains({{ColumnID{1}}, KeyConstraintType::UNIQUE}));

  // Insert single row into table A that has a duplicate value regarding column 1. Do not commit to first test that UCC
  // stays valid.
  auto transaction_context =
      _insert_row(_table_name_A,
                  TableColumnDefinitions{
                      {"A_a", DataType::Int, false}, {"b", DataType::Int, false}, {"c", DataType::String, false}},
                  {2, 3, "dublicate"}, false);

  // Re-validate UCCs.
  _validate_ucc_candidates(ucc_candidates);
  const auto& constraints_A_2 = _table_A->soft_key_constraints();

  EXPECT_EQ(constraints_A_2.size(), 1);
  EXPECT_TRUE(constraints_A_2.contains({{ColumnID{1}}, KeyConstraintType::UNIQUE}));
  EXPECT_TRUE(constraints_A_2.find(TableKeyConstraint{{ColumnID{1}}, KeyConstraintType::UNIQUE})->is_valid());

  // Commit transaction and re-validate UCCs.
  transaction_context->commit();

  // Validate for a second time. This time the UCC should be invalid.
  _validate_ucc_candidates(ucc_candidates);
  const auto& constraints_A_3 = _table_A->soft_key_constraints();

  EXPECT_EQ(constraints_A_3.size(), 1);
  EXPECT_TRUE(constraints_A_3.contains({{ColumnID{1}}, KeyConstraintType::UNIQUE}));
  EXPECT_FALSE(constraints_A_3.find(TableKeyConstraint{{ColumnID{1}}, KeyConstraintType::UNIQUE})->is_valid());
}

TEST_P(UccDiscoveryPluginMultiEncodingTest, InvalidateCandidatesAfterUpdate) {
  _encode_table(_table_A, GetParam());

  // Delete row of _table_A that had a duplicate value regarding column 1 such that column 1 is unique afterwards.
  _delete_row(_table_A, 3);

  // We are only interested in column 1, since it was not unique before the deletion but should be now.
  const auto ucc_candidates = UccCandidates{{"uniquenessTestTableA", ColumnID{1}}};

  _validate_ucc_candidates(ucc_candidates);

  // Collect constraints known for the tables.
  const auto& constraints_A = _table_A->soft_key_constraints();
  EXPECT_EQ(constraints_A.size(), 1);
  EXPECT_TRUE(constraints_A.contains({{ColumnID{1}}, KeyConstraintType::UNIQUE}));

  // Delete and then insert a row. After the delete was committed, the UCC should still be valid but after the insert
  // it should not.
  _delete_row(_table_A, 2, true);
  auto transaction_context =
      _insert_row(_table_name_A,
                  TableColumnDefinitions{
                      {"A_a", DataType::Int, false}, {"b", DataType::Int, false}, {"c", DataType::String, false}},
                  {2, 3, "dublicate"}, false);

  // Re-validate UCCs.
  _validate_ucc_candidates(ucc_candidates);
  const auto& constraints_A_2 = _table_A->soft_key_constraints();

  EXPECT_EQ(constraints_A_2.size(), 1);
  EXPECT_TRUE(constraints_A_2.contains({{ColumnID{1}}, KeyConstraintType::UNIQUE}));
  EXPECT_TRUE(constraints_A_2.find(TableKeyConstraint{{ColumnID{1}}, KeyConstraintType::UNIQUE})->is_valid());

  // Commit transaction and re-validate UCCs.
  transaction_context->commit();

  // Validate for a second time. This time the UCC should be invalid.
  _validate_ucc_candidates(ucc_candidates);
  const auto& constraints_A_3 = _table_A->soft_key_constraints();

  EXPECT_EQ(constraints_A_3.size(), 1);
  EXPECT_TRUE(constraints_A_3.contains({{ColumnID{1}}, KeyConstraintType::UNIQUE}));
  EXPECT_FALSE(constraints_A_3.find(TableKeyConstraint{{ColumnID{1}}, KeyConstraintType::UNIQUE})->is_valid());
}

TEST_P(UccDiscoveryPluginMultiEncodingTest, RevalidationUpdatesValidationTimestamp) {
  _encode_table(_table_A, GetParam());

  // Add permanent UCC to table A.
  _table_A->add_soft_constraint(TableKeyConstraint{{ColumnID{0}}, KeyConstraintType::UNIQUE});
  _delete_row(_table_A, 3);

  const auto ucc_candidates =
      UccCandidates{{"uniquenessTestTableA", ColumnID{0}}, {"uniquenessTestTableA", ColumnID{1}}};
  _validate_ucc_candidates(ucc_candidates);

  // Perform a transaction that does not affect table A but increments the global CommitID.
  _delete_row(_table_B, 0);

  // Collect constraints known for the tables.
  const auto& constraints_A = _table_A->soft_key_constraints();

  const auto search_constraint_A = TableKeyConstraint{{ColumnID{0}}, KeyConstraintType::UNIQUE};
  const auto column_0_constraint = constraints_A.find(search_constraint_A);
  EXPECT_NE(column_0_constraint, constraints_A.end());

  const auto search_constraint_B = TableKeyConstraint{{ColumnID{1}}, KeyConstraintType::UNIQUE};
  const auto column_1_constraint = constraints_A.find(search_constraint_B);
  EXPECT_NE(column_1_constraint, constraints_A.end());

  EXPECT_EQ(constraints_A.size(), 2);
  EXPECT_TRUE(constraints_A.contains({{ColumnID{0}}, KeyConstraintType::UNIQUE}));
  EXPECT_TRUE(constraints_A.contains({{ColumnID{1}}, KeyConstraintType::UNIQUE}));

  // The permanent UCC should remain permanent.
  EXPECT_EQ(column_0_constraint->last_validated_on().load(), 0);

  // The non-permanent UCC should have been validated.
  const auto first_validation_timestamp = column_1_constraint->last_validated_on().load();
  EXPECT_NE(first_validation_timestamp, MAX_COMMIT_ID);
  // The following validation should not change the timestamp as the validity of the UCC is visible by examining the
  // MVCC data of the tables chunks (see `is_constraint_confidently_valid`).
  _validate_ucc_candidates(ucc_candidates);
  EXPECT_EQ(column_1_constraint->last_validated_on().load(), first_validation_timestamp);

  EXPECT_TRUE(constraints_A.contains({{ColumnID{0}}, KeyConstraintType::UNIQUE}));
  EXPECT_TRUE(constraints_A.find(TableKeyConstraint{{ColumnID{0}}, KeyConstraintType::UNIQUE})->is_valid());

  EXPECT_TRUE(constraints_A.contains({{ColumnID{1}}, KeyConstraintType::UNIQUE}));
  EXPECT_TRUE(constraints_A.find(TableKeyConstraint{{ColumnID{1}}, KeyConstraintType::UNIQUE})->is_valid());
}

TEST_P(UccDiscoveryPluginMultiEncodingTest, DeletionOfModifiedUCC) {
  _encode_table(_table_A, GetParam());

  // Insert unique column as candidate.
  const auto ucc_candidates = UccCandidates{{"uniquenessTestTableA", ColumnID{0}}};

  _validate_ucc_candidates(ucc_candidates);

  // Collect constraints known for the tables.
  const auto& constraints_A = _table_A->soft_key_constraints();
  EXPECT_TRUE(constraints_A.contains({{ColumnID{0}}, KeyConstraintType::UNIQUE}));
  EXPECT_TRUE(constraints_A.find(TableKeyConstraint{{ColumnID{0}}, KeyConstraintType::UNIQUE})->is_valid());

  // Insert table data into the table again. -> Creates duplicate for every row.
  _insert_row(_table_name_A,
              TableColumnDefinitions{
                  {"A_a", DataType::Int, false}, {"b", DataType::Int, false}, {"c", DataType::String, false}},
              {6, 3, "dublicate"}, true);

  _validate_ucc_candidates(ucc_candidates);
  const auto& constraints_B = _table_A->soft_key_constraints();
  EXPECT_TRUE(constraints_B.contains({{ColumnID{0}}, KeyConstraintType::UNIQUE}));
  EXPECT_FALSE(constraints_B.find(TableKeyConstraint{{ColumnID{0}}, KeyConstraintType::UNIQUE})->is_valid());
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
  EXPECT_EQ(constraints_B.size(), 2);

  EXPECT_TRUE(constraints_A.contains({{_join_columnA->original_column_id}, KeyConstraintType::UNIQUE}));
  EXPECT_TRUE(constraints_A.find(TableKeyConstraint{{_join_columnA->original_column_id}, KeyConstraintType::UNIQUE})
                  ->is_valid());

  EXPECT_TRUE(constraints_A.contains({{_predicate_column_A->original_column_id}, KeyConstraintType::UNIQUE}));
  EXPECT_TRUE(
      constraints_A.find(TableKeyConstraint{{_predicate_column_A->original_column_id}, KeyConstraintType::UNIQUE})
          ->is_valid());

  EXPECT_TRUE(constraints_B.contains({{_join_columnB->original_column_id}, KeyConstraintType::UNIQUE}));
  EXPECT_TRUE(constraints_B.find(TableKeyConstraint{{_join_columnB->original_column_id}, KeyConstraintType::UNIQUE})
                  ->is_valid());

  EXPECT_TRUE(constraints_B.contains({{_predicate_column_B->original_column_id}, KeyConstraintType::UNIQUE}));
  EXPECT_FALSE(
      constraints_B.find(TableKeyConstraint{{_predicate_column_B->original_column_id}, KeyConstraintType::UNIQUE})
          ->is_valid());

  // Also test that invalidated UCC do are not in `valid_soft_key_constraints`. They are marked as invalid.
  const auto constraints_A_valid = _table_A->valid_soft_key_constraints();
  const auto constraints_B_valid = _table_B->valid_soft_key_constraints();
  EXPECT_TRUE(constraints_A_valid.contains({{_join_columnA->original_column_id}, KeyConstraintType::UNIQUE}));
  EXPECT_TRUE(constraints_A_valid.contains({{_predicate_column_A->original_column_id}, KeyConstraintType::UNIQUE}));
  EXPECT_TRUE(constraints_B_valid.contains({{_join_columnB->original_column_id}, KeyConstraintType::UNIQUE}));
  EXPECT_FALSE(constraints_B_valid.contains({{_predicate_column_B->original_column_id}, KeyConstraintType::UNIQUE}));

  // Ensure we clear the plan caches.
  EXPECT_EQ(Hyrise::get().default_lqp_cache->size(), 0);
  EXPECT_EQ(Hyrise::get().default_pqp_cache->size(), 0);
}

TEST_F(UccDiscoveryPluginMultiEncodingTest, PluginIntegrationTestWithInvalidation) {
  Hyrise::get().default_pqp_cache = std::make_shared<SQLPhysicalPlanCache>();
  Hyrise::get().default_lqp_cache = std::make_shared<SQLLogicalPlanCache>();

  const auto exec_groupby_statement = [&]() {
    const std::string sql = "SELECT b,c FROM " + _table_name_A + " GROUP BY b,c;";
    auto pipeline = SQLPipelineBuilder{sql}.create_pipeline();
    const auto& [pipeline_status, _] = pipeline.get_result_table();
    ASSERT_EQ(pipeline_status, SQLPipelineStatus::Success);
  };

  const auto exec_insert_statement = [&]() {
    auto transaction_context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
    const std::string sql = "INSERT INTO " + _table_name_A + " (A_a, b, c) VALUES (14, 12, 'duplicate_b');";
    auto pipeline = SQLPipelineBuilder{sql}.with_transaction_context(transaction_context).create_pipeline();
    const auto& [pipeline_status, _] = pipeline.get_result_table();
    ASSERT_EQ(pipeline_status, SQLPipelineStatus::Success);
    transaction_context->commit();
  };

  // Perform a single query to populate the LQPCache for `identify_ucc_candidates`.
  exec_groupby_statement();

  ASSERT_EQ(Hyrise::get().default_lqp_cache->size(), 1);

  // Discover UCCs used in previous query.
  auto& pm = Hyrise::get().plugin_manager;
  pm.load_plugin(build_dylib_path("libhyriseUccDiscoveryPlugin"));
  pm.exec_user_function("hyriseUccDiscoveryPlugin", "DiscoverUCCs");

  // Check that key constraint was added to the table.
  const auto constraint_A_c = TableKeyConstraint{{ColumnID{2}}, KeyConstraintType::UNIQUE};

  ASSERT_EQ(_table_A->soft_key_constraints().size(), 2);
  ASSERT_TRUE(_table_A->valid_soft_key_constraints().contains(constraint_A_c));

  exec_groupby_statement();
  exec_insert_statement();
  exec_insert_statement();

  // Check that validity of constraint is not guaranteed anymore.
  const auto constraint_A = _table_A->soft_key_constraints().find(constraint_A_c);
  ASSERT_NE(constraint_A, _table_A->soft_key_constraints().end());
  EXPECT_FALSE(is_constraint_confidently_valid(_table_A, *constraint_A));

  // Flush and repopulate the cache.
  Hyrise::get().default_pqp_cache->clear();
  Hyrise::get().default_lqp_cache->clear();
  exec_groupby_statement();

  // Check that key constraint is properly invalidated.
  pm.exec_user_function("hyriseUccDiscoveryPlugin", "DiscoverUCCs");

  ASSERT_EQ(_table_A->soft_key_constraints().size(), 2);
  ASSERT_FALSE(_table_A->valid_soft_key_constraints().contains(constraint_A_c));

  // Remove duplicates.
  {
    const std::string sql = "DELETE FROM " + _table_name_A + " WHERE b > 10;";
    auto pipeline = SQLPipelineBuilder{sql}.create_pipeline();
    pipeline.get_result_table();
  }

  // Repopulate the cache.
  exec_groupby_statement();
  pm.exec_user_function("hyriseUccDiscoveryPlugin", "DiscoverUCCs");

  // Check that the key constraint is valid again.
  ASSERT_EQ(_table_A->soft_key_constraints().size(), 2);
  ASSERT_TRUE(_table_A->valid_soft_key_constraints().contains(constraint_A_c));
}

INSTANTIATE_TEST_SUITE_P(UccDiscoveryPluginMultiEncodingTestInstances, UccDiscoveryPluginMultiEncodingTest,
                         ::testing::ValuesIn(all_segment_encoding_specs), all_segment_encoding_specs_formatter);

}  // namespace hyrise
