#include <map>
#include <memory>
#include <numeric>
#include <utility>
#include <vector>

#include "base_test.hpp"

#include "all_type_variant.hpp"
#include "operators/join_index.hpp"
#include "operators/join_verification.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/index/group_key/group_key_index.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

class OperatorsJoinIndexTest : public BaseTest {
 public:
  static void SetUpTestCase() {  // called ONCE before the tests
    const auto dummy_table =
        std::make_shared<Table>(TableColumnDefinitions{{"a", DataType::Int, false}}, TableType::Data);
    dummy_input = std::make_shared<TableWrapper>(dummy_table);

    // load and create the indexed tables
    _table_wrapper_a = load_table_with_index("resources/test_data/tbl/int_float.tbl", 2);
    _table_wrapper_b = load_table_with_index("resources/test_data/tbl/int_float2.tbl", 2);
    _table_wrapper_c = load_table_with_index("resources/test_data/tbl/int_string.tbl", 4);
    _table_wrapper_d = load_table_with_index("resources/test_data/tbl/string_int.tbl", 3);
    _table_wrapper_e = load_table_with_index("resources/test_data/tbl/int_int2.tbl", 4);
    _table_wrapper_f = load_table_with_index("resources/test_data/tbl/int_int3.tbl", 4);
    _table_wrapper_g = load_table_with_index("resources/test_data/tbl/int_int4.tbl", 4);
    _table_wrapper_h = load_table_with_index("resources/test_data/tbl/int_float_null_1.tbl", 20);
    _table_wrapper_h_no_index =
        std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int_float_null_1.tbl", 20));
    _table_wrapper_i = load_table_with_index("resources/test_data/tbl/int_float_null_2.tbl", 20);
    _table_wrapper_i_no_index =
        std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int_float_null_2.tbl", 20));

    // // execute all TableWrapper operators in advance
    _table_wrapper_a->execute();
    _table_wrapper_b->execute();
    _table_wrapper_c->execute();
    _table_wrapper_d->execute();
    _table_wrapper_e->execute();
    _table_wrapper_f->execute();
    _table_wrapper_g->execute();
    _table_wrapper_h->execute();
    _table_wrapper_h_no_index->execute();
    _table_wrapper_i->execute();
    _table_wrapper_i_no_index->execute();
  }

 protected:
  static std::shared_ptr<TableWrapper> load_table_with_index(const std::string& filename, const size_t chunk_size) {
    auto table = load_table(filename, chunk_size);

    ChunkEncoder::encode_all_chunks(table, SegmentEncodingSpec{EncodingType::Dictionary});

    for (ChunkID chunk_id{0}; chunk_id < table->chunk_count(); ++chunk_id) {
      const auto chunk = table->get_chunk(chunk_id);

      std::vector<ColumnID> columns{1};
      for (ColumnID column_id{0}; column_id < chunk->column_count(); ++column_id) {
        columns[0] = column_id;
        chunk->create_index<GroupKeyIndex>(columns);
      }
    }

    return std::make_shared<TableWrapper>(table);
  }

  // builds and executes the given Join and checks correctness of the output
  static void test_join_output(const std::shared_ptr<const AbstractOperator>& left,
                               const std::shared_ptr<const AbstractOperator>& right,
                               const OperatorJoinPredicate& primary_predicate, const JoinMode mode,
                               const size_t chunk_size, const bool using_index = true,
                               const IndexSide index_side = IndexSide::Right,
                               const bool single_chunk_reference_guarantee = true) {
    const auto join_verification = std::make_shared<JoinVerification>(left, right, mode, primary_predicate);

    join_verification->execute();

    const auto expected_result = join_verification->get_output();

    // build and execute join
    auto join = std::make_shared<JoinIndex>(left, right, mode, primary_predicate, std::vector<OperatorJoinPredicate>{},
                                            index_side);
    EXPECT_NE(join, nullptr) << "Could not build Join";
    join->execute();

    EXPECT_TABLE_EQ_UNORDERED(join->get_output(), expected_result);

    std::shared_ptr<const AbstractOperator> index_side_input;
    if (index_side == IndexSide::Left) {
      index_side_input = left;
    } else {
      index_side_input = right;
    }

    const auto& performance_data = static_cast<const JoinIndex::PerformanceData&>(join->performance_data());
    if (using_index && (index_side_input->get_output()->type() == TableType::Data ||
                        (mode == JoinMode::Inner && single_chunk_reference_guarantee))) {
      EXPECT_EQ(performance_data.chunks_scanned_with_index,
                static_cast<size_t>(index_side_input->get_output()->chunk_count()));
      EXPECT_EQ(performance_data.chunks_scanned_without_index, 0);
    } else {
      EXPECT_EQ(performance_data.chunks_scanned_with_index, 0);
      EXPECT_EQ(performance_data.chunks_scanned_without_index,
                static_cast<size_t>(index_side_input->get_output()->chunk_count()));
    }
  }

  inline static std::shared_ptr<TableWrapper> dummy_input, _table_wrapper_a, _table_wrapper_b, _table_wrapper_c,
      _table_wrapper_d, _table_wrapper_e, _table_wrapper_f, _table_wrapper_g, _table_wrapper_h, _table_wrapper_i,
      _table_wrapper_h_no_index, _table_wrapper_i_no_index;
};

TEST_F(OperatorsJoinIndexTest, Supports) {
  const auto primary_predicate = OperatorJoinPredicate{{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals};
  auto configuration = JoinConfiguration{};

  auto join_operator = std::make_shared<JoinIndex>(dummy_input, dummy_input, JoinMode::Inner, primary_predicate,
                                                   std::vector<OperatorJoinPredicate>{}, IndexSide::Left);

  EXPECT_THROW(join_operator->supports(configuration), std::logic_error);

  configuration.left_table_type = TableType::References;
  configuration.right_table_type = TableType::Data;
  configuration.index_side = IndexSide::Left;
  configuration.join_mode = JoinMode::FullOuter;
  EXPECT_FALSE(join_operator->supports(configuration));
}

TEST_F(OperatorsJoinIndexTest, DescriptionAndName) {
  const auto primary_predicate = OperatorJoinPredicate{{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals};
  const auto secondary_predicate = OperatorJoinPredicate{{ColumnID{0}, ColumnID{0}}, PredicateCondition::NotEquals};

  // INDEX SIDE LEFT
  const auto join_operator_index_left =
      std::make_shared<JoinIndex>(dummy_input, dummy_input, JoinMode::Inner, primary_predicate,
                                  std::vector<OperatorJoinPredicate>{secondary_predicate}, IndexSide::Left);
  // INDEX SIDE RIGHT
  const auto join_operator_index_right =
      std::make_shared<JoinIndex>(dummy_input, dummy_input, JoinMode::Inner, primary_predicate,
                                  std::vector<OperatorJoinPredicate>{secondary_predicate}, IndexSide::Right);

  EXPECT_EQ(join_operator_index_left->description(DescriptionMode::SingleLine),
            "JoinIndex (Inner Join where Column #0 = Column #0 AND Column #0 != Column #0) Index side: Left");
  EXPECT_EQ(join_operator_index_left->description(DescriptionMode::MultiLine),
            "JoinIndex\n(Inner Join where Column #0 = Column #0 AND Column #0 != Column #0)\nIndex side: Left");

  EXPECT_EQ(join_operator_index_right->description(DescriptionMode::SingleLine),
            "JoinIndex (Inner Join where Column #0 = Column #0 AND Column #0 != Column #0) Index side: Right");
  EXPECT_EQ(join_operator_index_right->description(DescriptionMode::MultiLine),
            "JoinIndex\n(Inner Join where Column #0 = Column #0 AND Column #0 != Column #0)\nIndex side: Right");

  dummy_input->execute();

  EXPECT_EQ(join_operator_index_left->description(DescriptionMode::SingleLine),
            "JoinIndex (Inner Join where a = a AND a != a) Index side: Left");
  EXPECT_EQ(join_operator_index_left->description(DescriptionMode::MultiLine),
            "JoinIndex\n(Inner Join where a = a AND a != a)\nIndex side: Left");

  EXPECT_EQ(join_operator_index_right->description(DescriptionMode::SingleLine),
            "JoinIndex (Inner Join where a = a AND a != a) Index side: Right");
  EXPECT_EQ(join_operator_index_right->description(DescriptionMode::MultiLine),
            "JoinIndex\n(Inner Join where a = a AND a != a)\nIndex side: Right");

  EXPECT_EQ(join_operator_index_left->name(), "JoinIndex");
}

TEST_F(OperatorsJoinIndexTest, DeepCopy) {
  const auto primary_predicate = OperatorJoinPredicate{{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals};
  const auto secondary_predicates =
      std::vector<OperatorJoinPredicate>{{{ColumnID{1}, ColumnID{1}}, PredicateCondition::NotEquals}};
  const auto join_operator =
      std::make_shared<JoinIndex>(dummy_input, dummy_input, JoinMode::Left, primary_predicate, secondary_predicates);
  const auto abstract_join_operator_copy = join_operator->deep_copy();
  const auto join_operator_copy = std::dynamic_pointer_cast<JoinIndex>(abstract_join_operator_copy);

  ASSERT_TRUE(join_operator_copy);

  EXPECT_EQ(join_operator_copy->mode(), JoinMode::Left);
  EXPECT_EQ(join_operator_copy->primary_predicate(), primary_predicate);
  EXPECT_EQ(join_operator_copy->secondary_predicates(), secondary_predicates);
  EXPECT_NE(join_operator_copy->input_left(), nullptr);
  EXPECT_NE(join_operator_copy->input_right(), nullptr);
}

TEST_F(OperatorsJoinIndexTest, PerformanceDataOutputToStream) {
  auto performance_data = JoinIndex::PerformanceData{};

  performance_data.executed = true;
  performance_data.has_output = true;
  performance_data.output_row_count = 2u;
  performance_data.output_chunk_count = 1u;
  performance_data.walltime = std::chrono::nanoseconds{999u};
  performance_data.chunks_scanned_with_index = 10u;
  performance_data.chunks_scanned_without_index = 5u;

  {
    std::stringstream stream;
    stream << performance_data;
    EXPECT_EQ(stream.str(), "2 row(s) in 1 chunk(s), 999 ns, indexes used for 10 of 15 chunk(s)");
  }

  {
    std::stringstream stream;
    performance_data.output_to_stream(stream, DescriptionMode::MultiLine);
    EXPECT_EQ(stream.str(), "2 row(s) in 1 chunk(s), 999 ns\nindexes used for 10 of 15 chunk(s)");
  }
}

TEST_F(OperatorsJoinIndexTest, InnerRefJoinNoIndex) {
  // scan that returns all rows
  auto scan_a = create_table_scan(_table_wrapper_h_no_index, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_a->execute();
  auto scan_b = create_table_scan(_table_wrapper_i_no_index, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_b->execute();

  test_join_output(scan_a, scan_b, {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::Inner, 1, false);
}

TEST_F(OperatorsJoinIndexTest, MultiJoinOnReferenceLeftIndexLeft) {
  // scan that returns all rows
  auto scan_a = create_table_scan(_table_wrapper_e, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_a->execute();
  auto scan_b = create_table_scan(_table_wrapper_f, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_b->execute();
  auto scan_c = create_table_scan(_table_wrapper_g, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_c->execute();

  auto join = std::make_shared<JoinIndex>(
      scan_a, scan_b, JoinMode::Inner, OperatorJoinPredicate{{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals});
  join->execute();

  // Referencing single chunk guarantee is not given since the left input of the index join is also an index join
  // and the IndexSide is left. The execution of the index join does not provide single chunk reference guarantee.
  test_join_output(join, scan_c, {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::Inner, 1, true,
                   IndexSide::Left, false);
}

TEST_F(OperatorsJoinIndexTest, RightJoinPruneInputIsRefIndexInputIsDataIndexSideIsRight) {
  // scan that returns all rows
  auto scan_a = create_table_scan(_table_wrapper_a, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_a->execute();

  test_join_output(scan_a, _table_wrapper_b, {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::Right,
                   1, true);
}

}  // namespace opossum
