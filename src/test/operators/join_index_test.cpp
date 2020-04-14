#include <map>
#include <memory>
#include <numeric>
#include <utility>
#include <vector>

#include "base_test.hpp"

#include "all_type_variant.hpp"
#include "operators/join_index.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/index/adaptive_radix_tree/adaptive_radix_tree_index.hpp"
#include "storage/index/b_tree/b_tree_index.hpp"
#include "storage/index/group_key/composite_group_key_index.hpp"
#include "storage/index/group_key/group_key_index.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

class OperatorsJoinIndexTest : public BaseTest {
 public:
  /*
  static void SetUpTestCase() {  // called ONCE before the tests
    // load and create the indexed tables
    _table_wrapper_a = load_table_with_index("resources/test_data/tbl/int_float.tbl", 2);
    _table_wrapper_a_no_index = std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int_float.tbl", 2));
    _table_wrapper_b = load_table_with_index("resources/test_data/tbl/int_float2.tbl", 2);
    _table_wrapper_b_no_index = std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int_float2.tbl", 2));
    _table_wrapper_c = load_table_with_index("resources/test_data/tbl/int_string.tbl", 4);
    _table_wrapper_d = load_table_with_index("resources/test_data/tbl/string_int.tbl", 3);
    _table_wrapper_e = load_table_with_index("resources/test_data/tbl/int_int.tbl", 4);
    _table_wrapper_f = load_table_with_index("resources/test_data/tbl/int_int2.tbl", 4);
    _table_wrapper_g = load_table_with_index("resources/test_data/tbl/int_int3.tbl", 4);
    _table_wrapper_h = load_table_with_index("resources/test_data/tbl/int_int4.tbl", 4);
    _table_wrapper_i = load_table_with_index("resources/test_data/tbl/int5.tbl", 1);
    _table_wrapper_j = load_table_with_index("resources/test_data/tbl/int3.tbl", 1);
    _table_wrapper_k = load_table_with_index("resources/test_data/tbl/int4.tbl", 1);
    _table_wrapper_l = load_table_with_index("resources/test_data/tbl/int.tbl", 1);
    _table_wrapper_m = load_table_with_index("resources/test_data/tbl/int_float_null_1.tbl", 20);
    _table_wrapper_n = load_table_with_index("resources/test_data/tbl/int_float_null_2.tbl", 20);

    // execute all TableWrapper operators in advance
    _table_wrapper_a->execute();
    _table_wrapper_a_no_index->execute();
    _table_wrapper_b->execute();
    _table_wrapper_b_no_index->execute();
    _table_wrapper_c->execute();
    _table_wrapper_d->execute();
    _table_wrapper_e->execute();
    _table_wrapper_f->execute();
    _table_wrapper_g->execute();
    _table_wrapper_h->execute();
    _table_wrapper_i->execute();
    _table_wrapper_j->execute();
    _table_wrapper_k->execute();
    _table_wrapper_l->execute();
    _table_wrapper_m->execute();
    _table_wrapper_n->execute();
  }
*/
 protected:
  void SetUp() override {
    const auto dummy_table =
        std::make_shared<Table>(TableColumnDefinitions{{"a", DataType::Int, false}}, TableType::Data);
    dummy_input = std::make_shared<TableWrapper>(dummy_table);
  }
/*
  static std::shared_ptr<TableWrapper> load_table_with_index(const std::string& filename, const size_t chunk_size) {
    auto table = load_table(filename, chunk_size);

    ChunkEncoder::encode_all_chunks(table, SegmentEncodingSpec{EncodingType::Dictionary});

    for (ChunkID chunk_id{0}; chunk_id < table->chunk_count(); ++chunk_id) {
      const auto chunk = table->get_chunk(chunk_id);

      std::vector<ColumnID> columns{1};
      for (ColumnID column_id{0}; column_id < chunk->column_count(); ++column_id) {
        columns[0] = column_id;
        chunk->create_index<DerivedIndex>(columns);
      }
    }

    return std::make_shared<TableWrapper>(table);
  }

  // builds and executes the given Join and checks correctness of the output
  static void test_join_output(const std::shared_ptr<const AbstractOperator>& left,
                               const std::shared_ptr<const AbstractOperator>& right,
                               const OperatorJoinPredicate& primary_predicate, const JoinMode mode,
                               const std::string& file_name, size_t chunk_size, bool using_index = true,
                               const IndexSide index_side = IndexSide::Right,
                               const bool single_chunk_reference_guarantee = true) {
    // load expected results from file
    std::shared_ptr<Table> expected_result = load_table(file_name, chunk_size);
    EXPECT_NE(expected_result, nullptr) << "Could not load expected result table";

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

  inline static std::shared_ptr<TableWrapper> _table_wrapper_a, _table_wrapper_a_no_index, _table_wrapper_b,
      _table_wrapper_b_no_index, _table_wrapper_c, _table_wrapper_d, _table_wrapper_e, _table_wrapper_f,
      _table_wrapper_g, _table_wrapper_h, _table_wrapper_i, _table_wrapper_j, _table_wrapper_k, _table_wrapper_l,
      _table_wrapper_m, _table_wrapper_n;
*/
  std::shared_ptr<AbstractOperator> dummy_input;
};
/*
typedef ::testing::Types<AdaptiveRadixTreeIndex, CompositeGroupKeyIndex, BTreeIndex, GroupKeyIndex> DerivedIndices;

TYPED_TEST_SUITE(OperatorsJoinIndexTest, DerivedIndices, );  // NOLINT(whitespace/parens)
*/
TEST_F(OperatorsJoinIndexTest, SupportsFail) {
  const auto primary_predicate = OperatorJoinPredicate{{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals};
  const auto configuration = JoinConfiguration{};

  auto join_operator = std::make_shared<JoinIndex>(dummy_input, dummy_input, JoinMode::Inner, primary_predicate, std::vector<OperatorJoinPredicate>{},
                                            IndexSide::Left);

  EXPECT_THROW(join_operator->supports(configuration), std::logic_error);
}

TEST_F(OperatorsJoinIndexTest, DescriptionAndName) {
  const auto primary_predicate = OperatorJoinPredicate{{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals};
  const auto secondary_predicate = OperatorJoinPredicate{{ColumnID{0}, ColumnID{0}}, PredicateCondition::NotEquals};

  const auto join_operator =
      std::make_shared<JoinIndex>(dummy_input, dummy_input, JoinMode::Inner, primary_predicate,
                                       std::vector<OperatorJoinPredicate>{secondary_predicate}, IndexSide::Left);

  EXPECT_EQ(join_operator->description(DescriptionMode::SingleLine),
            "JoinIndex (Inner Join where Column #0 = Column #0 AND Column #0 != Column #0) Index side: Left");
  EXPECT_EQ(join_operator->description(DescriptionMode::MultiLine),
            "JoinIndex\n(Inner Join where Column #0 = Column #0 AND Column #0 != Column #0)\nIndex side: Left");

  dummy_input->execute();
  EXPECT_EQ(join_operator->description(DescriptionMode::SingleLine),
            "JoinIndex (Inner Join where a = a AND a != a) Index side: Left");
  EXPECT_EQ(join_operator->description(DescriptionMode::MultiLine),
            "JoinIndex\n(Inner Join where a = a AND a != a)\nIndex side: Left");

  EXPECT_EQ(join_operator->name(), "JoinIndex");
}

TEST_F(OperatorsJoinIndexTest, DeepCopy) {
  const auto primary_predicate = OperatorJoinPredicate{{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals};
  const auto secondary_predicates =
      std::vector<OperatorJoinPredicate>{{{ColumnID{1}, ColumnID{1}}, PredicateCondition::NotEquals}};
  const auto join_operator = std::make_shared<JoinIndex>(dummy_input, dummy_input, JoinMode::Left,
                                                              primary_predicate, secondary_predicates);
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
  std::stringstream stream;

  performance_data.executed = true;
  performance_data.has_output = true;
  performance_data.output_row_count = 2u;
  performance_data.output_chunk_count = 1u;
  performance_data.walltime = std::chrono::nanoseconds{999u};
  performance_data.chunks_scanned_with_index = 10u;
  performance_data.chunks_scanned_without_index = 5u;
  stream << performance_data;
  EXPECT_EQ(stream.str(), "2 row(s) in 1 chunk(s), 999 ns / 10 of 15 chunks used an index");

  stream.str("");
  performance_data.output_to_stream(stream, DescriptionMode::MultiLine);
  EXPECT_EQ(stream.str(), "2 row(s) in 1 chunk(s), 999 ns\n10 of 15 chunks used an index");
}

/*
TYPED_TEST(OperatorsJoinIndexTest, LeftJoinFallBack) {
  this->test_join_output(this->_table_wrapper_a_no_index, this->_table_wrapper_b_no_index,
                         {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::Left,
                         "resources/test_data/tbl/join_operators/int_left_join_equals.tbl", 1, false);
}

TYPED_TEST(OperatorsJoinIndexTest, LeftJoin) {
  this->test_join_output(this->_table_wrapper_a, this->_table_wrapper_b,
                         {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::Left,
                         "resources/test_data/tbl/join_operators/int_left_join_equals.tbl", 1);
}

TYPED_TEST(OperatorsJoinIndexTest, LeftJoinOnString) {
  this->test_join_output(this->_table_wrapper_c, this->_table_wrapper_d,
                         {{ColumnID{1}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::Left,
                         "resources/test_data/tbl/join_operators/string_left_join.tbl", 1);
}

TYPED_TEST(OperatorsJoinIndexTest, RightJoin) {
  this->test_join_output(this->_table_wrapper_a, this->_table_wrapper_b,
                         {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::Right,
                         "resources/test_data/tbl/join_operators/int_right_join_equals.tbl", 1);
}

TYPED_TEST(OperatorsJoinIndexTest, RightJoinFallBack) {
  this->test_join_output(this->_table_wrapper_a_no_index, this->_table_wrapper_b_no_index,
                         {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::Right,
                         "resources/test_data/tbl/join_operators/int_right_join_equals.tbl", 1, false);
}

TYPED_TEST(OperatorsJoinIndexTest, InnerJoin) {
  this->test_join_output(this->_table_wrapper_a, this->_table_wrapper_b,
                         {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::Inner,
                         "resources/test_data/tbl/join_operators/int_inner_join.tbl", 1);
}

TYPED_TEST(OperatorsJoinIndexTest, InnerJoinOnString) {
  this->test_join_output(this->_table_wrapper_c, this->_table_wrapper_d,
                         {{ColumnID{1}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::Inner,
                         "resources/test_data/tbl/join_operators/string_inner_join.tbl", 1);
}

TYPED_TEST(OperatorsJoinIndexTest, InnerJoinSingleChunk) {
  this->test_join_output(this->_table_wrapper_e, this->_table_wrapper_f,
                         {{ColumnID{1}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::Inner,
                         "resources/test_data/tbl/join_operators/int_inner_join_single_chunk.tbl", 1);
}

TYPED_TEST(OperatorsJoinIndexTest, InnerRefJoin) {
  // scan that returns all rows
  auto scan_a = create_table_scan(this->_table_wrapper_a, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_a->execute();
  auto scan_b = create_table_scan(this->_table_wrapper_b, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_b->execute();

  this->test_join_output(scan_a, scan_b, {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::Inner,
                         "resources/test_data/tbl/join_operators/int_inner_join.tbl", 1);
}

TYPED_TEST(OperatorsJoinIndexTest, InnerRefJoinFiltered) {
  auto scan_a = create_table_scan(this->_table_wrapper_a, ColumnID{0}, PredicateCondition::GreaterThan, 1000);
  scan_a->execute();
  auto scan_b = create_table_scan(this->_table_wrapper_b, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_b->execute();

  this->test_join_output(scan_a, scan_b, {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::Inner,
                         "resources/test_data/tbl/join_operators/int_inner_join_filtered.tbl", 1);
}

TYPED_TEST(OperatorsJoinIndexTest, InnerDictJoin) {
  this->test_join_output(this->_table_wrapper_a, this->_table_wrapper_b,
                         {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::Inner,
                         "resources/test_data/tbl/join_operators/int_inner_join.tbl", 1);
}

TYPED_TEST(OperatorsJoinIndexTest, InnerDictJoinSwapTables) {
  this->test_join_output(this->_table_wrapper_a, this->_table_wrapper_b_no_index,
                         {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::Inner,
                         "resources/test_data/tbl/join_operators/int_inner_join.tbl", 1, true, IndexSide::Left);
}

TYPED_TEST(OperatorsJoinIndexTest, InnerRefDictJoin) {
  // scan that returns all rows
  auto scan_a = create_table_scan(this->_table_wrapper_a, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_a->execute();
  auto scan_b = create_table_scan(this->_table_wrapper_b, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_b->execute();

  this->test_join_output(scan_a, scan_b, {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::Inner,
                         "resources/test_data/tbl/join_operators/int_inner_join.tbl", 1);
}

TYPED_TEST(OperatorsJoinIndexTest, InnerRefDictJoinFiltered) {
  auto scan_a = create_table_scan(this->_table_wrapper_a, ColumnID{0}, PredicateCondition::GreaterThan, 1000);
  scan_a->execute();
  auto scan_b = create_table_scan(this->_table_wrapper_b, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_b->execute();

  this->test_join_output(scan_a, scan_b, {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::Inner,
                         "resources/test_data/tbl/join_operators/int_inner_join_filtered.tbl", 1);
}

TYPED_TEST(OperatorsJoinIndexTest, InnerJoinBig) {
  this->test_join_output(this->_table_wrapper_c, this->_table_wrapper_d,
                         {{ColumnID{0}, ColumnID{1}}, PredicateCondition::Equals}, JoinMode::Inner,
                         "resources/test_data/tbl/join_operators/int_string_inner_join.tbl", 1);
}

TYPED_TEST(OperatorsJoinIndexTest, InnerRefJoinFilteredBig) {
  auto scan_c = create_table_scan(this->_table_wrapper_c, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_c->execute();
  auto scan_d = create_table_scan(this->_table_wrapper_d, ColumnID{1}, PredicateCondition::GreaterThanEquals, 6);
  scan_d->execute();

  this->test_join_output(scan_c, scan_d, {{ColumnID{0}, ColumnID{1}}, PredicateCondition::Equals}, JoinMode::Inner,
                         "resources/test_data/tbl/join_operators/int_string_inner_join_filtered.tbl", 1);
}

TYPED_TEST(OperatorsJoinIndexTest, OuterJoin) {
  this->test_join_output(this->_table_wrapper_a, this->_table_wrapper_b,
                         {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::FullOuter,
                         "resources/test_data/tbl/join_operators/int_outer_join.tbl", 1);
}

TYPED_TEST(OperatorsJoinIndexTest, OuterJoinWithNull) {
  if constexpr (std::is_same_v<TypeParam, CompositeGroupKeyIndex>) {
    return;  // CompositeGroupKeyIndex is currently not null-aware (#1818)
  }

  this->test_join_output(this->_table_wrapper_m, this->_table_wrapper_n,
                         {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::FullOuter,
                         "resources/test_data/tbl/join_operators/int_outer_join_null.tbl", 1);
}

TYPED_TEST(OperatorsJoinIndexTest, OuterJoinDict) {
  this->test_join_output(this->_table_wrapper_a, this->_table_wrapper_b,
                         {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::FullOuter,
                         "resources/test_data/tbl/join_operators/int_outer_join.tbl", 1);
}

TYPED_TEST(OperatorsJoinIndexTest, SmallerInnerJoin) {
  // Joining two Integer Columns
  this->test_join_output(this->_table_wrapper_a, this->_table_wrapper_b,
                         {{ColumnID{0}, ColumnID{0}}, PredicateCondition::LessThan}, JoinMode::Inner,
                         "resources/test_data/tbl/join_operators/int_smaller_inner_join.tbl", 1);

  // Joining two Float Columns
  this->test_join_output(this->_table_wrapper_a, this->_table_wrapper_b,
                         {{ColumnID{1}, ColumnID{1}}, PredicateCondition::LessThan}, JoinMode::Inner,
                         "resources/test_data/tbl/join_operators/float_smaller_inner_join.tbl", 1);
}

TYPED_TEST(OperatorsJoinIndexTest, SmallerInnerJoinDict) {
  // Joining two Integer Columns
  this->test_join_output(this->_table_wrapper_a, this->_table_wrapper_b,
                         {{ColumnID{0}, ColumnID{0}}, PredicateCondition::LessThan}, JoinMode::Inner,
                         "resources/test_data/tbl/join_operators/int_smaller_inner_join.tbl", 1);

  // Joining two Float Columns
  this->test_join_output(this->_table_wrapper_a, this->_table_wrapper_b,
                         {{ColumnID{1}, ColumnID{1}}, PredicateCondition::LessThan}, JoinMode::Inner,
                         "resources/test_data/tbl/join_operators/float_smaller_inner_join.tbl", 1);
}

TYPED_TEST(OperatorsJoinIndexTest, SmallerInnerJoin2) {
  // Joining two Integer Columns
  this->test_join_output(this->_table_wrapper_j, this->_table_wrapper_i,
                         {{ColumnID{0}, ColumnID{0}}, PredicateCondition::LessThan}, JoinMode::Inner,
                         "resources/test_data/tbl/join_operators/int_smaller_inner_join_2.tbl", 1);
}

TYPED_TEST(OperatorsJoinIndexTest, SmallerOuterJoin) {
  this->test_join_output(this->_table_wrapper_k, this->_table_wrapper_l,
                         {{ColumnID{0}, ColumnID{0}}, PredicateCondition::LessThan}, JoinMode::FullOuter,
                         "resources/test_data/tbl/join_operators/int_smaller_outer_join.tbl", 1);
}

TYPED_TEST(OperatorsJoinIndexTest, SmallerEqualInnerJoin) {
  // Joining two Integer Columns
  this->test_join_output(this->_table_wrapper_a, this->_table_wrapper_b,
                         {{ColumnID{0}, ColumnID{0}}, PredicateCondition::LessThanEquals}, JoinMode::Inner,
                         "resources/test_data/tbl/join_operators/int_smallerequal_inner_join.tbl", 1);

  // Joining two Float Columns
  this->test_join_output(this->_table_wrapper_a, this->_table_wrapper_b,
                         {{ColumnID{1}, ColumnID{1}}, PredicateCondition::LessThanEquals}, JoinMode::Inner,
                         "resources/test_data/tbl/join_operators/float_smallerequal_inner_join.tbl", 1);
}

TYPED_TEST(OperatorsJoinIndexTest, SmallerEqualInnerJoin2) {
  // Joining two Integer Columns
  this->test_join_output(this->_table_wrapper_j, this->_table_wrapper_i,
                         {{ColumnID{0}, ColumnID{0}}, PredicateCondition::LessThanEquals}, JoinMode::Inner,
                         "resources/test_data/tbl/join_operators/int_smallerequal_inner_join_2.tbl", 1);
}

TYPED_TEST(OperatorsJoinIndexTest, SmallerEqualOuterJoin) {
  this->test_join_output(this->_table_wrapper_k, this->_table_wrapper_l,
                         {{ColumnID{0}, ColumnID{0}}, PredicateCondition::LessThanEquals}, JoinMode::FullOuter,
                         "resources/test_data/tbl/join_operators/int_smallerequal_outer_join.tbl", 1);
}

TYPED_TEST(OperatorsJoinIndexTest, GreaterInnerJoin) {
  // Joining two Integer Columns
  this->test_join_output(this->_table_wrapper_a, this->_table_wrapper_b,
                         {{ColumnID{0}, ColumnID{0}}, PredicateCondition::GreaterThan}, JoinMode::Inner,
                         "resources/test_data/tbl/join_operators/int_greater_inner_join.tbl", 1);

  // Joining two Float Columns
  this->test_join_output(this->_table_wrapper_a, this->_table_wrapper_b,
                         {{ColumnID{1}, ColumnID{1}}, PredicateCondition::GreaterThan}, JoinMode::Inner,
                         "resources/test_data/tbl/join_operators/float_greater_inner_join.tbl", 1);
}

TYPED_TEST(OperatorsJoinIndexTest, GreaterInnerJoinDict) {
  // Joining two Integer Columns
  this->test_join_output(this->_table_wrapper_a, this->_table_wrapper_b,
                         {{ColumnID{0}, ColumnID{0}}, PredicateCondition::GreaterThan}, JoinMode::Inner,
                         "resources/test_data/tbl/join_operators/int_greater_inner_join.tbl", 1);

  // Joining two Float Columns
  this->test_join_output(this->_table_wrapper_a, this->_table_wrapper_b,
                         {{ColumnID{1}, ColumnID{1}}, PredicateCondition::GreaterThan}, JoinMode::Inner,
                         "resources/test_data/tbl/join_operators/float_greater_inner_join.tbl", 1);
}

TYPED_TEST(OperatorsJoinIndexTest, GreaterInnerJoin2) {
  // Joining two Integer Columns
  this->test_join_output(this->_table_wrapper_i, this->_table_wrapper_j,
                         {{ColumnID{0}, ColumnID{0}}, PredicateCondition::GreaterThan}, JoinMode::Inner,
                         "resources/test_data/tbl/join_operators/int_greater_inner_join_2.tbl", 1);
}

TYPED_TEST(OperatorsJoinIndexTest, GreaterOuterJoin) {
  this->test_join_output(this->_table_wrapper_l, this->_table_wrapper_k,
                         {{ColumnID{0}, ColumnID{0}}, PredicateCondition::GreaterThan}, JoinMode::FullOuter,
                         "resources/test_data/tbl/join_operators/int_greater_outer_join.tbl", 1);
}

TYPED_TEST(OperatorsJoinIndexTest, GreaterEqualInnerJoin) {
  // Joining two Integer Columns
  this->test_join_output(this->_table_wrapper_a, this->_table_wrapper_b,
                         {{ColumnID{0}, ColumnID{0}}, PredicateCondition::GreaterThanEquals}, JoinMode::Inner,
                         "resources/test_data/tbl/join_operators/int_greaterequal_inner_join.tbl", 1);

  // Joining two Float Columns
  this->test_join_output(this->_table_wrapper_a, this->_table_wrapper_b,
                         {{ColumnID{1}, ColumnID{1}}, PredicateCondition::GreaterThanEquals}, JoinMode::Inner,
                         "resources/test_data/tbl/join_operators/float_greaterequal_inner_join.tbl", 1);
}

TYPED_TEST(OperatorsJoinIndexTest, GreaterEqualInnerJoinDict) {
  // Joining two Integer Columns
  this->test_join_output(this->_table_wrapper_a, this->_table_wrapper_b,
                         {{ColumnID{0}, ColumnID{0}}, PredicateCondition::GreaterThanEquals}, JoinMode::Inner,
                         "resources/test_data/tbl/join_operators/int_greaterequal_inner_join.tbl", 1);

  // Joining two Float Columns
  this->test_join_output(this->_table_wrapper_a, this->_table_wrapper_b,
                         {{ColumnID{1}, ColumnID{1}}, PredicateCondition::GreaterThanEquals}, JoinMode::Inner,
                         "resources/test_data/tbl/join_operators/float_greaterequal_inner_join.tbl", 1);
}

TYPED_TEST(OperatorsJoinIndexTest, GreaterEqualOuterJoin) {
  this->test_join_output(this->_table_wrapper_l, this->_table_wrapper_k,
                         {{ColumnID{0}, ColumnID{0}}, PredicateCondition::GreaterThanEquals}, JoinMode::FullOuter,
                         "resources/test_data/tbl/join_operators/int_greaterequal_outer_join.tbl", 1);
}

TYPED_TEST(OperatorsJoinIndexTest, GreaterEqualInnerJoin2) {
  // Joining two Integer Columns
  this->test_join_output(this->_table_wrapper_i, this->_table_wrapper_j,
                         {{ColumnID{0}, ColumnID{0}}, PredicateCondition::GreaterThanEquals}, JoinMode::Inner,
                         "resources/test_data/tbl/join_operators/int_greaterequal_inner_join_2.tbl", 1);
}

TYPED_TEST(OperatorsJoinIndexTest, NotEqualInnerJoin) {
  // Joining two Integer Columns
  this->test_join_output(this->_table_wrapper_a, this->_table_wrapper_b,
                         {{ColumnID{0}, ColumnID{0}}, PredicateCondition::NotEquals}, JoinMode::Inner,
                         "resources/test_data/tbl/join_operators/int_notequal_inner_join.tbl", 1);
  // Joining two Float Columns
  this->test_join_output(this->_table_wrapper_a, this->_table_wrapper_b,
                         {{ColumnID{1}, ColumnID{1}}, PredicateCondition::NotEquals}, JoinMode::Inner,
                         "resources/test_data/tbl/join_operators/float_notequal_inner_join.tbl", 1);
}

TYPED_TEST(OperatorsJoinIndexTest, NotEqualInnerJoinDict) {
  // Joining two Integer Columns
  this->test_join_output(this->_table_wrapper_a, this->_table_wrapper_b,
                         {{ColumnID{0}, ColumnID{0}}, PredicateCondition::NotEquals}, JoinMode::Inner,
                         "resources/test_data/tbl/join_operators/int_notequal_inner_join.tbl", 1);
  // Joining two Float Columns
  this->test_join_output(this->_table_wrapper_a, this->_table_wrapper_b,
                         {{ColumnID{1}, ColumnID{1}}, PredicateCondition::NotEquals}, JoinMode::Inner,
                         "resources/test_data/tbl/join_operators/float_notequal_inner_join.tbl", 1);
}

TYPED_TEST(OperatorsJoinIndexTest, JoinLessThanOnDictAndDict) {
  this->test_join_output(this->_table_wrapper_a, this->_table_wrapper_b,
                         {{ColumnID{0}, ColumnID{0}}, PredicateCondition::LessThanEquals}, JoinMode::Inner,
                         "resources/test_data/tbl/join_operators/int_float_leq_dict.tbl", 1);
}

TYPED_TEST(OperatorsJoinIndexTest, JoinOnReferenceSegmentAndDict) {
  // scan that returns all rows
  auto scan_a = create_table_scan(this->_table_wrapper_a, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_a->execute();

  this->test_join_output(scan_a, this->_table_wrapper_b, {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals},
                         JoinMode::Inner, "resources/test_data/tbl/join_operators/int_inner_join.tbl", 1);
}

TYPED_TEST(OperatorsJoinIndexTest, JoinOnDictAndReferenceSegment) {
  // scan that returns all rows
  auto scan_b = create_table_scan(this->_table_wrapper_b, ColumnID{0}, PredicateCondition::GreaterThan, 100);
  scan_b->execute();

  this->test_join_output(this->_table_wrapper_a, scan_b, {{ColumnID{0}, ColumnID{0}}, PredicateCondition::NotEquals},
                         JoinMode::Inner, "resources/test_data/tbl/join_operators/int_inner_join_neq.tbl", 1);
}

TYPED_TEST(OperatorsJoinIndexTest, MultiJoinOnReferenceLeftIndexLeft) {
  // scan that returns all rows
  auto scan_a = create_table_scan(this->_table_wrapper_f, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_a->execute();
  auto scan_b = create_table_scan(this->_table_wrapper_g, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_b->execute();
  auto scan_c = create_table_scan(this->_table_wrapper_h, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_c->execute();

  auto join = std::make_shared<JoinIndex>(
      scan_a, scan_b, JoinMode::Inner, OperatorJoinPredicate{{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals});
  join->execute();

  // Referencing single chunk guarantee is not given since the left input of the index join is also an index join
  // and the IndexSide is left. The execution of the index join does not provide single chunk reference guarantee.
  this->test_join_output(join, scan_c, {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::Inner,
                         "resources/test_data/tbl/join_operators/int_inner_multijoin_ref_ref_ref_left.tbl", 1, true,
                         IndexSide::Left, false);
}

TYPED_TEST(OperatorsJoinIndexTest, MultiJoinOnReferenceLeftIndexRight) {
  // scan that returns all rows
  auto scan_a = create_table_scan(this->_table_wrapper_f, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_a->execute();
  auto scan_b = create_table_scan(this->_table_wrapper_g, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_b->execute();
  auto scan_c = create_table_scan(this->_table_wrapper_h, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_c->execute();

  auto join = std::make_shared<JoinIndex>(
      scan_a, scan_b, JoinMode::Inner, OperatorJoinPredicate{{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals});
  join->execute();

  this->test_join_output(join, scan_c, {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::Inner,
                         "resources/test_data/tbl/join_operators/int_inner_multijoin_ref_ref_ref_left.tbl", 1, true,
                         IndexSide::Right, true);
}

TYPED_TEST(OperatorsJoinIndexTest, MultiJoinOnReferenceRightIndexLeft) {
  // scan that returns all rows
  auto scan_a = create_table_scan(this->_table_wrapper_f, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_a->execute();
  auto scan_b = create_table_scan(this->_table_wrapper_g, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_b->execute();
  auto scan_c = create_table_scan(this->_table_wrapper_h, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_c->execute();

  auto join = std::make_shared<JoinIndex>(
      scan_a, scan_b, JoinMode::Inner, OperatorJoinPredicate{{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals});
  join->execute();

  this->test_join_output(scan_c, join, {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::Inner,
                         "resources/test_data/tbl/join_operators/int_inner_multijoin_ref_ref_ref_right.tbl", 1, true,
                         IndexSide::Left, true);
}

TYPED_TEST(OperatorsJoinIndexTest, MultiJoinOnReferenceRightIndexRight) {
  // scan that returns all rows
  auto scan_a = create_table_scan(this->_table_wrapper_f, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_a->execute();
  auto scan_b = create_table_scan(this->_table_wrapper_g, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_b->execute();
  auto scan_c = create_table_scan(this->_table_wrapper_h, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_c->execute();

  auto join = std::make_shared<JoinIndex>(
      scan_a, scan_b, JoinMode::Inner, OperatorJoinPredicate{{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals});
  join->execute();

  // Referencing single chunk guarantee is not given since the right input of the index join is also an index join
  // and the IndexSide is right. The execution of the index join does not provide single chunk reference guarantee.
  this->test_join_output(scan_c, join, {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::Inner,
                         "resources/test_data/tbl/join_operators/int_inner_multijoin_ref_ref_ref_right.tbl", 1, true,
                         IndexSide::Right, false);
}

TYPED_TEST(OperatorsJoinIndexTest, MultiJoinOnReferenceLeftFiltered) {
  // scan that returns all rows
  auto scan_a = create_table_scan(this->_table_wrapper_f, ColumnID{0}, PredicateCondition::GreaterThan, 6);
  scan_a->execute();
  auto scan_b = create_table_scan(this->_table_wrapper_g, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_b->execute();
  auto scan_c = create_table_scan(this->_table_wrapper_h, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_c->execute();

  auto join = std::make_shared<JoinIndex>(
      scan_a, scan_b, JoinMode::Inner, OperatorJoinPredicate{{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals});
  join->execute();

  this->test_join_output(join, scan_c, {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals}, JoinMode::Inner,
                         "resources/test_data/tbl/join_operators/int_inner_multijoin_ref_ref_ref_left_filtered.tbl", 1);
}

TYPED_TEST(OperatorsJoinIndexTest, MultiJoinOnRefOuter) {
  auto join =
      std::make_shared<JoinIndex>(this->_table_wrapper_f, this->_table_wrapper_g, JoinMode::Left,
                                  OperatorJoinPredicate{{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals});
  join->execute();

  this->test_join_output(join, this->_table_wrapper_h, {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals},
                         JoinMode::Inner,
                         "resources/test_data/tbl/join_operators/int_inner_multijoin_val_val_val_leftouter.tbl", 1);
}

TYPED_TEST(OperatorsJoinIndexTest, RightJoinPruneInputIsRefIndexInputIsDataIndexSideIsRight) {
  // scan that returns all rows
  auto scan_a = create_table_scan(this->_table_wrapper_a, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_a->execute();

  this->test_join_output(scan_a, this->_table_wrapper_b, {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals},
                         JoinMode::Right, "resources/test_data/tbl/join_operators/int_right_join_equals.tbl", 1);
}

TYPED_TEST(OperatorsJoinIndexTest, RightJoinPruneInputIsRefIndexInputIsDataIndexSideIsLeft) {
  // scan that returns all rows
  auto scan_a = create_table_scan(this->_table_wrapper_a, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_a->execute();

  EXPECT_THROW(
      this->test_join_output(scan_a, this->_table_wrapper_b, {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals},
                             JoinMode::Right, "resources/test_data/tbl/join_operators/int_right_join_equals.tbl", 1,
                             true, IndexSide::Left),
      std::logic_error);
}

TYPED_TEST(OperatorsJoinIndexTest, LeftJoinPruneInputIsRefIndexInputIsDataIndexSideIsLeft) {
  // scan that returns all rows
  auto scan_b = create_table_scan(this->_table_wrapper_b, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_b->execute();

  this->test_join_output(this->_table_wrapper_a, scan_b, {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals},
                         JoinMode::Left, "resources/test_data/tbl/join_operators/int_left_join_equals.tbl", 1, true,
                         IndexSide::Left);
}

TYPED_TEST(OperatorsJoinIndexTest, LeftJoinPruneInputIsRefIndexInputIsDataIndexSideIsRight) {
  // scan that returns all rows
  auto scan_b = create_table_scan(this->_table_wrapper_b, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_b->execute();

  EXPECT_THROW(
      this->test_join_output(this->_table_wrapper_a, scan_b, {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals},
                             JoinMode::Left, "resources/test_data/tbl/join_operators/int_left_join_equals.tbl", 1, true,
                             IndexSide::Right),
      std::logic_error);
}

TYPED_TEST(OperatorsJoinIndexTest, RightJoinPruneInputIsEmptyRefIndexInputIsDataIndexSideIsRight) {
  // scan that returns no rows
  auto scan_a = create_table_scan(this->_table_wrapper_a, ColumnID{0}, PredicateCondition::Equals, 0);
  scan_a->execute();

  this->test_join_output(scan_a, this->_table_wrapper_b, {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals},
                         JoinMode::Right, "resources/test_data/tbl/join_operators/int_join_empty.tbl", 1);
}

TYPED_TEST(OperatorsJoinIndexTest, RightJoinPruneInputIsEmptyRefIndexInputIsDataIndexSideIsLeft) {
  // scan that returns no rows
  auto scan_a = create_table_scan(this->_table_wrapper_a, ColumnID{0}, PredicateCondition::Equals, 0);
  scan_a->execute();

  EXPECT_THROW(
      this->test_join_output(scan_a, this->_table_wrapper_b, {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals},
                             JoinMode::Right, "resources/test_data/tbl/join_operators/int_join_empty.tbl", 1, true,
                             IndexSide::Left),
      std::logic_error);
}

TYPED_TEST(OperatorsJoinIndexTest, LeftJoinPruneInputIsEmptyRefIndexInputIsDataIndexSideIsLeft) {
  // scan that returns no rows
  auto scan_b = create_table_scan(this->_table_wrapper_b, ColumnID{0}, PredicateCondition::Equals, 0);
  scan_b->execute();

  this->test_join_output(this->_table_wrapper_b, scan_b, {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals},
                         JoinMode::Left, "resources/test_data/tbl/join_operators/int_join_empty_left.tbl", 1, true,
                         IndexSide::Left);
}

TYPED_TEST(OperatorsJoinIndexTest, LeftJoinPruneInputIsEmptyRefIndexInputIsDataIndexSideIsRight) {
  // scan that returns no rows
  auto scan_b = create_table_scan(this->_table_wrapper_b, ColumnID{0}, PredicateCondition::Equals, 0);
  scan_b->execute();

  EXPECT_THROW(
      this->test_join_output(this->_table_wrapper_b, scan_b, {{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals},
                             JoinMode::Left, "resources/test_data/tbl/join_operators/int_join_empty_left.tbl", 1, true,
                             IndexSide::Right),
      std::logic_error);
}*/

}  // namespace opossum
