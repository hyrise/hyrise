#include <map>
#include <memory>
#include <numeric>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"

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

template <typename DerivedIndex>
class JoinIndexTest : public BaseTest {
 public:
  static void SetUpTestCase() {  // called ONCE before the tests
    // load and create the indexed tables
    _table_wrapper_a = load_table_with_index("src/test/tables/int_float.tbl", 2);
    _table_wrapper_a_no_index = std::make_shared<TableWrapper>(load_table("src/test/tables/int_float.tbl", 2));
    _table_wrapper_b = load_table_with_index("src/test/tables/int_float2.tbl", 2);
    _table_wrapper_b_no_index = std::make_shared<TableWrapper>(load_table("src/test/tables/int_float2.tbl", 2));
    _table_wrapper_c = load_table_with_index("src/test/tables/int_string.tbl", 4);
    _table_wrapper_d = load_table_with_index("src/test/tables/string_int.tbl", 3);
    _table_wrapper_e = load_table_with_index("src/test/tables/int_int.tbl", 4);
    _table_wrapper_f = load_table_with_index("src/test/tables/int_int2.tbl", 4);
    _table_wrapper_g = load_table_with_index("src/test/tables/int_int3.tbl", 4);
    _table_wrapper_h = load_table_with_index("src/test/tables/int_int4.tbl", 4);
    _table_wrapper_i = load_table_with_index("src/test/tables/int5.tbl", 1);
    _table_wrapper_j = load_table_with_index("src/test/tables/int3.tbl", 1);
    _table_wrapper_k = load_table_with_index("src/test/tables/int4.tbl", 1);
    _table_wrapper_l = load_table_with_index("src/test/tables/int.tbl", 1);
    _table_wrapper_m =
        load_table_with_index("src/test/tables/aggregateoperator/groupby_int_1gb_0agg/input_null.tbl", 20);
    _table_wrapper_n =
        load_table_with_index("src/test/tables/aggregateoperator/groupby_int_1gb_1agg/input_null.tbl", 20);

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

 protected:
  void SetUp() override {}

  static std::shared_ptr<TableWrapper> load_table_with_index(const std::string& filename, const size_t chunk_size) {
    auto table = load_table(filename, chunk_size);

    ChunkEncoder::encode_all_chunks(table, SegmentEncodingSpec{EncodingType::Dictionary});

    for (ChunkID chunk_id{0}; chunk_id < table->chunk_count(); ++chunk_id) {
      auto chunk = table->get_chunk(chunk_id);

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
                               const std::pair<ColumnID, ColumnID>& column_ids,
                               const PredicateCondition predicate_condition, const JoinMode mode,
                               const std::string& file_name, size_t chunk_size, bool using_index = true) {
    // load expected results from file
    std::shared_ptr<Table> expected_result = load_table(file_name, chunk_size);
    EXPECT_NE(expected_result, nullptr) << "Could not load expected result table";

    // build and execute join
    auto join = std::make_shared<JoinIndex>(left, right, mode, column_ids, predicate_condition);
    EXPECT_NE(join, nullptr) << "Could not build Join";
    join->execute();

    EXPECT_TABLE_EQ_UNORDERED(join->get_output(), expected_result);
    const auto& performance_data = static_cast<const JoinIndex::PerformanceData&>(join->performance_data());
    if (using_index && right->get_output()->type() == TableType::Data) {
      // We can't execute the index join on referencing tables
      EXPECT_EQ(performance_data.chunks_scanned_with_index, static_cast<size_t>(right->get_output()->chunk_count()));
      EXPECT_EQ(performance_data.chunks_scanned_without_index, 0);
    } else {
      EXPECT_EQ(performance_data.chunks_scanned_with_index, 0);
      EXPECT_EQ(performance_data.chunks_scanned_without_index, static_cast<size_t>(right->get_output()->chunk_count()));
    }
  }

  inline static std::shared_ptr<TableWrapper> _table_wrapper_a, _table_wrapper_a_no_index, _table_wrapper_b,
      _table_wrapper_b_no_index, _table_wrapper_c, _table_wrapper_d, _table_wrapper_e, _table_wrapper_f,
      _table_wrapper_g, _table_wrapper_h, _table_wrapper_i, _table_wrapper_j, _table_wrapper_k, _table_wrapper_l,
      _table_wrapper_m, _table_wrapper_n;
};

typedef ::testing::Types<AdaptiveRadixTreeIndex, CompositeGroupKeyIndex, BTreeIndex /* , GroupKeyIndex */>
    DerivedIndices;

TYPED_TEST_CASE(JoinIndexTest, DerivedIndices);

TYPED_TEST(JoinIndexTest, LeftJoinFallBack) {
  this->test_join_output(this->_table_wrapper_a_no_index, this->_table_wrapper_b_no_index,
                         std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals,
                         JoinMode::Left, "src/test/tables/joinoperators/int_left_join.tbl", 1, false);
}

TYPED_TEST(JoinIndexTest, LeftJoin) {
  this->test_join_output(this->_table_wrapper_a, this->_table_wrapper_b,
                         std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals,
                         JoinMode::Left, "src/test/tables/joinoperators/int_left_join.tbl", 1);
}

TYPED_TEST(JoinIndexTest, LeftJoinOnString) {
  this->test_join_output(this->_table_wrapper_c, this->_table_wrapper_d,
                         std::pair<ColumnID, ColumnID>(ColumnID{1}, ColumnID{0}), PredicateCondition::Equals,
                         JoinMode::Left, "src/test/tables/joinoperators/string_left_join.tbl", 1);
}

TYPED_TEST(JoinIndexTest, RightJoin) {
  this->test_join_output(this->_table_wrapper_a, this->_table_wrapper_b,
                         std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals,
                         JoinMode::Right, "src/test/tables/joinoperators/int_right_join.tbl", 1);
}

TYPED_TEST(JoinIndexTest, RightJoinFallBack) {
  this->test_join_output(this->_table_wrapper_a_no_index, this->_table_wrapper_b_no_index,
                         std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals,
                         JoinMode::Right, "src/test/tables/joinoperators/int_right_join.tbl", 1, false);
}

TYPED_TEST(JoinIndexTest, InnerJoin) {
  this->test_join_output(this->_table_wrapper_a, this->_table_wrapper_b,
                         std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals,
                         JoinMode::Inner, "src/test/tables/joinoperators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinIndexTest, InnerJoinOnString) {
  this->test_join_output(this->_table_wrapper_c, this->_table_wrapper_d,
                         std::pair<ColumnID, ColumnID>(ColumnID{1}, ColumnID{0}), PredicateCondition::Equals,
                         JoinMode::Inner, "src/test/tables/joinoperators/string_inner_join.tbl", 1);
}

TYPED_TEST(JoinIndexTest, InnerJoinSingleChunk) {
  this->test_join_output(this->_table_wrapper_e, this->_table_wrapper_f,
                         std::pair<ColumnID, ColumnID>(ColumnID{1}, ColumnID{0}), PredicateCondition::Equals,
                         JoinMode::Inner, "src/test/tables/joinoperators/int_inner_join_single_chunk.tbl", 1);
}

TYPED_TEST(JoinIndexTest, InnerRefJoin) {
  // scan that returns all rows
  auto scan_a = this->create_table_scan(this->_table_wrapper_a, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_a->execute();
  auto scan_b = this->create_table_scan(this->_table_wrapper_b, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_b->execute();

  this->test_join_output(scan_a, scan_b, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}),
                         PredicateCondition::Equals, JoinMode::Inner,
                         "src/test/tables/joinoperators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinIndexTest, InnerRefJoinFiltered) {
  auto scan_a = this->create_table_scan(this->_table_wrapper_a, ColumnID{0}, PredicateCondition::GreaterThan, 1000);
  scan_a->execute();
  auto scan_b = this->create_table_scan(this->_table_wrapper_b, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_b->execute();

  this->test_join_output(scan_a, scan_b, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}),
                         PredicateCondition::Equals, JoinMode::Inner,
                         "src/test/tables/joinoperators/int_inner_join_filtered.tbl", 1);
}

TYPED_TEST(JoinIndexTest, InnerDictJoin) {
  this->test_join_output(this->_table_wrapper_a, this->_table_wrapper_b,
                         std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals,
                         JoinMode::Inner, "src/test/tables/joinoperators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinIndexTest, InnerRefDictJoin) {
  // scan that returns all rows
  auto scan_a = this->create_table_scan(this->_table_wrapper_a, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_a->execute();
  auto scan_b = this->create_table_scan(this->_table_wrapper_b, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_b->execute();

  this->test_join_output(scan_a, scan_b, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}),
                         PredicateCondition::Equals, JoinMode::Inner,
                         "src/test/tables/joinoperators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinIndexTest, InnerRefDictJoinFiltered) {
  auto scan_a = this->create_table_scan(this->_table_wrapper_a, ColumnID{0}, PredicateCondition::GreaterThan, 1000);
  scan_a->execute();
  auto scan_b = this->create_table_scan(this->_table_wrapper_b, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_b->execute();

  this->test_join_output(scan_a, scan_b, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}),
                         PredicateCondition::Equals, JoinMode::Inner,
                         "src/test/tables/joinoperators/int_inner_join_filtered.tbl", 1);
}

TYPED_TEST(JoinIndexTest, InnerJoinBig) {
  this->test_join_output(this->_table_wrapper_c, this->_table_wrapper_d,
                         std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{1}), PredicateCondition::Equals,
                         JoinMode::Inner, "src/test/tables/joinoperators/int_string_inner_join.tbl", 1);
}

TYPED_TEST(JoinIndexTest, InnerRefJoinFilteredBig) {
  auto scan_c = this->create_table_scan(this->_table_wrapper_c, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_c->execute();
  auto scan_d = this->create_table_scan(this->_table_wrapper_d, ColumnID{1}, PredicateCondition::GreaterThanEquals, 6);
  scan_d->execute();

  this->test_join_output(scan_c, scan_d, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{1}),
                         PredicateCondition::Equals, JoinMode::Inner,
                         "src/test/tables/joinoperators/int_string_inner_join_filtered.tbl", 1);
}

TYPED_TEST(JoinIndexTest, OuterJoin) {
  this->test_join_output(this->_table_wrapper_a, this->_table_wrapper_b,
                         std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals,
                         JoinMode::Outer, "src/test/tables/joinoperators/int_outer_join.tbl", 1);
}

/* currently disabled because it breaks with AdaptiveRadixTreeIndex */
TYPED_TEST(JoinIndexTest, DISABLED_OuterJoinWithNull /* #670 */) {
  this->test_join_output(this->_table_wrapper_m, this->_table_wrapper_n,
                         std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals,
                         JoinMode::Outer, "src/test/tables/joinoperators/int_outer_join_null.tbl", 1);
}

TYPED_TEST(JoinIndexTest, OuterJoinDict) {
  this->test_join_output(this->_table_wrapper_a, this->_table_wrapper_b,
                         std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals,
                         JoinMode::Outer, "src/test/tables/joinoperators/int_outer_join.tbl", 1);
}

TYPED_TEST(JoinIndexTest, SmallerInnerJoin) {
  // Joining two Integer Columns
  this->test_join_output(this->_table_wrapper_a, this->_table_wrapper_b,
                         std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), PredicateCondition::LessThan,
                         JoinMode::Inner, "src/test/tables/joinoperators/int_smaller_inner_join.tbl", 1);

  // Joining two Float Columns
  this->test_join_output(this->_table_wrapper_a, this->_table_wrapper_b,
                         std::pair<ColumnID, ColumnID>(ColumnID{1}, ColumnID{1}), PredicateCondition::LessThan,
                         JoinMode::Inner, "src/test/tables/joinoperators/float_smaller_inner_join.tbl", 1);
}

TYPED_TEST(JoinIndexTest, SmallerInnerJoinDict) {
  // Joining two Integer Columns
  this->test_join_output(this->_table_wrapper_a, this->_table_wrapper_b,
                         std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), PredicateCondition::LessThan,
                         JoinMode::Inner, "src/test/tables/joinoperators/int_smaller_inner_join.tbl", 1);

  // Joining two Float Columns
  this->test_join_output(this->_table_wrapper_a, this->_table_wrapper_b,
                         std::pair<ColumnID, ColumnID>(ColumnID{1}, ColumnID{1}), PredicateCondition::LessThan,
                         JoinMode::Inner, "src/test/tables/joinoperators/float_smaller_inner_join.tbl", 1);
}

TYPED_TEST(JoinIndexTest, SmallerInnerJoin2) {
  // Joining two Integer Columns
  this->test_join_output(this->_table_wrapper_j, this->_table_wrapper_i,
                         std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), PredicateCondition::LessThan,
                         JoinMode::Inner, "src/test/tables/joinoperators/int_smaller_inner_join_2.tbl", 1);
}

TYPED_TEST(JoinIndexTest, SmallerOuterJoin) {
  this->test_join_output(this->_table_wrapper_k, this->_table_wrapper_l,
                         std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), PredicateCondition::LessThan,
                         JoinMode::Outer, "src/test/tables/joinoperators/int_smaller_outer_join.tbl", 1);
}

TYPED_TEST(JoinIndexTest, SmallerEqualInnerJoin) {
  // Joining two Integer Columns
  this->test_join_output(this->_table_wrapper_a, this->_table_wrapper_b,
                         std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), PredicateCondition::LessThanEquals,
                         JoinMode::Inner, "src/test/tables/joinoperators/int_smallerequal_inner_join.tbl", 1);

  // Joining two Float Columns
  this->test_join_output(this->_table_wrapper_a, this->_table_wrapper_b,
                         std::pair<ColumnID, ColumnID>(ColumnID{1}, ColumnID{1}), PredicateCondition::LessThanEquals,
                         JoinMode::Inner, "src/test/tables/joinoperators/float_smallerequal_inner_join.tbl", 1);
}

TYPED_TEST(JoinIndexTest, SmallerEqualInnerJoin2) {
  // Joining two Integer Columns
  this->test_join_output(this->_table_wrapper_j, this->_table_wrapper_i,
                         std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), PredicateCondition::LessThanEquals,
                         JoinMode::Inner, "src/test/tables/joinoperators/int_smallerequal_inner_join_2.tbl", 1);
}

TYPED_TEST(JoinIndexTest, SmallerEqualOuterJoin) {
  this->test_join_output(this->_table_wrapper_k, this->_table_wrapper_l,
                         std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), PredicateCondition::LessThanEquals,
                         JoinMode::Outer, "src/test/tables/joinoperators/int_smallerequal_outer_join.tbl", 1);
}

TYPED_TEST(JoinIndexTest, GreaterInnerJoin) {
  // Joining two Integer Columns
  this->test_join_output(this->_table_wrapper_a, this->_table_wrapper_b,
                         std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), PredicateCondition::GreaterThan,
                         JoinMode::Inner, "src/test/tables/joinoperators/int_greater_inner_join.tbl", 1);

  // Joining two Float Columns
  this->test_join_output(this->_table_wrapper_a, this->_table_wrapper_b,
                         std::pair<ColumnID, ColumnID>(ColumnID{1}, ColumnID{1}), PredicateCondition::GreaterThan,
                         JoinMode::Inner, "src/test/tables/joinoperators/float_greater_inner_join.tbl", 1);
}

TYPED_TEST(JoinIndexTest, GreaterInnerJoinDict) {
  // Joining two Integer Columns
  this->test_join_output(this->_table_wrapper_a, this->_table_wrapper_b,
                         std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), PredicateCondition::GreaterThan,
                         JoinMode::Inner, "src/test/tables/joinoperators/int_greater_inner_join.tbl", 1);

  // Joining two Float Columns
  this->test_join_output(this->_table_wrapper_a, this->_table_wrapper_b,
                         std::pair<ColumnID, ColumnID>(ColumnID{1}, ColumnID{1}), PredicateCondition::GreaterThan,
                         JoinMode::Inner, "src/test/tables/joinoperators/float_greater_inner_join.tbl", 1);
}

TYPED_TEST(JoinIndexTest, GreaterInnerJoin2) {
  // Joining two Integer Columns
  this->test_join_output(this->_table_wrapper_i, this->_table_wrapper_j,
                         std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), PredicateCondition::GreaterThan,
                         JoinMode::Inner, "src/test/tables/joinoperators/int_greater_inner_join_2.tbl", 1);
}

TYPED_TEST(JoinIndexTest, GreaterOuterJoin) {
  this->test_join_output(this->_table_wrapper_l, this->_table_wrapper_k,
                         std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), PredicateCondition::GreaterThan,
                         JoinMode::Outer, "src/test/tables/joinoperators/int_greater_outer_join.tbl", 1);
}

TYPED_TEST(JoinIndexTest, GreaterEqualInnerJoin) {
  // Joining two Integer Columns
  this->test_join_output(this->_table_wrapper_a, this->_table_wrapper_b,
                         std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), PredicateCondition::GreaterThanEquals,
                         JoinMode::Inner, "src/test/tables/joinoperators/int_greaterequal_inner_join.tbl", 1);

  // Joining two Float Columns
  this->test_join_output(this->_table_wrapper_a, this->_table_wrapper_b,
                         std::pair<ColumnID, ColumnID>(ColumnID{1}, ColumnID{1}), PredicateCondition::GreaterThanEquals,
                         JoinMode::Inner, "src/test/tables/joinoperators/float_greaterequal_inner_join.tbl", 1);
}

TYPED_TEST(JoinIndexTest, GreaterEqualInnerJoinDict) {
  // Joining two Integer Columns
  this->test_join_output(this->_table_wrapper_a, this->_table_wrapper_b,
                         std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), PredicateCondition::GreaterThanEquals,
                         JoinMode::Inner, "src/test/tables/joinoperators/int_greaterequal_inner_join.tbl", 1);

  // Joining two Float Columns
  this->test_join_output(this->_table_wrapper_a, this->_table_wrapper_b,
                         std::pair<ColumnID, ColumnID>(ColumnID{1}, ColumnID{1}), PredicateCondition::GreaterThanEquals,
                         JoinMode::Inner, "src/test/tables/joinoperators/float_greaterequal_inner_join.tbl", 1);
}

TYPED_TEST(JoinIndexTest, GreaterEqualOuterJoin) {
  this->test_join_output(this->_table_wrapper_l, this->_table_wrapper_k,
                         std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), PredicateCondition::GreaterThanEquals,
                         JoinMode::Outer, "src/test/tables/joinoperators/int_greaterequal_outer_join.tbl", 1);
}

TYPED_TEST(JoinIndexTest, GreaterEqualInnerJoin2) {
  // Joining two Integer Columns
  this->test_join_output(this->_table_wrapper_i, this->_table_wrapper_j,
                         std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), PredicateCondition::GreaterThanEquals,
                         JoinMode::Inner, "src/test/tables/joinoperators/int_greaterequal_inner_join_2.tbl", 1);
}

TYPED_TEST(JoinIndexTest, NotEqualInnerJoin) {
  // Joining two Integer Columns
  this->test_join_output(this->_table_wrapper_a, this->_table_wrapper_b,
                         std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), PredicateCondition::NotEquals,
                         JoinMode::Inner, "src/test/tables/joinoperators/int_notequal_inner_join.tbl", 1);
  // Joining two Float Columns
  this->test_join_output(this->_table_wrapper_a, this->_table_wrapper_b,
                         std::pair<ColumnID, ColumnID>(ColumnID{1}, ColumnID{1}), PredicateCondition::NotEquals,
                         JoinMode::Inner, "src/test/tables/joinoperators/float_notequal_inner_join.tbl", 1);
}

TYPED_TEST(JoinIndexTest, NotEqualInnerJoinDict) {
  // Joining two Integer Columns
  this->test_join_output(this->_table_wrapper_a, this->_table_wrapper_b,
                         std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), PredicateCondition::NotEquals,
                         JoinMode::Inner, "src/test/tables/joinoperators/int_notequal_inner_join.tbl", 1);
  // Joining two Float Columns
  this->test_join_output(this->_table_wrapper_a, this->_table_wrapper_b,
                         std::pair<ColumnID, ColumnID>(ColumnID{1}, ColumnID{1}), PredicateCondition::NotEquals,
                         JoinMode::Inner, "src/test/tables/joinoperators/float_notequal_inner_join.tbl", 1);
}

TYPED_TEST(JoinIndexTest, JoinLessThanOnDictAndDict) {
  this->test_join_output(this->_table_wrapper_a, this->_table_wrapper_b,
                         std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), PredicateCondition::LessThanEquals,
                         JoinMode::Inner, "src/test/tables/joinoperators/int_float_leq_dict.tbl", 1);
}

TYPED_TEST(JoinIndexTest, JoinOnReferenceSegmentAndDict) {
  // scan that returns all rows
  auto scan_a = this->create_table_scan(this->_table_wrapper_a, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_a->execute();

  this->test_join_output(scan_a, this->_table_wrapper_b, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}),
                         PredicateCondition::Equals, JoinMode::Inner,
                         "src/test/tables/joinoperators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinIndexTest, JoinOnDictAndReferenceSegment) {
  // scan that returns all rows
  auto scan_b = this->create_table_scan(this->_table_wrapper_b, ColumnID{0}, PredicateCondition::GreaterThan, 100);
  scan_b->execute();

  this->test_join_output(this->_table_wrapper_a, scan_b, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}),
                         PredicateCondition::NotEquals, JoinMode::Inner,
                         "src/test/tables/joinoperators/int_inner_join_neq.tbl", 1);
}

TYPED_TEST(JoinIndexTest, MultiJoinOnReferenceLeft) {
  // scan that returns all rows
  auto scan_a = this->create_table_scan(this->_table_wrapper_f, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_a->execute();
  auto scan_b = this->create_table_scan(this->_table_wrapper_g, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_b->execute();
  auto scan_c = this->create_table_scan(this->_table_wrapper_h, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_c->execute();

  auto join =
      std::make_shared<JoinIndex>(scan_a, scan_b, JoinMode::Inner,
                                  std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals);
  join->execute();

  this->test_join_output(join, scan_c, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}),
                         PredicateCondition::Equals, JoinMode::Inner,
                         "src/test/tables/joinoperators/int_inner_multijoin_ref_ref_ref_left.tbl", 1);
}

TYPED_TEST(JoinIndexTest, MultiJoinOnReferenceRight) {
  // scan that returns all rows
  auto scan_a = this->create_table_scan(this->_table_wrapper_f, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_a->execute();
  auto scan_b = this->create_table_scan(this->_table_wrapper_g, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_b->execute();
  auto scan_c = this->create_table_scan(this->_table_wrapper_h, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_c->execute();

  auto join =
      std::make_shared<JoinIndex>(scan_a, scan_b, JoinMode::Inner,
                                  std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals);
  join->execute();

  this->test_join_output(scan_c, join, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}),
                         PredicateCondition::Equals, JoinMode::Inner,
                         "src/test/tables/joinoperators/int_inner_multijoin_ref_ref_ref_right.tbl", 1);
}

TYPED_TEST(JoinIndexTest, MultiJoinOnReferenceLeftFiltered) {
  // scan that returns all rows
  auto scan_a = this->create_table_scan(this->_table_wrapper_f, ColumnID{0}, PredicateCondition::GreaterThan, 6);
  scan_a->execute();
  auto scan_b = this->create_table_scan(this->_table_wrapper_g, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_b->execute();
  auto scan_c = this->create_table_scan(this->_table_wrapper_h, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_c->execute();

  auto join =
      std::make_shared<JoinIndex>(scan_a, scan_b, JoinMode::Inner,
                                  std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals);
  join->execute();

  this->test_join_output(join, scan_c, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}),
                         PredicateCondition::Equals, JoinMode::Inner,
                         "src/test/tables/joinoperators/int_inner_multijoin_ref_ref_ref_left_filtered.tbl", 1);
}

TYPED_TEST(JoinIndexTest, MultiJoinOnRefOuter) {
  auto join =
      std::make_shared<JoinIndex>(this->_table_wrapper_f, this->_table_wrapper_g, JoinMode::Left,
                                  std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals);
  join->execute();

  this->test_join_output(join, this->_table_wrapper_h, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}),
                         PredicateCondition::Equals, JoinMode::Inner,
                         "src/test/tables/joinoperators/int_inner_multijoin_val_val_val_leftouter.tbl", 1);
}

TYPED_TEST(JoinIndexTest, RightJoinRefSegment) {
  // scan that returns all rows
  auto scan_a = this->create_table_scan(this->_table_wrapper_a, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_a->execute();

  this->test_join_output(scan_a, this->_table_wrapper_b, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}),
                         PredicateCondition::Equals, JoinMode::Right,
                         "src/test/tables/joinoperators/int_right_join.tbl", 1);
}

TYPED_TEST(JoinIndexTest, LeftJoinRefSegment) {
  // scan that returns all rows
  auto scan_b = this->create_table_scan(this->_table_wrapper_b, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_b->execute();

  this->test_join_output(this->_table_wrapper_a, scan_b, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}),
                         PredicateCondition::Equals, JoinMode::Left, "src/test/tables/joinoperators/int_left_join.tbl",
                         1);
}

TYPED_TEST(JoinIndexTest, RightJoinEmptyRefSegment) {
  // scan that returns no rows
  auto scan_a = this->create_table_scan(this->_table_wrapper_a, ColumnID{0}, PredicateCondition::Equals, 0);
  scan_a->execute();

  this->test_join_output(scan_a, this->_table_wrapper_b, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}),
                         PredicateCondition::Equals, JoinMode::Right,
                         "src/test/tables/joinoperators/int_join_empty.tbl", 1);
}

TYPED_TEST(JoinIndexTest, LeftJoinEmptyRefSegment) {
  // scan that returns no rows
  auto scan_b = this->create_table_scan(this->_table_wrapper_b, ColumnID{0}, PredicateCondition::Equals, 0);
  scan_b->execute();

  this->test_join_output(this->_table_wrapper_b, scan_b, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}),
                         PredicateCondition::Equals, JoinMode::Left,
                         "src/test/tables/joinoperators/int_join_empty_left.tbl", 1);
}

}  // namespace opossum
