#include <map>
#include <memory>
#include <numeric>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "all_type_variant.hpp"
#include "operators/join_index.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/table_scan.hpp"
#include "storage/dictionary_compression.hpp"
#include "storage/index/adaptive_radix_tree/adaptive_radix_tree_index.hpp"
#include "storage/index/group_key/composite_group_key_index.hpp"
#include "storage/index/group_key/group_key_index.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

template <typename DerivedIndex>
class OperatorsJoinIndexTest : public BaseTest {
 protected:
  void SetUp() override {
    _index_type = get_index_type_of<DerivedIndex>();

    auto left_table = std::make_shared<Table>(5);
    left_table->add_column("left.a", DataType::Int);
    left_table->add_column("left.b", DataType::Int);
    for (int i = 0; i <= 24; i += 1) left_table->append({i, 100 + i});
    DictionaryCompression::compress_table(*left_table);

    auto right_table = std::make_shared<Table>(5);
    right_table->add_column("right.a", DataType::Int);
    right_table->add_column("right.b", DataType::Int);
    for (int i = 0; i <= 48; i += 2) right_table->append({i, 100 + i});
    DictionaryCompression::compress_table(*right_table);

    _chunk_ids = std::vector<ChunkID>(right_table->chunk_count());
    std::iota(_chunk_ids.begin(), _chunk_ids.end(), ChunkID{0u});

    _column_ids = std::vector<ColumnID>{ColumnID{0u}};

    for (const auto& chunk_id : _chunk_ids) {
      auto& chunk = right_table->get_chunk(chunk_id);
      chunk.create_index<DerivedIndex>(_column_ids);
    }

    _table_wrapper_left = std::make_shared<TableWrapper>(left_table);
    _table_wrapper_left->execute();

    _table_wrapper_right = std::make_shared<TableWrapper>(right_table);
    _table_wrapper_right->execute();
  }

  void test_join_output(const std::shared_ptr<const AbstractOperator> left,
                        const std::shared_ptr<const AbstractOperator> right,
                        const std::pair<ColumnID, ColumnID>& column_ids, const ScanType scan_type, const JoinMode mode,
                        std::shared_ptr<Table> expected_result, size_t chunk_size) {
    // build and execute join
    auto join = std::make_shared<JoinIndex>(left, right, mode, column_ids, scan_type);
    EXPECT_NE(join, nullptr) << "Could not build Join";
    join->execute();

    auto result = join->get_output();

    EXPECT_TABLE_EQ_UNORDERED(join->get_output(), expected_result);
  }

  std::shared_ptr<TableWrapper> _table_wrapper_left;
  std::shared_ptr<TableWrapper> _table_wrapper_right;
  std::shared_ptr<TableWrapper> _result_table_wrapper;
  std::vector<ChunkID> _chunk_ids;
  std::vector<ColumnID> _column_ids;
  ColumnIndexType _index_type;
};

template <typename DerivedIndex>
class JoinIndexTest : public BaseTest {
 protected:
  void SetUp() override {
    // load and create regular ValueColumn tables
    _table_wrapper_a = load_table_index("src/test/tables/int_float.tbl", 2);
    _table_wrapper_b = load_table_index("src/test/tables/int_float2.tbl", 2);
    _table_wrapper_c = load_table_index("src/test/tables/int_string.tbl", 4);
    _table_wrapper_d = load_table_index("src/test/tables/string_int.tbl", 3);
    _table_wrapper_e = load_table_index("src/test/tables/int_int.tbl", 4);
    _table_wrapper_f = load_table_index("src/test/tables/int_int2.tbl", 4);
    _table_wrapper_g = load_table_index("src/test/tables/int_int3.tbl", 4);
    _table_wrapper_h = load_table_index("src/test/tables/int_int4.tbl", 4);
    _table_wrapper_i = load_table_index("src/test/tables/int5.tbl", 1);
    _table_wrapper_j = load_table_index("src/test/tables/int3.tbl", 1);
    _table_wrapper_k = load_table_index("src/test/tables/int4.tbl", 1);
    _table_wrapper_l = load_table_index("src/test/tables/int.tbl", 1);
    _table_wrapper_m = load_table_index("src/test/tables/aggregateoperator/groupby_int_1gb_0agg/input_null.tbl", 20);
    _table_wrapper_n = load_table_index("src/test/tables/aggregateoperator/groupby_int_1gb_1agg/input_null.tbl", 20);

    // load and create DictionaryColumn tables
    auto table = load_table("src/test/tables/int_float.tbl", 2);
    DictionaryCompression::compress_chunks(*table, {ChunkID{0}, ChunkID{1}});
    _table_wrapper_a_dict = std::make_shared<TableWrapper>(std::move(table));

    table = load_table("src/test/tables/int_float2.tbl", 2);
    DictionaryCompression::compress_chunks(*table, {ChunkID{0}, ChunkID{1}});
    _table_wrapper_b_dict = std::make_shared<TableWrapper>(std::move(table));

    table = load_table("src/test/tables/int_float.tbl", 2);
    DictionaryCompression::compress_chunks(*table, {ChunkID{0}});
    _table_wrapper_c_dict = std::make_shared<TableWrapper>(std::move(table));

    table = load_table("src/test/tables/aggregateoperator/groupby_int_1gb_0agg/input_null.tbl", 20);
    DictionaryCompression::compress_chunks(*table, {ChunkID{0}});
    _table_wrapper_m_dict = std::make_shared<TableWrapper>(std::move(table));

    table = load_table("src/test/tables/aggregateoperator/groupby_int_1gb_1agg/input_null.tbl", 20);
    DictionaryCompression::compress_chunks(*table, {ChunkID{0}});
    _table_wrapper_n_dict = std::make_shared<TableWrapper>(std::move(table));

    // execute all TableWrapper operators in advance
    _table_wrapper_a->execute();
    _table_wrapper_b->execute();
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
    _table_wrapper_a_dict->execute();
    _table_wrapper_b_dict->execute();
    _table_wrapper_c_dict->execute();
    _table_wrapper_m_dict->execute();
    _table_wrapper_n_dict->execute();
  }

  std::shared_ptr<TableWrapper> load_table_index(const std::string& filename, size_t chunk_size){
    auto table = load_table(filename, chunk_size);

    DictionaryCompression::compress_table(*table);

    for(ChunkID chunk_id{0}; chunk_id < table->chunk_count(); ++chunk_id){
        auto& chunk = table->get_chunk(chunk_id);


        std::vector <ColumnID> columns{1};
        for(ColumnID column_id{0}; column_id < chunk.column_count(); ++column_id){
            columns[0]=column_id;
            chunk.create_index<DerivedIndex>(columns);
        }
    }

    return std::make_shared<TableWrapper>(table);

  }

  // builds and executes the given Join and checks correctness of the output
  void test_join_output(const std::shared_ptr<const AbstractOperator> left,
                        const std::shared_ptr<const AbstractOperator> right,
                        const std::pair<ColumnID, ColumnID>& column_ids, const ScanType scan_type, const JoinMode mode,
                        const std::string& file_name, size_t chunk_size) {
    // load expected results from file
    std::shared_ptr<Table> expected_result = load_table(file_name, chunk_size);
    EXPECT_NE(expected_result, nullptr) << "Could not load expected result table";

    // build and execute join
    auto join = std::make_shared<JoinIndex>(left, right, mode, column_ids, scan_type);
    EXPECT_NE(join, nullptr) << "Could not build Join";
    join->execute();

    EXPECT_TABLE_EQ_UNORDERED(join->get_output(), expected_result);
  }

  std::shared_ptr<TableWrapper> _table_wrapper_a, _table_wrapper_b, _table_wrapper_c, _table_wrapper_d,
      _table_wrapper_e, _table_wrapper_f, _table_wrapper_g, _table_wrapper_h, _table_wrapper_i, _table_wrapper_j,
      _table_wrapper_k, _table_wrapper_l, _table_wrapper_m, _table_wrapper_n, _table_wrapper_a_dict,
      _table_wrapper_b_dict, _table_wrapper_c_dict, _table_wrapper_m_dict, _table_wrapper_n_dict;
};

typedef ::testing::Types<AdaptiveRadixTreeIndex, CompositeGroupKeyIndex> DerivedIndices;

TYPED_TEST_CASE(OperatorsJoinIndexTest, DerivedIndices);
TYPED_TEST_CASE(JoinIndexTest, DerivedIndices);

TYPED_TEST(OperatorsJoinIndexTest, SimpleInnerJoin) {
  auto result_table = std::make_shared<Table>();
  result_table->add_column("left.a", DataType::Int);
  result_table->add_column("left.b", DataType::Int);
  result_table->add_column("right.a", DataType::Int);
  result_table->add_column("right.b", DataType::Int);
  for (int i = 0; i <= 24; i += 2) result_table->append({i, i + 100, i, i + 100});
  DictionaryCompression::compress_table(*result_table);

  this->test_join_output(this->_table_wrapper_left, this->_table_wrapper_right,
                         std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), ScanType::OpEquals, JoinMode::Inner,
                         result_table, 1);
}

TYPED_TEST(OperatorsJoinIndexTest, SimpleLeftJoin) {
  auto result_table = std::make_shared<Table>();
  result_table->add_column("left.a", DataType::Int);
  result_table->add_column("left.b", DataType::Int);
  result_table->add_column("right.a", DataType::Int, true);
  result_table->add_column("right.b", DataType::Int, true);
  for (int i = 0; i <= 24; i += 2) result_table->append({i, i + 100, i, i + 100});
  for (int i = 1; i <= 24; i += 2) result_table->append({i, i + 100, NullValue{}, NullValue{}});
  DictionaryCompression::compress_table(*result_table);

  this->test_join_output(this->_table_wrapper_left, this->_table_wrapper_right,
                         std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), ScanType::OpEquals, JoinMode::Left,
                         result_table, 1);
}

TYPED_TEST(OperatorsJoinIndexTest, SimpleRightJoin) {
  auto result_table = std::make_shared<Table>();
  result_table->add_column("left.a", DataType::Int, true);
  result_table->add_column("left.b", DataType::Int, true);
  result_table->add_column("right.a", DataType::Int);
  result_table->add_column("right.b", DataType::Int);
  for (int i = 0; i <= 24; i += 2) result_table->append({i, i + 100, i, i + 100});
  for (int i = 26; i <= 48; i += 2) result_table->append({NullValue{}, NullValue{}, i, i + 100});
  DictionaryCompression::compress_table(*result_table);

  this->test_join_output(this->_table_wrapper_left, this->_table_wrapper_right,
                         std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), ScanType::OpEquals, JoinMode::Right,
                         result_table, 1);
}

TYPED_TEST(OperatorsJoinIndexTest, SimpleOuterJoin) {
  auto result_table = std::make_shared<Table>();
  result_table->add_column("left.a", DataType::Int, true);
  result_table->add_column("left.b", DataType::Int, true);
  result_table->add_column("right.a", DataType::Int, true);
  result_table->add_column("right.b", DataType::Int, true);
  for (int i = 0; i <= 24; i += 2) result_table->append({i, i + 100, i, i + 100});
  for (int i = 26; i <= 48; i += 2) result_table->append({NullValue{}, NullValue{}, i, i + 100});
  for (int i = 1; i <= 24; i += 2) result_table->append({i, i + 100, NullValue{}, NullValue{}});
  DictionaryCompression::compress_table(*result_table);

  this->test_join_output(this->_table_wrapper_left, this->_table_wrapper_right,
                         std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), ScanType::OpEquals, JoinMode::Outer,
                         result_table, 1);
}


TYPED_TEST(JoinIndexTest, LeftJoin) {
  this->test_join_output(
      this->_table_wrapper_a, this->_table_wrapper_b, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}),
      ScanType::OpEquals, JoinMode::Left, "src/test/tables/joinoperators/int_left_join.tbl", 1);
}

TYPED_TEST(JoinIndexTest, LeftJoinOnString) {
  this->test_join_output(
      this->_table_wrapper_c, this->_table_wrapper_d, std::pair<ColumnID, ColumnID>(ColumnID{1}, ColumnID{0}),
      ScanType::OpEquals, JoinMode::Left, "src/test/tables/joinoperators/string_left_join.tbl", 1);
}

TYPED_TEST(JoinIndexTest, RightJoin) {
  this->test_join_output(
      this->_table_wrapper_a, this->_table_wrapper_b, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}),
      ScanType::OpEquals, JoinMode::Right, "src/test/tables/joinoperators/int_right_join.tbl", 1);
}

TYPED_TEST(JoinIndexTest, OuterJoin) {
  this->test_join_output(
      this->_table_wrapper_a, this->_table_wrapper_b, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}),
      ScanType::OpEquals, JoinMode::Outer, "src/test/tables/joinoperators/int_outer_join.tbl", 1);
}

TYPED_TEST(JoinIndexTest, InnerJoin) {
  this->test_join_output(
      this->_table_wrapper_a, this->_table_wrapper_b, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}),
      ScanType::OpEquals, JoinMode::Inner, "src/test/tables/joinoperators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinIndexTest, InnerJoinOnString) {
  this->test_join_output(
      this->_table_wrapper_c, this->_table_wrapper_d, std::pair<ColumnID, ColumnID>(ColumnID{1}, ColumnID{0}),
      ScanType::OpEquals, JoinMode::Inner, "src/test/tables/joinoperators/string_inner_join.tbl", 1);
}

TYPED_TEST(JoinIndexTest, InnerRefJoin) {
  // scan that returns all rows
  auto scan_a = std::make_shared<TableScan>(this->_table_wrapper_a, ColumnID{0}, ScanType::OpGreaterThanEquals, 0);
  scan_a->execute();
  auto scan_b = std::make_shared<TableScan>(this->_table_wrapper_b, ColumnID{0}, ScanType::OpGreaterThanEquals, 0);
  scan_b->execute();

  this->test_join_output(scan_a, scan_b, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}),
                                             ScanType::OpEquals, JoinMode::Inner,
                                             "src/test/tables/joinoperators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinIndexTest, InnerValueDictJoin) {
  this->test_join_output(
      this->_table_wrapper_a, this->_table_wrapper_b_dict, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}),
      ScanType::OpEquals, JoinMode::Inner, "src/test/tables/joinoperators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinIndexTest, InnerDictValueJoin) {
  this->test_join_output(
      this->_table_wrapper_a_dict, this->_table_wrapper_b, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}),
      ScanType::OpEquals, JoinMode::Inner, "src/test/tables/joinoperators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinIndexTest, InnerValueDictRefJoin) {
  // scan that returns all rows
  auto scan_a = std::make_shared<TableScan>(this->_table_wrapper_a, ColumnID{0}, ScanType::OpGreaterThanEquals, 0);
  scan_a->execute();
  auto scan_b = std::make_shared<TableScan>(this->_table_wrapper_b_dict, ColumnID{0}, ScanType::OpGreaterThanEquals, 0);
  scan_b->execute();

  this->test_join_output(scan_a, scan_b, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}),
                                             ScanType::OpEquals, JoinMode::Inner,
                                             "src/test/tables/joinoperators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinIndexTest, InnerDictValueRefJoin) {
  // scan that returns all rows
  auto scan_a = std::make_shared<TableScan>(this->_table_wrapper_a_dict, ColumnID{0}, ScanType::OpGreaterThanEquals, 0);
  scan_a->execute();
  auto scan_b = std::make_shared<TableScan>(this->_table_wrapper_b, ColumnID{0}, ScanType::OpGreaterThanEquals, 0);
  scan_b->execute();

  this->test_join_output(scan_a, scan_b, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}),
                                             ScanType::OpEquals, JoinMode::Inner,
                                             "src/test/tables/joinoperators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinIndexTest, InnerRefJoinFiltered) {
  auto scan_a = std::make_shared<TableScan>(this->_table_wrapper_a, ColumnID{0}, ScanType::OpGreaterThan, 1000);
  scan_a->execute();
  auto scan_b = std::make_shared<TableScan>(this->_table_wrapper_b, ColumnID{0}, ScanType::OpGreaterThanEquals, 0);
  scan_b->execute();

  this->test_join_output(scan_a, scan_b, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}),
                                             ScanType::OpEquals, JoinMode::Inner,
                                             "src/test/tables/joinoperators/int_inner_join_filtered.tbl", 1);
}

TYPED_TEST(JoinIndexTest, InnerDictJoin) {
  this->test_join_output(
      this->_table_wrapper_a_dict, this->_table_wrapper_b_dict, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}),
      ScanType::OpEquals, JoinMode::Inner, "src/test/tables/joinoperators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinIndexTest, InnerRefDictJoin) {
  // scan that returns all rows
  auto scan_a = std::make_shared<TableScan>(this->_table_wrapper_a_dict, ColumnID{0}, ScanType::OpGreaterThanEquals, 0);
  scan_a->execute();
  auto scan_b = std::make_shared<TableScan>(this->_table_wrapper_b_dict, ColumnID{0}, ScanType::OpGreaterThanEquals, 0);
  scan_b->execute();

  this->test_join_output(scan_a, scan_b, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}),
                                             ScanType::OpEquals, JoinMode::Inner,
                                             "src/test/tables/joinoperators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinIndexTest, InnerRefDictJoinFiltered) {
  auto scan_a = std::make_shared<TableScan>(this->_table_wrapper_a_dict, ColumnID{0}, ScanType::OpGreaterThan, 1000);
  scan_a->execute();
  auto scan_b = std::make_shared<TableScan>(this->_table_wrapper_b_dict, ColumnID{0}, ScanType::OpGreaterThanEquals, 0);
  scan_b->execute();

  this->test_join_output(scan_a, scan_b, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}),
                                             ScanType::OpEquals, JoinMode::Inner,
                                             "src/test/tables/joinoperators/int_inner_join_filtered.tbl", 1);
}

TYPED_TEST(JoinIndexTest, InnerJoinBig) {
  this->test_join_output(
      this->_table_wrapper_c, this->_table_wrapper_d, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{1}),
      ScanType::OpEquals, JoinMode::Inner, "src/test/tables/joinoperators/int_string_inner_join.tbl", 1);
}

TYPED_TEST(JoinIndexTest, InnerRefJoinFilteredBig) {
  auto scan_c = std::make_shared<TableScan>(this->_table_wrapper_c, ColumnID{0}, ScanType::OpGreaterThanEquals, 0);
  scan_c->execute();
  auto scan_d = std::make_shared<TableScan>(this->_table_wrapper_d, ColumnID{1}, ScanType::OpGreaterThanEquals, 6);
  scan_d->execute();

  this->test_join_output(scan_c, scan_d, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{1}),
                                             ScanType::OpEquals, JoinMode::Inner,
                                             "src/test/tables/joinoperators/int_string_inner_join_filtered.tbl", 1);
}

TYPED_TEST(JoinIndexTest, SelfJoin) {
  this->test_join_output(
      this->_table_wrapper_a, this->_table_wrapper_a, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}),
      ScanType::OpEquals, JoinMode::Self, "src/test/tables/joinoperators/int_self_join.tbl", 1);
}

TYPED_TEST(JoinIndexTest, JoinOnMixedValueAndDictionaryColumns) {
  this->test_join_output(
      this->_table_wrapper_c_dict, this->_table_wrapper_b, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}),
      ScanType::OpEquals, JoinMode::Inner, "src/test/tables/joinoperators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinIndexTest, JoinOnMixedValueAndReferenceColumns) {
  // scan that returns all rows
  auto scan_a = std::make_shared<TableScan>(this->_table_wrapper_a, ColumnID{0}, ScanType::OpGreaterThanEquals, 0);
  scan_a->execute();

  this->test_join_output(
      scan_a, this->_table_wrapper_b, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), ScanType::OpEquals,
      JoinMode::Inner, "src/test/tables/joinoperators/int_inner_join.tbl", 1);
}

TYPED_TEST(JoinIndexTest, RightJoinRefColumn) {
  // scan that returns all rows
  auto scan_a = std::make_shared<TableScan>(this->_table_wrapper_a, ColumnID{0}, ScanType::OpGreaterThanEquals, 0);
  scan_a->execute();

  this->test_join_output(
      scan_a, this->_table_wrapper_b, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), ScanType::OpEquals,
      JoinMode::Right, "src/test/tables/joinoperators/int_right_join.tbl", 1);
}

TYPED_TEST(JoinIndexTest, LeftJoinRefColumn) {
  // scan that returns all rows
  auto scan_b = std::make_shared<TableScan>(this->_table_wrapper_b, ColumnID{0}, ScanType::OpGreaterThanEquals, 0);
  scan_b->execute();

  this->test_join_output(
      this->_table_wrapper_a, scan_b, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), ScanType::OpEquals,
      JoinMode::Left, "src/test/tables/joinoperators/int_left_join.tbl", 1);
}

TYPED_TEST(JoinIndexTest, RightJoinEmptyRefColumn) {
  // scan that returns no rows
  auto scan_a = std::make_shared<TableScan>(this->_table_wrapper_a, ColumnID{0}, ScanType::OpEquals, 0);
  scan_a->execute();

  this->test_join_output(
      scan_a, this->_table_wrapper_b, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), ScanType::OpEquals,
      JoinMode::Right, "src/test/tables/joinoperators/int_join_empty.tbl", 1);
}

TYPED_TEST(JoinIndexTest, LeftJoinEmptyRefColumn) {
  // scan that returns no rows
  auto scan_b = std::make_shared<TableScan>(this->_table_wrapper_b, ColumnID{0}, ScanType::OpEquals, 0);
  scan_b->execute();

  this->test_join_output(
      this->_table_wrapper_b, scan_b, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), ScanType::OpEquals,
      JoinMode::Left, "src/test/tables/joinoperators/int_join_empty_left.tbl", 1);
}

}  // namespace opossum
