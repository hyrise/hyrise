#include <map>
#include <memory>
#include <numeric>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "operators/join_index.hpp"
#include "operators/table_wrapper.hpp"
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
    left_table->add_column("a", DataType::Int);
    left_table->add_column("b", DataType::Int);
    for (int i = 0; i <= 24; i += 2) left_table->append({i, 100 + i});
    DictionaryCompression::compress_table(*left_table);

    auto right_table = std::make_shared<Table>(5);
    right_table->add_column("a", DataType::Int);
    right_table->add_column("b", DataType::Int);
    for (int i = 0; i <= 24; i += 2) right_table->append({i, 100 + i});
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

typedef ::testing::Types<GroupKeyIndex, AdaptiveRadixTreeIndex, CompositeGroupKeyIndex /* add further indices */>
    DerivedIndices;
TYPED_TEST_CASE(OperatorsJoinIndexTest, DerivedIndices);

TYPED_TEST(OperatorsJoinIndexTest, SimpleJoin) {
  auto result_table = std::make_shared<Table>();
  result_table->add_column("a", DataType::Int);
  result_table->add_column("b", DataType::Int);
  result_table->add_column("a", DataType::Int);
  result_table->add_column("b", DataType::Int);
  for (int i = 0; i <= 24; i += 2) result_table->append({i, i + 100, i, i + 100});
  DictionaryCompression::compress_table(*result_table);

  this->test_join_output(this->_table_wrapper_left, this->_table_wrapper_right,
                         std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), ScanType::OpEquals, JoinMode::Inner,
                         result_table, 1);
}

}  // namespace opossum
