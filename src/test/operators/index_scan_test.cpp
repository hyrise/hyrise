#include <map>
#include <memory>
#include <numeric>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "operators/index_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/index/adaptive_radix_tree/adaptive_radix_tree_index.hpp"
#include "storage/index/group_key/composite_group_key_index.hpp"
#include "storage/index/group_key/group_key_index.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

template <typename DerivedIndex>
class OperatorsIndexScanTest : public BaseTest {
 protected:
  void SetUp() override {
    _index_type = get_index_type_of<DerivedIndex>();

    TableColumnDefinitions column_definitions;
    column_definitions.emplace_back("a", DataType::Int);
    column_definitions.emplace_back("b", DataType::Int);
    auto table = std::make_shared<Table>(column_definitions, TableType::Data, 5);
    for (int i = 0; i <= 24; i += 2) table->append({i, 100 + i});
    ChunkEncoder::encode_all_chunks(table);

    _chunk_ids = std::vector<ChunkID>(table->chunk_count());
    std::iota(_chunk_ids.begin(), _chunk_ids.end(), ChunkID{0u});

    _column_ids = std::vector<ColumnID>{ColumnID{0u}};

    for (const auto& chunk_id : _chunk_ids) {
      auto chunk = table->get_chunk(chunk_id);
      chunk->template create_index<DerivedIndex>(_column_ids);
    }

    _table_wrapper = std::make_shared<TableWrapper>(table);
    _table_wrapper->execute();

    auto empty_table = std::make_shared<Table>(column_definitions, TableType::Data, 5);

    _empty_table_wrapper = std::make_shared<TableWrapper>(empty_table);
    _empty_table_wrapper->execute();
  }

  void ASSERT_COLUMN_EQ(std::shared_ptr<const Table> table, const ColumnID& column_id,
                        std::vector<AllTypeVariant> expected) {
    for (auto chunk_id = ChunkID{0u}; chunk_id < table->chunk_count(); ++chunk_id) {
      const auto chunk = table->get_chunk(chunk_id);

      for (auto chunk_offset = ChunkOffset{0u}; chunk_offset < chunk->size(); ++chunk_offset) {
        const auto& column = *chunk->get_column(column_id);

        const auto found_value = column[chunk_offset];
        const auto comparator = [found_value](const AllTypeVariant expected_value) {
          // returns equivalency, not equality to simulate std::multiset.
          // multiset cannot be used because it triggers a compiler / lib bug when built in CI
          return !(found_value < expected_value) && !(expected_value < found_value);
        };

        auto search = std::find_if(expected.begin(), expected.end(), comparator);

        ASSERT_TRUE(search != expected.end());
        expected.erase(search);
      }
    }

    ASSERT_EQ(expected.size(), 0u);
  }

  std::shared_ptr<TableWrapper> _table_wrapper;
  std::shared_ptr<TableWrapper> _empty_table_wrapper;
  std::vector<ChunkID> _chunk_ids;
  std::vector<ColumnID> _column_ids;
  ColumnIndexType _index_type;
};

typedef ::testing::Types<GroupKeyIndex, AdaptiveRadixTreeIndex, CompositeGroupKeyIndex /* add further indices */>
    DerivedIndices;
TYPED_TEST_CASE(OperatorsIndexScanTest, DerivedIndices);

TYPED_TEST(OperatorsIndexScanTest, SingleColumnScanOnDataTable) {
  // we do not need to check for a non existing value, because that happens automatically when we scan the second chunk

  const auto right_values = std::vector<AllTypeVariant>{AllTypeVariant{4}};
  const auto right_values2 = std::vector<AllTypeVariant>{AllTypeVariant{9}};

  std::map<PredicateCondition, std::vector<AllTypeVariant>> tests;
  tests[PredicateCondition::Equals] = {104};
  tests[PredicateCondition::NotEquals] = {100, 102, 106, 108, 110, 112, 114, 116, 118, 120, 122, 124};
  tests[PredicateCondition::LessThan] = {100, 102};
  tests[PredicateCondition::LessThanEquals] = {100, 102, 104};
  tests[PredicateCondition::GreaterThan] = {106, 108, 110, 112, 114, 116, 118, 120, 122, 124};
  tests[PredicateCondition::GreaterThanEquals] = {104, 106, 108, 110, 112, 114, 116, 118, 120, 122, 124};
  tests[PredicateCondition::Between] = {104, 106, 108};

  for (const auto& test : tests) {
    auto scan = std::make_shared<IndexScan>(this->_table_wrapper, this->_index_type, this->_column_ids, test.first,
                                            right_values, right_values2);

    scan->execute();

    this->ASSERT_COLUMN_EQ(scan->get_output(), ColumnID{1u}, test.second);
  }
}

TYPED_TEST(OperatorsIndexScanTest, SingleColumnScanValueGreaterThanMaxDictionaryValue) {
  const auto all_rows = std::vector<AllTypeVariant>{100, 102, 104, 106, 108, 110, 112, 114, 116, 118, 120, 122, 124};
  const auto no_rows = std::vector<AllTypeVariant>{};

  const auto right_values = std::vector<AllTypeVariant>{AllTypeVariant{30}};
  const auto right_values2 = std::vector<AllTypeVariant>{AllTypeVariant{34}};

  std::map<PredicateCondition, std::vector<AllTypeVariant>> tests;
  tests[PredicateCondition::Equals] = no_rows;
  tests[PredicateCondition::NotEquals] = all_rows;
  tests[PredicateCondition::LessThan] = all_rows;
  tests[PredicateCondition::LessThanEquals] = all_rows;
  tests[PredicateCondition::GreaterThan] = no_rows;
  tests[PredicateCondition::GreaterThanEquals] = no_rows;

  for (const auto& test : tests) {
    auto scan = std::make_shared<IndexScan>(this->_table_wrapper, this->_index_type, this->_column_ids, test.first,
                                            right_values, right_values2);
    scan->execute();

    this->ASSERT_COLUMN_EQ(scan->get_output(), ColumnID{1u}, test.second);
  }
}

TYPED_TEST(OperatorsIndexScanTest, SingleColumnScanValueLessThanMinDictionaryValue) {
  const auto all_rows = std::vector<AllTypeVariant>{100, 102, 104, 106, 108, 110, 112, 114, 116, 118, 120, 122, 124};
  const auto no_rows = std::vector<AllTypeVariant>{};

  const auto right_values = std::vector<AllTypeVariant>{AllTypeVariant{-10}};
  const auto right_values2 = std::vector<AllTypeVariant>{AllTypeVariant{34}};

  std::map<PredicateCondition, std::vector<AllTypeVariant>> tests;
  tests[PredicateCondition::Equals] = no_rows;
  tests[PredicateCondition::NotEquals] = all_rows;
  tests[PredicateCondition::LessThan] = no_rows;
  tests[PredicateCondition::LessThanEquals] = no_rows;
  tests[PredicateCondition::GreaterThan] = all_rows;
  tests[PredicateCondition::GreaterThanEquals] = all_rows;

  for (const auto& test : tests) {
    auto scan = std::make_shared<IndexScan>(this->_table_wrapper, this->_index_type, this->_column_ids, test.first,
                                            right_values, right_values2);
    scan->execute();

    this->ASSERT_COLUMN_EQ(scan->get_output(), ColumnID{1}, test.second);
  }
}

TYPED_TEST(OperatorsIndexScanTest, OperatorName) {
  const auto right_values = std::vector<AllTypeVariant>(this->_column_ids.size(), AllTypeVariant{0});

  auto scan = std::make_shared<opossum::IndexScan>(this->_table_wrapper, this->_index_type, this->_column_ids,
                                                   PredicateCondition::GreaterThanEquals, right_values);

  EXPECT_EQ(scan->name(), "IndexScan");
}

TYPED_TEST(OperatorsIndexScanTest, InvalidIndexTypeThrows) {
  const auto right_values = std::vector<AllTypeVariant>(this->_column_ids.size(), AllTypeVariant{0});

  auto scan = std::make_shared<opossum::IndexScan>(this->_table_wrapper, ColumnIndexType::Invalid, this->_column_ids,
                                                   PredicateCondition::GreaterThan, right_values);
  EXPECT_THROW(scan->execute(), std::logic_error);
}

}  // namespace opossum
